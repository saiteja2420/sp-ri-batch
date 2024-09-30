import pandas as pd
import time
import awswrangler as wr
import os
import json
from datetime import datetime
import logging
import mysql.connector
import numpy as np
import boto3
import requests
import random
import re


from apply_sp_v3_spill import  apply_savings_plan,data_merger
from apply_ri import decremental_deduction_optimized_ri


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def execute_query(query, params=None, config=None):
    """
    Execute a SQL query with dynamic parameters.

    :param query: The SQLurl query to execute.
    :param params: A tuple of parameters to use with the query.
    :param config: A dictionary containing MySQL connection parameters.
    :return: None
    """
    try:
        # Establish a database connection
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Execute the query
        cursor.execute(query, params or ())
        
        # Commit the transaction if needed
        connection.commit()
        logger.info("Query executed successfully.")

    except mysql.connector.Error as db_err:
        logger.error(f"Database error occurred: {db_err}")
        raise
    except Exception as sql_err:
        logger.error(f"An unexpected error occurred during SQL operation: {sql_err}")
        raise
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
        

def main():
    try:
        aws_batch_job_id = os.getenv("AWS_BATCH_JOB_ID")
        logger.info(f"AWS Batch Job ID: {aws_batch_job_id}")
        body = os.getenv("body")
        
        if body:
            body = json.loads(body)
            # query = body.get("query")
            bucket = body.get("bucket", "aws-athena-query-results-730335661255-us-west-2")
            savings_plan_type = body.get("savings_plan_type")
            input = body.get("input")
            request_id = input["request_id"]
        else:
            raise ValueError("Environment variable 'body' is not set or empty")

        
        if savings_plan_type == "SP":
            savings_plan_amount = input["savings_plan_amount"]
            savings_plan_term_length = input["savings_plan_term_length"]
            savings_plan_name = "sp"
            filename = input["filepaths"]
            summary_df,outputfiles= apply_savings_plan(filename,savings_plan_amount, savings_plan_term_length, savings_plan_name,aws_batch_job_id,request_id,bucket)
            print("successfully completed the savings_plan task")
        
        if savings_plan_type == "RI": 
            
            ri_cost = 0
            ri_cost_per_hour = 0
            spill_ri_cost = 0

            filename = input["filepaths"]
            
            if "cur_files" in filename and filename["cur_files"]!="":
                
                original_df = data_merger(filename["cur_files"])
                # df = pd.read_csv("/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/cloudfront.csv")
                df = original_df
                df['line_item_usage_start_date'] = pd.to_datetime(df['line_item_usage_start_date'])
                df['Covered'] = 100-df['avail_percent']
                df['compute_col_remaining'] = df['line_item_normalized_usage_amount'] * (df['avail_percent'] / 100)
                original_total_hrs = original_df['line_item_usage_start_date'].nunique()
                print("original_total_hrs",original_total_hrs)
                mergerd_on_demand_cost = df["od_cost"].sum()
                actual_od_cost = (df["od_cost"] * (df['avail_percent'] / 100)).sum()
                
            if "new_ris" in  filename and  filename["new_ris"] != '':
                
                # ri_df = pd.read_csv("/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/ri_plans.csv")
                # ri_df = pd.read_csv(filename["new_ris"])
                # ri_df = wr.s3.read_parquet(filename["new_ris"])
                ri_df = data_merger(filename["new_ris"])
                ri_df['reservation_availability_zone'] = ri_df['reservation_availability_zone'].fillna('missing')
                ri_df['line_item_normalization_factor'] = pd.to_numeric(ri_df['line_item_normalization_factor'], errors='coerce')
                ri_df['reservation_number_of_reservations'] = pd.to_numeric(ri_df['reservation_number_of_reservations'], errors='coerce')

                # Extract and process columns
                ri_df['product_region_code'] = ri_df['product'].apply(lambda x: re.search(r'region=([\w-]+)', x).group(1) if pd.notnull(x) else None)
                ri_df['instance_family_type'] = ri_df['line_item_usage_type'].str.split(':').str[1].str.split('.').str[0]
                ri_df['size_flex'] = ri_df['product'].apply(lambda x: re.search(r'size_flex=(\w+)', x).group(1) if pd.notnull(x) else None)
                ri_df['total_normalize_units'] = np.where(
                    ri_df['size_flex'].str.lower() == 'true', 
                    ri_df['line_item_normalization_factor'] * ri_df['reservation_number_of_reservations'], 
                    ri_df['reservation_number_of_reservations']
                )
                ri_df["spill_usage"] = ri_df['total_normalize_units']
                ri_cost_per_hour = (ri_df["line_item_blended_rate"]*ri_df["reservation_number_of_reservations"]).sum()
                ri_cost = ri_df["line_item_blended_rate"]*ri_df["reservation_number_of_reservations"]*original_total_hrs
                ri_cost = ri_cost.sum()
                # Ensure the date column is in datetime format

            if  "sp_spill_files" in  filename and  filename["sp_spill_files"] != '':
                # Process each group individually and collect results
                    # spill_data = pd.read_csv(ri_spill_file)
                    spill_data= data_merger(filename["sp_spill_files"])
                    # print(len(spill_data))
                    spill_data['line_item_usage_start_date_spill'] = pd.to_datetime(spill_data['line_item_usage_start_date_spill'])
                    spill_data['reservation_availability_zone'] = spill_data['reservation_availability_zone'].fillna('missing')
                    spill_data['product_region_code'] = spill_data['product'].apply(lambda x: re.search(r'region=([\w-]+)', x).group(1) if pd.notnull(x) else None)
                    spill_data['size_flex'] = spill_data['product'].apply(lambda x: re.search(r'size_flex=(\w+)', x).group(1) if pd.notnull(x) else None)
                    spill_data['instance_family_type'] = spill_data['line_item_usage_type'].str.split(':').str[1].str.split('.').str[0]
                    
                    # filtered_spill_data = spill_data[spill_data["spill_usage"] != 0]
                    # spill_ri_cost = ri_cost + (filtered_spill_data["line_item_blended_rate"]*filtered_spill_data["reservation_number_of_reservations"]).sum()

                    spill_ri_cost = ri_cost + (spill_data["line_item_blended_rate"]*spill_data["reservation_number_of_reservations"]).sum()
                    spill_data_hrs = spill_data['line_item_usage_start_date_spill'].nunique()
                    print("spill_data_hrs",spill_data_hrs)
                    print("spill_ri_cost",spill_ri_cost)
            
            result_list = []
            spill_list = []
            spill_added = True
            spill_df = pd.DataFrame()
            ri_spill_mergerd = pd.DataFrame()
            
            for _, group in df.groupby('line_item_usage_start_date'):
                line_item_usage_start_date = group['line_item_usage_start_date'].iloc[0]
                line_item_usage_start_date = pd.Timestamp(line_item_usage_start_date)
                # print(line_item_usage_start_date)
                if "new_ris" in  filename and  filename["new_ris"] != '':
                    ri_df['line_item_usage_start_date_spill'] = group['line_item_usage_start_date'].iloc[0]
                    ri_spill_mergerd = pd.concat([ri_spill_mergerd,ri_df])
                    # print("ri len",len(ri_spill_mergerd))
                if  "sp_spill_files" in  filename and  filename["sp_spill_files"] != '':
                    # print(spill_data['line_item_usage_start_date_spill'].dtype)
                    # print(type(line_item_usage_start_date))

                    filtered = spill_data[spill_data['line_item_usage_start_date_spill'] == line_item_usage_start_date]
                    ri_spill_mergerd = pd.concat([ri_spill_mergerd,filtered])


                result_group, spill_group = decremental_deduction_optimized_ri(group,spill_added,ri_spill_mergerd)
                result_list.append(result_group)
                spill_list.append(spill_group)

    # Concatenate all results into final DataFrames
            result_df = pd.concat(result_list, ignore_index=True)
            spill_df = pd.concat(spill_list, ignore_index=True)
            
            summary_df = pd.DataFrame()
            spill_sum = spill_df["spill_usage"].sum()
            total_on_demand_cost_applied = result_df["on_demand_cost_applied"].sum()
            profit = actual_od_cost-(ri_cost+total_on_demand_cost_applied)
            profit_percent = (profit / actual_od_cost) * 100 if actual_od_cost != 0 else 0
            summary_df = pd.DataFrame({
            "type" : ["RI"],
            "term_length": [""],
            "input_value": [spill_ri_cost],
            'actual_input': [ri_cost_per_hour],
            "merged_on_demand_cost":[mergerd_on_demand_cost],
            "sum_of_actual_on_demand_cost": [actual_od_cost],
            "sum_of_on_demand_cost_applied": [total_on_demand_cost_applied],
            "total_savings_plan":[ri_cost],
            "total_amount": ri_cost+total_on_demand_cost_applied,
            "total_hours" : original_total_hrs,
            "profit": actual_od_cost-(ri_cost+total_on_demand_cost_applied),
            "spill_sum" : [spill_sum],
            "profit_percent":[profit_percent],
        })
            
            
            resultfile = f"s3://{bucket}/data/{request_id}/{aws_batch_job_id}/cur_data.parquet"
            ri_spill_file = f"s3://{bucket}/data/{request_id}/{aws_batch_job_id}/spill.parquet"
            summary_filename = f"s3://{bucket}/data/{request_id}/{aws_batch_job_id}/savingsplan_result.parquet"
            
            wr.s3.to_parquet(df=summary_df, path=summary_filename,dataset=True, mode="overwrite")
            wr.s3.to_parquet(df=result_df, path=resultfile,dataset=True, mode="overwrite")    
            wr.s3.to_parquet(df=spill_df, path=ri_spill_file,dataset=True, mode="overwrite")
            print("successfully completed the RIs task")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    
    main()