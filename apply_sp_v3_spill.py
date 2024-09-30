import pandas as pd
import re
import numpy as np
import warnings
import os 
import awswrangler as wr

def data_merger(spill_data_list, output_file=None):
    """
    Function to merge multiple spill CUR data files.
    
    Parameters:
    - spill_data_list (list): List of paths to spill CUR data files.
    - output_file (str, optional): Path where the combined DataFrame should be saved as a CSV. If None, it won't be saved.
    
    Returns:
    - combined_df (pd.DataFrame): Concatenated DataFrame from all spill data.
    """
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame
    
    # Loop through the spill data list and merge each spill file
    for spill_data in spill_data_list:
        print(f"Merging spill data from: {spill_data}")
        # spill_df = pd.read_csv(spill_data)
        # combined_df = pd.concat([combined_df, spill_df], ignore_index=True)
        spill_df =  wr.s3.read_parquet(spill_data)
        combined_df = pd.concat([combined_df, spill_df], ignore_index=True)
    # If an output file is specified, save the combined DataFrame to the output path
    if output_file:
        combined_df.to_csv(output_file, index=False)
        print(f"Combined DataFrame saved to {output_file}")
    
    return combined_df

def adjust_covered_percentage_savings_plan(df,spill_df,amount, savings_plan_name, savings_plan_term_length):
    column_mapping = {
    ('sp', 1): {
        'compute': 'sp1y_compute',
        'ec2': 'sp1y_ec2',
        'disc_compute': 'sp1y_disc_compute',
        'disc_ec2': 'sp1y_disc_ec2',
    },
    ('sp', 3): {
        'compute': 'sp3y_compute',
        'ec2': 'sp3y_ec2',
        'disc_compute': 'sp3y_disc_compute',
        'disc_ec2': 'sp3y_disc_ec2',
    },
    ('ri_conv', 1): {
        'compute': 'ri1y_conv',
        'disc_compute': 'ri1y_disc_conv',
    },
    ('ri_conv', 3): {
        'compute': 'ri3y_conv',
        'disc_compute': 'ri3y_disc_conv',
    },
    ('ri_std', 1): {
        'compute': 'ri1y_std',
        'disc_compute': 'ri1y_disc_std',
    },
    ('ri_std', 3): {
        'compute': 'ri3y_std',
        'disc_compute': 'ri3y_disc_std',
    }
}
    # Create a copy of the DataFrame to avoid modifying the original DataFrame
    df = df.copy()
    if spill_df is not None and not spill_df.empty:
        # print(df.iloc[0]["line_item_usage_start_date"])

        target_date = df.iloc[0]["line_item_usage_start_date"]
        spill_df["line_item_usage_start_date"]=pd.to_datetime(spill_df['line_item_usage_start_date'])
        # Step 2: Filter the rows in spill_df where 'line_item_usage_start_date' matches target_date
        matching_rows = spill_df[spill_df["line_item_usage_start_date"] == target_date]
        amount = amount + matching_rows["spill_usage"].values[0]
        
    # Get the column names based on the savings plan and term length
    columns = column_mapping.get((savings_plan_name, savings_plan_term_length))

    if not columns:
        raise ValueError(f"No valid column mapping found for {savings_plan_name} with term length {savings_plan_term_length} years")

    compute_col = columns['compute']
    disc_compute_col = columns['disc_compute']

    # Create a copy of the compute column
    df['original_' + compute_col] = df[compute_col].copy()
    df['adjusted_saving_plan_per_hour'] = amount
    # Adjust the compute column based on availability percentage
    df[compute_col] *= df['avail_percent'] / 100

    # Sort the DataFrame based on the discount compute column
    df = df.sort_values(by=disc_compute_col, ascending=False)
    # df['Covered'] = 0.0
    
    # Calculate cumulative cost based on the adjusted compute column
    cumulative_cost = df[compute_col].cumsum()
    covered_percentage = np.where(cumulative_cost <= amount, 100.0, 0)
    first_exceed_index = np.argmax(cumulative_cost > amount)
    
    if cumulative_cost.iloc[first_exceed_index] > amount and first_exceed_index > 0:
        covered_percentage[first_exceed_index] = ((amount - cumulative_cost.iloc[first_exceed_index-1]) / df[compute_col].iloc[first_exceed_index]) * 100.0
    
    df['Covered'] = covered_percentage
    
    # Calculate savings plans covered cost and on-demand cost applied
    df['savings_plans_covered_cost'] = df[compute_col] * (df['Covered'] / 100)
    df['on_demand_cost_applied'] = df['od_cost'] * (1 - df['Covered'] / 100)
    
    # Update avail_percent based on covered percentage
    df['avail_percent'] = 100 - df['Covered']
    
    return df

# Function to run the simulation
def apply_savings_plan(filename,saving_plan_amount, savings_plan_term_length, savings_plan_name,batch_id,request_id,bucket,simulation_index=None,account_id=None):
    
    batch_id = batch_id
    request_id = request_id
    outputfiles = {}
    summary_list = []
    spill_df = pd.DataFrame()
    resultfile = f"s3://{bucket}/data/{request_id}/{batch_id}/cur_data.parquet"
    sp_spill_file = f"s3://{bucket}/data/{request_id}/{batch_id}/spill.parquet"
    summary_filename = f"s3://{bucket}/data/{request_id}/{batch_id}/savingsplan_result.parquet"
    
    if "cur_files" in filename and filename["cur_files"]!="":
        original_df = data_merger(filename["cur_files"])
        original_df['adjusted_saving_plan_per_hour'] = 0
        original_df['line_item_usage_start_date'] =pd.to_datetime(original_df['line_item_usage_start_date'])

    if "sp_spill_files" in filename and filename["sp_spill_files"]!="":
        spill_df = data_merger(filename["sp_spill_files"])
    # print(original_df['line_item_usage_account_id'].dtype)
     
    if account_id: 
        print("account id present")
        df = original_df[original_df['line_item_usage_account_id'] == int(account_id)]
    else:
        # print("account id not present")
        df = original_df
        
        
    original_total_hrs = original_df['line_item_usage_start_date'].nunique()
    filtered_total_hrs=df['line_item_usage_start_date'].nunique()
    
    mereged_ondemand_cost = original_df['od_cost'].sum()
    df = df[df['avail_percent'] > 0]
    
    weighted_costs = df['od_cost'] * (df['avail_percent'] / 100)
    weighted_sum_of_on_demand_cost = weighted_costs.sum()
    sum_of_on_demand_cost = weighted_sum_of_on_demand_cost    
    
    print("original_total_hrs",original_total_hrs)
    print("filtered_total_hrs",filtered_total_hrs)
    
    input_value = saving_plan_amount
    
    
    if len(df)==0:
        print("Data frame is emptly")
        # resultfile = f"data/{batch_id}/cur_data_{saving_plan_amount}.csv"
        # parent = os.path.dirname(resultfile)
        # if not os.path.exists(parent):
        #     os.makedirs(parent)
        outputfiles["out_cur_file"] = resultfile
        # original_df.to_csv(resultfile, index=False)
        wr.s3.to_parquet(df=original_df, path=resultfile,dataset=True, mode="overwrite")
        print(resultfile)
        
        summary_df=pd.DataFrame()
        summary_df["line_item_usage_start_date"]=original_df["line_item_usage_start_date"]
        summary_df["savings_plans_covered_cost"]=0
        summary_df["on_demand_cost_applied"]=0
        summary_df['total_usage_cost']=0
        
        if not spill_df.empty:
            mask = summary_df['line_item_usage_start_date'].isin(spill_df['line_item_usage_start_date'])
            summary_df['line_item_usage_start_date'] = pd.to_datetime(summary_df['line_item_usage_start_date'])
            spill_df['line_item_usage_start_date'] = pd.to_datetime(spill_df['line_item_usage_start_date'])

    # Create a Series with the spill usage values based on the matches
            # print(spill_usage_values)
            # Use np.where to update 'spill_usage' in summary_df
            spill_usage_values = spill_df.set_index('line_item_usage_start_date')['spill_usage']
            summary_df['spill_usage'] = np.where(mask,
                summary_df['line_item_usage_start_date'].map(spill_usage_values) + saving_plan_amount,
                saving_plan_amount
            )
            summary_df['saving_plan_per_hour']=summary_df['spill_usage']
        else:
            summary_df['spill_usage']=saving_plan_amount
            summary_df['saving_plan_per_hour']=saving_plan_amount
        # sp_spill_file = f"data/{batch_id}/spill_{saving_plan_amount}.csv"
        input_value_spill = summary_df['spill_usage'].min()
        # sp_spill_file = f"s3://{bucket}/data/{request_id}/{batch_id}/spill.parquet"
        wr.s3.to_parquet(df=summary_df, path=sp_spill_file, dataset=True,mode="overwrite")
        print(sp_spill_file)
        
        # parent = os.path.dirname(sp_spill_file)
    # Check if the directory exists, and create it if it doesn't
        # if not os.path.exists(parent):
        #     os.makedirs(parent)
        # summary_df.to_csv(sp_spill_file)
        outputfiles["sp_spill_files"] = sp_spill_file
        total_amount = summary_df['on_demand_cost_applied'].sum() + summary_df['saving_plan_per_hour'].sum()

        # Calculate profit
        profit = sum_of_on_demand_cost - total_amount

        # Calculate profit percentage
        profit_percent = (profit / sum_of_on_demand_cost) * 100 if sum_of_on_demand_cost != 0 else 0

        summary_list.append({
            "type" : ["SP"],
            "term_length": [savings_plan_term_length],
            'input_value': input_value_spill,
            'actual_input' : input_value,
            'merged_on_demand_cost' : mereged_ondemand_cost,
            'sum_of_actual_on_demand_cost': sum_of_on_demand_cost,
            'sum_of_on_demand_cost_applied': summary_df['on_demand_cost_applied'].sum(),
            'total_savings_plan': summary_df['saving_plan_per_hour'].sum(),
            'total_amount': total_amount,
            'total_hours' : original_total_hrs,
            'profit': profit,
            'spill_sum': summary_df['spill_usage'].sum(),
            'profit_percent': profit_percent
        })

        summary_output_df = pd.DataFrame(summary_list)
        # Save result to CSV with proper string formatting
        # summary_filename = f"s3://{bucket}/data/{request_id}/{batch_id}/savingsplan_result.parquet"
        # summary_filename = f"data/{batch_id}/savingsplan_result_{saving_plan_amount}.csv"
        # parent = os.path.dirname(summary_filename)

    # Check if the directory exists, and create it if it doesn't
        # if not os.path.exists(parent):
        #     os.makedirs(parent)
        # summary_output_df.to_csv(summary_filename, index=False)
        outputfiles["summary_filename"] = summary_filename
        wr.s3.to_parquet(df=summary_output_df, path=summary_filename,dataset=True, mode="overwrite")
        print(summary_filename)
        return summary_output_df,outputfiles
    
    summary_list = []
    result_df = pd.DataFrame()
    
    zero_df = original_df[original_df['avail_percent'] == 0.0]
    # print(len(zero_df))
    df_grouped = df.groupby('line_item_usage_start_date', as_index=False)
    result_df = df_grouped.apply(lambda x: adjust_covered_percentage_savings_plan(x,spill_df,input_value,savings_plan_name, savings_plan_term_length)).reset_index(drop=True)
    
    # resultfile = f"data/{batch_id}/cur_data_{saving_plan_amount}.csv"
    # resultfile = f"s3://{bucket}/data/{request_id}/{batch_id}/cur_data.parquet"
#     parent = os.path.dirname(resultfile)

# # Check if the directory exists, and create it if it doesn't
#     if not os.path.exists(parent):
#         os.makedirs(parent)
    outputfiles["out_cur_file"] = resultfile
    
    summary_df = result_df.groupby('line_item_usage_start_date').agg({
        'savings_plans_covered_cost': 'sum',
        'on_demand_cost_applied': 'sum',
        'adjusted_saving_plan_per_hour': 'min'
    }).reset_index()

    
    
    input_value_spill = summary_df["adjusted_saving_plan_per_hour"].iloc[0]
    summary_df['total_usage_cost'] = summary_df['savings_plans_covered_cost'] + summary_df['on_demand_cost_applied']
    summary_df['saving_plan_per_hour'] = input_value
    summary_df['spill_usage'] = np.maximum(0, summary_df['adjusted_saving_plan_per_hour'] - summary_df['total_usage_cost'])
    # print(len(summary_df['spill_usage']),"summary_df['spill_usage'])")
    if (original_total_hrs)>(filtered_total_hrs):
        # input_value_spill =  summary_df["adjusted_saving_plan_per_hour"].iloc[0]
        print("filtered df has less columns")
        filtered_unique_times = df['line_item_usage_start_date'].unique()
        original_df_unique_times = original_df['line_item_usage_start_date'].unique()
        missing_times_in_filtered = set(original_df_unique_times) - set(filtered_unique_times)
        print(len(missing_times_in_filtered))
        new_rows = pd.DataFrame({'line_item_usage_start_date': list(missing_times_in_filtered)})
        new_rows['line_item_usage_start_date'] = pd.to_datetime(new_rows['line_item_usage_start_date'])
        spill_df['line_item_usage_start_date'] = pd.to_datetime(spill_df['line_item_usage_start_date'])
        if not spill_df.empty:
            mask = new_rows['line_item_usage_start_date'].isin(spill_df['line_item_usage_start_date'])
            # print(len(mask))
        
    # Create a Series with the spill usage values based on the matches
            spill_usage_values = spill_df.set_index('line_item_usage_start_date')['spill_usage']
            # Use np.where to update 'spill_usage' in summary_df
            new_rows['spill_usage'] = np.where(mask,
                new_rows['line_item_usage_start_date'].map(spill_usage_values) + saving_plan_amount,
                saving_plan_amount
            )
            new_rows['saving_plan_per_hour']=new_rows['spill_usage']
            new_rows['adjusted_saving_plan_per_hour']=new_rows['spill_usage']
        else:
            new_rows['spill_usage']=saving_plan_amount
            new_rows['saving_plan_per_hour']=new_rows['spill_usage']
            new_rows['adjusted_saving_plan_per_hour']=new_rows['spill_usage']
        summary_df = pd.concat([summary_df,new_rows ], ignore_index=True)
    # sp_spill_file = f"data/{batch_id}/spill_{saving_plan_amount}.csv"
    # sp_spill_file = f"s3://{bucket}/data/{request_id}/{batch_id}/spill.parquet"
    wr.s3.to_parquet(df=summary_df, path=sp_spill_file,dataset=True, mode="overwrite")
    print(sp_spill_file)
#     parent = os.path.dirname(sp_spill_file)

# # Check if the directory exists, and create it if it doesn't
#     if not os.path.exists(parent):
#         os.makedirs(parent)
#     summary_df.to_csv(sp_spill_file)
    outputfiles["sp_spill_files"] = sp_spill_file
    total_amount = summary_df['on_demand_cost_applied'].sum() + summary_df['adjusted_saving_plan_per_hour'].sum()

    # Calculate profit
    profit = sum_of_on_demand_cost - total_amount

    # Calculate profit percentage
    profit_percent = (profit / sum_of_on_demand_cost) * 100 if sum_of_on_demand_cost != 0 else 0

    summary_list.append({
        "type" : ["SP"],
        "term_length": [savings_plan_term_length],
        'input_value': input_value_spill,
        'actual_input' : input_value,
        'sum_of_actual_on_demand_cost': sum_of_on_demand_cost,
        'merged_on_demand_cost' : mereged_ondemand_cost,
        'sum_of_on_demand_cost_applied': summary_df['on_demand_cost_applied'].sum(),
        'total_savings_plan': summary_df['adjusted_saving_plan_per_hour'].sum(),
        'total_amount': total_amount,
        'total_hours' : original_total_hrs,
        'profit': profit,
        'spill_sum': summary_df['spill_usage'].sum(),
        'profit_percent': profit_percent
    })

    summary_output_df = pd.DataFrame(summary_list)
    # Save result to CSV with proper string formatting
    # summary_filename = f"data/{batch_id}/savingsplan_result_{saving_plan_amount}.csv"
    # summary_filename = f"s3://{bucket}/data/{request_id}/{batch_id}/savingsplan_result.parquet"
    wr.s3.to_parquet(df=summary_output_df, dataset=True,path=summary_filename, mode="overwrite")
    print(summary_filename)
#     parent = os.path.dirname(summary_filename)

# # Check if the directory exists, and create it if it doesn't
#     if not os.path.exists(parent):
#         os.makedirs(parent)
#     summary_output_df.to_csv(summary_filename, index=False)
    outputfiles["summary_filename"] = summary_filename
    result_df = pd.concat([result_df,zero_df ], ignore_index=True)
    wr.s3.to_parquet(df=result_df, path=resultfile,dataset=True, mode="overwrite")
    print(resultfile)
    # result_df.to_csv(resultfile, index=False)
    return summary_output_df,outputfiles
