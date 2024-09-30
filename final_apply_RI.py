import pandas as pd
import re
import numpy as np
import warnings
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


# from apply_sp_v2 import adjust_covered_percentage_savings_plan,apply_savings_plan
from apply_ri import decremental_deduction_optimized_ri

if __name__ == '__main__':
    
    savingsplan_type ="RI"
    
    if savingsplan_type =="RI":
        # savings_plan_name_2 = "ri"
        filename={}
        filename["cur_files"]=["/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/cloudfront.csv"]
        filename["ri_spill_files"] = ''
        filename["new_ris"] = '/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/ri_plans.csv'
        
        
        if "cur_files" in filename and filename["cur_files"]!="":
            original_df = data_merger(filename["cur_files"])
            # df = pd.read_csv("/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/cloudfront.csv")
            df = original_df
            df['line_item_usage_start_date'] = pd.to_datetime(df['line_item_usage_start_date'])
            # df['line_item_usage_start_date'] = pd.to_datetime(df['line_item_usage_start_date'])
            df['Covered'] = 100-df['avail_percent']
            df['compute_col_remaining'] = df['line_item_normalized_usage_amount'] * (df['avail_percent'] / 100)
            print(len(df))
            
        if filename["new_ris"] != '':
            
            # ri_df = pd.read_csv("/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/ri_plans.csv")
            ri_df = pd.read_csv(filename["new_ris"])
# spill_data = pd.read_csv("spill_df_.csv")
            
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

            # Ensure the date column is in datetime format

        if filename["ri_spill_files"] != '':
            # Process each group individually and collect results
                # spill_data = pd.read_csv(ri_spill_file)
                spill_data= data_merger(filename["ri_spill_files"])
                # print(len(spill_data))
                spill_data['line_item_usage_start_date_spill'] = pd.to_datetime(spill_data['line_item_usage_start_date_spill'])
                spill_data['reservation_availability_zone'] = spill_data['reservation_availability_zone'].fillna('missing')
                spill_data['product_region_code'] = spill_data['product'].apply(lambda x: re.search(r'region=([\w-]+)', x).group(1) if pd.notnull(x) else None)
                spill_data['size_flex'] = spill_data['product'].apply(lambda x: re.search(r'size_flex=(\w+)', x).group(1) if pd.notnull(x) else None)
                spill_data['instance_family_type'] = spill_data['line_item_usage_type'].str.split(':').str[1].str.split('.').str[0]
                

            # # Loop through each group in spill_data based on 'line_item_usage_start_date_spill'
            #     for id_1,gp in spill_data.groupby('line_item_usage_start_date_spill'):
            #             # Concatenate the current row from ri_df with the current group from spill_data
            #         ri_df['line_item_usage_start_date_spill'] = gp['line_item_usage_start_date_spill'].iloc[0]
            #         ri_spill_mergerd = pd.concat([ri_spill_mergerd,gp,ri_df])
                        
            #             # Append the concatenated DataFrame to ri_spill_mergerd
            #         # print(len(ri_spill_mergerd))  
            # else:
            #     for _, df_group in df.groupby('line_item_usage_start_date'):
            #         ri_df['line_item_usage_start_date_spill'] = df_group['line_item_usage_start_date'].iloc[0]    
            #         ri_spill_mergerd = pd.concat([ri_spill_mergerd,ri_df])
                    
                # summary_df,outputfiles= apply_savings_plan(filename,savings_plan_amount, savings_plan_term_length_2, savings_plan_name_2)
                # print(outputfiles)
        result_list = []
        spill_list = []
        spill = "/home/prasanna/Music/cloudevolve/new-savingsplan-simulations/spill_df_.csv"
        spill_added = True
        spill_df = pd.DataFrame()
        ri_spill_mergerd = pd.DataFrame()
        
        for _, group in df.groupby('line_item_usage_start_date'):
            line_item_usage_start_date = group['line_item_usage_start_date'].iloc[0]
            line_item_usage_start_date = pd.Timestamp(line_item_usage_start_date)
            # print(line_item_usage_start_date)

            ri_spill_mergerd = pd.DataFrame()
            if  filename["new_ris"] != '':
                ri_df['line_item_usage_start_date_spill'] = group['line_item_usage_start_date'].iloc[0]
                ri_spill_mergerd = pd.concat([ri_spill_mergerd,ri_df])
                # print("ri len",len(ri_spill_mergerd))
            if filename["ri_spill_files"]!='' :
                # print(spill_data['line_item_usage_start_date_spill'].dtype)
                # print(type(line_item_usage_start_date))

                filtered = spill_data[spill_data['line_item_usage_start_date_spill'] == line_item_usage_start_date]
                # print("filtered ri",len(filtered))
                ri_spill_mergerd = pd.concat([ri_spill_mergerd,filtered])
            print("spill_added",len(ri_spill_mergerd),len(group))
            result_group, spill_group = decremental_deduction_optimized_ri(group,spill_added,ri_spill_mergerd)
            result_list.append(result_group)
            spill_list.append(spill_group)

    # Concatenate all results into final DataFrames
    result_df = pd.concat(result_list, ignore_index=True)
    spill_df = pd.concat(spill_list, ignore_index=True)

    # Save the resulting DataFrames to CSV files
    result_df.to_csv("final_result_df_using_spill.csv", index=False)
    spill_df.to_csv("final_spill_df_new.csv", index=False)