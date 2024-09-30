import pandas as pd
import re
import numpy as np
import warnings


warnings.filterwarnings("ignore", category=DeprecationWarning)

def decremental_deduction_optimized_ri(group, spill_added = False,spill=None):
    group['Covered'] = group['Covered'].astype(float)
    group['avail_percent'] = group['avail_percent'].astype(float)

    # group['Covered'] = group['Covered'].astype('float64')
    line_item_usage_start_date = group["line_item_usage_start_date"].iloc[0]
    # print("line_item_usage_start_date",line_item_usage_start_date)
    
    spill_df = pd.DataFrame()
    # if not spill.empty:
        
    updated_data2 = spill
    
    filtered_false_df = updated_data2[
        (updated_data2["size_flex"] == "false") & 
        (updated_data2["line_item_usage_start_date_spill"] == line_item_usage_start_date)
    ]
    filtered_true_df = updated_data2[
    (updated_data2["size_flex"] == "true") & 
    (updated_data2["line_item_usage_start_date_spill"] == line_item_usage_start_date)]

    
        
    for index, row in filtered_false_df.iterrows():
        new_row = row.copy()
        new_row["line_item_usage_start_date_spill"] = line_item_usage_start_date
        mask = (
            (group["line_item_operation"] == row["line_item_operation"]) &
            (group["product_region_code"] == row["product_region_code"]) &
            (group["instance_family_type"] == row["instance_family_type"]) &
            (group["reservation_availability_zone"] == row["reservation_availability_zone"]) &
            (group["line_item_usage_type"] == row["line_item_usage_type"])&
            (group["line_item_usage_start_date"]==row["line_item_usage_start_date_spill"])&
            (row["spill_usage"]!=0)
        )
        
        matching_rows = group.loc[mask]
        total_units = row['spill_usage']
        if not matching_rows.empty:
            num_matching_rows = len(matching_rows)
            
            # Distribute units across matching rows
            if num_matching_rows < total_units:
                new_row["spill_usage"] = total_units - num_matching_rows
                
                for i, idx in enumerate(matching_rows.index):
                    units_to_assign = min(1, total_units - i)  # Assign units, ensuring we don't exceed total_units
                    group.loc[idx, 'Covered'] = 100  # Assuming full coverage per unit
                    group.loc[idx, 'savings_plans_covered_cost'] = group.loc[idx, "line_item_blended_rate"] * (group.loc[idx, 'Covered'] / 100)
                    # group.loc[idx, 'on_demand_cost_applied'] = group.loc[idx, 'od_cost'] * (1 - group.loc[idx, 'Covered'] / 100)
                    group.loc[idx, 'avail_percent'] = 100 - group.loc[idx, 'Covered']
                    
            else:
                new_row["spill_usage"] = 0
                for i in range(total_units):
                    idx = matching_rows.index[i]  # Select the i-th matching row
                    group.loc[idx, 'Covered'] = 100
                    group.loc[idx, 'savings_plans_covered_cost'] = group.loc[idx, "line_item_blended_rate"] * (group.loc[idx, 'Covered'] / 100)
                    # group.loc[idx, 'on_demand_cost_applied'] = group.loc[idx, 'od_cost'] * (1 - group.loc[idx, 'Covered'] / 100)
                    group.loc[idx, 'avail_percent'] = 100 - group.loc[idx, 'Covered']
                    
            spill_df = pd.concat([spill_df, new_row.to_frame().T], ignore_index=True)
        else:
            # print("not matching rows found in flase")
            new_row["spill_usage"] = total_units
            spill_df = pd.concat([spill_df, new_row.to_frame().T], ignore_index=True)
        group['on_demand_cost_applied'] = group['od_cost'] * (1 - group['Covered'] / 100)

    for index, row in filtered_true_df.iterrows():
        new_row = row.copy()
        new_row["line_item_usage_start_date_spill"] = line_item_usage_start_date

        mask = (
            (group["line_item_operation"] == row["line_item_operation"]) &
            (group["product_region_code"] == row["product_region_code"]) &
            (group["instance_family_type"] == row["instance_family_type"]) & 
            (group["Covered"] < 100.0)&
            (group["line_item_usage_start_date"] == row["line_item_usage_start_date_spill"])&
            (row["spill_usage"]!=0)
        )
        matching_rows = group.loc[mask]
        total_units = row['spill_usage']
        # print("initial total units",total_units,row['spill_usage'])
        if not matching_rows.empty:
            # print("not empty")
            compute_col_ = 'line_item_normalized_usage_amount'
            compute_col = 'compute_col_remaining'
            matching_rows[compute_col] = matching_rows[compute_col_] * (matching_rows['avail_percent'] / 100)
            # matching_rows[["Covered", 'avail_percent',compute_col,'line_item_normalized_usage_amount']].to_csv(f"{index}-original.csv")
            # cumulative_cost = matching_rows[compute_col].cumsum()
            cumulative_cost = matching_rows[compute_col].cumsum()
            covered_percentage = np.where(cumulative_cost <= total_units, 100.0, 0)
            first_exceed_index = np.argmax(cumulative_cost > total_units)
            # print("first_exceed_index",first_exceed_index)
            if cumulative_cost.iloc[first_exceed_index] > total_units and first_exceed_index > 0:
                covered_percentage[first_exceed_index] = (
                    (total_units - cumulative_cost.iloc[first_exceed_index-1]) / 
                    matching_rows[compute_col].iloc[first_exceed_index]
                ) * 100.0
            elif first_exceed_index == 0 :
                # print("covered_percentage[first_exceed_index]:",covered_percentage[first_exceed_index])
                covered_percentage[first_exceed_index] = (
                    ( total_units ) / 
                    matching_rows[compute_col].iloc[first_exceed_index]
                ) * 100.0
                # print("else:",covered_percentage[first_exceed_index])
            # max_cumulative_cost = cumulative_cost.max()
            max_cumulative_cost = cumulative_cost.max()
            print(total_units,max_cumulative_cost)
            new_row["spill_usage"] = total_units - max_cumulative_cost if max_cumulative_cost < total_units else 0
            spill_df = pd.concat([spill_df, new_row.to_frame().T], ignore_index=True)
           

            # print("matched",new_row["spill_usage"])   
            # Cast the covered_percentage to a compatible dtype before assignment
            group.loc[mask, 'Covered'] = (group.loc[mask, compute_col]/group.loc[mask, compute_col_]*covered_percentage) + group.loc[mask, 'Covered']
            # print(group.loc[mask, 'Covered'],covered_percentage , group.loc[mask, 'Covered'])
            group.loc[mask, 'savings_plans_covered_cost'] = row["line_item_blended_rate"] * (covered_percentage.astype('float64') / 100)
            # group.loc[mask, 'on_demand_cost_applied'] = group.loc[mask, 'od_cost'] * (1 - covered_percentage.astype('float64') / 100)
            group.loc[mask, 'avail_percent'] = 100 - group.loc[mask, 'Covered']
            group.loc[mask,compute_col] =  group.loc[mask,compute_col_] * (group.loc[mask, 'avail_percent'] / 100)

            # print("matchinf lenth",len(group.loc[mask, 'avail_percent']),len(group.loc[mask, 'on_demand_cost_applied']),len(group.loc[mask, 'savings_plans_covered_cost']) ,len(group.loc[mask, 'Covered']),len(matching_rows),len(cumulative_cost),len(covered_percentage),total_units,cumulative_cost.min())
            columns_to_save = ['Covered', 'avail_percent', compute_col, 'line_item_normalized_usage_amount']
            # group.loc[mask, columns_to_save].to_csv(f"{index}_group.csv")
        
        else:
            # print("not matching rows found in True")
            new_row["spill_usage"] = total_units
            # print(new_row["spill_usage"])
            new_row["line_item_usage_start_date_spill"] = line_item_usage_start_date
            spill_df = pd.concat([spill_df, new_row.to_frame().T], ignore_index=True)
        group['on_demand_cost_applied'] = group['od_cost'] * (1 - group['Covered'] / 100)    
   
    return group, spill_df
    
    