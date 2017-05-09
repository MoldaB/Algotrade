import sys
import os
import pandas as pd
import numpy as nm

path = os.getcwd()
path_orders_file = "orders.csv"
path_output_file = "output_file.csv"
orders_file = None
orders_head = None
order_lines = []
orders_set = None


new_features = ['avr_price'
                , 'max_price'
                , 'avr_volume'
                , 'max_volume'
                , 'std_volume'
                , 'QGFlag'
                , 'percent_canceled'
                , 'percent_updated'
                , 'percent_Completed'
                , 'buy_percent'
                , 'sell_percent'
                , 'IOC//None'
                , 'partial_vol_percent'
                , 'avr_num_of_Updates_per_order'
                , 'max_num_of_Updates_per_order'
                , 'std_num_of_Updates_per_order'
                # boyman
                , 'avr_Actions_perSec'
                , 'max_Actions_perSec'
                , 'std_Actions_perSec'
                , 'avr_Actions_per2Sec'
                , 'max_Actions_per2Sec'
                , 'std_Actions_per2Sec'
                , 'avr_Actions_per10Sec'
                , 'max_Actions_per10Sec'
                , 'std__Actions_per10Sec'
                , 'avr_Actions_per60Sec'
                , 'max_Actions_per60Sec'
                , 'std_Actions_per60Sec'
                , 'activity_hours_09_to_14'
                , 'activity_hours_15_to_21'
                , 'activity_hours_22_to_03'
                , 'activity_hours_04_to_08']
new_df = pd.DataFrame

try:
    file_abs_path = os.path.abspath(os.path.join(os.getcwd(), "orders_1.csv"))
    with open(file_abs_path, 'r') as f:
        orders_set = pd.read_csv(f)

        f = {'Price': {'avr_price': 'mean', 'max_price': 'max'}, 'Vol': {'avr_volume': 'mean', 'max_volume': 'max', 'std_volume': 'std'}, 'QGFlag': {'QG/None': 'count'}}
        grouped1 = orders_set.groupby(['Id']).agg(f, axis=1)

        grouped_status= pd.get_dummies(orders_set['Status'], prefix='status')\
            .groupby(orders_set['Id'])\
            .sum() \
            .apply( lambda x: x / x.sum() * 100, axis=1)

        grouped_buy_sell = pd.get_dummies(orders_set['Side'], prefix='side')\
            .groupby(orders_set['Id'])\
            .sum()\
            .apply(lambda x: x / x.sum() * 100, axis=1)

        print("finished building all features")
        new_df = grouped1.join(grouped_status, how='outer')\
            .join(grouped_buy_sell, how='outer')

        new_df.to_csv('new_df.csv')

except Exception as inst:
    print(inst.args)
    sys.exit()
