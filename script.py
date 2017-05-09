import sys
import os
from pandas import *
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
                , 'avr_volium'
                , 'max_volium'
                , 'std_volium'
                , 'partial_vol_percent'
                , 'QGFlag'
                , 'percent_canceled'
                , 'percent_updated'
                , 'percent_Completed'
                , 'IOC//None'
                , 'buy_percent'
                , 'sell_percent'
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
new_df = pandas.DataFrame

try:
    file_abs_path = os.path.abspath(os.path.join(os.getcwd(), "orders.csv"))
    with open(file_abs_path, 'r') as f:
        orders_set = pandas.read_csv(f)

        f = {'Price': {'avr_price': 'mean', 'max_price': 'max'}, 'Vol': {'avr_volume': 'mean', 'max_volume': 'max', 'std_volume': 'std'}, 'QGFlag': {'QG/None': 'count'}}
        grouped1 = orders_set.groupby(['Id']).agg(f)
        new_df1 = grouped1.reset_index()

        canceled_df = orders_set[(orders_set['Status'] == 'X')].groupby('Id').agg({'Status': {'canceled': 'count'}})
        updated_df = orders_set[(orders_set['Status'] == 'O')].groupby('Id').agg({'Status': {'updated': 'count'}})
        completed_df = orders_set[(orders_set['Status'] == 'M')].groupby('Id').agg({'Status': {'completed': 'count'}})

        new_df = pandas.merge(canceled_df.reset_index(),updated_df.reset_index(), on='Id', how='outer').merge(completed_df.reset_index(), on='Id', how='outer').merge(new_df1, on='Id', how='outer')

        new_df.to_csv('new_df.csv')

        print("bla bla")

except Exception as inst:
    print(inst.args)
    sys.exit()
