import os
import pandas as pd

new_features = ['avr_price'
                , 'max_price'
                , 'avr_volume'
                , 'max_volume'
                , 'std_volume'
                , 'percent_canceled'
                , 'percent_updated'
                , 'percent_Completed'
                , 'buy_percent'
                , 'sell_percent'
                , 'partial_vol_percent'
                , 'QGFlag'
                , 'IOC//None'

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
                , 'std_Actions_per10Sec'
                , 'avr_Actions_per60Sec'
                , 'max_Actions_per60Sec'
                , 'std_Actions_per60Sec'
                , 'activity_hours_09_to_14'
                , 'activity_hours_15_to_21'
                , 'activity_hours_22_to_03'
                , 'activity_hours_04_to_08']
new_df = pd.DataFrame

def findOrdersUpdates(data_set):
    orders = {}
    # gather order lists
    for index, row in orders_set.iterrows():
        new_order_chain = {
            'acc_num': row['Id'],
            'prev_order': row['OrigOrdNo'],
            'next_order': row['ReplOrdNo']
        }
        if row['OrigOrdNo'] in orders.keys():
            orders[row['OrigOrdNo']]['next_order'] = new_order_chain
        elif row['ReplOrdNo'] in orders.keys():
            orders[row['ReplOrdNo']]['next_order']= new_order_chain
        else:
            orders[row['OrdNo']] = new_order_chain
    for orderKey in orders:
        order = orders[orderKey]
        last_order_link = order['next_order']
        updates_count = 0
        while type(last_order_link) is not int:
            updates_count += 1
            last_order_link = last_order_link['next_order']
        order['updates_count'] = updates_count
    return list(map(lambda x: {'Id': orders[x]['acc_num'],
                               'OrdNo': x,
                               'order_updates_count': orders[x]['updates_count']}, orders))

file_abs_path = os.path.abspath(os.path.join(os.getcwd(), "orders_1.csv"))
with open(file_abs_path, 'r') as f:
    orders_set = pd.read_csv(f)

    orders_set['QGFlag'] = orders_set['QGFlag'].map({'QG': 1})
    orders_set['Cond'] = orders_set['Cond'].map({'IOC': 1})

    orders_set['partial_vol_percent'] = orders_set['MatVol'] / orders_set['Vol'] * 100
    f = {'Price': {'avr_price': 'mean',
                   'max_price': 'max'},
         'Vol': {'avr_volume': 'mean',
                 'max_volume': 'max',
                 'std_volume': 'std',
                 'sum_volume': 'sum'},
         'MatVol': {'sum_mat_volume': 'sum'},
         'QGFlag': {'QGFlag/None': 'sum'},
         'Cond': {'IOC/None': 'sum'}
         }
    # extracts price and volume features
    grouped1 = orders_set.groupby(['Id']).agg(f, axis=1)
    # gets partial volume percent
    grouped1['partial_vol_percent'] = grouped1['MatVol']['sum_mat_volume'] / grouped1['Vol']['sum_volume'] * 100
    # removes redundent rows
    grouped1['MatVol'] = None
    grouped1['Vol']['sum_volume'] = None
    print(grouped1.head())

    grouped_status = pd.get_dummies(orders_set['Status'], prefix='status')\
        .groupby(orders_set['Id'])\
        .sum() \
        .apply(lambda x: x / x.sum() * 100, axis=1)
    print(grouped_status.head())

    grouped_buy_sell = pd.get_dummies(orders_set['Side'], prefix='side')\
        .groupby(orders_set['Id'])\
        .sum()\
        .apply(lambda x: x / x.sum() * 100, axis=1)
    print(grouped_buy_sell.head())

    orders_updates = findOrdersUpdates(orders_set)
    def_orders = pd.DataFrame.from_dict(orders_updates, orient='columns')
    f1 = {'order_updates_count': {'avr_num_of_Updates_per_order': 'mean',
                                  'max_num_of_Updates_per_order': 'max',
                                  'std_num_of_Updates_per_order': 'std'}}
    grouped_orders = def_orders.groupby('Id').agg(f1)
    print(grouped_orders.head())

    new_df = grouped1.join(grouped_status, how='outer')\
        .join(grouped_buy_sell, how='outer').join(grouped_orders, how='outer')
    new_df.to_csv('new_df.csv')
