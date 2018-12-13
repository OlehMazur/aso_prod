#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 15:03:14 2018

@author: oleh
"""

import os
import dask.dataframe as dd 
import pandas as pd
import numpy as np
import datetime
'''
#parametrs
script_dir = '/home/oleh/ASO/sample' ##os.path.dirname(__file__) #absolute dir the script is in
csv_file_main_data = 'BG_2018_v2.csv'
csv_file_SKU = 'BG_V2_SKU.csv'
csv_file_Shop = 'BG_V2_Shop.csv'
    
period = 'month' # period month/week/day
network = 'Пятёрочка'
category = 'Пиво'
producer = 'BALTIKA'
city = 'Москва'
    
encoding = 'cp1251'
delimiter = ";"
#parametrs
'''
      

    


def get_week(year, month , day):
    dt = datetime.date(year, month, day)
    wk = dt.isocalendar()[1]
    return wk

def get_data(script_dir, csv_file_main_data, csv_file_SKU, csv_file_Shop, period, network, category, producer, city, encoding, delimiter):

    rel_path_source_main = "source/"+csv_file_main_data
    abs_file_source_main= os.path.join(script_dir, rel_path_source_main)
    
    rel_path_source_sku = "source/"+csv_file_SKU
    abs_file_source_sku= os.path.join(script_dir, rel_path_source_sku)
    
    rel_path_source_shop = "source/"+csv_file_Shop
    abs_file_source_shop= os.path.join(script_dir, rel_path_source_shop)
    
    df = dd.read_csv( abs_file_source_main, encoding = encoding, delimiter = delimiter , decimal = "," 
                          ,dtype = {'Количество продукции Шт': 'float64'}
                          )
    df.columns = ['Shop_Id', 'SKU_Id', 'Date', 'Quantity', 'SalesValueDal', 'SalesValue']
    
    
    sku_dic = pd.read_csv( abs_file_source_sku,  encoding = encoding, delimiter = delimiter , engine = 'python',  error_bad_lines=False)
    sku_dic_data = sku_dic.iloc[:, [0,1,2,3,4,5,6]]
    sku_dic_data.columns = ['SKU_Id', 'Client', 'xcode', 'xname', 'Category', 'Producer', 'SKU_Nielsen_Id']
    
    shop_dic = pd.read_csv( abs_file_source_shop, encoding = encoding, delimiter = delimiter , engine = 'python',  error_bad_lines=False)
    shop_dic_data = shop_dic.iloc[:, [0,1,2,4,6,8,9,10,11,12,13]]
    shop_dic_data.columns = ['Shop_Id', 'Network', 'Store_nb', 'Format', 
                             'UnionNetwork', 'SubChannel', 'Region', 'Province', 'City', 'Population', 'UnionRegion' ]
    
    
    ff  = dd.merge(df[["Shop_Id", "SKU_Id", "Date", "Quantity", "SalesValue"]],
             shop_dic_data[["Shop_Id", "Network", "City"]],
             how = 'left',
             on = 'Shop_Id')
    
    ff2 = dd.merge(ff, 
                   sku_dic_data[["SKU_Id","Producer", "Category", "xname"]], 
                   how = 'left',
                   on = "SKU_Id"               
                   )
    
    ff2 = ff2[ (ff2["Network"] == network) & (ff2["Category"] == category ) & (ff2["Producer"] == producer) & (ff2["City"] == city )]
    
    
    tt = ff2.groupby(['Shop_Id']).agg({'Quantity': np.sum, 'SalesValue': np.sum})
    tt["Assortment"] = ff2.groupby(['Shop_Id'])['SKU_Id'].nunique()
    
    return [tt, ff2]
  

    