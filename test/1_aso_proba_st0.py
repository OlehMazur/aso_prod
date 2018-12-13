#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 20:50:15 2018

@author: oleh
"""

import dask.dataframe as dd 
import pandas as pd
import numpy as np
from sklearn.preprocessing import Imputer
from sklearn.cluster import KMeans
import os
import time

import lib.data_with_cluster_or_not as dfc
import lib.transaction_data_preparation as transaction


# main parametrs
script_dir = '/home/oleh/ASO/sample' ##os.path.dirname(__file__) #absolute dir the script is in
csv_file_main_data = 'BG_2018_v2.csv'
csv_file_SKU = 'BG_V2_SKU.csv'
csv_file_Shop = 'BG_V2_Shop.csv'
    
period = 'month' # period month/week/day
#report_period = 'semester' #quarter, year
network = 'Пятёрочка'
category = 'Пиво'
producer = 'BALTIKA'
city = 'Москва'
    
encoding = 'cp1251'
delimiter = ";"
# main parametrs


data_version = '001' #096
use_clustering = False
n_clusters= 4 #get number from 1_data_preparation_cluster script 
ff2 = dfc.get_data(script_dir, csv_file_main_data, csv_file_SKU, csv_file_Shop, period, network, category, producer, city, encoding, delimiter)[1]

time1 = time.time()

if use_clustering:
    result = dfc.get_data(script_dir, csv_file_main_data, csv_file_SKU, csv_file_Shop, period, network, category, producer, city, encoding, delimiter)[0].compute()
    X = result.iloc[:, [1,2]] # SalesValue and Assortments
    imputer = Imputer(missing_values = 'NaN', strategy = "mean", axis = 0)
    imputer = imputer.fit(X)
    X = imputer.transform(X)
    
    kmeans = KMeans(
            n_clusters= n_clusters , # choose number of clusters 
            init = 'k-means++', 
            max_iter = 300,
            n_init = 10,
            random_state=0)
    
    y_kmeans = kmeans.fit_predict(X)  
    
    #data with cluster
    result["Cluster"] = y_kmeans
    
    for ind, el in enumerate(np.unique(y_kmeans )):
    
        data_cl = result.loc[result["Cluster"] == el]
        data_cl["Shop_Id"] = data_cl.index
    
        #input file preparation 
        data_with_shop_cluster = dd.merge(ff2, 
             data_cl[['Shop_Id']],
             how = 'inner',
             on = 'Shop_Id'
            )
        
        data_with_shop_cluster_df= data_with_shop_cluster.compute()
        
        data_with_shop_cluster_df["year"]=  (data_with_shop_cluster_df["Date"].astype(int)/10000).astype(int)
    
        data_with_shop_cluster_df["month"]= (data_with_shop_cluster_df["Date"].astype(int)/100).astype(int) - \
                                        (data_with_shop_cluster_df["Date"].astype(int)/10000).astype(int) *100
    
        data_with_shop_cluster_df["day"] = (data_with_shop_cluster_df["Date"].astype(int)).astype(int)  - \
                                        (data_with_shop_cluster_df["Date"].astype(int)/100).astype(int) *100
    
         
        data_with_shop_cluster_df["week"]  = pd.to_datetime(data_with_shop_cluster_df[["year", "month", "day"]]).dt.week                      
    
    
        data_with_shop_cluster_df["SalesNumber"] = data_with_shop_cluster_df["SKU_Id"]
    
        main = data_with_shop_cluster_df.groupby([period, 'year', 'Shop_Id', 'SKU_Id', 'xname', 'Network', 'Category', 'Producer', 'City'], as_index = False) \
                    .agg({'Quantity': np.sum, 'SalesValue': np.sum, 'SalesNumber': np.size})
                 
        main["AssKey"] = main[period].astype(str) + main["Shop_Id"].astype(str)
    
    
    
        tns = data_with_shop_cluster_df.groupby([period, 'Shop_Id'], as_index = False) \
                    .agg({'Quantity': [np.min, np.max, np.sum]})
                    
        tns["TotalNumOfSales"] = data_with_shop_cluster_df.groupby([period, 'Shop_Id'])['SKU_Id'].nunique().reset_index(drop=True)
        tns.columns = [period, 'Shop_Id', 'minQuantity', 'maxQuantity', 'TotalSalesPerAss' , 'TotalNumOfSales']
        tns["AssKey"] = tns[period].astype(str) + tns["Shop_Id"].astype(str)
    
    
        rev = data_with_shop_cluster_df.groupby(['SKU_Id'], as_index = False) \
                    .agg({'SalesValue': np.mean})
        rev.columns = ['SKU_Id', 'Revenue']
    
    
        output_data_p1 = pd.merge (
            main[['AssKey','year', period,'Shop_Id', 'SKU_Id', 'xname', 'Network', 'Category', 'Producer', 'City', 'Quantity', 'SalesValue', 'SalesNumber'] ], 
            tns[['AssKey', 'minQuantity', 'maxQuantity', 'TotalSalesPerAss','TotalNumOfSales']],
            how='left',
            on= 'AssKey')
    
        output_data_p2 = pd.merge (
            output_data_p1,
            rev,
            how= 'left',
            on = 'SKU_Id'
            )
    
        output_data_p2["No-purchaseProb"] = 0.1+0.2*( output_data_p2["TotalNumOfSales"] / (output_data_p2["maxQuantity"] - output_data_p2["minQuantity"] ) )
        output_data_p2["PurchaseProb"] = (1 - output_data_p2["No-purchaseProb"] ) * (output_data_p2["Quantity"] / output_data_p2["TotalSalesPerAss"] ) 
        output_data_p2 = output_data_p2[ (output_data_p2["No-purchaseProb"] != np.inf) &  (output_data_p2["PurchaseProb"] != np.inf )]
        output_data_p2 = output_data_p2[[ "AssKey", "SKU_Id","year",period, "xname","Network", "Category", "Producer", "City", "Revenue", "No-purchaseProb", "PurchaseProb"]]
    
        #encoding = 'cp1251'
        rel_path_source_main = "data/"+"GDT_Input_" + str(el)+"_"+network+"_"+city+".csv"
        abs_file_source_main= os.path.join(script_dir, rel_path_source_main)
        output_data_p2.to_csv(abs_file_source_main, sep = '|', index = False )
        print("file " + abs_file_source_main + " is ready !" )
        
        #should be transaction .dat file creation next
        #........
else:
        ff2_df = ff2.compute()
         
        ff2_df["year"]=  (ff2_df["Date"].astype(int)/10000).astype(int)
    
        ff2_df["month"]= (ff2_df["Date"].astype(int)/100).astype(int) - \
                                        (ff2_df["Date"].astype(int)/10000).astype(int) *100
    
        ff2_df["day"] = (ff2_df["Date"].astype(int)).astype(int)  - \
                                        (ff2_df["Date"].astype(int)/100).astype(int) *100
    
         
        ff2_df["week"]  = pd.to_datetime(ff2_df[["year", "month", "day"]]).dt.week                      
    
    
        ff2_df["SalesNumber"] = ff2_df["SKU_Id"]
        
        #if report_period == 'semester': #quarter, year
        ff2_df = ff2_df[ (ff2_df['month'] >=7) & (ff2_df['month'] <= 8)] #!!!!!!!!!!!!
    
        main = ff2_df.groupby([period, 'year', 'Shop_Id', 'SKU_Id', 'xname', 'Network', 'Category', 'Producer', 'City'], as_index = False) \
                    .agg({'Quantity': np.sum, 'SalesValue': np.sum, 'SalesNumber': np.size})
                 
        main["AssKey"] = main[period].astype(str) + main["Shop_Id"].astype(str)
     
        tns = ff2_df.groupby([period, 'Shop_Id'], as_index = False) \
                    .agg({'Quantity': [np.min, np.max, np.sum]})
                    
        tns["TotalNumOfSales"] = ff2_df.groupby([period, 'Shop_Id'])['SKU_Id'].nunique().reset_index(drop=True)
        tns.columns = [period, 'Shop_Id', 'minQuantity', 'maxQuantity', 'TotalSalesPerAss' , 'TotalNumOfSales']
        tns["AssKey"] = tns[period].astype(str) + tns["Shop_Id"].astype(str)
    
    
        rev = ff2_df.groupby(['SKU_Id'], as_index = False) \
                    .agg({'SalesValue': np.mean})
        rev.columns = ['SKU_Id', 'Revenue']
    
    
        output_data_p1 = pd.merge (
            main[['AssKey','year', period,'Shop_Id', 'SKU_Id', 'xname', 'Network', 'Category', 'Producer', 'City', 'Quantity', 'SalesValue', 'SalesNumber'] ], 
            tns[['AssKey', 'minQuantity', 'maxQuantity', 'TotalSalesPerAss','TotalNumOfSales']],
            how='left',
            on= 'AssKey')
    
        output_data_p2 = pd.merge (
            output_data_p1,
            rev,
            how= 'left',
            on = 'SKU_Id'
            )
    
        output_data_p2["No-purchaseProb"] = 0.1+0.2*( output_data_p2["TotalNumOfSales"] / (output_data_p2["maxQuantity"] - output_data_p2["minQuantity"] ) )
        output_data_p2["PurchaseProb"] = (1 - output_data_p2["No-purchaseProb"] ) * (output_data_p2["Quantity"] / output_data_p2["TotalSalesPerAss"] ) 
        output_data_p2 = output_data_p2[ (output_data_p2["No-purchaseProb"] != np.inf) &  (output_data_p2["PurchaseProb"] != np.inf )]
        output_data_p2 = output_data_p2[[ "AssKey", "SKU_Id","year",period, "xname","Network", "Category", "Producer", "City", "Revenue", "No-purchaseProb", "PurchaseProb"]]
    
        #encoding = 'cp1251'
        rel_path_source_main = "data/"+"GDT_Input_" +network+"_"+city+".csv"
        abs_file_source_main= os.path.join(script_dir, rel_path_source_main)
        output_data_p2.to_csv(abs_file_source_main, sep = '|', index = False )
        print("file " + abs_file_source_main + " is ready !" )
        
        print('transaction dat file generation...')
        
        dataset = output_data_p2.set_index(['AssKey', 'SKU_Id'])
        #dataset = dataset[dataset["PurchaseProb"] >=0.05] #0.05,  0.1
        transaction.create_transaction_dat_file(data_version, dataset, network,category, producer,city, script_dir)

time2 = time.time()    
print('Execution time ' , time2-time1, ' sec')        