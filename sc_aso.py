import pandas as pd
import numpy as np
from pulp import *
import pickle
import time
import datetime
import os
import lib.fcns_asstopt_PULP as fcns_asstopt
import sys

try:
    network = sys.argv[1]
    city =  sys.argv[2]
    data_version = sys.argv[3]
    min_capacity = int(sys.argv[4])
    max_capacity = int(sys.argv[5])
   
except:
    print('Error; wrong input parameter, please specify: python ' +
          '@network ' +  
          '@city '+
          '@data_version '+ 
          '@min_capacity '+
          '@max_capacity '
          )

#network = 'Пятёрочка'
#city = 'Москва'
#data_version =  '001' 
#min_capacity = 0
#max_capacity = 34 #100 no capacity constraint    
    
# python  3_aso_script_st3_PULP.py Пятёрочка Москва 001 0 34  

print("Optimization of the assortment given a choice model")

algo_chosen = 'GDT'
 
filename_transaction        = 'transaction_data_'+ network + '_'+ city +'_'+data_version + '.dat'
filename_choice_model_GDT   = 'choice_model_GDT_PULP_'+ network + '_'+ city +'_' + data_version + '.dat'
filename_log = 'aso_result_PULP_'+network + '_'+ city +'_' + data_version + '.csv'
#filename_aso  = 'aso_bal_'+  data_version + '.dat'
#filename_output  = 'output_bal_'+  data_version + '.dat'

script_dir =os.path.dirname(__file__) 
rel_path_transaction = "data/"+filename_transaction
abs_file_transaction = os.path.join(script_dir, rel_path_transaction)

rel_path_choice_model_GDT = "data/"+filename_choice_model_GDT
abs_file_choice_model_GDT = os.path.join(script_dir, rel_path_choice_model_GDT)

rel_path_log= "data/"+ filename_log
abs_file_log = os.path.join(script_dir, rel_path_log)
#rel_path_aso = "data/"+filename_aso
#abs_file_aso = os.path.join(script_dir, rel_path_aso)

#rel_path_output = "data/"+filename_output
#abs_file_output = os.path.join(script_dir, rel_path_output)

with open(abs_file_transaction, 'rb') as sales:
    my_depickler = pickle.Unpickler(sales)
    Inventories =     my_depickler.load()
    Proba_product =   my_depickler.load()
    Revenue =         my_depickler.load()
    dic =             my_depickler.load()
    Rev_Baseline =    my_depickler.load()
    Prod_List_Max =   my_depickler.load()
    real_revenue =    my_depickler.load()
    predicted_rev_data = my_depickler.load()
    products_data = my_depickler.load()
    ass_data = my_depickler.load()
    rev_all_products = my_depickler.load()
    max_ass_num_manual = my_depickler.load()
    prod_name = my_depickler.load()
    

if(algo_chosen=='GDT'):    
    print("Opening choice model, format GDT")
    with open(abs_file_choice_model_GDT, 'rb') as sales:
        my_depickler = pickle.Unpickler(sales)
        sigma_GDT_sorted =   my_depickler.load()
        lambda_GDT_sorted =   my_depickler.load()    
else:
    print("Error; wrong input parameter, which algorithm do you wish to use?")
    
(nb_asst, nb_prod) = Inventories.shape   

if(algo_chosen=='GDT' or algo_chosen=='gen' ):
    Lambda = lambda_GDT_sorted
    Sigma = sigma_GDT_sorted  
    t1 = time.time()
    [x_found, obj_val] = fcns_asstopt.run_asstopt_GDT(Lambda, Sigma, Revenue[:len(Sigma.T)], min_capacity, max_capacity)
    t2 = time.time()

d = []   
for el in dic:
    d.append(el)  

aso_result = []

'''        
for val in enumerate(d):
    if (val[0] in x_found):
       aso_result.append(val[1][1])
'''
for el in x_found:
       for p, v in enumerate(d):
           if( el == p and p >0 and el > 0):
                   aso_result.append(v[1])
       

same_prod= np.intersect1d(Prod_List_Max,  aso_result)
diff_prod = np.setdiff1d(aso_result, Prod_List_Max)   
exc_prod = np.setdiff1d(Prod_List_Max , same_prod)

def sigma_digest(sigmas, lambdas, nb_prod):
    a = sigmas[np.nonzero(lambdas)]
    b = lambdas[np.nonzero(lambdas)]
    sigmas_sorted = a[np.argsort(b), :][::-1]
    lambdas_sorted = b[np.argsort(b)][::-1]
    for i in range(len(sigmas_sorted)):
        nb_prod_indiff = (sigmas_sorted[i, :] == nb_prod - 1).sum()
        prod_prefered = np.argsort(sigmas_sorted[i, :])  # including the indiff products
        print("Sigma number ", i, ", probability associated:", lambdas_sorted[i], ", prefered products in order:")
        # print the list of preferred in order of preference
        print(prod_prefered[:nb_prod - nb_prod_indiff])
    return 1


def sigma_digest2(sigmas, lambdas, nb_prod):
    a = sigmas[np.nonzero(lambdas)]
    b = lambdas[np.nonzero(lambdas)]
    sigmas_sorted = a[np.argsort(b), :][::-1]
    lambdas_sorted = b[np.argsort(b)][::-1]
    for i in range(len(sigmas_sorted)):
        nb_prod_indiff = (sigmas_sorted[i, :] == nb_prod - 1).sum()
        prod_prefered = np.argsort(sigmas_sorted[i, :])  # including the indiff products
        print("Sigma number ", i, ", probability associated:", lambdas_sorted[i], ", prefered products in order:")
        # print the list of preferred in order of preference
        
        res = []
        dd = prod_prefered[:nb_prod - nb_prod_indiff]
    
        for el in dd:
            for p, v in enumerate(d):
                for sku in prod_name:
                    if( el == p and p >0 and el > 0 and v[1] == sku[0]):
                        res.append(sku[1]) #v[1]
        print(res)
               
    return 1

#sigma_digest2(Sigma,Lambda, nb_prod)

def sigma_digest_save_result(sigmas, lambdas, nb_prod):
    sigma_res= []
    
    a = Sigma[np.nonzero(Lambda)]
    b = Lambda[np.nonzero(Lambda)]
    sigmas_sorted = a[np.argsort(b), :][::-1]
    lambdas_sorted = b[np.argsort(b)][::-1]
    for i in range(len(sigmas_sorted)):
        nb_prod_indiff = (sigmas_sorted[i, :] == nb_prod - 1).sum()
        prod_prefered = np.argsort(sigmas_sorted[i, :])  # including the indiff products
        #sigma_res.append((i,lambdas_sorted[i]))    
        # print the list of preferred in order of preference
        
        res = []
        dd = prod_prefered[:nb_prod - nb_prod_indiff]
    
        for el in dd:
            for p, v in enumerate(d):
                if( el == p and p >0 and el > 0):
                    res.append(v[1])
        #print(res)
        sigma_res.append( (i,lambdas_sorted[i], res) )
               
    return sigma_res



#sigma_result = sigma_digest_save_result (Sigma,Lambda, nb_prod)
'''
#get sigmas in right format
sigma_prod_list = []
for item in sigma_result:
    sigma_prod_list.append((item[1], item[2]))
'''    
 
#optimal assortmnet with SKU names    
aso_result_SKU_name = []
for sku_el in prod_name:
    for sku_id in aso_result:
        if sku_el[0] == sku_id:
            aso_result_SKU_name.append(sku_el[1])
    
#same list of products
aso_result_SKU_name_same = []
for sku_el in prod_name:
    for sku_id in same_prod:
        if sku_el[0] == sku_id:
            aso_result_SKU_name_same.append(sku_el[1])
            
#new list of products
aso_result_SKU_name_new = []
for sku_el in prod_name:
    for sku_id in diff_prod:
        if sku_el[0] == sku_id:
            aso_result_SKU_name_new.append(sku_el[1])  
            
#excluded list of products
aso_result_SKU_name_ex = []
for sku_el in prod_name:
    for sku_id in exc_prod:
        if sku_el[0] == sku_id:
            aso_result_SKU_name_ex.append(sku_el[1])              

print('')
print('Date: '+ str(datetime.date.today()) )
print("Network:",  network)
print("City:",  city)
print("transaction file: ", filename_transaction, "with nb_prod =", nb_prod, "products")
print('')
print('min_capacity: ' , min_capacity)
print('max_capacity: ' , max_capacity)
print('')
print(nb_asst, "assortments available in transaction dataset")
print('')
print("Optimal assortment found in", t2-t1, "seconds. Number of products", len(aso_result), ".Products present in optimal assortment:")
print('')
print(aso_result) #x_found
print('')
print(aso_result_SKU_name) 
print('')
print("List of common products in assorment with higher real revenue and optimal assortment: ")
print('')
print(list(same_prod))
print('')
print(aso_result_SKU_name_same)
print('')
print("List of new recommended products: ")
print('')
print(list(diff_prod))
print('')
print(aso_result_SKU_name_new)
print('')
print("List of excluded products: ")
print('')
print (list(exc_prod))
print('')
print(aso_result_SKU_name_ex)
print('')
print("Expected revenue of the optimal assortment:")
print('{:04.2f}'.format(obj_val)) #obj_val
print("Expected baseline revenue according to GDT model:")
print('{:04.2f}'.format(Rev_Baseline))
print("Increase of revenue vs baseline:")
print('{:04.2%}'.format( (obj_val - Rev_Baseline ) / Rev_Baseline))
print('')
print('Customer behavior (probabilities): ')
print('')
sigma_digest2(Sigma,Lambda, nb_prod)
print('')
print("Assortment optimization completed")   

               
a = Sigma[np.nonzero(Lambda)]
b = Lambda[np.nonzero(Lambda)]  

with open(abs_file_log, 'w') as log:
    log.write("Date: "+ str(datetime.date.today()) + '\n')
    log.write("Network:" + network+ '\n')
    log.write("City:" +  city + '\n')
    log.write("transaction file: " +filename_transaction + " with nb_prod = " + str(nb_prod) +" products" + '\n')
    log.write('min_capacity: ' + str(min_capacity) +'\n')
    log.write('max_capacity: ' + str(max_capacity) +'\n')
    log.write(str(nb_asst) + " assortments available in transaction dataset"+ '\n') 
    log.write("Optimal assortment found in "+ str(t2-t1)+ " seconds. Number of products "+ str(len(aso_result)) + '\n' )
    log.write('\n')
    log.write('Products present in optimal assortment:' + '\n')
    log.write(str(aso_result) + '\n') #x_found
    log.write('\n')
    log.write(str(aso_result_SKU_name) + '\n') 
    log.write('\n')
    log.write("List of common products in assorment with higher real revenue and optimal assortment: " + '\n')
    log.write(str(list(same_prod)) + '\n')
    log.write('\n')
    log.write(str(aso_result_SKU_name_same) + '\n')
    log.write('\n')
    log.write("List of new recommended products: "+ '\n')
    log.write(str(list(diff_prod)) +'\n' )
    log.write('\n')
    log.write(str(aso_result_SKU_name_new) + '\n')
    log.write('\n')
    log.write("List of excluded products: " +  '\n') 
    log.write (str(list(exc_prod)) +  '\n')
    log.write('\n')
    log.write(str(aso_result_SKU_name_ex) + '\n')
    log.write('\n')
    log.write("Expected revenue of the optimal assortment:" + '\n')
    log.write('{:04.2f}'.format(obj_val) + '\n') #obj_val
    log.write("Expected baseline revenue according to GDT model:" + '\n')
    log.write('{:04.2f}'.format(Rev_Baseline) + '\n') 
    log.write("Increase of revenue vs baseline:" + '\n')
    log.write('{:04.2%}'.format( (obj_val - Rev_Baseline ) / Rev_Baseline)  + '\n')
    log.write('\n')
    log.write('Customer behavior (probabilities): '+ '\n')
    log.write('\n')
    
    sigmas_sorted = a[np.argsort(b), :][::-1]
    lambdas_sorted = b[np.argsort(b)][::-1]
    for i in range(len(sigmas_sorted)):
        nb_prod_indiff = (sigmas_sorted[i, :] == nb_prod - 1).sum()
        prod_prefered = np.argsort(sigmas_sorted[i, :])  # including the indiff products
        log.write("Sigma number "+ str(i)+ ", probability associated:"+ str(lambdas_sorted[i])+ ", prefered products in order:")
        # print the list of preferred in order of preference
        
        res = []
        dd = prod_prefered[:nb_prod - nb_prod_indiff]
    
        for el in dd:
            for p, v in enumerate(d):
                for sku in prod_name:
                    if( el == p and p >0 and el > 0 and v[1] == sku[0]):
                        res.append(sku[1]) #v[1]
        log.write(str(res) + '\n')
    log.write('\n')
    log.write("Assortment optimization completed")  
    
print('')    
print("log file has been saved to ", abs_file_log)


