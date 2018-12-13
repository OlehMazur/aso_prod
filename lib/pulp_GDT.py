#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 19:20:16 2018

@author: oleh
"""
from pulp import *
import numpy as np
import time
import os


def restricted_master_main_PULP(A, v, verbose = False):
    
    #os.chdir("/media/ramdisk")
    prob = pulp.LpProblem("findingLambda", pulp.LpMinimize)
    
    (nb_col,nb_prod,nb_asst) = A.shape    
    lmbda = {}
     
    # Create variables
    for k in range(nb_col):
        lmbda[k] = pulp.LpVariable('lambda_%s' % k,  cat='Continuous', lowBound= 0)
    if nb_col==0:
        lmbda[0] = pulp.LpVariable('lambda_0',  cat='Continuous', lowBound= 0)

    eps_p = {}
    eps_m = {}
    
           
    for i in range(nb_prod):
        for m in range(nb_asst):
            eps_p[i,m] = pulp.LpVariable('eps_p_%s_%s' % (i,m), lowBound= 0)           
            eps_m[i,m] = pulp.LpVariable('eps_m_%s_%s' % (i,m), lowBound= 0)       
    
     
    prob+=lpSum([eps_p, eps_m ])   
    
    #Create constraints
    var = [lmbda[k] for k in range(nb_col)]
    
 
    if nb_asst <=1000:       
        for i in range(nb_prod):
            for m in range(nb_asst):            
                prob+= lpSum( A[:,i,m] * var) + eps_p[i,m]- eps_m[i,m] - v[i,m] == 0 , 'distance_%s_%s' % (i,m)
        
    else:          
        chunk_nb = int(nb_asst/1000)
        nb_asst_chunks = np.array_split(range(nb_asst), chunk_nb)
        #print(nb_asst_chunks)
        for chunk in nb_asst_chunks:
            for i in range(nb_prod):
                for m in chunk:            
                    prob+= lpSum( A[:,i,m] * var) + eps_p[i,m]- eps_m[i,m] - v[i,m] == 0 , 'distance_%s_%s' % (i,m)
    
          
    if nb_col > 0:
        prob+= lpSum( var) == 1 , 'sum_to_%s' %1
    
    #print("the model is ready to be solved...")
    
    if verbose:
        prob.writeLP("findingLambda.lp")
        
    #prob.solve(pulp.COIN_CMD(keepFiles=1,fracGap=0.50, msg=1)) #maxSeconds=60,threads=2 path = "/media/ramdisk"
    prob.solve()
    #(pulp.PULP_CBC_CMD(msg=1,keepFiles=1,threads =2, dual = 1)) #PULP_CBC_CMD
    #print(value(prob.objective))
    #print("Status:", LpStatus[prob.status])
    
    #print('done !')
    
    
    #definition of the return variables with expected shape
    return_lmbda_pulp = np.zeros(max(nb_col,1))
    alpha_pulp = np.zeros((nb_prod, nb_asst))
    nu_pulp = np.zeros(1)
        
    # Extraction of the solutions
    obj_value = value(prob.objective)
        
      
    if nb_col > 0:
        for k in range(nb_col):
            return_lmbda_pulp[k] = lmbda[k].varValue 
    else:
        return_lmbda_pulp[0] = 0
        
    #Extraction of the dual values at optimality
    
    constraints_pulp = prob.constraints
     #the constraint sum_to_1 is the last one recorded; we take its dual value
    nu_pulp[0] = constraints_pulp[list(constraints_pulp)[-1]].pi    
        
    #We take the dual value of the constraints distance_i_m
    constr_list_pulp = list(constraints_pulp)
    for i in range(nb_prod):
        for m in range(nb_asst):
            alpha_pulp[i,m] = constraints_pulp[constr_list_pulp[i*nb_asst+m]].pi
    
  
    return([repair_lambda(return_lmbda_pulp), alpha_pulp, nu_pulp, obj_value])      


def repair_lambda(lambda_found):
    lambda_found[lambda_found<0]=0
    return lambda_found/lambda_found.sum()