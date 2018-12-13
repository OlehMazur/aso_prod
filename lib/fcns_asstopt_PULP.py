#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
from pulp import *
import pickle
import time
import os


def run_asstopt_GDT(Lambda, Sigma, Revenue, min_capacity, max_capacity):
    # definition of the parameters according to the data
    (nb_col, nb_prod) = Sigma.shape
    prob=LpProblem("MIO",LpMaximize)
    
    real_products = range(0, nb_prod)
    
    columns = [] 
    for k in range(nb_col):
        columns.append(k)
        
    products = [] 
    for i in range(nb_prod):
        products.append(i)
           
    x={}
    y = {}
    for i in real_products:
        x[i] = pulp.LpVariable('x_%s' % i,  cat='Binary')
        
    '''  
    for k in range(nb_col):
        for i in range(nb_prod):
           y[k, i]  =  pulp.LpVariable('y_%s_%s' % (k,i) , cat= 'Continuous')
    '''
    for k in range(nb_col):
        for i in range(nb_prod):
           y[k, i] =  Lambda[k]*Revenue[i]
           
    dicision_var_matrix = []
    for k in range(nb_col):
        for i in range(nb_prod):
           dicision_var_matrix.append((k,i)) 
    
    des_var = LpVariable.dicts(name = "Dic", indexs= dicision_var_matrix, lowBound=0, upBound=1, cat = pulp.LpInteger)
    
    obj_fun = []
    for k in range(nb_col):
        for i in range(nb_prod):
            if (k,i) in des_var.keys():
                obj_fun.append( [  des_var[(k,i)] * y[k,i] ] )
                
    prob+= lpSum(  [ des_var[(k,i)] * y[k,i] for k in range(nb_col) for i in range(nb_prod) ] )    
    
    #des_var[(k,i)] 
    
    for k in range(nb_col):
        for i in real_products:
            for j in range(0,i):
                if Sigma[k,j]==Sigma[k,i]:
                    prob+= des_var[(k,i)] - des_var[(k,j)] <= 2 - x[i] - x[j]
                    prob+= -des_var[(k,i)] + des_var[(k,j)] <= 2 - x[i] - x[j]
                     
    '''
    for k in range(nb_col):
            prob+= lpSum( des_var[(k,i)] for i in range(nb_prod)) == 1
    '''
    
    for k in range(nb_col):
        for i in real_products:
            prob+= des_var[(k,i)]<= x[i]    
    
    
    for k in range(nb_col):
        for i in real_products:
            new_constraint = pulp.LpAffineExpression()
            #LpAffineExpression()
            new_constraint = 0
            for j in range(nb_prod):
                if Sigma[k, j] > Sigma[k, i]:
                    new_constraint += des_var[(k,i)]
            prob+= new_constraint <= 1 - x[i]       
    
    for k in range(nb_col):
            new_constraint = pulp.LpAffineExpression()
            new_constraint = 0
            for j in real_products:
                if Sigma[k,j]>Sigma[k,0]:
                    new_constraint += des_var[(k,i)]
            prob+= new_constraint == 0
    
    
    prob+=pulp.lpSum(x[i] for i in range(nb_prod)) <= max_capacity + 1
    prob+=pulp.lpSum(x[i] for i in range(nb_prod)) >= min_capacity + 1  
    
    #prob.writeLP("SO_pulp.lp")
    
    prob.solve()

    #print(value(prob.objective))
    #print("Status:", LpStatus[prob.status])
    
    ##pulp.GLPK_CMD()
    ##COIN_CMD()
    x_found = []
    obj_value = 0
    
    if (prob.status == 1): #Optimal
        for v in prob.variables():
            if (v.varValue >0 and "x_" in str(v.name)):
                x_found.append(int(v.name.split("_")[1]))
        obj_value = value(prob.objective)
              
    return[ x_found , obj_value]
