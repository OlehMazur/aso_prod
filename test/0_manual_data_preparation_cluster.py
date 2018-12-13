#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 12:27:17 2018

@author: oleh
"""
import numpy as np
from sklearn.preprocessing import Imputer
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
import lib.data_for_cluster as dfc
result = dfc.get_data_for_cluster()[0].compute()


# make a cluster
X = result.iloc[:, [1,2]] # SalesValue and Assortments
imputer = Imputer(missing_values = 'NaN', strategy = "mean", axis = 0)
imputer = imputer.fit(X)
X = imputer.transform(X)

# Step 1 check with Elbow Method'
wcss =[]
for i in range(1,11):
    kmeans = KMeans(
            n_clusters=i, 
            init = 'k-means++', 
            max_iter = 300, 
            n_init= 10, 
            random_state=0)
    kmeans.fit(X)
    wcss.append(kmeans.inertia_)
plt.plot(range(1,11), wcss)
plt.title('The Elbow Method')
plt.xlabel('Number of clusters')
plt.ylabel('WCSS')
plt.savefig('Elbow Method', dpi = 300)
plt.show()

#Step 2 create clusters with Elbow Method output
kmeans = KMeans(
        n_clusters=4 , # choose number of clusters (we change only this param)
        init = 'k-means++', 
        max_iter = 300,
        n_init = 10,
        random_state=0)

y_kmeans = kmeans.fit_predict(X)                    

colors = ['red','blue', 'green', 'magenta', 'yellow' ]
for ind, el in enumerate(np.unique(y_kmeans )):
    plt.scatter(X[y_kmeans ==el, 0], X[y_kmeans == el, 1], s = 50, c = colors[ind], label = 'Cluster' + str(ind))

plt.scatter(kmeans.cluster_centers_[:,0], kmeans.cluster_centers_[:,1],s = 100, c = 'gray', label = 'Centroids')    
plt.title('Clustering the stores (k-means++)')  
plt.xlabel('Sales Value')   
plt.ylabel('Assortment)')  
plt.legend()
plt.savefig('Clusters', dpi = 300)
plt.show()       
