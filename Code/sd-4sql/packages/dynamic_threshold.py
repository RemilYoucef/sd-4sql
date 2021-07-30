import numpy as np
import pandas as pd
from SDDeclinations import *
from scipy.stats import expon, gamma
import math

def get_threshold_duration (requetes, pvalue) :
    
    requetes['durationMSDecales'] = requetes['durationMS'] - 5000
    expo = expon(scale=requetes['durationMSDecales'].mean(),loc=0)
    for i in np.arange(0,100000,100):
        if expo.cdf(i) < pvalue and expo.cdf(i+100) > pvalue :
            break
    print(i + 100 + 5000)


def get_dynamic_target_duration (requetes, pvalue) :
    
    requetes['durationMSDecales'] = requetes['durationMS'] - 5000
    expo = expon(scale=requetes['durationMSDecales'].mean(),loc=0)
    requetes['pvalue_duration'] = requetes['durationMSDecales'].apply(lambda x : expo.cdf(x))
    requetes['class_duration'] = requetes ['pvalue_duration'].apply(lambda x : discretize_duration(x,pvalue))


def get_dynamic_target_class(requetes, pvalue) :
    
    # pvalues duration MS
    requetes['durationMSDecales'] = requetes['durationMS'] - 5000
    expo = expon(scale=requetes['durationMSDecales'].mean(),loc=0)
    requetes['pvalue_duration'] = requetes['durationMSDecales'].apply(lambda x : expo.cdf(x))
    
    # pvalues nbLignes
    esp_nbLignes = requetes[cond2]['durationMS'].mean() # esp = k * theta
    var_nbLignes = requetes[cond2]['durationMS'].var() # var = k * (theta)**2
    theta_nbLignes = var_nbLignes / esp_nbLignes
    k_nbLignes = esp_nbLignes / theta_nbLignes
    gamma_nbLignes = gamma(a = k_nbLignes*2, scale=theta_nbLignes,loc=0)
    requetes['pvalue_nbLignes'] = requetes['nbLignes'].apply(lambda x : 1 - gamma_nbLignes.cdf(x))
    
    # product pvalues 
    requetes['product_pvalue'] = requetes['pvalue_duration'] * requetes['pvalue_nbLignes']
    
    #pvalues of product of pvalues
    esp_pvalues = requetes['product_pvalue'].mean() # esp = k * theta
    var_pvalues = requetes['product_pvalue'].var() # var = k * (theta)**2
    theta_pvalues = var_pvalues / esp_pvalues
    k_pvalues = esp_pvalues / theta_pvalues
    gamma_product_pvalues = gamma(a = k_pvalues, scale = theta_pvalues, loc = 0)
    requetes['pvalue_pvalues'] = requetes['product_pvalue'].apply(lambda x : gamma_product_pvalues.cdf(x))
    requetes['class'] = requetes['pvalue_pvalues'].apply(lambda x : discretize_duration(x,pvalue))