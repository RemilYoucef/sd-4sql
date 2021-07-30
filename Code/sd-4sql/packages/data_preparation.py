import numpy as np
import pandas as pd

def transform_elements(req):
    
    ind = 0 
    while ind >= 0 :
        
        ind = req.find('elements')
        to_replace_blank = req[ind:ind+30].split(' ',1)[0]
        last_parenthese = to_replace_blank.find(")")
        to_replace = to_replace_blank[ : to_replace_blank.find(")") + 1]
        new_elt = to_replace[to_replace.find("(") + 1:to_replace.find(")")]
        req = req.replace(to_replace,new_elt)
    
    return req 


def swap_orderby_groupby (x) :
    
    groupby_stmnt = x[x.find('group by'):]
    orderby_stmnt = x[x.find('order by'):x.find('group by')]

    tmp = x[:x.find('order by')] + groupby_stmnt + ' ' + orderby_stmnt
    return tmp[:-1]

def handleAlerts () :
    
    dict_template = dict.fromkeys(type_alertes,None)
    liste = []
    for i in range (df_req_inst.shape[0]) :
        
        dict_req = dict(dict_template)
        time = df_req_inst['time'].iloc[i]
        instance = df_req_inst['instanceCode'].iloc[i]
        df_tmp = df_alertes[(df_alertes['instanceCode'] == instance) & (time > df_alertes['dateDebut']) & (time < df_alertes['dateFin'])]
        
        if not df_tmp.empty :
            
            metrics = df_tmp[['metrique','niveauAlerte']].values
            for j in range(0,metrics.shape[0]) :
                dict_req[metrics[j][0]] = metrics[j][1]
                
        liste.append(dict_req)
        del dict_req
        
    df_req_inst['alertes'] = pd.Series(liste)
    df_req_inst.drop('alertes', axis = 1)