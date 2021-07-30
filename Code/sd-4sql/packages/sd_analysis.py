import numpy as np
import pandas as pd

def get_index_tables(df):
    
    i = 0 
    first_time = False
    for column in list(df.columns) :
        if 'FROM_' in column :
            if not first_time : 
                a = i 
                first_time = True

        else :
            if first_time :
                b = i
                break
        i = i + 1
    return (a,b)


def get_max_tables (requetes_) :
    
    debut = get_index_tables(requetes_)[0]
    fin = get_index_tables(requetes_)[1]

    data = requetes_.sort_values(by=['time'],ascending = False)[:250]
    tables = data.astype(bool).sum(axis=0)[debut:fin].sort_values(ascending = False)[:5].index
    for table in tables :
        print(table, 'with nb lines = ' ,requetes_[requetes_[table] > 0]['nrows'].mean())


def get_maxavg_tables (requetes_) :
    
    debut = get_index_tables(requetes_)[0]
    fin = get_index_tables(requetes_)[1]
    l = []
    for i in range(debut,fin):
        l.append((requetes_.columns[i],requetes_[requetes_.iloc[:,i]>0]['time'].mean()))
    l.sort(key = lambda tup : tup[1])
    for table in l[-5:] :
        print(table[0], 'with nb lines = ' ,requetes_[requetes_[table[0]] > 0]['nrows'].mean())


def analyse (df) :
    
    i_from = 0
    i_join = 0 
    i_proj = 0 
    i_where = 0 
    i_having = 0
    i_group = 0
    i_order = 0

    for column in list(df.columns) :
        if 'FROM_' in column :
            i_from = i_from + 1
        if 'JOIN_' in column :
            i_join = i_join + 1
        if 'SELECT_' in column :
            i_proj = i_proj + 1
        if 'HAVING_' in column :
            i_having = i_having + 1
        if 'ORDERBY_' in column :
            i_order = i_order + 1
        if 'WHERE_' in column :
            i_where = i_where + 1
        if 'GROUPBY_' in column :
            i_group = i_group + 1

    print('From tables:',i_from)
    print('Join tables:',i_join)
    print('Projections :',i_proj)
    print('Where atts:',i_where)
    print('Having by atts:',i_having)
    print('Group by atts:',i_group)
    print('Order by atts:',i_order)


def discretize_duration(duration, threshold) :
    if duration > threshold : return True
    else : return False


def get_analysis(requetes_) :

    print('Number of queries :', requetes_.shape[0])
    print('Number of unique queries :',requetes_['query'].unique().size)
    print('All features :', requetes_.shape[1] -1)
    analyse(requetes_)
    p_zeros = (requetes_.size - requetes_.astype(bool).sum(axis=0).sum()) / requetes_.size * 100
    print('Pourcentage of zeros values is :', p_zeros, '%')



def get_duration (requetes_, threshold, trace) :
    
    requetes_['time_disc'] = requetes_['time'].apply(lambda x : discretize_duration(x,threshold))
    if trace :
        print('Pourcentage of queries above', threshold,'is :',requetes_['time_disc'].value_counts()[1]/requetes_.shape[0]*100)
    return requetes_

def get_df_declination (requetes_jf, declination) :
    
    dict_conds = {'declination' : declination}
    return get_df_conditions(requetes_jf, dict_conds)


def get_df_table (requetes_jf, table) :

    table = 'FROM_' + table
    dict_conds = {'Tables' : {table : 1}}
    return get_df_conditions(requetes_jf, dict_conds)

    return requetes_

def get_df_instance (requetes_jf, instanceCode) : 
    
    dict_conds = {'serverName' : instanceCode}
    return get_df_conditions(requetes_jf, dict_conds)

    return requetes_


def get_df_conditions(requetes_jf, dict_conds) :

    requetes_ = requetes_jf.copy() 
    for key in dict_conds :
        if key in requetes_.columns : # conditions simples
            if isinstance(dict_conds[key], str) : # string columns
                requetes_ = requetes_[requetes_[key] == dict_conds[key]]
                requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]
            
            elif isinstance(dict_conds[key], dict) : # int or float or list of string (and | or) columns 

                if isinstance (list(dict_conds[key].values())[0], list) :
                    op = list(dict_conds[key].keys())[0]
                    value = list(dict_conds[key].values())[0]
                    if op == 'or' :
                        requetes_tmp = requetes_.copy()
                        for i, elt in enumerate (value) : # elt is string
                            if i == 0 :
                                requetes_ = requetes_tmp[requetes_[key] == elt]
                            else :
                                requetes_ = requetes_.append(requetes_tmp[requetes_tmp[key] == elt])
                        
                        requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]
                        del requetes_tmp 
                        
                    else : # op = 'and'
                        for elt in value :
                            requetes_ = requetes_tmp[requetes_[key] == elt]
                            requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]
                        
                else :
                    op = list(dict_conds[key].keys())[0]
                    if op == 'eq' :
                        requetes_ = requetes_[requetes_[key] == list(dict_conds[key].values())[0]]

                    elif op == 'gt' :
                        requetes_ = requetes_[requetes_[key] > list(dict_conds[key].values())[0]] 

                    elif op == 'lt' :
                        requetes_ = requetes_[requetes_[key] < list(dict_conds[key].values())[0]]

                    elif op == 'btw' :
                        requetes_ = requetes_[(requetes_[key] >= list(dict_conds[key].values())[0][0]) & (requetes_[key] <= list(dict_conds[key].values())[0][1])]
                    requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]
                     

        else : # conditions embarquées
            if isinstance(dict_conds[key], dict) :
                column = list(dict_conds[key].keys())[0]
                if isinstance(list(dict_conds[key].values())[0], str) : # Alerts 
                    requetes_ = requetes_[requetes_[column] == list(dict_conds[key].values())[0]]
                    requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]  

                elif isinstance(list(dict_conds[key].values())[0], int) : # tables, projections, atts 
                    requetes_ = requetes_[requetes_[column] >= list(dict_conds[key].values())[0]]
                    requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]

                elif isinstance(list(dict_conds[key].values())[0], dict) : # ASH
                    op = list(list(dict_conds[key].values())[0].keys())[0]
                    value = list(list(dict_conds[key].values())[0].values())[0]

                    if op == 'eq' :
                        requetes_ = requetes_[requetes_[column] == value]

                    elif op == 'gt' :
                        requetes_ = requetes_[requetes_[key] > value]

                    elif op == 'lt' :
                        requetes_ = requetes_[requetes_[key] < value]

                    elif op == 'btw' :
                        requetes_ = requetes_[(requetes_[key] >= value[0]) & (requetes_[key] <= value[1])]
                    requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)]  

                elif isinstance(list(dict_conds[key].values())[0], list) : # List of all :
                    op = column
                    if op  == 'or' :

                        requetes_tmp = requetes_.copy()
                        for i, elt in enumerate (list(dict_conds[key].values())[0]) :
                            column = list(elt.keys())[0]

                            if isinstance(list(elt.values())[0], str) : # Alerts
                                if i == 0 : 
                                    requetes_ = requetes_tmp[requetes_tmp[column] == list(elt.values())[0]]
                                else :
                                    requetes_ = requetes_.append(requetes_tmp[requetes_tmp[column] == list(elt.values())[0]])  

                            elif isinstance(list(elt.values())[0], int) : # tables, projections, atts
                                if i == 0 : 
                                    requetes_ = requetes_tmp[requetes_tmp[column] >= list(elt.values())[0]]
                                else :
                                    requetes_ = requetes_.append(requetes_tmp[requetes_tmp[column] >= list(elt.values())[0]])

                            elif isinstance(list(elt.values())[0], dict) : # ASH
                                op = list(list(elt.values())[0].keys())[0]
                                value = list(list(elt.values())[0].values())[0]

                                if op == 'eq' :
                                    if i == 0 :
                                        requetes_ = requetes_tmp[requetes_tmp[column] == value]
                                    else : 
                                        requetes_ = requetes_.append(requetes_tmp[requetes_tmp[column] == value])

                                elif op == 'gt' :
                                    if i == 0 :
                                        requetes_ = requetes_tmp[requetes_tmp[key] > value]
                                    else :
                                        requetes_ = requetes_.append(requetes_tmp[requetes_tmp[key] > value])

                                elif op == 'lt' :
                                    if i == 0 :
                                        requetes_ = requetes_tmp[requetes_tmp[key] < value]
                                    else :
                                        requetes_ = requetes.append(requetes_tmp[requetes_tmp[key] < value])

                                elif op == 'btw' :
                                    if i == 0 :
                                        requetes_ = requetes_tmp[(requetes_tmp[key] >= value[0]) & (requetes_tmp[key] <= value[1])]
                                    else : 
                                        requetes_ = requetes_.append(requetes_tmp[(requetes_tmp[key] >= value[0]) & (requetes_tmp[key] <= value[1])])
                                
                            requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)] 

                    
                    else : # and
                        for elt in list(dict_conds[key].values())[0] : # elt is dict
                            column = list(elt.keys())[0]
                            if isinstance(list(elt.values())[0], str) : # Alerts 
                                requetes_ = requetes_[requetes_[column] == list(elt.values())[0]]

                            elif isinstance(list(elt.values())[0], int) : # tables, projections, atts 
                                requetes_ = requetes_[requetes_[column] >= list(elt.values())[0]]

                            elif isinstance(list(elt.values())[0], dict) : # ASH
                                op = list(list(elt.values())[0].keys())[0]
                                value = list(list(elt.values())[0].values())[0]

                                if op == 'eq' :
                                    requetes_ = requetes_[requetes_[column] == value]

                                elif op == 'gt' :
                                    requetes_ = requetes_[requetes_[column] > value]

                                elif op == 'lt' :
                                    requetes_ = requetes_[requetes_[column] < value]

                                elif op == 'btw' :
                                    requetes_ = requetes_[(requetes_[column] >= value[0]) & (requetes_[column] <= value[1])]
                                
                            requetes_ = requetes_.loc[:, (requetes_ != 0).any(axis=0)] 
                             
    
    for column in requetes_.columns :
        if requetes_[column].unique().size == 1 : 
            del requetes_[column]
    
    return requetes_  














                

                


















