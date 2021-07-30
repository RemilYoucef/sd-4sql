import numpy as np
import pandas as pd
from sd_analysis import *
import pysubgroup as ps
import time
import timeit

def sd_binary_declination (requetes_jf, declination, _target, mesure, _depth, threshold, result_size, algorithm, _beam_width = None, features_ignore = [], trace = False) :

    requetes_ = get_df_declination (requetes_jf, declination)
    if trace :  
        get_analysis(requetes_) 
    get_duration (requetes_, threshold, trace) 
    
    target = ps.BinaryTarget (_target, True)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])

    if mesure == 'Support' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimplePositivesQF())

    elif mesure == 'Lift' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.LiftQF())

    elif mesure == 'WRAcc' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.WRAccQF())
    
    elif mesure == 'Chi2' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.ChiSquaredQF(direction='positive'))

    elif mesure == 'Binomial' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimpleBinomialQF())
    
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)

    return result


def sd_numerical_declination(requetes_jf, declination, _target, mesure ,coef_sg_size ,_depth, result_size, algorithm, _beam_width= None, features_ignore = [], trace = False) :
    
    requetes_ = get_df_declination (requetes_jf, declination) 
    if trace :
        get_analysis(requetes_)
    
    if 'time_disc' in list(requetes_.columns) : 
        del requetes_['time_disc']

    # pysubgroup 

    target = ps.NumericTarget (_target)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])

    if mesure == 'mean' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumeric(coef_sg_size))
    
    elif mesure == 'median' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericMedian(coef_sg_size))

    elif mesure == 't-score' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericTscore(coef_sg_size))                
    
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)
    
    return result



def sd_binary_table (requetes_jf, table, _target, mesure, _depth, threshold, result_size, algorithm, _beam_width = None, features_ignore = [], trace = False) :

    requetes_ = get_df_table (requetes_jf, table)
    if trace : 
        get_analysis(requetes_) 
    get_duration (requetes_, threshold, trace) 
    
    target = ps.BinaryTarget (_target, True)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])
    
    if mesure == 'Support' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimplePositivesQF())

    elif mesure == 'Lift' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.LiftQF())

    elif mesure == 'WRAcc' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.WRAccQF())
    
    elif mesure == 'Chi2' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.ChiSquaredQF(direction='positive'))

    elif mesure == 'Binomial' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimpleBinomialQF())
    
    start = timeit.default_timer()
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)
    if trace :
        print('Time: ', timeit.default_timer() - start)  

    return result


def sd_numerical_table (requetes_jf, table, _target, mesure ,coef_sg_size ,_depth, result_size, algorithm, _beam_width= None, features_ignore = [], trace= False) :
    
    requetes_ = get_df_table (requetes_jf, table)
    if trace :  
        get_analysis(requetes_) # different features of the new dataframe
    if 'time_disc' in list(requetes_.columns) : 
        del requetes_['time_disc']

    target = ps.NumericTarget (_target)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])
    if mesure == 'mean' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumeric(coef_sg_size))
    
    elif mesure == 'median' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericMedian(coef_sg_size))

    elif mesure == 't-score' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericTscore(coef_sg_size))   
    
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)
    
    return result    

def sd_binary_instance (requetes_jf, instance, _target, mesure, _depth, threshold, result_size, algorithm, _beam_width = None, features_ignore = [], trace = False) :

    requetes_ = get_df_instance (requetes_jf, instance)
    if trace :  
        get_analysis(requetes_) # different features of the new dataframe
    get_duration (requetes_, threshold, trace) # pourcentage of queries above a threshold
    
    target = ps.BinaryTarget (_target, True)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])
    
    if mesure == 'Support' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimplePositivesQF())

    elif mesure == 'Lift' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.LiftQF())

    elif mesure == 'WRAcc' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.WRAccQF())
    
    elif mesure == 'Chi2' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.ChiSquaredQF(direction='positive'))

    elif mesure == 'Binomial' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimpleBinomialQF())
    
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)

    return result


def sd_numerical_instance (requetes_jf, instance, _target,mesure ,coef_sg_size ,_depth, result_size, algorithm, _beam_width= None, features_ignore = [], trace = False) :
    
    requetes_ = get_df_instance (requetes_jf, instance)
    if trace : 
        get_analysis(requetes_) 
    if 'time_disc' in list(requetes_.columns) : 
        del requetes_['time_disc']
    
    target = ps.NumericTarget (_target)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])
    if mesure == 'mean':
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumeric(coef_sg_size))
    
    elif mesure == 'median' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericMedian(coef_sg_size))

    elif mesure == 't-score' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericTscore(coef_sg_size))   
    
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)
    
    return result


def sd_binary_conds (requetes_jf, dict_conds, _target, mesure, _depth, threshold, result_size, algorithm, _beam_width = 100, features_ignore = [], trace = False) :

    requetes_ = get_df_conditions(requetes_jf, dict_conds) 
    if trace :
        get_analysis(requetes_) # different features of the new dataframe
    
    get_duration (requetes_, threshold, trace) # pourcentage of queries above a threshold
    
    target = ps.BinaryTarget (_target, True)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])
    
    if mesure == 'Support' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimplePositivesQF())

    elif mesure == 'Lift' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.LiftQF())

    elif mesure == 'WRAcc' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.WRAccQF())
    
    elif mesure == 'Chi2' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.ChiSquaredQF(direction='positive'))

    elif mesure == 'Binomial' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth,
            qf = ps.SimpleBinomialQF())
    
    start = timeit.default_timer()

    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)
    
    if trace :
        print('Time: ', timeit.default_timer() - start)  

    return result

def sd_numerical_conds (requetes_jf, dict_conds, _target, mesure ,coef_sg_size ,_depth, result_size, algorithm, _beam_width= 100, features_ignore = [], trace = False) :
    
    requetes_ = get_df_conditions(requetes_jf, dict_conds) 
    #get_analysis(requetes_) # different features of the new dataframe

    if 'time_disc' in list(requetes_.columns) : 
        del requetes_['time_disc']
    target = ps.NumericTarget (_target)
    searchspace = ps.create_selectors(requetes_, ignore = features_ignore + [_target,'query'])
    if mesure == 'mean':
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumeric(coef_sg_size))
    
    elif mesure == 'median' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericMedian(coef_sg_size))

    elif mesure == 't-score' :
        task = ps.SubgroupDiscoveryTask (
            requetes_, 
            target, 
            searchspace, 
            result_set_size = result_size, 
            depth = _depth, 
            qf = ps.StandardQFNumericTscore(coef_sg_size))   
    
    start = timeit.default_timer()
    if algorithm == 'Beam Search' :
        result = ps.BeamSearch(beam_width = _beam_width).execute(task)
    
    elif algorithm == 'DFS' :
        result = ps.SimpleDFS().execute(task)
    
    if trace :
        print('Time: ', timeit.default_timer() - start)  

    return result 


