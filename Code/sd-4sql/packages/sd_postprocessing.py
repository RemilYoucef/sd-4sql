import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from sklearn.metrics import jaccard_score
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.spatial.distance import squareform

def plot_sgbars(result_df, size_results ,ylabel="target share", title="Discovered Subgroups", dynamic_widths=False, _suffix=""):
    
    result_df = result_df[:size_results]
    shares_sg = result_df["target_share_sg"]
    shares_compl = result_df["target_share_complement"]
    sg_relative_sizes = result_df["relative_size_sg"]
    x = np.arange(len(result_df))

    base_width = 0.8
    if dynamic_widths:
        width_sg = 0.02 + base_width * sg_relative_sizes
        width_compl = base_width - width_sg
    else:
        width_sg = base_width / 2
        width_compl = base_width / 2

    fig, ax = plt.subplots()
    rects1 = ax.bar(x, shares_sg, width_sg, align='edge')
    rects2 = ax.bar(x + width_sg, shares_compl, width_compl, align='edge', color='#61b76f')

    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x + base_width / 2)
    ax.set_xticklabels(result_df.subgroup, rotation=90)

    ax.legend((rects1[0], rects2[0]), ('subgroup', 'complement'))
    fig.set_size_inches(len(result_df),5)


def plot_npspace(result_df, size_results, data, annotate=True, fixed_limits=False):

    result_df = result_df[:size_results]
    fig, ax = plt.subplots()
    for i, sg in result_df.iterrows():
        target_share_sg = sg['target_share_sg']
        size_sg = sg['size_sg']
        ax.plot(size_sg, target_share_sg, 'ro', color='b')
        ax.set_ylabel('target share')
        ax.set_xlabel('size of subgroup')
        if annotate:
            ax.annotate(str(i), (size_sg, target_share_sg + 0.005))

def jaccard (sg, sg2, data) : 
    return jaccard_score(sg.covers(data), sg2.covers(data))

def similarity_sgs(results_descriptions, result_size ,data, color=True):
    results_descriptions = results_descriptions[:result_size]
    sgs = [x[1] for x in results_descriptions]
    dists = [[jaccard(sg, sg2, data) for sg2 in sgs] for sg in sgs]
    dist_df = pd.DataFrame(dists)
    if color:
        dist_df = dist_df.style.background_gradient()
    return dist_df

def similarity_dendrogram(result_descriptions, result_size, data, truncated = False, p = None):
    fig, _ = plt.subplots(figsize=(18, 9))
    dist_df = similarity_sgs(result_descriptions, result_size, data, color=False)
    sgNames = [str(x[1]) for x in result_descriptions[:result_size]]
    mat = 1 - dist_df.values
    dists = squareform(mat)
    linkage_matrix = linkage(dists, "single")
    if truncated == True :
        r = dendrogram(linkage_matrix, labels=sgNames, leaf_rotation=90, p = p, truncate_mode='lastp')
        count = 0
        l_count = []
        for sg in r['ivl'] :
            if '(' in sg : # number 
                print(sgNames[count])
                l_count.append(count)
                count = count + int(sg[sg.find('(') + 1 : sg.find(')')])
            else :
                print(sg)
                l_count.append(count)
                count = count + 1
        
        jaccard_threshold = 1- min([j for i in r['dcoord'] for j in i[1:-1]])
        print(jaccard_threshold)
        return l_count
    
    else :
        dendrogram(linkage_matrix, labels=sgNames, leaf_rotation=90)
    



def greedy_jaccard(results_descriptions, result_size ,data, threshold) :
    results_descriptions = results_descriptions[:result_size]
    sgs = [x[1] for x in results_descriptions]
    sgNames = [str(x[1]) for x in results_descriptions[:result_size]]
    dists = pd.DataFrame([[jaccard(sg, sg2, data) for sg2 in sgs] for sg in sgs]).values
    d = {}
    d_names = {}
    l = []
    for i in range(dists.shape[0]) :
        if i not in l :
            d[i] = []
            d_names[sgNames[i]] = []
            for j in range(i+1,dists.shape[0]) :
                if dists[i,j] >= threshold :
                    l.append(j)
                    d[i] = d.get(i,[]) + [j]
                    d_names[sgNames[i]] = d_names.get(sgNames[i],[]) + [sgNames[j]]
    return(d,d_names,sgNames)


def plot_distribution_numeric(sg, data, bins, target):
    fig, _ = plt.subplots(figsize=(4, 3))
    target_values_sg = data[sg.covers(data)][target].values
    target_values_data = data[target].values
    plt.hist(target_values_sg, bins= 100, range = (np.amin(target_values_data),np.amax(target_values_data)) ,alpha=0.5, label="subgroup(1)", density=True)
    plt.hist(target_values_data, bins=100,range = (np.amin(target_values_data),np.amax(target_values_data)) ,alpha=0.5, label="Overall Data", density=True)
    plt.xlim(0, np.amax(target_values_sg))
    plt.xlabel('time')
    #plt.yscale('log')
    plt.ticklabel_format(axis="x", style="sci", scilimits=(0,0))
    plt.legend(loc='upper right')


def plot_distribution_numeric_sgs(sgs, result_size ,data, bins, target):
    fig, _ = plt.subplots(figsize=(4, 2))
    target_values_data = data[target].values
    maxlim = 0
    for i in range(result_size):
        sg = sgs[i][1]
        target_values_sg = data[sg.covers(data)][target].values
        plt.hist(target_values_sg, bins= 20, range = (np.amin(target_values_data),np.amax(target_values_data)),linewidth=1.5,histtype=u'step' ,alpha=0.5, label="subgroup"+str(i+1), density=True)
        maxlim = max(maxlim,np.amax(target_values_sg))
    plt.hist(target_values_data, bins=20,range = (np.amin(target_values_data),np.amax(target_values_data)),linewidth=1.5 ,alpha=0.5, label="Overall Data", density=True)
    plt.xlim(0, maxlim + 42000)
    plt.xlabel('time')
    #plt.yscale('log')
    plt.ticklabel_format(axis="x", style="sci", scilimits=(0,0))
    plt.legend(loc='upper right')


def compare_distributions_numeric(sgd_results, data, bins, target):
    fig, _ = plt.subplots()
    sgs = [x[1] for x in sgd_results]
    for sg in sgs:
        target_values_sg = data[sg.covers(data)][target].values
        plt.hist(target_values_sg, bins, alpha=0.3, label=str(sg), density=True)
    plt.legend(loc='upper right')

def plot_npspace_numeric (result_df, size_results, data, annotate=True, fixed_limits=False):

    result_df = result_df[:size_results]
    print('mean of the dataset :',result_df['mean_dataset'].unique()[0])
    fig, ax = plt.subplots()
    for i, sg in result_df.iterrows():
        mean_sg = sg['mean_sg']
        size_sg = sg['size_sg']
        ax.plot(size_sg, mean_sg, 'ro', color='b')
        ax.set_ylabel('mean_sg')
        ax.set_xlabel('size of subgroup')
        if annotate:
            ax.annotate(str(i), (size_sg, mean_sg + 0.005))