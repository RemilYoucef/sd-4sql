import sys
import os
import numpy as np
import pandas as pd
from bokeh.models import CustomJS, ColumnDataSource, Row, Select, Column, RangeSlider, DataTable, TableColumn, DateFormatter, RadioButtonGroup, RadioGroup, TextInput, Dropdown
from bokeh.plotting import figure, show, curdoc
from bokeh.models.widgets import Button
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.getcwd())),'sd-4sql\\packages'))

from sd_analysis import *
from subgroup_discovery import *

# LOAD THE DATASET
saved_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))),'Data\\saved-data\\')

url = saved_path + sys.argv[1]
requetes = pd.read_csv(url, index_col=[0])


requetes = requetes[requetes['declination'].notna()]

# FILTERS AND DISPLAYING 
x = list(requetes['nrows'])
y = list(requetes['time'])
instance = list(requetes['serverName'])
declination = list(requetes['declination'])
day = list(requetes['day'])
hour = list(requetes['hour'])
length = list(requetes['length'])

# LEFT FIGURE
s1 = ColumnDataSource(data=dict(x=x, y=y, instance = instance, declination = declination, day = day, hour = hour, length = length))
p1 = figure(plot_width=800, plot_height=400, tools="lasso_select, box_select, pan, zoom_in, zoom_out, reset", 
            title="Queries",
            tooltips=[("instance :", "@instance"), ("declination :", "@declination"), ("day :", "@day")])
p1.circle('x', 'y', source=s1, alpha=0.6)

# SELECTED POINTS
s2 = ColumnDataSource(data=dict(x=[], y=[], instance = [], declination = [], day = [], hour = [], length = []))
p2 = figure(plot_width=800, plot_height=400,tools="pan", title="Selected queries",
            tooltips=[("instance :", "@instance"), ("declination :", "@declination")])
p2.circle('x', 'y', source=s2, alpha=0.6)


# THE RESULTS
result_binary = ColumnDataSource(data=dict(subgroup = [], size_sg = [], positives_sg = [], size_dataset = [], positives_dataset = [], ratio = []))
result_numeric = ColumnDataSource(data=dict(subgroup = [], size_sg = [], mean_sg = [], median_sg = [], min_sg = [], max_sg = [], size_dataset = [], mean_dataset = [], median_dataset = []))


# SELECTION PROCESS
s1.selected.js_on_change('indices', CustomJS(args = dict(s1 = s1, s2 = s2), code = """
        
        var inds = cb_obj.indices;
        var d1 = s1.data;
        var d2 = s2.data;
        d2['x'] = []
        d2['y'] = []
        d2['instance'] = []
        d2['declination'] = []
        d2['day'] = []
        d2['hour'] = []
        d2['length'] = []
        for (var i = 0; i < inds.length; i++) {
            d2['x'].push(d1['x'][inds[i]])
            d2['y'].push(d1['y'][inds[i]])
            d2['instance'].push(d1['instance'][inds[i]])
            d2['declination'].push(d1['declination'][inds[i]])
            d2['day'].push(d1['day'][inds[i]])
            d2['hour'].push(d1['hour'][inds[i]])
            d2['length'].push(d1['length'][inds[i]])
        }
        s2.change.emit();
        s2.data = d2  """))

# FILTERS
declination_list = ['All'] + list(requetes['declination'].unique())
instance_list = ['All'] + list(requetes['serverName'].unique())
days_list = ['All'] + list(requetes['day'].unique())

controls = {
    "declination": Select(title="Declination", value="All", options=declination_list),
    "instance": Select(title="Server", value="All", options=instance_list),
    "day" : Select(title="Day", value="All", options=days_list),
    "hour" : RangeSlider(title = 'Hour', start = 0, end = 23, value = (0,23), step = 1),
    "length" : RangeSlider(title = 'Length', start = 50, end = 120000, value = (50,120000), step = 100)
}


controls_array = controls.values()
callback = CustomJS(args=dict(source=s1, controls=controls), code="""
    
    if (!window.full_data_save) {
        window.full_data_save = JSON.parse(JSON.stringify(source.data));
    }
    var full_data = window.full_data_save;
    var full_data_length = full_data.x.length;
    var new_data = { x: [], y: [], instance: [], declination : [], day : [], hour : [], length : []}
    for (var i = 0; i < full_data_length; i++) {
        if (full_data.declination[i] === null )
            continue;

        if (
            (full_data.length[i] >= controls.length.value[0] && full_data.length[i] <= controls.length.value[1]) &&
            (full_data.hour[i] >= controls.hour.value[0] && full_data.hour[i] <= controls.hour.value[1]) &&
            (controls.declination.value === 'All' || full_data.declination[i] === controls.declination.value) && 
            (controls.instance.value === 'All' || full_data.instance[i] === controls.instance.value) &&
            (controls.day.value === 'All' || full_data.day[i] === controls.day.value) 
        ) {
            Object.keys(new_data).forEach(key => new_data[key].push(full_data[key][i]));
        }
    }
    source.data = new_data;
    source.change.emit();
""")

for single_control in controls_array:
    single_control.js_on_change('value', callback)    

inputs_column = Row (*controls_array)

radio_button_labels = ["Selected data for SD", "Positive data for SD", "Positive and filtred data for SD"]
radio_button_group = RadioButtonGroup(labels = radio_button_labels, active=0)
radio_button_group.on_change('active', lambda attr, old, new : update_task())

def update_task () :
    if radio_button_group.active == 1 or radio_button_group.active == 2:
        
        target_select.disabled = True
        measureb_select.disabled = False 
        measuren_select.disabled = True
        text_input_threshold.disabled = True
    
    else :
        target_select.disabled = False 
        if target_select.value == 'Binary' : 
            measureb_select.disabled = False
            text_input_threshold.disabled = False
        else :
            measuren_select.disabled = False

    



target_select = Select(title="Target:", value="Numerical", options=["Binary", "Numerical"])
measureb_select = Select(title="Binary Measure:", value="Lift", options=["Support","Lift", "WRAcc","Binomial"])
measuren_select = Select(title="Numerical Measure:", value="mean", options=["mean", "median","t-score"])
algorithm_select = Select(title="Algorithm:", value="Beam Search", options=["DFS", "BFS","Beam Search"])
target_select.on_change('value', lambda attr, old, new : update_target())

def update_target () :
    if target_select.value == 'Binary' :
        measureb_select.disabled = False 
        measuren_select.disabled = True
        text_input_threshold.disabled = False

    else : 
        measureb_select.disabled = True 
        measuren_select.disabled = False
        text_input_threshold.disabled = True
    update_data_table ()  

text_input_threshold = TextInput(value="10", title="Threshold (seconds):", width = 200)
text_input_depth = TextInput(value="2", title="Depth :", width = 200)
text_input_patterns = TextInput(value="10", title="#Patterns :", width = 200)

columns_df_binary = [TableColumn(field="subgroup", title="Subgroup"),
                    TableColumn(field="size_sg", title="Size of subgroup"),
                    TableColumn(field="positives_sg", title="Positives of subgroup"),   
                    TableColumn(field="size_dataset", title="Size of dataset"), 
                    TableColumn(field="positives_dataset", title="Positives of dataset"),
                    TableColumn(field="ratio", title="Ratio of positives")    
            ]

columns_df_numeric = [TableColumn(field="subgroup", title="Subgroup"),
                    TableColumn(field="size_sg", title="Size of subgroup"),
                    TableColumn(field="mean_sg", title="Mean of subgroup"),
                    TableColumn(field="median_sg", title="Median of subgroup"), 
                    TableColumn(field="min_sg", title="Min of subgroup"),
                    TableColumn(field="max_sg", title="Max of subgroup"),
                    TableColumn(field="size_dataset", title="Size of dataset"), 
                    TableColumn(field="mean_dataset", title="Mean of dataset"),   
                    TableColumn(field="median_dataset", title="Median of dataset")  
            ]

data_table = DataTable(source = result_numeric, columns = columns_df_numeric, width=1550)
print(result_numeric.data)

def update_data_table () :
    
    if target_select.value == 'Binary' : 
        data_table.columns = columns_df_binary
        data_table.source.data = dict(subgroup = result_binary.data['subgroup'],
                                    size_sg = result_binary.data['size_sg'],
                                    positives_sg = result_binary.data['positives_sg'],
                                    size_dataset = result_binary.data['size_dataset'],
                                    positives_dataset = result_binary.data['positives_dataset'],
                                    ratio = result_binary.data['ratio'])

    else :
        data_table.columns = columns_df_numeric
        data_table.source = result_numeric


def selected_sd():

    dict_conds = {}

    if controls["declination"].value != 'All' :
        dict_conds['declination'] = controls["declination"].value
    
    if controls["instance"].value != 'All' : 
        dict_conds['serverName'] = controls["instance"].value
    
    if controls["day"].value != 'All' :
        dict_conds['day'] = controls["day"].value 
    
    dict_conds['hour'] = {'btw' : (controls["hour"].value[0], controls["hour"].value[1])}
    dict_conds['length'] = {'btw' : (controls["length"].value[0], controls["length"].value[1])}
    
    df_filtred = get_df_conditions(requetes, dict_conds)
    df_filtred = df_filtred.reset_index(drop = True)
    df_sd = df_filtred[df_filtred.index.isin (s1.selected.indices)]
    df_sd = df_sd.loc[:, (df_sd != 0).any(axis=0)]
    
    return df_sd
        
def selected_positive() :
    
    requetes['class'] = 0
    dict_conds = {}

    if controls["declination"].value != 'All' :
        dict_conds['declination'] = controls["declination"].value
    
    if controls["instance"].value != 'All' : 
        dict_conds['serverName'] = controls["instance"].value
    
    if controls["day"].value != 'All' :
        dict_conds['day'] = controls["day"].value 
    
    dict_conds['hour'] = {'btw' : (controls["hour"].value[0], controls["hour"].value[1])}
    dict_conds['length'] = {'btw' : (controls["length"].value[0], controls["length"].value[1])}
    
    df_filtred = get_df_conditions(requetes, dict_conds)
    df_filtred = df_filtred.reset_index()

    
    for i in s1.selected.indices : 
        requetes.loc[df_filtred.loc[i,'index'],'class'] = 1
    
    return requetes


def selected_sd_positive():
    
    dict_conds = {}

    if controls["declination"].value != 'All' :
        dict_conds['declination'] = controls["declination"].value
    
    if controls["instance"].value != 'All' : 
        dict_conds['serverName'] = controls["instance"].value
    
    if controls["day"].value != 'All' :
        dict_conds['day'] = controls["day"].value 
    
    dict_conds['hour'] = {'btw' : (controls["hour"].value[0], controls["hour"].value[1])}
    dict_conds['length'] = {'btw' : (controls["length"].value[0], controls["length"].value[1])}
    
    df_sd = get_df_conditions(requetes, dict_conds)
    df_sd = df_sd.reset_index(drop = True)
    df_sd['class'] = 0
    
    for i in s1.selected.indices : 
        df_sd.loc[i,'class'] = 1
    
    print(df_sd[df_sd['class'] == 0])
    print('--------------------------------------')
    print(df_sd[df_sd['class'] == 1])
    return df_sd




def launch_sd () :

    update_data_table()
    #result_binary.data = dict (subgroup=[], size_sg=[], positives_sg=[],size_dataset=[], positives_dataset =[], ratio =[])
    #result_numeric.data = dict (subgroup = [], size_sg = [], mean_sg = [], median_sg = [], min_sg = [], max_sg = [], size_dataset = [], mean_dataset = [], median_dataset = [], ratio = [])
    depth = int(text_input_depth.value)
    if radio_button_group.active == 0 : # Selected data for SD (Binary or numeric)
        reqs_sd = selected_sd()
        dict_conds = {}
        
        if target_select.value == 'Binary' : # Binary : 
            #perform binary sd on reqs_sd
            threshold =  int(text_input_threshold.value) # getting the threshold
            res = sd_binary_conds (reqs_sd,
                    dict_conds = dict_conds,
                    _target = 'time_disc',
                    mesure  = measureb_select.value,
                    _depth  = depth,
                    threshold = threshold * 1000,
                    result_size = 10,
                    algorithm   = algorithm_select.value)
            res = res.to_dataframe()
            res['ratio'] = res['positives_sg'] / res['size_sg'] * 100
            result_binary.data = dict (subgroup = list(res['subgroup']),
                    size_sg = list(res['size_sg']),
                    positives_sg = list(res['positives_sg']),
                    size_dataset = list(res['size_dataset']),
                    positives_dataset = list(res['positives_dataset']),
                    ratio = list(res['ratio']))

            update_data_table()

        else : # numerical 
            #perform numerical sd on reqs_sd
            res = sd_numerical_conds (reqs_sd,
                                    dict_conds = dict_conds,
                                    _target = 'time',
                                    mesure = measuren_select.value,
                                    coef_sg_size = 0.5,
                                    _depth  = depth,
                                    result_size = 10,
                                    algorithm   = algorithm_select.value)
            res = res.to_dataframe()    
            
            result_numeric.data = dict (subgroup = list(res['subgroup']),
                    size_sg = list(res['size_sg']),
                    mean_sg = list(res['mean_sg']),
                    median_sg = list(res['median_sg']),
                    min_sg = list(res['min_sg']),
                    max_sg = list(res['max_sg']),
                    size_dataset = list(res['size_dataset']),
                    mean_dataset = list(res['mean_dataset']),
                    median_dataset = list(res['median_dataset']))   
         

    elif radio_button_group.active == 1 : # Positive data for SD
        
        measureb_select.disabled = False 
        measuren_select.disabled = True
        text_input_threshold.disabled = True

        requetes = selected_positive()
        print(requetes[requetes['class'] == 1])

        if 'time_disc' in requetes.columns :
            del requetes['time_disc']

        dict_conds = {}
        threshold =  int(text_input_threshold.value)
        res = sd_binary_conds (requetes,
            dict_conds = dict_conds,
            _target = 'class',
            mesure  = measureb_select.value,
            _depth  = depth,
            threshold = threshold * 1000,
            result_size = 100,
            algorithm   = algorithm_select.value,
            _beam_width = 100)
        res = res.to_dataframe()
        res['ratio'] = res['positives_sg'] / res['size_sg'] * 100
        result_binary.data = dict (subgroup = list(res['subgroup']),
                    size_sg = list(res['size_sg']),
                    positives_sg = list(res['positives_sg']),
                    size_dataset = list(res['size_dataset']),
                    positives_dataset = list(res['positives_dataset']),
                    ratio = list(res['ratio']))
        update_data_table()
        del requetes['class']                        


    else : # Positive and filtred data for SD
        
        measureb_select.disabled = False 
        measuren_select.disabled = True
        text_input_threshold.disabled = False

        df_sd = selected_sd_positive()
        if 'time_disc' in df_sd.columns :
            del df_sd['time_disc']

        dict_conds = {}
        threshold =  int(text_input_threshold.value)
        res = sd_binary_conds (df_sd,
                dict_conds = dict_conds,
                _target = 'class',
                mesure  = measureb_select.value,
                _depth  = depth,
                threshold = threshold * 1000,
                result_size = 100,
                algorithm   = algorithm_select.value,
                _beam_width = 100)
        res = res.to_dataframe()

        res['ratio'] = res['positives_sg'] / res['size_sg'] * 100
        result_binary.data = dict (subgroup = list(res['subgroup']),
                            size_sg = list(res['size_sg']),
                            positives_sg = list(res['positives_sg']),
                            size_dataset = list(res['size_dataset']),
                            positives_dataset = list(res['positives_dataset']),
                            ratio = list(res['ratio']))
        update_data_table()         



sd_selectors = Row(target_select, measureb_select, measuren_select, algorithm_select)
button = Button(label = "Launch SD Task", button_type="primary")
button.on_click(launch_sd)

text_inputs = Row(text_input_threshold, text_input_depth,text_input_patterns)
#curdoc().theme = 'contrast'
curdoc().add_root(Column(inputs_column, Row(p1, p2), radio_button_group,sd_selectors, text_inputs, button, data_table))

