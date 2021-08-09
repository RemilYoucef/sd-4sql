# SD-4SQL: What makes my queries slow?<br/> Subgroup Discovery for SQL Workload Analysis

[This paper](https://hal.archives-ouvertes.fr/hal-03318172/document)

- the current repository contains both the implemented code source and the data used in the experiments.

 └── sd-4sql
     ├── Data
     │    └── original-data
     │    └── saved-data
     │
     └── Code


Data : 
- original-data repository contains the orignal data gathered from our ERP (env-features), databases (sql queries, oracle-ash) and monitoring system (alerts)
- once queries are parsed and joined (extended) with all the remaining variables, they are stored in the saved-data repository (queries-all)
- we provide also the data used in the experiments (RQ1-D1 : dataset-d1 | RQ1-D2 : dataset-d2 | RQ2-D3 : dataset-d3 | RQ3-D4 : dataset-d4)

Code :  

 └── Code
      ├── extended-pysubgroup
      ├── extened-mozilla-parser
      └── sd-4sql
           └── packages
           └── pre-processing
           └── notebooks
           └── interactive-sd
           └── supplementary

- the extended-pysubgroup repository contains the forked repository of the original library pysubgroup with additional metrics (support for binary target & median and t-score for numerical target)
- the extended-mozilla-parser contains the forked repository of the original library moz-sql-parser with improvements (as discussed in the paper).
- the repository sd-4sql contains :
	-- the packages folder 
	-- pre-processing : contains two notebooks to perform data preperation and data preprocessing 
	-- notebook : contains the experiment notebooks (for each use case)
	-- interactive-sd : contains the bokeh app to run the interactive tool 

NOTES : (1) to launch the interactive tool execute the following command : bokeh serve --show interactive.py --session-token-expiration 900000 
	(2) to install the extended pysubgroup package or the customized parser : pip install -e <path> (e.g pip install -e ../extended-pysubgroup/pysubgroup-hack)
