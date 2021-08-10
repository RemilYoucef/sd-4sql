# SD-4SQL: What makes my queries slow?: Subgroup Discovery for SQL Workload Analysis

OVERVIEW

In this [work](https://www.researchgate.net/publication/353776691_What_makes_my_queries_slow_Subgroup_Discovery_for_SQL_Workload_Analysis) we adressed SQL workload analysis problem to pinpoint schema issues and improve performances. We seek to automatically identify subsets of queries that share some properties only i.e a pattern (e.g., sql clauses and/or environment features) and foster at the same time some target measures, such as execution time or concurrency issues. To this aim we design a generic-framework rooted on a data mining approach known as Subgroup Discovery. This work has been published in the 36th IEEE/ACM International Conferenceon Automated Software Engineering (ASE)(Core 2021 A*). For further details, please refer to [our paper](https://www.researchgate.net/publication/353776691_What_makes_my_queries_slow_Subgroup_Discovery_for_SQL_Workload_Analysis).

In this framework we :
- propose a data preprocessing step to _parse_ queries but also augment them with relevant features.
- provide heuristic and exact algorithms to identify relevant subgroups of interest w.r.t a target problem (e.g higher execution time) with a diverse set of interestingness measures.
- integrate a visual tool to enable the user to interact iteratively with the framework.


Our experimental study was conducted on an SQL workload containing _Hibernate_ queries run executed on our clients' servers at [INFOLOGIC](https://www.infologic-copilote.fr/) company. 

![overview](Docs/Images/overviewSchemaNew.png)

## HOW TO USE IT ?
### 1. Query parser 
