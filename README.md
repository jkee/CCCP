# CCCP
UNFINISHED



Cluster: Bunch of servers
Cluster Dataset: Data distributed on cluster with same index
Dataset Index: Index on which Cluster Dataset data is sharded and distributed
Region: one piece of data in dataset for specific index range. Have an unique ID
Table: tables on cluster with one scheme

Cluster can contain multiple datasets. And one dataset can include multiple tables with same distribution index.



# On one node

Dataset is database; Table is table prefix, region ID is table suffix

Example:
We have 'metrika' dataset with tables 'hits' and 'visits'.
And we have one region (ID 42) for our dataset.

Then, we have tables:
metrika.hits_42
metrika.visits_42


