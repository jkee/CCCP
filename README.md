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


# Data distribution and region split

Index (complex or not) can be imagined as one axis.
One region is an interval on that axis. All regions cover up full axis range.

When region become large enough to split:
1. Select index median from real data
2. Create two (?) new regions. First is [left_bound, median) and second is [median, right_bound)


# What's wrong with external implementation

 - Index split should be made on client.




TODO
regionid -> long
client should not build table name

lower, upper -> left, right
maxtabletsize -> maxtabletsizemb

