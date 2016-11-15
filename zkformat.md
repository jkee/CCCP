# Zookeeper metadata format

### Cluster

All cluster info is stored under cluster_name prefix.
There are:

* Nodes (hardware)
* Datasets
* Cluster-level parameters

```
zookeeper_root
    *cluster_name
        parameters
            format -> value
        datasets
            *dataset_name
        nodes
            *node_host
```
            
            
Cluster-level parameters list is empty for now.
            
### Dataset
                        
Dataset information is stored under dataset name.

* Dataset index
* Tables
* Dataset-level parameters
       
```
dataset_name
    parameters
        replication_factor -> value
        max_region_size -> value
    index -> index_json
    tables
        *table_name
    regions
        *region_<incremental_num> -> value
```
 
Dataset-level parameters

* `replication_factor` - How many times data is copied on cluster
* `max_region_size_mb` - Desired maximum region size in megabytes. Maximum isn't guaranteed.
 
### Host

Host is just FQDN and value is his datacenter ID
```
node_host -> node_dc
```   
       
### Table

Table information: everything required to handle tables.
Version will be required later for alters and other DDL.
```
table_name
    version -> value
    create_statement -> value
    
```  