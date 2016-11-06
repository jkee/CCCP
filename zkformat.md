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
```
 
### Host

Host is just FQDN and value is his datacenter ID
```
node_host -> node_dc
```   
       
### Table

TODO table
```
table_name
    
```  