# IdentifyCassandraLargePartition

## Summary
This app will scan the partitions of a table and if the size in megabytes is larger than 100 it will write the keys, token value, size in megabytes, record count, and current time to the partition_metrics table.</br></br>
This information is useful when diagnosing partition skew.</br></br>
The app uses a custom CassandraSQLContext, and a custom DataFrameReader to ensure all the data from a single partition in Cassandra is in a single partition in the DataFrame. The app also uses a custom List which has a reduceByKey function to sum the size and count of records in a single partition.

## Configuration
There is a config.yaml file which will need to be updated for the environment it is run in
<UL>
<LI> defaultKeyspace - This keyspace is required by the Spark Cassandra Connector. It can be any key space that exists in the cluster
<LI> outputTable - Table name the app will write to (default is partition_metrics)
<LI> outputKeyspace - Key space the output table is in
<LI> spark.cassandra.auth.username - Cassandra user name
<LI> spark.cassandra.auth.password - Cassandra password
<LI> spark.cassandra.connection.local_dc - Only required if there are multiple data centers. Set to the data center the output key space is in.
</UL>

## Usage
The app takes 3 parameters in the following order:
<UL>
<LI> Target key space name - Key space the target table is in
<LI> Target table name - Name of the table the app will collect metrics for
<LI> Path to and name of config file - default when running locally should be config.yaml
</UL>
