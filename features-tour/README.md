# Features Tour

Demonstrate multiple features of Spark in a [sequence of Notebooks](notebooks).

In order to run the [first notebook](notebooks/01-config-mount.py), we need to define two [environment variables](https://docs.microsoft.com/en-us/azure/databricks/clusters/configure#environment-variables) on our cluster:

Environment Variable|Value
-|-
LAKE_SOURCE|Source data lake folder of format abfss://\<`container`\>@\<`storage account name`\>.dfs.core.windows.net/\<`root folder`\>
LAKE_DESTINATION|Destination data lake folder (same format)

