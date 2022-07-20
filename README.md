# dbt-spark-cde

The `dbt-spark-cde` adapter allows you to use [dbt](https://www.getdbt.com/) [Cloudera Data Platform](https://cloudera.com) with CDE API server support. This code bases use the dbt-spark project (https://github.com/dbt-labs/dbt-spark), and provides CDE API based connectivity support over it. 

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Requirements

Python >= 3.8

dbt-core >= 1.1.0

pyspark

sqlparams

### Installing dbt-spark-cde

`pip install dbt-spark-cde`

### Profile Setup

```
demo_project:
  target: dev
  outputs:
    dev:
     type: spark_cde
     method: cde
     schema: my_db
     auth_endpoint: https://service.spark-cde-gateway.my.org.com/
     host: https://spark-cde-gateway.my.org.com/dex/api/v1/
     user: my_user
     password: my_pass
```

- To obtain auth_endpoint follow the steps here: (https://docs.cloudera.com/data-engineering/cloud/api-access/topics/cde-api-get-access-token.html)

### Caveats
- While using cde , in the Livy UI if you notice sessions change state to dead from starting instead of idle, make sure there is a proper mapping for the user in the IDBroker mapping section 
- Actions > Manage Access > IDBroker Mappings . [Reference](https://docs.cloudera.com/cdf-datahub/7.2.15/flink-analyzing-data/topics/cdf-datahub-sa-create-idbroker-mapping.html)
- Also make sure the workload password is set either through UI or CLI. [Reference](https://docs.cloudera.com/management-console/cloud/user-management/topics/mc-setting-the-ipa-password.html)

## Supported features
Please see the original adapter documentation: https://github.com/dbt-labs/dbt-spark and https://docs.getdbt.com/reference/warehouse-profiles/spark-profile
