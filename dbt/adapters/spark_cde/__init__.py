from dbt.adapters.spark_cde.connections import SparkConnectionManager  # noqa
from dbt.adapters.spark_cde.connections import SparkCredentials
from dbt.adapters.spark_cde.relation import SparkRelation  # noqa
from dbt.adapters.spark_cde.column import SparkColumn  # noqa
from dbt.adapters.spark_cde.impl import SparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import spark_cde

Plugin = AdapterPlugin(
    adapter=SparkAdapter, credentials=SparkCredentials, include_path=spark_cde.PACKAGE_PATH
)
