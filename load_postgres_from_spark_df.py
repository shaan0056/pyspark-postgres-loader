"""
A class to load (upsert/insert) Postgres tables using Spark DataFrame.
All configuration details required for this class are pulled from config.ini.

Steps:
    -   Instantiate the source class corresponding to the source passed as input and
        get the source data as spark DataFrame.
    -   Cast the DataFrame in accordance with the target Postgres table column data types.
        Note: The spark DataFrame column names must match with the corresponding target Postgres
        column names.
    -   Upsert/Insert the casted DataFrame to target Postgres table in batches. Handle errors
        in the process as well.
    -   Print out the load statistics once Upsert/Insert is completed.
"""

from typing import Dict, List
from configparser import ConfigParser
from pathlib import Path
from collections import OrderedDict
from importlib import import_module
import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from sql import pg_col_data_type_query, pg_unique_index_query, pg_primary_key_query


class LoadPostgresFromSparkDataFrame:

    def __init__(self,
                 pg_package,
                 target_pg_table,
                 batch_size,
                 parallelism,
                 spark,
                 source,
                 *source_class_args,
                 **source_class_kwargs):

        # Import the desired database helper module.
        if pg_package == "asyncpg":
            import asyncpg_database_helper
            self._db_helper = asyncpg_database_helper
        elif pg_package == "psycopg2":
            import psycopg2_database_helper
            self._db_helper = psycopg2_database_helper
        else:
            raise Exception(f"Unknown postgres python package provided - {pg_package}")

        self._target_pg_table = target_pg_table
        self._batch_size = batch_size
        self._parallelism = parallelism
        self._spark = spark

        # Load config.ini.
        self._config = ConfigParser()
        self._config.read(
            str(Path(__file__).resolve().parents[0]) + '\\config.ini'
        )

        # Get the Source Dataframe based on the data source type.
        self._source_dataframe = self._get_source_dataframe(
            source, *source_class_args, **source_class_kwargs
        )

        # Get the Target Postgres Database credentials from config.ini.
        self._database_credentials = self._get_config_section(
            "my_database_credentials"
        )

    def load_postgres(self) -> None:
        """
        Load the source DataFrame to target postgres table by,
            - Select only those columns from DataFrame which
               exist in target postgres table.
            - Cast the matching DataFrame columns based on the
               corresponding postgres data type.
            - Load the casted DataFrame using upsert_spark_df_to_postgres
               function.
        :return:None
        """

        # Get the target postgres table column and corresponding data types.
        pg_col_dtypes = self._get_col_dtypes()

        # Cast the source DataFrame columns as per the corresponding
        # postgres table data types.
        casted_dataframe = self._cast_df_per_pg_col_dtypes(
            pg_col_dtypes=pg_col_dtypes
        )

        # Get the target postgres table unique index.
        target_table_unique_key = self._get_unique_key()

        # Load the casted DataFrame to target postgres table.
        self._db_helper.upsert_spark_df_to_postgres(
            dataframe_to_upsert=casted_dataframe,
            table_name=self._target_pg_table,
            table_unique_key=target_table_unique_key,
            database_credentials=self._database_credentials,
            batch_size=self._batch_size,
            parallelism=self._parallelism
        )

    def _get_col_dtypes(self) -> OrderedDict:
        """
        Get the postgres table columns and data types.
        :return: OrderedDict of column names and data types.
        """

        [schema, table] = self._target_pg_table.split('.')

        query_to_run = pg_col_data_type_query % (schema, table)

        results = self._db_helper.fetch_query_results(
            query_to_run=query_to_run,
            database_credentials=self._database_credentials
        )
        pg_col_data_types = OrderedDict(
            (col, [datatype, datatype_with_mod]) for
            col, datatype, datatype_with_mod in results
        )
        return pg_col_data_types

    def _cast_df_per_pg_col_dtypes(self,
                                   pg_col_dtypes: OrderedDict) -> DataFrame:
        """
        Cast DataFrame as per the matching postgres column data types.
        :param pg_col_dtypes:
        :return: Casted DataFrame.
        """
        data_type_mapping = self._get_config_section(
            "pg_to_spark_data_type_mapping"
        )

        # Select only those columns from source dataframe which
        # exists in target postgres table.
        df_cols_existing_in_tgt_table = [
            col for col in self._source_dataframe.schema.names
            if col in pg_col_dtypes
        ]

        if not df_cols_existing_in_tgt_table:
            raise Exception(
                "None of the columns in source exist "
                f"in target postgres table -> {self._target_pg_table}"
            )
        df_cast_mapping = OrderedDict()

        for col in df_cols_existing_in_tgt_table:
            pg_data_type = pg_col_dtypes[col]
            spark_data_type = data_type_mapping.get(pg_data_type[0], "string")
            if pg_data_type[0] == "numeric":
                spark_data_type = pg_data_type[1].replace("numeric", "decimal")
            df_cast_mapping[col] = spark_data_type

        casted_df_cols = [
            f.col(col).cast(data_type).alias(col)
            for col, data_type in df_cast_mapping.items()
        ]
        return self._source_dataframe.select(casted_df_cols)

    def _get_config_section(self, section: str) -> Dict[str, str]:
        """
        Get the required configuration details from config.ini
        based on the section name.
        :param section: config section name.
        :return: config details for the section as dict.
        """
        if section not in self._config.sections():
            raise Exception(f"Section {section} not found in config.ini")

        return dict(self._config[section])

    def _get_source_dataframe(self, source, *args, **kwargs) -> DataFrame:
        """
        Get the source data as Spark DataFrame based on source type details
        defined in config.ini 'source_data_class_mapping' section.
        :param source: source type.
        :param args: source class arguments.
        :param kwargs: source class keyword arguments.
        :return: source data as DataFrame.
        """
        source_class_mappings = self._get_config_section(
            "source_data_class_mapping"
        )

        class_name = source_class_mappings[source + "_class"]
        module_name = source_class_mappings[source + "_module"]

        module = import_module(module_name)
        class_instance = getattr(module, class_name)

        return class_instance(*args, **kwargs).get_source_data_as_df()

    def _get_unique_key(self) -> List[str] or None:
        """
        Get the primary key of a postgres table. If not, get the unique index.
        Else, return None.
        :return: list of columns which form an unique index or None.
        """

        unique_index = None
        [schema, table] = self._target_pg_table.split('.')
        for query in pg_primary_key_query, pg_unique_index_query:

            query_to_run = query % (schema, table)
            result = self._db_helper.fetch_query_results(
                query_to_run=query_to_run,
                database_credentials=self._database_credentials
            )
            if result:
                result_tuple = result[0]
                if result_tuple is not None:
                    unique_index = result_tuple[0].split(',')
                    print(
                        "Unique Key found for "
                        f"{self._target_pg_table} -> {unique_index}"
                    )
                    break

        return unique_index
