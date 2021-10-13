from get_source_data_as_df import GetSourceDataAsDf
import boto3
from pyspark.sql import DataFrame

s3 = boto3.client("s3")


class GetS3DataAsDf(GetSourceDataAsDf):

    def __init__(self, spark, s3_path, s3_file_format, *args, **kwargs):
        self._spark = spark
        self._s3_path = s3_path
        self._s3_file_format = s3_file_format
        self._args = args
        self._kwargs = kwargs

    def get_source_data_as_df(self) -> DataFrame:
        """
        Return source data as DataFrame as per input arguments.
        :return: Source Data as DataFrame.
        """

        if self._s3_file_format == "csv":
            return self._spark_read_csv_from_s3()

        elif self._s3_file_format == "parquet":
            return self._spark_read_parquet_from_s3()

    def _spark_read_csv_from_s3(self) -> DataFrame:
        """
        Read s3 csv file and return as spark DataFrame.
        :return: Spark DataFrame.
        """
        df = self._spark.read.format("com.databricks.spark.csv")\
            .options(**self._kwargs).load(self._s3_path)

        return df.toDF(
            *[col.lower() for col in df.columns]
        )

    def _spark_read_parquet_from_s3(self) -> DataFrame:
        """
        Read s3 parquet file and return as spark DataFrame.
        :return: Spark DataFrame.
        """
        df = self._spark.read.options(**self._kwargs).parquet(self._s3_path)

        return df.toDF(
            *[col.lower() for col in df.columns]
        )
