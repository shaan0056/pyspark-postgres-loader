from argparse import ArgumentParser
from pyspark.sql import SparkSession
from load_postgres_from_spark_df import LoadPostgresFromSparkDataFrame


def main() -> None:
    """
    Postgres Loader.
    :return:None
    """

    # Create Spark Session.
    spark = SparkSession.builder.appName("Postgres Loader").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Resolve Arguments.
    parser = ArgumentParser()
    parser.add_argument(
        "--pg_python_package", type=str,
        help="The postgres python package to use, i.e. asyncpg or psycopg2.",
        required=False, default="asyncpg"
    )
    parser.add_argument(
        "--target_pg_table", type=str,
        help="Fully qualified target postgres table to load.",
        required=True
    )
    parser.add_argument(
        "--batch_size", type=int,
        help="Batch size for loading the source data to target postgres table.",
        required=False, default=1000
    )
    parser.add_argument(
        "--parallelism", type=int,
        help="No. of parallel connections to postgres.",
        required=False, default=1
    )
    parser.add_argument(
        "--partition_cols", type=int,
        help="Columns to partition the DataFrame.",
        required=False, default=None
    )
    parser.add_argument(
        "--source", type=str,
        help="Data source type.", required=True
    )
    parser.add_argument(
        "--source_arg",
        help="Additional args to be passed to source data class.",
        required=False, action="append",
        type=lambda kv: kv.split("="), dest="source_args",
        default={}
    )

    args = parser.parse_args()

    # Start Postgres Load Process.
    LoadPostgresFromSparkDataFrame(
        pg_package=args.pg_python_package,
        target_pg_table=args.target_pg_table,
        batch_size=args.batch_size,
        parallelism=args.parallelism,
        partition_cols=args.partition_cols,
        spark=spark,
        source=args.source,
        **args.source_args
    ).load_postgres()

    spark.stop()


if __name__ == "__main__":
    main()
