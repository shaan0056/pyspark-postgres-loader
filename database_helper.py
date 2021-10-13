"""
Helper code to Upsert Spark DataFrame to Postgres using psycopg2.
"""
from typing import List, Iterable, Dict, Any, Tuple
from contextlib import contextmanager
from psycopg2 import connect, DatabaseError, Error
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row


@contextmanager
def savepoint(sp_cur,
              sp_name: str,
              func,
              *args,
              **kwargs) -> tuple:
    """
    A context manager to set savepoint, execute a database action
    and rollback to the savepoint in the event of an exception or
    yield the output and release the savepoint.
    :param sp_cur: psycopg2 cursor.
    :param sp_name: savepoint name.
    :param func: function to execute.
    :param args: any function arguments.
    :param kwargs: any function keyword arguments.
    :return: return tuple of function output and None or
    None and error as applicable.
    """
    try:
        sp_cur.execute(f"SAVEPOINT {sp_name};")
        output = func(*args, **kwargs)
    except (Exception, Error) as error:
        sp_cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
        yield None, error
    else:
        try:
            yield output, None
        finally:
            sp_cur.execute(f"RELEASE SAVEPOINT {sp_name};")


def get_postgres_connection(host: str,
                            database: str,
                            user: str,
                            password: str,
                            port: str):
    """
    Connect to postgres database and get the connection.
    :param host: host name of database instance.
    :param database: name of the database to connect to.
    :param user: user name.
    :param password: password for the user name.
    :param port: port to connect.
    :return: Database connection.
    """
    try:
        conn = connect(
            host=host, database=database,
            user=user, password=password,
            port=port
        )

    except (Exception, DatabaseError) as ex:
        print("Unable to connect to database !!")
        raise ex

    return conn


def execute_values_with_err_handling(db_cur,
                                     batch_list: List[List[Row]],
                                     sql: str) -> Tuple[int, List[str]]:
    """
    Execute a database action with error handling.
    :param db_cur: psycopg2 cursor.
    :param batch_list: List of batches to load.
    :param sql: query to execute.
    :return: total error count and list of error messages.
    """

    total_error_count = 0
    total_error_msgs = []

    while batch_list:
        batch = batch_list.pop()

        with savepoint(
                db_cur, 'my_sp',
                execute_values, cur=db_cur, sql=sql,
                argslist=batch, page_size=len(batch)
        ) as (output, error):

            if error:
                split_batches = batch_error_handler(batch=batch)

                if split_batches:
                    batch_list.extend(split_batches)
                else:
                    total_error_count += 1
                    total_error_msgs.append(str(error))

    return total_error_count, total_error_msgs


def batch_error_handler(batch: List[Row]) -> List[List[Row]] or None:
    """
    Split the rejected batch into two equal halves and return the same.
    If however, the batch has only one record, return None to indicate
    that it is an error record.
    :param batch: Rejected batch.
    :return: List of split batches or None as applicable.
    """
    batch_size = len(batch)

    if batch_size == 1:
        return None

    chunk_size = batch_size // 2
    split_batches = [batch[i:i + chunk_size] for i in range(0, batch_size, chunk_size)]
    return split_batches


def batch_and_upsert(dataframe_partition: Iterable[Row],
                     sql: str,
                     database_credentials: dict,
                     batch_size: int = 1000):
    """
    Batch the input dataframe_partition as per batch_size and insert/update
    to postgres using psycopg2 execute values with built in error handling.
    :param dataframe_partition: Pyspark DataFrame partition or any iterable.
    :param sql: query to insert/upsert the spark dataframe partition to postgres.
    :param database_credentials: postgres database credentials.
        Example: database_credentials = {
                host: <host>,
                database: <database>,
                user: <user>,
                password: <password>,
                port: <port>
            }
    :param batch_size: size of batch per round trip to database.
    :return: total records processed.
    """
    conn, cur = None, None
    counter, error_counter = 0, 0
    batch, batch_list, final_error_msgs = [], [], []

    for record in dataframe_partition:

        counter += 1
        batch.append(record)

        if not conn:
            conn = get_postgres_connection(**database_credentials)
            cur = conn.cursor()

        if counter % batch_size == 0:
            batch_list.append(batch)
            total_error_count, total_error_msgs = execute_values_with_err_handling(
                db_cur=cur,
                batch_list=batch_list,
                sql=sql
            )
            conn.commit()
            error_counter += total_error_count
            final_error_msgs.append(total_error_msgs)
            batch, batch_list = [], []

    if batch:
        batch_list.append(batch)
        total_error_count, total_error_msgs = execute_values_with_err_handling(
                db_cur=cur,
                batch_list=batch_list,
                sql=sql
            )
        conn.commit()
        error_counter += total_error_count
        final_error_msgs.append(total_error_msgs)

    if cur:
        cur.close()
    if conn:
        conn.close()

    yield counter, error_counter, final_error_msgs


def build_upsert_query(cols: List[str],
                       table_name: str,
                       unique_key: List[str],
                       cols_not_for_update: List[str] = None) -> str:
    """
    Builds postgres upsert query using input arguments.

    Example : build_upsert_query(
        ['col1', 'col2', 'col3', 'col4'],
        "my_table",
        ['col1'],
        ['col2']
    ) ->
    INSERT INTO my_table (col1, col2, col3, col4) VALUES %s
    ON CONFLICT (col1) DO UPDATE SET (col3, col4) = (EXCLUDED.col3, EXCLUDED.col4) ;

    :param cols: the postgres table columns required in the
        insert part of the query.
    :param table_name: the postgres table name.
    :param unique_key: unique_key of the postgres table for checking
        unique constraint violations.
    :param cols_not_for_update: columns in cols which are not required in
        the update part of upsert query.
    :return: Upsert query as per input arguments.
    """

    cols_str = ', '.join(cols)

    insert_query = """ INSERT INTO %s (%s) VALUES %%s """ % (
        table_name, cols_str
    )

    if cols_not_for_update is not None:
        cols_not_for_update.extend(unique_key)
    else:
        cols_not_for_update = [col for col in unique_key]

    unique_key_str = ', '.join(unique_key)

    update_cols = [col for col in cols if col not in cols_not_for_update]
    update_cols_str = ', '.join(update_cols)

    update_cols_with_excluded_markers = [f'EXCLUDED.{col}' for col in update_cols]
    update_cols_with_excluded_markers_str = ', '.join(update_cols_with_excluded_markers)

    if len(update_cols) > 1:
        equality_clause = "(%s) = (%s)"
    else:
        equality_clause = "%s = %s"

    on_conflict_clause = f""" ON CONFLICT (%s) DO UPDATE SET {equality_clause} ;"""

    on_conflict_clause = on_conflict_clause % (
        unique_key_str,
        update_cols_str,
        update_cols_with_excluded_markers_str
    )

    return insert_query + on_conflict_clause


def upsert_spark_df_to_postgres(dataframe_to_upsert: DataFrame,
                                table_name: str,
                                table_pkey: List[str],
                                database_credentials: Dict[str:str],
                                batch_size: int = 1000,
                                parallelism: int = 1) -> None:
    """
    Upsert a spark DataFrame into a postgres table with error handling.

    :param dataframe_to_upsert: spark DataFrame to upsert to postgres.
    :param table_name: postgres table name to upsert.
    :param table_pkey: postgres table primary key.
    :param database_credentials: database credentials.
    :param batch_size: desired batch size for upsert.
    :param parallelism: No. of parallel connections to postgres database.
    :return:None
    """
    upsert_query = build_upsert_query(
        cols=dataframe_to_upsert.schema.names,
        table_name=table_name, unique_key=table_pkey
    )
    upsert_stats = dataframe_to_upsert.coalesce(parallelism).rdd.mapPartitions(
        lambda dataframe_partition: batch_and_upsert(
            dataframe_partition=dataframe_partition,
            sql=upsert_query,
            database_credentials=database_credentials,
            batch_size=batch_size
        )
    )

    total_recs_upserted = 0
    total_recs_rejects = 0
    error_msgs = []

    for counter, error_counter, final_error_msgs in upsert_stats.collect():
        total_recs_upserted += counter
        total_recs_rejects += error_counter
        error_msgs.extend(final_error_msgs)

    print("")
    print("#################################################")
    print(f" Total records upserted - {total_recs_upserted}")
    print(f" Total records rejected - {total_recs_rejects}")
    print("#################################################")
    print("")

    for err_msg in error_msgs:
        print(" Started Printing Error Messages ....")
        print(err_msg)
        print(" Completed Printing Error Messages ....")
