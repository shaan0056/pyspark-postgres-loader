"""
Helper code to Upsert Spark DataFrame to Postgres using asyncpg.
"""
import asyncio
from typing import List, Iterable, Dict, Tuple
from contextlib import asynccontextmanager
from asyncpg import connect
from pyspark.sql import DataFrame, Row


@asynccontextmanager
async def _savepoint(sp_conn,
                     sp_name: str,
                     func,
                     *args,
                     **kwargs) -> tuple:
    """
    An async context manager to set savepoint, execute a database action
    and rollback to the savepoint in the event of an exception or
    yield the output and release the savepoint.
    :param sp_conn: asyncpg connection.
    :param sp_name: savepoint name.
    :param func: function to execute.
    :param args: any function arguments.
    :param kwargs: any function keyword arguments.
    :return: return tuple of function output and None or
    None and error as applicable.
    """
    try:
        await sp_conn.execute(f"SAVEPOINT {sp_name};")
        output = func(*args, **kwargs)
    except Exception as error:
        await sp_conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
        yield None, error
    else:
        try:
            yield output, None
        finally:
            await sp_conn.execute(f"RELEASE SAVEPOINT {sp_name};")


async def get_postgres_connection(host: str,
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
        conn = await connect(
            host=host, database=database,
            user=user, password=password,
            port=port
        )

    except Exception as ex:
        print("Unable to connect to database !!")
        raise ex

    return conn


async def _execute_many_with_err_handling(db_conn,
                                          batch_list: List[List[Row]],
                                          sql: str) -> Tuple[int, List[str]]:
    """
    Execute a database action with error handling.
    :param db_conn: asyncpg connection.
    :param batch_list: List of batches to load.
    :param sql: query to execute.
    :return: total error count and list of error messages.
    """

    total_error_count = 0
    total_error_msgs = []

    while batch_list:
        batch = batch_list.pop()

        async with _savepoint(
                db_conn, 'my_sp',
                db_conn.executemany,
                command=sql, args=batch
        ) as (output, error):

            if error:
                split_batches = _batch_error_handler(batch=batch)

                if split_batches:
                    batch_list.extend(split_batches)
                else:
                    total_error_count += 1
                    total_error_msgs.append(str(error))

    return total_error_count, total_error_msgs


def _batch_error_handler(batch: List[Row]) -> List[List[Row]] or None:
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


async def _batch_and_upsert(dataframe_partition: Iterable[Row],
                            sql: str,
                            database_credentials: dict,
                            batch_size: int = 1000):
    """
    Batch the input dataframe_partition as per batch_size and insert/update
    to postgres using asyncpg executemany with built in error handling.
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
            conn = await get_postgres_connection(**database_credentials)

        if counter % batch_size == 0:
            batch_list.append(batch)

            async_transaction = conn.transaction()
            await async_transaction.start()
            total_error_count, total_error_msgs = await _execute_many_with_err_handling(
                db_conn=conn,
                batch_list=batch_list,
                sql=sql,
            )
            await async_transaction.commit()

            error_counter += total_error_count
            final_error_msgs.append(total_error_msgs)
            batch, batch_list = [], []

            if total_error_count == batch_size:
                break

    if batch:
        batch_list.append(batch)

        async_transaction = conn.transaction()
        await async_transaction.start()
        total_error_count, total_error_msgs = await _execute_many_with_err_handling(
            db_conn=conn,
            batch_list=batch_list,
            sql=sql,
        )
        await async_transaction.commit()

        error_counter += total_error_count
        final_error_msgs.append(total_error_msgs)

    if conn:
        await conn.close()

    return counter, error_counter, final_error_msgs


def _build_upsert_query(cols: List[str],
                        table_name: str,
                        unique_key: List[str] = None,
                        cols_not_for_update: List[str] = None) -> str:
    """
    Builds postgres upsert query using input arguments.
    Note: In the absence of unique_key, this will be just an insert query.

    Example : build_upsert_query(
        ['col1', 'col2', 'col3', 'col4'],
        "my_table",
        ['col1'],
        ['col2']
    ) ->
    INSERT INTO my_table (col1, col2, col3, col4) VALUES $1, $2, $3, $4
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
    parameters = ', '.join([f"${index + 1}" for index in range(len(cols))])

    insert_query = """ INSERT INTO %s (%s) VALUES %s """ % (
        table_name, cols_str, parameters
    )

    if not unique_key:
        return insert_query

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


def fetch_query_results(query_to_run: str, database_credentials: Dict[str, str]):
    """
    Execute a select query and fetch it's results.
    :param query_to_run: query to execute.
    :param database_credentials: database credentials.
        Example: database_credentials = {
                    host: <host>,
                    database: <database>,
                    user: <user>,
                    password: <password>,
                    port: <port>
                }
    :return: query results.
    """

    async def _fetch_query_results_async():
        conn = None
        try:
            conn = await get_postgres_connection(**database_credentials)
            results = await conn.fetch(query_to_run)
            return results
        except Exception as ex:
            print(f"Error while fetching query results for -> {query_to_run}")
            raise ex
        finally:
            if conn:
                await conn.close()

    return _run_coroutine(_fetch_query_results_async())


def upsert_spark_df_to_postgres(dataframe_to_upsert: DataFrame,
                                table_name: str,
                                table_unique_key: List[str],
                                database_credentials: Dict[str, str],
                                batch_size: int = 1000,
                                parallelism: int = 1,
                                partition_cols: List[str] = None) -> None:
    """
    Upsert a spark DataFrame into a postgres table with error handling.
    Note: If the target table lacks any unique index, data will be appended through
    INSERTS as UPSERTS in postgres require a unique constraint to be present in the table.
    :param dataframe_to_upsert: spark DataFrame to upsert to postgres.
    :param table_name: postgres table name to upsert.
    :param table_unique_key: postgres table primary key.
    :param database_credentials: database credentials.
        Example: database_credentials = {
                    host: <host>,
                    database: <database>,
                    user: <user>,
                    password: <password>,
                    port: <port>
                }
    :param batch_size: desired batch size for upsert.
    :param parallelism: No. of parallel connections to postgres database.
    :param partition_cols: partitioning columns.
    :return:None
    """

    def _yield_batch_and_upsert(dataframe_partition):

        yield _run_coroutine(
            _batch_and_upsert(
                dataframe_partition=dataframe_partition,
                sql=upsert_query,
                database_credentials=database_credentials,
                batch_size=batch_size
            )
        )

    # Build Upsert query.
    upsert_query = _build_upsert_query(
        cols=dataframe_to_upsert.schema.names,
        table_name=table_name, unique_key=table_unique_key
    )

    # Partition the DataFrame as per input arguments.
    if partition_cols:
        dataframe_to_upsert = dataframe_to_upsert.repartition(parallelism, *partition_cols)
    else:
        dataframe_to_upsert = dataframe_to_upsert.coalesce(parallelism)

    # Upsert the DataFrame to Postgres.
    load_stats = dataframe_to_upsert.coalesce(parallelism).rdd.mapPartitions(
        lambda dataframe_partition: _yield_batch_and_upsert(dataframe_partition)
    )

    # Print Load Stats.
    total_recs_loaded = 0
    total_recs_rejects = 0
    error_msgs = []

    for counter, error_counter, final_error_msgs in load_stats.collect():
        total_recs_loaded += counter
        total_recs_rejects += error_counter
        error_msgs.extend(final_error_msgs)

    print("")
    print("#################################################")
    print(f" Total records loaded - {total_recs_loaded}")
    print(f" Total records rejected - {total_recs_rejects}")
    print("#################################################")
    print("")

    for err_msg in error_msgs:
        print(" Started Printing Error Messages ....")
        print(err_msg)
        print(" Completed Printing Error Messages ....")


def _run_coroutine(coroutine):
    """
    Run a coroutine using asyncio.
    :param coroutine: coroutine to run.
    :return: output returned by coroutine.
    """
    return asyncio.get_event_loop().run_until_complete(coroutine)

