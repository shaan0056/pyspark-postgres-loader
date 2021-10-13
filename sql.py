
pg_col_data_type_query = """
SELECT
    pg_attribute.attname AS column_name,
    pg_attribute.atttypid::regtype as data_type,
    pg_catalog.format_type(pg_attribute.atttypid, 
    pg_attribute.atttypmod) AS data_type_with_mod
FROM
    pg_catalog.pg_attribute
INNER JOIN
    pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
INNER JOIN
    pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
WHERE
    pg_attribute.attnum > 0
AND NOT 
    pg_attribute.attisdropped
AND 
    pg_namespace.nspname = '%s'
AND 
    pg_class.relname = '%s'
ORDER BY
    pg_attribute.attnum ;
"""

pg_primary_key_query = """
SELECT 
    string_agg(a.attname, ',') AS index_cols
FROM  
    pg_index pgi
INNER JOIN 
    pg_class idx ON idx.oid = pgi.indexrelid
INNER JOIN 
    pg_namespace insp ON insp.oid = idx.relnamespace
INNER JOIN 
    pg_class tbl ON tbl.oid = pgi.indrelid
INNER JOIN 
    pg_namespace tnsp ON tnsp.oid = tbl.relnamespace
INNER JOIN 
    pg_attribute a ON a.attrelid = idx.oid
WHERE
    pgi.indisunique AND indisprimary
AND
    tnsp.nspname = '%s'
AND
    tbl.relname = '%s' ;
"""

pg_unique_index_query = """
SELECT
    index_cols FROM (
    SELECT 
        idx.relname AS index_name, 
        insp.nspname AS index_schema,
        tbl.relname AS table_name,
        tnsp.nspname AS table_schema,
        string_agg(a.attname, ',') as index_cols
    FROM
        pg_index pgi
    INNER JOIN 
        pg_class idx ON idx.oid = pgi.indexrelid
    INNER JOIN 
        pg_namespace insp ON insp.oid = idx.relnamespace
    INNER JOIN 
        pg_class tbl ON tbl.oid = pgi.indrelid
    INNER JOIN 
        pg_namespace tnsp ON tnsp.oid = tbl.relnamespace
    INNER JOIN 
        pg_attribute a ON a.attrelid = idx.oid    
    WHERE
        pgi.indisunique AND NOT indisprimary
    AND
        tnsp.nspname = '%s'
    AND
        tbl.relname = '%s' 
    GROUP BY 
        index_name, index_schema, table_name, table_schema
    LIMIT 1
    ) a ;
"""
