-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH source AS (
SELECT DISTINCT
    contract_address,
    COALESCE(CAST(decimals AS BIGINT), 18) as decimals,
    symbol,
    standard,
    created_timestamp,
    last_refreshed,
	substr(contract_address, 3, 2) as hash_partition,
	date_partition
FROM source_database.ethereum_tokens_metadata
    WHERE created_timestamp > timestamp'filter_value'
)

SELECT * FROM source;

-- incremental load
INSERT INTO target_database.table_name
WITH source AS (
SELECT DISTINCT
    contract_address,
    COALESCE(CAST(decimals AS BIGINT), 18) as decimals,
    symbol,
    standard,
    created_timestamp,
    last_refreshed,
	substr(contract_address, 3, 2) as hash_partition,
	date_partition
FROM source_database.ethereum_tokens_metadata
    WHERE created_timestamp > timestamp'filter_value'
)

SELECT source.*
FROM source
	LEFT JOIN target_database.table_name AS target ON source.contract_address = target.contract_address
	AND source.date_partition = target.date_partition
	AND source.hash_partition = target.hash_partition
	AND source.last_refreshed = target.last_refreshed
WHERE target.contract_address IS NULL;
