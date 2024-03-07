-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array ['date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH source AS (
SELECT DISTINCT
	address,
	bytecode,
	function_sighashes,
	is_erc20,
	is_erc721,
    block_timestamp,
	substr(address, 3, 2) as hash_partition,
	date_partition
FROM source_database.ethereum_contracts
    WHERE block_timestamp > timestamp'filter_value'
)

SELECT cast(uuid() as varchar) as uuid, * FROM source;

-- incremental load
INSERT INTO target_database.table_name
WITH source AS (
SELECT DISTINCT
	address,
	bytecode,
	function_sighashes,
	is_erc20,
	is_erc721,
	block_timestamp,
	substr(address, 3, 2) as hash_partition,
	date_partition
FROM source_database.ethereum_contracts
	WHERE block_timestamp > timestamp'filter_value'
)

SELECT cast(uuid() as varchar) as uuid, * FROM source
WHERE NOT EXISTS (
	SELECT 1 FROM target_database.table_name as target
	WHERE target.address = source.address
	AND target.block_timestamp = source.block_timestamp
	AND target.hash_partition = source.hash_partition
	AND target.date_partition = source.date_partition
);
