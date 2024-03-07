-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH source AS (
SELECT DISTINCT
    token_address,
	from_address,
	to_address,
	coalesce(try_cast(value as decimal), 0) as value,
	transaction_hash,
	log_index,
	cast(to_unixtime(cast(block_timestamp as timestamp)) as bigint) AS block_timestamp,
	block_timestamp as block_timestamp_readable,
	block_number,
	block_hash,
	date_partition
FROM source_database.ethereum_token_transfers
    WHERE block_number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source;

-- incremental load
INSERT INTO target_database.table_name
WITH source AS (
SELECT DISTINCT
	token_address,
	from_address,
	to_address,
	coalesce(try_cast(value as decimal), 0) as value,
	transaction_hash,
	log_index,
	cast(to_unixtime(cast(block_timestamp as timestamp)) as bigint) AS block_timestamp,
	block_timestamp as block_timestamp_readable,
	block_number,
	block_hash,
	date_partition
FROM source_database.ethereum_token_transfers
	WHERE block_number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source
WHERE NOT EXISTS (
	SELECT 1 FROM target_database.table_name as target
	WHERE target.transaction_hash = source.transaction_hash
	AND target.log_index = source.log_index
	AND target.block_number = source.block_number
	AND target.date_partition = source.date_partition
);
