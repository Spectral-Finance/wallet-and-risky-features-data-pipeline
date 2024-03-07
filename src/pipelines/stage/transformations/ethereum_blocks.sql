-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH source AS (
SELECT DISTINCT
    cast(to_unixtime(cast(timestamp as timestamp)) as bigint) AS timestamp,
    timestamp AS timestamp_readable,
	number,
	hash,
	parent_hash,
	nonce,
	sha3_uncles,
	logs_bloom,
	transactions_root,
	state_root,
	receipts_root,
	miner,
	difficulty,
	total_difficulty,
	size,
	extra_data,
	gas_limit,
	gas_used,
	coalesce(transaction_count,0) as transaction_count,
	base_fee_per_gas,
	date_partition
FROM source_database.ethereum_blocks
   WHERE number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source;

-- incremental load
INSERT INTO target_database.table_name
WITH source AS (
SELECT DISTINCT
    cast(to_unixtime(cast(timestamp as timestamp)) as bigint) AS timestamp,
    timestamp AS timestamp_readable,
	number,
	hash,
	parent_hash,
	nonce,
	sha3_uncles,
	logs_bloom,
	transactions_root,
	state_root,
	receipts_root,
	miner,
	difficulty,
	total_difficulty,
	size,
	extra_data,
	gas_limit,
	gas_used,
	coalesce(transaction_count,0) as transaction_count,
	base_fee_per_gas,
	date_partition
FROM source_database.ethereum_blocks
   WHERE number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source
WHERE NOT EXISTS (
	SELECT 1 FROM target_database.table_name as target
	WHERE target.hash = source.hash
	AND target.number = source.number
	AND target.date_partition = source.date_partition
);
