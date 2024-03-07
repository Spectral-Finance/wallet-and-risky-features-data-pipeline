-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH source AS (
SELECT DISTINCT
    hash,
	nonce,
	transaction_index,
	from_address,
	to_address,
	value,
	gas,
	gas_price,
	input,
	receipt_cumulative_gas_used,
	receipt_gas_used,
	receipt_contract_address,
	receipt_root,
	receipt_status,
	cast(to_unixtime(cast(block_timestamp as timestamp)) as bigint) AS block_timestamp,
	block_timestamp as block_timestamp_readable,
	block_number,
	block_hash,
	max_fee_per_gas,
	max_priority_fee_per_gas,
	transaction_type,
	receipt_effective_gas_price,
	date_partition
FROM source_database.ethereum_transactions
    WHERE block_number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source;

-- incremental load
INSERT INTO target_database.table_name
WITH source AS (
SELECT DISTINCT
	hash,
	nonce,
	transaction_index,
	from_address,
	to_address,
	value,
	gas,
	gas_price,
	input,
	receipt_cumulative_gas_used,
	receipt_gas_used,
	receipt_contract_address,
	receipt_root,
	receipt_status,
	cast(to_unixtime(cast(block_timestamp as timestamp)) as bigint) AS block_timestamp,
	block_timestamp as block_timestamp_readable,
	block_number,
	block_hash,
	max_fee_per_gas,
	max_priority_fee_per_gas,
	transaction_type,
	receipt_effective_gas_price,
	date_partition
FROM source_database.ethereum_transactions
	WHERE block_number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source
WHERE NOT EXISTS (
	SELECT 1 FROM target_database.table_name as target
	WHERE target.hash = source.hash
	AND target.transaction_index = source.transaction_index
	AND target.block_number = source.block_number
	AND target.date_partition = source.date_partition
);
