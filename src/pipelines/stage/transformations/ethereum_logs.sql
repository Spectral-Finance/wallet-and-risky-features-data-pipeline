-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH source AS (
SELECT DISTINCT
    log_index,
	transaction_hash,
	transaction_index,
	address,
	data,
	case
        when length(topics[1]) = 1
        then split(replace(replace(replace(replace(cast(array_join(topics, ',') as varchar), '[', ''), ']', ''), ',,,', ';'), ',', ''), ';')
        else topics
    end as topics,
	cast(to_unixtime(cast(block_timestamp as timestamp)) as bigint) AS block_timestamp,
	block_timestamp as block_timestamp_readable,
	block_number,
	block_hash,
	date_partition
FROM source_database.ethereum_logs
    WHERE block_number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source;

-- incremental load
INSERT INTO target_database.table_name
WITH source AS (
SELECT DISTINCT
	log_index,
	transaction_hash,
	transaction_index,
	address,
	data,
	case
        when length(topics[1]) = 1
        then split(replace(replace(replace(replace(cast(array_join(topics, ',') as varchar), '[', ''), ']', ''), ',,,', ';'), ',', ''), ';')
        else topics
    end as topics,
	cast(to_unixtime(cast(block_timestamp as timestamp)) as bigint) AS block_timestamp,
	block_timestamp as block_timestamp_readable,
	block_number,
	block_hash,
	date_partition
FROM source_database.ethereum_logs
	WHERE block_number >= filter_value
)

SELECT cast(uuid() as varchar) as uuid, * FROM source
WHERE NOT EXISTS (
	SELECT 1 FROM target_database.table_name as target
	WHERE target.transaction_hash = source.transaction_hash
	AND target.block_number = source.block_number
	AND target.log_index = source.log_index
	AND target.date_partition = source.date_partition
);
