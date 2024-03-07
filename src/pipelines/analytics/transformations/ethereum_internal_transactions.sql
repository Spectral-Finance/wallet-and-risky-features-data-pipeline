-- full load
CREATE TABLE target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = ARRAY['date_partition'],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH internal_transactions_source AS (
	SELECT DISTINCT
		t.block_number,
		t.block_timestamp AS timestamp,
		t.transaction_hash AS hash,
		t.from_address,
		t.to_address,
		t.value,
		'ETH' AS contract_address,
		'ETH' AS token_name,
		'ETH' AS token_symbol,
		18 AS token_decimal,
		t.input,
		t.gas,
		t.gas_used,
		t.trace_id,
		t.error AS error_code,
        CASE
            WHEN tc.status IS NULL OR tc.status = 1 THEN false
            ELSE true
        END AS is_error,
		t.date_partition
	FROM source_database.ethereum_traces AS t -- internal
	LEFT JOIN source_database.ethereum_transactions AS nt --normal
		ON nt.hash = t.transaction_hash AND nt.date_partition = t.date_partition and nt.block_number = t.block_number
	WHERE t.block_number >= filter_value
		AND t.value > 0 -- only transactions where the value is gt 0.
		AND (t.call_type NOT IN ('delegatecall', 'staticcall', 'callcode') OR t.call_type is null)
		AND NOT (t.from_address = nt.from_address AND t.to_address = nt.to_address AND t.value = nt.value) -- where these conditions are true, it means that the internal transaction is the same as the normal transaction, so we need to filter
),
rewards_transactions as (
    SELECT DISTINCT
		t.block_number,
		t.block_timestamp AS timestamp,
		'REWARD_' || t.to_address AS hash,
		'REWARD_' || t.reward_type as from_address,
		t.to_address,
		t.value,
		'ETH' AS contract_address,
		'ETH' AS token_name,
		'ETH' AS token_symbol,
		18 AS token_decimal,
		t.input,
		t.gas,
		t.gas_used,
		t.trace_id,
		t.error AS error_code,
		CASE
			WHEN t.status = 1 THEN false ELSE true
		END AS is_error,
		t.date_partition
	FROM db_stage_prod.ethereum_traces AS t -- internal
    WHERE t.block_number >= filter_value
		AND (t.call_type NOT IN ('delegatecall', 'staticcall', 'callcode') OR t.call_type is null)
    	AND t.trace_type = 'reward'
),
internal_and_rewards_transactions as (
    SELECT * FROM internal_transactions_source
    UNION ALL
    SELECT * FROM rewards_transactions
),
internal_transactions_final AS (
	SELECT DISTINCT
		t.block_number,
		t.timestamp,
		t.hash,
		t.from_address,
		t.to_address,
		t.value / (
			POWER(10, t.token_decimal / 2) * POWER(10, t.token_decimal / 2)
		) AS current_value,
		t.contract_address,
		t.token_name,
		t.token_symbol,
		t.token_decimal,
		t.gas,
		t.gas_used,
		t.input,
		t.trace_id,
		t.error_code,
		t.is_error,
		CASE
			WHEN c_1.address IS NOT NULL THEN true ELSE false
		END AS from_is_contract,
		CASE
			WHEN c_2.address IS NOT NULL THEN true ELSE false
		END AS to_is_contract,
		CASE
			WHEN r_1.token_address IS NOT NULL THEN true ELSE false
		END AS is_rugpull,
		CASE
			WHEN r_2.token_address IS NOT NULL THEN true ELSE false
		END AS to_address_is_rugpull,
        substr(t.from_address, 3, 2) as from_hash_partition,
        substr(t.to_address, 3, 2) as to_hash_partition,
        t.date_partition
	FROM internal_and_rewards_transactions AS t
		LEFT JOIN source_database.ethereum_contracts AS c_1 -- when the from address is a contract
            ON c_1.address = t.from_address
            AND c_1.hash_partition = SUBSTR(t.from_address, 3, 2)
		LEFT JOIN source_database.ethereum_contracts AS c_2 -- when the to address is a contract
            ON c_2.address = t.to_address
            AND c_2.hash_partition = SUBSTR(t.to_address, 3, 2)
		LEFT JOIN db_analytics_prod.rugpull_market_data AS r_1 -- when it is a rugpull
			ON lower(r_1.token_address) = t.contract_address
			AND r_1.date_partition = t.date_partition
			AND (
				(DATE(r_1.date_time) < DATE(FROM_UNIXTIME(t.timestamp)) AND DATE(r_1.date_time) >= DATE(FROM_UNIXTIME(t.timestamp)) - INTERVAL '1' DAY)
				OR (DATE(r_1.date_time) = DATE(FROM_UNIXTIME(t.timestamp)))
			)
		LEFT JOIN db_analytics_prod.rugpull_market_data AS r_2 -- when to_address is a rugpull
			ON lower(r_2.token_address) = t.to_address
			AND r_2.date_partition = t.date_partition
			AND (
				(DATE(r_2.date_time) < DATE(FROM_UNIXTIME(t.timestamp)) AND DATE(r_2.date_time) >= DATE(FROM_UNIXTIME(t.timestamp)) - INTERVAL '1' DAY)
				OR (DATE(r_2.date_time) = DATE(FROM_UNIXTIME(t.timestamp)))
			)
)

SELECT CAST(uuid() AS varchar) AS uuid, * FROM internal_transactions_final;

-- incremental load
INSERT INTO target_database.table_name
WITH internal_transactions_source AS (
	SELECT DISTINCT
		t.block_number,
		t.block_timestamp AS timestamp,
		t.transaction_hash AS hash,
		t.from_address,
		t.to_address,
		t.value,
		'ETH' AS contract_address,
		'ETH' AS token_name,
		'ETH' AS token_symbol,
		18 AS token_decimal,
		t.input,
		t.gas,
		t.gas_used,
		t.trace_id,
		t.error AS error_code,
		CASE
			WHEN t.status = 1 THEN false ELSE true
		END AS is_error,
		t.date_partition
	FROM source_database.ethereum_traces AS t -- internal
	LEFT JOIN source_database.ethereum_transactions AS nt --normal
		ON nt.hash = t.transaction_hash AND nt.date_partition = t.date_partition and nt.block_number = t.block_number
	WHERE t.block_number >= filter_value
		AND t.value > 0 -- only transactions where the value is gt 0.
		AND (t.call_type NOT IN ('delegatecall', 'staticcall', 'callcode') OR t.call_type is null)
		AND NOT (t.from_address = nt.from_address AND t.to_address = nt.to_address AND t.value = nt.value) -- where these conditions are true, it means that the internal transaction is the same as the normal transaction, so we need to filter
),
rewards_transactions as (
    SELECT DISTINCT
		t.block_number,
		t.block_timestamp AS timestamp,
		'REWARD_' || t.to_address AS hash,
		'REWARD_' || t.reward_type as from_address,
		t.to_address,
		t.value,
		'ETH' AS contract_address,
		'ETH' AS token_name,
		'ETH' AS token_symbol,
		18 AS token_decimal,
		t.input,
		t.gas,
		t.gas_used,
		t.trace_id,
		t.error AS error_code,
		CASE
			WHEN t.status = 1 THEN false ELSE true
		END AS is_error,
		t.date_partition
	FROM db_stage_prod.ethereum_traces AS t -- internal
    WHERE t.block_number >= filter_value
		AND (t.call_type NOT IN ('delegatecall', 'staticcall', 'callcode') OR t.call_type is null)
    	AND t.trace_type = 'reward'
),
internal_and_rewards_transactions as (
    SELECT * FROM internal_transactions_source
    UNION ALL
    SELECT * FROM rewards_transactions
),
internal_transactions_final AS (
	SELECT DISTINCT
		t.block_number,
		t.timestamp,
		t.hash,
		t.from_address,
		t.to_address,
		t.value / (
			POWER(10, t.token_decimal / 2) * POWER(10, t.token_decimal / 2)
		) AS current_value,
		t.contract_address,
		t.token_name,
		t.token_symbol,
		t.token_decimal,
		t.gas,
		t.gas_used,
		t.input,
		t.trace_id,
		t.error_code,
		t.is_error,
		CASE
			WHEN c_1.address IS NOT NULL THEN true ELSE false
		END AS from_is_contract,
		CASE
			WHEN c_2.address IS NOT NULL THEN true ELSE false
		END AS to_is_contract,
		CASE
			WHEN r_1.token_address IS NOT NULL THEN true ELSE false
		END AS is_rugpull,
		CASE
			WHEN r_2.token_address IS NOT NULL THEN true ELSE false
		END AS to_address_is_rugpull,
        substr(t.from_address, 3, 2) as from_hash_partition,
        substr(t.to_address, 3, 2) as to_hash_partition,
        t.date_partition
	FROM internal_and_rewards_transactions AS t
		LEFT JOIN source_database.ethereum_contracts AS c_1 -- when the from address is a contract
            ON c_1.address = t.from_address
            AND c_1.hash_partition = SUBSTR(t.from_address, 3, 2)
		LEFT JOIN source_database.ethereum_contracts AS c_2 -- when the to address is a contract
            ON c_2.address = t.to_address
            AND c_2.hash_partition = SUBSTR(t.to_address, 3, 2)
		LEFT JOIN db_analytics_prod.rugpull_market_data AS r_1 -- when it is a rugpull
			ON lower(r_1.token_address) = t.contract_address
			AND r_1.date_partition = t.date_partition
			AND (
				(DATE(r_1.date_time) < DATE(FROM_UNIXTIME(t.timestamp)) AND DATE(r_1.date_time) >= DATE(FROM_UNIXTIME(t.timestamp)) - INTERVAL '1' DAY)
				OR (DATE(r_1.date_time) = DATE(FROM_UNIXTIME(t.timestamp)))
			)
		LEFT JOIN db_analytics_prod.rugpull_market_data AS r_2 -- when to_address is a rugpull
			ON lower(r_2.token_address) = t.to_address
			AND r_2.date_partition = t.date_partition
			AND (
				(DATE(r_2.date_time) < DATE(FROM_UNIXTIME(t.timestamp)) AND DATE(r_2.date_time) >= DATE(FROM_UNIXTIME(t.timestamp)) - INTERVAL '1' DAY)
				OR (DATE(r_2.date_time) = DATE(FROM_UNIXTIME(t.timestamp)))
			)
)

SELECT CAST(uuid() AS varchar) AS uuid, *
FROM internal_transactions_final AS source
WHERE NOT EXISTS (
	SELECT 1
	FROM target_database.table_name AS target
	WHERE target.hash = source.hash
	AND target.block_number = source.block_number
	AND target.trace_id = source.trace_id
	AND target.from_address = source.from_address
	AND target.to_address = source.to_address
	AND target.date_partition = source.date_partition
);
