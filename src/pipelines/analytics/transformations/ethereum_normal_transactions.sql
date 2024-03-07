-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH normal_transactions_source AS (
    SELECT DISTINCT
        ts.block_number,
        ts.block_timestamp AS timestamp,
        ts.nonce,
        ts.hash,
        ts.block_hash,
        ts.transaction_index,
        ts.from_address,
        ts.to_address,
        ts.value,
        ts.gas,
        ts.gas_price,
        b.base_fee_per_gas as gas_fee,
        CASE
            WHEN tc.status IS NULL OR tc.status = 1 THEN false
            ELSE true
        END AS is_error,
        ts.input,
        'ETH' AS contract_address,
        'ETH' AS token_name,
        'ETH' AS token_symbol,
        18 AS token_decimal,
        ts.receipt_gas_used AS gas_used,
        ts.date_partition
    FROM source_database.ethereum_transactions ts
    INNER JOIN source_database.ethereum_blocks as b ON b.number = ts.block_number
        AND b.date_partition = ts.date_partition
    LEFT JOIN source_database.ethereum_traces tc ON tc.transaction_hash = ts.hash
        AND tc.block_number = ts.block_number
        AND tc.date_partition = ts.date_partition
        AND tc.from_address = ts.from_address
        AND tc.to_address = ts.to_address
    WHERE ts.block_number >= filter_value
),
normal_transactions_final AS (
    SELECT DISTINCT
        t.block_number,
        t.timestamp,
        t.hash,
        t.nonce,
        t.block_hash,
        t.transaction_index,
        t.from_address,
        t.to_address,
        t.value / (POWER(10, t.token_decimal / 2) * POWER(10, t.token_decimal / 2)) AS current_value,
        t.gas,
        t.gas_price,
        t.gas_used * (t.gas_price / (POWER(10, 18))) AS tx_fee,
        t.gas_fee,
        t.is_error,
        t.input,
        t.contract_address,
        t.token_name,
        t.token_symbol,
        t.token_decimal,
        t.gas_used,
        CASE
            WHEN c_1.address IS NOT NULL THEN true
            ELSE false
        END AS from_is_contract,
        CASE
            WHEN c_2.address IS NOT NULL THEN true
            ELSE false
        END AS to_is_contract,
        CASE
            WHEN r_1.token_address IS NOT NULL THEN true
            ELSE false
        END AS is_rugpull,
        CASE
            WHEN r_2.token_address IS NOT NULL THEN true
            ELSE false
        END AS to_address_is_rugpull,
        SUBSTR(t.from_address, 3, 2) AS from_hash_partition,
        SUBSTR(t.to_address, 3, 2) AS to_hash_partition,
        t.date_partition
    FROM normal_transactions_source AS t
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

SELECT CAST(uuid() AS varchar) AS uuid, * FROM normal_transactions_final;

-- incremental load
INSERT INTO target_database.table_name
WITH normal_transactions_source AS (
    SELECT DISTINCT
        ts.block_number,
        ts.block_timestamp AS timestamp,
        ts.nonce,
        ts.hash,
        ts.block_hash,
        ts.transaction_index,
        ts.from_address,
        ts.to_address,
        ts.value,
        ts.gas,
        ts.gas_price,
        b.base_fee_per_gas as gas_fee,
        CASE
            WHEN tc.status = 1 THEN false
            ELSE true
        END AS is_error,
        ts.input,
        'ETH' AS contract_address,
        'ETH' AS token_name,
        'ETH' AS token_symbol,
        18 AS token_decimal,
        ts.receipt_gas_used AS gas_used,
        ts.date_partition
    FROM source_database.ethereum_transactions ts
    INNER JOIN source_database.ethereum_blocks as b ON b.number = ts.block_number
        AND b.date_partition = ts.date_partition
    LEFT JOIN source_database.ethereum_traces tc ON tc.transaction_hash = ts.hash
        AND tc.block_number = ts.block_number
        AND tc.date_partition = ts.date_partition
        AND tc.from_address = ts.from_address
        AND tc.to_address = ts.to_address
    WHERE ts.block_number >= filter_value
),
normal_transactions_final AS (
    SELECT DISTINCT
        t.block_number,
        t.timestamp,
        t.hash,
        t.nonce,
        t.block_hash,
        t.transaction_index,
        t.from_address,
        t.to_address,
        t.value / (POWER(10, t.token_decimal / 2) * POWER(10, t.token_decimal / 2)) AS current_value,
        t.gas,
        t.gas_price,
        t.gas_used * (t.gas_price / (POWER(10, 18))) AS tx_fee,
        t.gas_fee,
        t.is_error,
        t.input,
        t.contract_address,
        t.token_name,
        t.token_symbol,
        t.token_decimal,
        t.gas_used,
        CASE
            WHEN c_1.address IS NOT NULL THEN true
            ELSE false
        END AS from_is_contract,
        CASE
            WHEN c_2.address IS NOT NULL THEN true
            ELSE false
        END AS to_is_contract,
        CASE
            WHEN r_1.token_address IS NOT NULL THEN true
            ELSE false
        END AS is_rugpull,
        CASE
            WHEN r_2.token_address IS NOT NULL THEN true
            ELSE false
        END AS to_address_is_rugpull,
        SUBSTR(t.from_address, 3, 2) AS from_hash_partition,
        SUBSTR(t.to_address, 3, 2) AS to_hash_partition,
        t.date_partition
    FROM normal_transactions_source AS t
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
FROM normal_transactions_final as source
	WHERE NOT EXISTS (
		SELECT 1
		FROM target_database.table_name as target
		WHERE target.hash = source.hash
		AND target.block_number = source.block_number
		AND target.from_address = source.from_address
		AND target.to_address = source.to_address
		AND target.date_partition = source.date_partition
);
