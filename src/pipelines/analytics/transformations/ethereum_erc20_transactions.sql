-- full load
CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'date_partition' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH tokens_metadata AS (
    SELECT
        contract_address,
        decimals,
        symbol,
        type,
        hash_partition
    FROM (
        SELECT
            contract_address,
            decimals,
            symbol,
            standard as type,
            hash_partition,
            ROW_NUMBER() OVER (
                PARTITION BY contract_address
                ORDER BY last_refreshed DESC
            ) AS rn
        FROM source_database.ethereum_tokens_metadata
            WHERE standard = 'ERC-20'
    )
    WHERE rn = 1
),
erc20_transactions_source AS (
    SELECT DISTINCT
        tf.block_number,
        tf.block_timestamp AS timestamp,
        tf.transaction_hash AS hash,
        tf.from_address,
        tf.to_address,
        tf.token_address,
        tf.value,
        ts.nonce,
        ts.transaction_index,
        CASE
            WHEN tc.status IS NULL OR tc.status = 1 THEN false
            ELSE true
        END AS is_error,
        tf.date_partition
    FROM
        source_database.ethereum_token_transfers AS tf
        INNER JOIN source_database.ethereum_traces tc ON
            tc.transaction_hash = tf.transaction_hash
            AND tc.block_number = tf.block_number
            AND tc.date_partition = tf.date_partition
        INNER JOIN source_database.ethereum_transactions ts ON
            ts.hash = tf.transaction_hash
            AND ts.block_number = tf.block_number
            AND ts.date_partition = tf.date_partition
        WHERE tf.block_number >= filter_value
        AND tc.status = 1
        AND tf.value > 0
),
erc20_transactions_final AS (
    SELECT DISTINCT
        t.block_number,
        t.timestamp,
        t.hash,
        t.from_address,
        t.to_address,
        t.value / (POWER(10, COALESCE(TRY_CAST(tk.decimals AS decimal), 18) / 2) * POWER(10, COALESCE(TRY_CAST(tk.decimals AS decimal), 18) / 2)) AS current_value,
        tk.contract_address,
        tk.symbol AS token_symbol,
        COALESCE(TRY_CAST(tk.decimals AS decimal), 18) AS token_decimal,
        tk.type AS token_type,
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
        SUBSTR(t.from_address, 3, 2) AS from_hash_partition,
        SUBSTR(t.to_address, 3, 2) AS to_hash_partition,
        t.date_partition
    FROM erc20_transactions_source AS t
        INNER join tokens_metadata as tk
            ON tk.contract_address = t.token_address
            AND tk.hash_partition = SUBSTR(t.token_address, 3, 2)
    	LEFT JOIN source_database.ethereum_contracts AS c_1 -- when the from address is a contract
            ON c_1.address = t.from_address
            AND c_1.hash_partition = SUBSTR(t.from_address, 3, 2)
    	LEFT JOIN source_database.ethereum_contracts AS c_2 -- when the to addresses is a contract
            ON c_2.address = t.to_address
            AND c_2.hash_partition = SUBSTR(t.to_address, 3, 2)
        LEFT JOIN db_analytics_prod.rugpull_market_data AS r_1 -- when it is a rugpull
            ON lower(r_1.token_address) = t.token_address
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

select CAST(uuid() AS varchar) AS uuid, * from erc20_transactions_final

-- incremental load
INSERT INTO target_database.table_name
WITH tokens_metadata AS (
    SELECT
        contract_address,
        decimals,
        symbol,
        type,
        hash_partition
    FROM (
        SELECT
            contract_address,
            decimals,
            symbol,
            standard as type,
            hash_partition,
            ROW_NUMBER() OVER (
                PARTITION BY contract_address
                ORDER BY last_refreshed DESC
            ) AS rn
        FROM source_database.ethereum_tokens_metadata
            WHERE standard = 'ERC-20'
    )
    WHERE rn = 1
),
erc20_transactions_source AS (
    SELECT DISTINCT
        tf.block_number,
        tf.block_timestamp AS timestamp,
        tf.transaction_hash AS hash,
        tf.from_address,
        tf.to_address,
        tf.token_address,
        tf.value,
        ts.nonce,
        ts.transaction_index,
        CASE
            WHEN tc.status = 1 THEN false ELSE true
        END AS is_error,
        tf.date_partition
    FROM
        source_database.ethereum_token_transfers AS tf
        INNER JOIN source_database.ethereum_traces tc ON
            tc.transaction_hash = tf.transaction_hash
            AND tc.block_number = tf.block_number
            AND tc.date_partition = tf.date_partition
        INNER JOIN source_database.ethereum_transactions ts ON
            ts.hash = tf.transaction_hash
            AND ts.block_number = tf.block_number
            AND ts.date_partition = tf.date_partition
        WHERE tf.block_number >= filter_value
        AND tc.status = 1
        AND tf.value > 0
),
erc20_transactions_final AS (
    SELECT DISTINCT
        t.block_number,
        t.timestamp,
        t.hash,
        t.from_address,
        t.to_address,
        t.value / (POWER(10, COALESCE(TRY_CAST(tk.decimals AS decimal), 18) / 2) * POWER(10, COALESCE(TRY_CAST(tk.decimals AS decimal), 18) / 2)) AS current_value,
        tk.contract_address,
        tk.symbol AS token_symbol,
        COALESCE(TRY_CAST(tk.decimals AS decimal), 18) AS token_decimal,
        tk.type AS token_type,
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
        SUBSTR(t.from_address, 3, 2) AS from_hash_partition,
        SUBSTR(t.to_address, 3, 2) AS to_hash_partition,
        t.date_partition
    FROM erc20_transactions_source AS t
        INNER join tokens_metadata as tk
            ON tk.contract_address = t.token_address
            AND tk.hash_partition = SUBSTR(t.token_address, 3, 2)
    	LEFT JOIN source_database.ethereum_contracts AS c_1 -- when the from address is a contract
            ON c_1.address = t.from_address
            AND c_1.hash_partition = SUBSTR(t.from_address, 3, 2)
    	LEFT JOIN source_database.ethereum_contracts AS c_2 -- when the to addresses is a contract
            ON c_2.address = t.to_address
            AND c_2.hash_partition = SUBSTR(t.to_address, 3, 2)
        LEFT JOIN db_analytics_prod.rugpull_market_data AS r_1 -- when it is a rugpull
            ON lower(r_1.token_address) = t.token_address
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

select CAST(uuid() AS varchar) AS uuid, * from erc20_transactions_final AS source
	WHERE NOT EXISTS (
		SELECT 1 FROM target_database.table_name as target
		WHERE target.hash = source.hash
		AND target.block_number = source.block_number
		AND target.from_address = source.from_address
		AND target.to_address = source.to_address
		AND target.date_partition = source.date_partition
	);
