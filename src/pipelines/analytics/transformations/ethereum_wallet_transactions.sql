-- full load is executed using EMR with Pyspark scritp: src/utils/processing_wallet_transactions_pyspark.py
-- incremental load
INSERT INTO target_database.table_name
WITH wallet_transactions_source AS (
    -- erc20
    SELECT
        t.block_number,
        t.timestamp,
        t.hash,
        NULL AS transaction_index,
        t.from_address,
        t.to_address,
        t.current_value,
        t.contract_address,
        t.token_symbol,
        t.token_decimal,
        0 AS tx_fee,
        t.is_error,
        t.from_is_contract,
        t.to_is_contract,
        t.is_rugpull,
        t.to_address_is_rugpull,
        'erc20' AS transaction_type,
        3 AS priority,
        t.from_hash_partition,
        t.to_hash_partition,
        t.date_partition
    FROM source_database.ethereum_erc20_transactions t
    WHERE t.block_number >= filter_value
        AND NOT (t.from_is_contract = true AND t.to_is_contract = true)
    UNION ALL
    -- normal
    SELECT
        t.block_number,
        t.timestamp,
        t.hash,
        t.transaction_index,
        t.from_address,
        t.to_address,
        CASE WHEN t.is_error = true THEN 0 ELSE t.current_value END AS current_value,
        t.contract_address,
        t.token_symbol,
        t.token_decimal,
        t.tx_fee,
        t.is_error,
        t.from_is_contract,
        t.to_is_contract,
        t.is_rugpull,
        t.to_address_is_rugpull,
        'normal' AS transaction_type,
        1 AS priority,
        t.from_hash_partition,
        t.to_hash_partition,
        t.date_partition
    FROM source_database.ethereum_normal_transactions t
    WHERE t.block_number >= filter_value
        AND NOT (t.from_is_contract = true AND t.to_is_contract = true)
    UNION ALL
    -- internal
    SELECT
        t.block_number,
        t.timestamp,
        t.hash,
        NULL AS transaction_index,
        t.from_address,
        t.to_address,
        CASE WHEN t.is_error = true THEN 0 ELSE t.current_value END AS current_value,
        t.contract_address,
        t.token_symbol,
        t.token_decimal,
        0 AS tx_fee,
        t.is_error,
        t.from_is_contract,
        t.to_is_contract,
        t.is_rugpull,
        t.to_address_is_rugpull,
        'internal' AS transaction_type,
        2 AS priority,
        t.from_hash_partition,
        t.to_hash_partition,
        t.date_partition
    FROM source_database.ethereum_internal_transactions t
    WHERE t.block_number >= filter_value
        AND NOT (t.from_is_contract = true AND t.to_is_contract = true)
),
wallet_transactions_sender AS (
    SELECT
        wt.block_number,
        wt.timestamp,
        cast(current_timestamp as timestamp) AS insert_at,
        wt.hash,
        wt.transaction_index,
        wt.from_address AS wallet_address,
        wt.to_address as interacted_with,
        'sender' AS address_role,
        wt.current_value,
        wt.contract_address,
        wt.token_symbol,
        wt.token_decimal,
        tx_fee,
        wt.is_error,
        wt.to_is_contract,
        wt.is_rugpull,
        wt.to_address_is_rugpull,
        wt.transaction_type,
        wt.priority,
        wt.from_hash_partition AS address_partition,
        wt.date_partition
    FROM
        wallet_transactions_source AS wt
    WHERE wt.from_is_contract <> true
    AND wt.from_address <> 'GENESIS'
    AND wt.from_address <> '0x0000000000000000000000000000000000000000'
),
wallet_transactions_receiver AS (
    SELECT
        wt.block_number,
        wt.timestamp,
        cast(current_timestamp as timestamp) AS insert_at,
        wt.hash,
        wt.transaction_index,
        wt.to_address AS wallet_address,
        wt.from_address as interacted_with,
        'receiver' AS address_role,
        wt.current_value,
        wt.contract_address,
        wt.token_symbol,
        wt.token_decimal,
        0 as tx_fee,
        wt.is_error,
        wt.from_is_contract,
        wt.is_rugpull,
        wt.to_address_is_rugpull,
        wt.transaction_type,
        wt.priority,
        wt.to_hash_partition AS address_partition,
        wt.date_partition
    FROM
        wallet_transactions_source AS wt
    WHERE wt.to_is_contract <> true
    AND wt.to_address <> '0x0000000000000000000000000000000000000000'
),
wallet_transactions_final as (
    SELECT * FROM wallet_transactions_sender
    UNION ALL
    SELECT * FROM wallet_transactions_receiver
)
SELECT * FROM wallet_transactions_final as source
WHERE NOT EXISTS (
    SELECT 1 FROM target_database.table_name as target
    WHERE target.block_number >= filter_value
    AND target.hash = source.hash
    AND target.timestamp = source.timestamp
    AND target.block_number = source.block_number
    AND target.wallet_address = source.wallet_address
    AND target.address_role = source.address_role
    AND target.address_partition = source.address_partition
    AND target.date_partition = source.date_partition
    AND target.transaction_type = source.transaction_type
    AND target.current_value = source.current_value
)
AND source.address_partition IN chunk -- filter by tuple of address partitions
