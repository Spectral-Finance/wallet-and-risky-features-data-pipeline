-- full load
CREATE TABLE target_database.table_name WITH (
    format = 'parquet',
    write_compression = 'SNAPPY',
    location = 's3://bucket_name/layer/data_source/table_name/',
    table_type = 'ICEBERG',
    is_external = false,
    partitioning = ARRAY['address_partition']
) AS
WITH coin_balances as (
    SELECT
        wt.wallet_address,
        wt.contract_address,
        wt.token_symbol,
        wt.timestamp,
        CASE
            WHEN wt.transaction_type = 'internal' THEN 999
            WHEN wt.transaction_type = 'erc20' THEN 1000
            ELSE wt.transaction_index
        END AS transaction_index,
        CASE
            WHEN round(SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index) , 5) = -0.0
            THEN 0.0
            ELSE round(SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index), 5)
            END AS total_balance,
        SUM(CASE WHEN wt.address_role = 'receiver' THEN wt.current_value ELSE 0 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_incoming_value,
        SUM(CASE WHEN wt.address_role = 'sender' THEN wt.current_value ELSE 0 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_outgoing_value,
        SUM(wt.tx_fee)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_tx_fee,
        COUNT(CASE WHEN wt.address_role = 'receiver' THEN 1 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS incoming_transactions_count,
        COUNT(CASE WHEN wt.address_role = 'sender' THEN 1 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS outgoing_transactions_count,
        COUNT(*)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS transactions_count,
        MIN(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS first_transaction_timestamp,
        MAX(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS last_transaction_timestamp,
        current_timestamp(6) as inserted_at,
        current_timestamp(6) as updated_at,
        address_partition
    FROM source_database.ethereum_wallet_transactions AS wt
    WHERE wt.address_partition = '0000' --and date_partition between '2015-01' and '2023-12' and contract_address = 'ETH' and wallet_address = '0x0000000000a0aa7908bda39fbb8f95e5a0a6ee42'
),
coin_balances_with_min_max as (
    SELECT
        cb.wallet_address,
        cb.contract_address,
        cb.token_symbol,
        cb.timestamp,
        cb.transaction_index,
        cb.total_balance,
        MIN(cb.total_balance) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition) AS min_balance_in_ever,
        MAX(cb.total_balance) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition) AS max_balance_in_ever,
        cb.total_incoming_value,
        cb.total_outgoing_value,
        cb.total_tx_fee,
        cb.incoming_transactions_count,
        cb.outgoing_transactions_count,
        cb.transactions_count,
        cb.first_transaction_timestamp,
        cb.last_transaction_timestamp,
        cb.inserted_at,
        cb.updated_at,
        cb.address_partition
    FROM coin_balances AS cb
),
latest_transactions AS (
    SELECT
        wallet_address,
        contract_address,
        timestamp as max_timestamp,
        transaction_index as max_transaction_index
    FROM (
        SELECT
            wallet_address,
            contract_address,
            timestamp,
            transaction_index,
            ROW_NUMBER() OVER (PARTITION BY wallet_address, contract_address ORDER BY timestamp DESC, transaction_index DESC) as rn
        FROM
            coin_balances_with_min_max
    ) t
    WHERE rn = 1
)

SELECT
    distinct
    cb.wallet_address,
    cb.contract_address,
    cb.token_symbol,
    CASE
        WHEN cb.total_balance < 0
        THEN 0
        ELSE cb.total_balance
    END as total_balance,
    cb.min_balance_in_ever,
    cb.max_balance_in_ever,
    cb.total_incoming_value,
    cb.total_outgoing_value,
    cb.total_tx_fee,
    cb.incoming_transactions_count,
    cb.outgoing_transactions_count,
    cb.transactions_count,
    cb.first_transaction_timestamp,
    cb.last_transaction_timestamp,
    cb.inserted_at,
    cb.updated_at,
    CASE
        WHEN cb.total_balance < 0
        THEN true
        ELSE false
    END as has_negative_balance,
    cb.address_partition
FROM coin_balances_with_min_max AS cb
INNER JOIN latest_transactions AS lt
    ON cb.wallet_address = lt.wallet_address
    AND cb.contract_address = lt.contract_address
    AND cb.timestamp = lt.max_timestamp
    AND cb.transaction_index = lt.max_transaction_index

-- incremental load
MERGE INTO target_database.table_name
USING (
    WITH last_transaction_timestamp_inserted as (
        SELECT
            MAX(last_transaction_timestamp) AS last_transaction_timestamp
        FROM target_database.table_name
    ),
    coin_balances as (
        SELECT
            wt.wallet_address,
            wt.contract_address,
            wt.token_symbol,
            wt.timestamp,
            CASE
                WHEN wt.transaction_type = 'internal' THEN 999
                WHEN wt.transaction_type = 'erc20' THEN 1000
                ELSE wt.transaction_index
            END AS transaction_index,
            CASE
                WHEN round(SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index) , 5) = -0.0
                THEN 0.0
                ELSE round(SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index), 5)
                END AS total_balance,
            SUM(CASE WHEN wt.address_role = 'receiver' THEN wt.current_value ELSE 0 END)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_incoming_value,
            SUM(CASE WHEN wt.address_role = 'sender' THEN wt.current_value ELSE 0 END)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_outgoing_value,
            SUM(wt.tx_fee)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_tx_fee,
            COUNT(CASE WHEN wt.address_role = 'receiver' THEN 1 END)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS incoming_transactions_count,
            COUNT(CASE WHEN wt.address_role = 'sender' THEN 1 END)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS outgoing_transactions_count,
            COUNT(*)
                OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS transactions_count,
            MIN(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS first_transaction_timestamp,
            MAX(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS last_transaction_timestamp,
            current_timestamp(6) as inserted_at,
            current_timestamp(6) as updated_at,
            address_partition
        FROM source_database.ethereum_wallet_transactions AS wt
        CROSS JOIN last_transaction_timestamp_inserted l
        WHERE wt.timestamp > l.last_transaction_timestamp
            AND wt.address_partition in filter_value
        ),
    coin_balances_with_min_max as (
        SELECT
            cb.wallet_address,
            cb.contract_address,
            cb.token_symbol,
            cb.timestamp,
            cb.transaction_index,
            cb.total_balance,
            MIN(cb.total_balance) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition) AS min_balance_in_ever,
            MAX(cb.total_balance) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition) AS max_balance_in_ever,
            cb.total_incoming_value,
            cb.total_outgoing_value,
            cb.total_tx_fee,
            cb.incoming_transactions_count,
            cb.outgoing_transactions_count,
            cb.transactions_count,
            cb.first_transaction_timestamp,
            cb.last_transaction_timestamp,
            cb.inserted_at,
            cb.updated_at,
            cb.address_partition
        FROM coin_balances AS cb
    ),
    latest_transactions AS (
        SELECT
            wallet_address,
            contract_address,
            timestamp as max_timestamp,
            transaction_index as max_transaction_index
        FROM (
            SELECT
                wallet_address,
                contract_address,
                timestamp,
                transaction_index,
                ROW_NUMBER() OVER (PARTITION BY wallet_address, contract_address ORDER BY timestamp DESC, transaction_index DESC) as rn
            FROM
                coin_balances_with_min_max
        ) t
        WHERE rn = 1
    ),
    new_coin_balances as (
        SELECT
            cb.wallet_address,
            cb.contract_address,
            cb.token_symbol,
            cb.transaction_index,
            CASE
                WHEN cb.total_balance < 0
                THEN 0
                ELSE cb.total_balance
            END as total_balance,
            cb.min_balance_in_ever,
            cb.max_balance_in_ever,
            cb.total_incoming_value,
            cb.total_outgoing_value,
            cb.total_tx_fee,
            cb.incoming_transactions_count,
            cb.outgoing_transactions_count,
            cb.transactions_count,
            cb.first_transaction_timestamp,
            cb.last_transaction_timestamp,
            cb.inserted_at,
            cb.updated_at,
            CASE
                WHEN cb.total_balance < 0
                THEN true
                ELSE false
            END as has_negative_balance,
            cb.address_partition
        FROM coin_balances_with_min_max AS cb
        INNER JOIN latest_transactions AS lt
            ON cb.wallet_address = lt.wallet_address
            AND cb.contract_address = lt.contract_address
            AND cb.timestamp = lt.max_timestamp
            AND cb.transaction_index = lt.max_transaction_index
    ),
    last_transaction_timestamps_by_contract AS (
        SELECT
            lcb.wallet_address,
            lcb.contract_address,
            MAX(lcb.last_transaction_timestamp) OVER (PARTITION BY lcb.contract_address) AS last_transaction_timestamp
        FROM target_database.ethereum_wallet_coin_balances AS lcb
        WHERE EXISTS (SELECT 1 FROM new_coin_balances as tb where tb.wallet_address = lcb.wallet_address)
    ),
    merged_coin_balances as (
        SELECT
            ncb.wallet_address,
            ncb.contract_address,
            ncb.token_symbol,
            ncb.transaction_index,
            ncb.total_balance + COALESCE(lcb.total_balance, 0) AS total_balance,
            CASE WHEN ncb.min_balance_in_ever < lcb.min_balance_in_ever
                THEN ncb.min_balance_in_ever
                ELSE lcb.min_balance_in_ever
            END AS min_balance_in_ever,
            CASE WHEN ncb.max_balance_in_ever > lcb.max_balance_in_ever
                THEN ncb.max_balance_in_ever
                ELSE lcb.max_balance_in_ever
            END AS max_balance_in_ever,
            ncb.total_incoming_value + COALESCE(lcb.total_incoming_value, 0) AS total_incoming_value,
            ncb.total_outgoing_value + COALESCE(lcb.total_outgoing_value, 0) AS total_outgoing_value,
            ncb.incoming_transactions_count + COALESCE(lcb.incoming_transactions_count, 0) AS incoming_transactions_count,
            ncb.outgoing_transactions_count + COALESCE(lcb.outgoing_transactions_count, 0) AS outgoing_transactions_count,
            ncb.transactions_count + COALESCE(lcb.transactions_count, 0) AS transactions_count,
            ncb.total_tx_fee + COALESCE(lcb.total_tx_fee, 0) AS total_tx_fee,
            COALESCE(lcb.first_transaction_timestamp, ncb.first_transaction_timestamp) AS first_transaction_timestamp,
            ncb.last_transaction_timestamp AS last_transaction_timestamp,
            ncb.address_partition
        FROM new_coin_balances as ncb
        LEFT JOIN target_database.ethereum_wallet_coin_balances AS lcb -- last coin balances
            ON ncb.wallet_address = lcb.wallet_address
            AND ncb.contract_address = lcb.contract_address
            AND ncb.address_partition = lcb.address_partition
        LEFT JOIN last_transaction_timestamps_by_contract AS ltt
            ON ncb.wallet_address = ltt.wallet_address
            AND ncb.contract_address = ltt.contract_address
        WHERE ncb.last_transaction_timestamp <> ltt.last_transaction_timestamp OR ltt.last_transaction_timestamp IS NULL
    )
    select * from merged_coin_balances

    ) coin_balances_updated
ON coin_balances_updated.wallet_address = ethereum_wallet_coin_balances.wallet_address
AND coin_balances_updated.contract_address = ethereum_wallet_coin_balances.contract_address
WHEN MATCHED THEN
    UPDATE SET
        total_balance = coin_balances_updated.total_balance,
        min_balance_in_ever = coin_balances_updated.min_balance_in_ever,
        max_balance_in_ever = coin_balances_updated.max_balance_in_ever,
        total_incoming_value = coin_balances_updated.total_incoming_value,
        total_outgoing_value = coin_balances_updated.total_outgoing_value,
        incoming_transactions_count = coin_balances_updated.incoming_transactions_count,
        outgoing_transactions_count = coin_balances_updated.outgoing_transactions_count,
        transactions_count = coin_balances_updated.transactions_count,
        total_tx_fee = coin_balances_updated.total_tx_fee,
        first_transaction_timestamp = coin_balances_updated.first_transaction_timestamp,
        last_transaction_timestamp = coin_balances_updated.last_transaction_timestamp,
        updated_at = current_timestamp(6)
WHEN NOT MATCHED THEN
    INSERT (
        wallet_address,
        contract_address,
        token_symbol,
        total_balance,
        min_balance_in_ever,
        max_balance_in_ever,
        total_incoming_value,
        total_outgoing_value,
        incoming_transactions_count,
        outgoing_transactions_count,
        transactions_count,
        total_tx_fee,
        first_transaction_timestamp,
        last_transaction_timestamp,
        inserted_at,
        updated_at,
        address_partition
    )
    VALUES (
        coin_balances_updated.wallet_address,
        coin_balances_updated.contract_address,
        coin_balances_updated.token_symbol,
        coin_balances_updated.total_balance,
        coin_balances_updated.min_balance_in_ever,
        coin_balances_updated.max_balance_in_ever,
        coin_balances_updated.total_incoming_value,
        coin_balances_updated.total_outgoing_value,
        coin_balances_updated.incoming_transactions_count,
        coin_balances_updated.outgoing_transactions_count,
        coin_balances_updated.transactions_count,
        coin_balances_updated.total_tx_fee,
        coin_balances_updated.first_transaction_timestamp,
        coin_balances_updated.last_transaction_timestamp,
        current_timestamp(6),
        current_timestamp(6),
        coin_balances_updated.address_partition
    )
