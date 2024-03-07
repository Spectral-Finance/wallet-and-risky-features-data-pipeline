-- full load
CREATE TABLE target_database.table_name WITH (
	format = 'PARQUET',
	write_compression = 'SNAPPY',
    optimize_rewrite_delete_file_threshold = 10,
    vacuum_max_snapshot_age_seconds = 259200,
	location = 's3://bucket_name/layer/data_source/table_name/',
    table_type = 'ICEBERG',
    is_external = FALSE
) AS
WITH rugpull_transactions AS ( -- Fetches all transactions related to rugpulls
    SELECT
        wt.timestamp,
        wt.wallet_address,
        wt.current_value,
        wt.contract_address,
        wt.address_role
    FROM target_database.ethereum_wallet_transactions as wt
    INNER JOIN (
        SELECT contract_address FROM db_stage_prod.ethereum_tokens_metadata where decimals > 0
        UNION ALL
        -- This query is needed because we don't have the ETH contract on tokens_metadata table
        SELECT 'ETH' as contract_address) as tm
        ON tm.contract_address = wt.contract_address
    WHERE date_partition >= '2020-06' -- data starts in this month
        AND (is_rugpull = TRUE OR to_address_is_rugpull = TRUE)
),

daily_token_prices AS (
    SELECT
        DISTINCT
        tp.address,
        tp.price,
        tp.timestamp
    FROM target_database.features_daily_token_prices as tp
    INNER JOIN rugpull_transactions as rt
        ON tp.address = rt.contract_address
        AND date(from_unixtime(tp.timestamp)) <= date(from_unixtime(rt.timestamp))
        AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(rt.timestamp)) <= 7
),
daily_token_prices_ranked AS (
    SELECT
        tp.address,
        CASE WHEN tp.address = 'ETH'
            THEN 1
            ELSE tp.price
        END as price,
        tp.timestamp,
        rt.timestamp AS rt_timestamp,
        ROW_NUMBER() OVER (PARTITION BY rt.contract_address, rt.timestamp ORDER BY ABS(date_diff('second', from_unixtime(tp.timestamp), from_unixtime(rt.timestamp)))) AS rank
    FROM rugpull_transactions as rt
    CROSS JOIN daily_token_prices AS tp
    WHERE tp.address = rt.contract_address
        AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(rt.timestamp)) <= 7
),

rugpull_features as (
    SELECT -- Creates history rugpull features for each wallet
        r.wallet_address,
        COALESCE(SUM(r.current_value * COALESCE(rp.price, CASE WHEN r.contract_address = 'ETH' THEN 1 ELSE 0 END)) FILTER (WHERE address_role = 'sender'), 0) as total_amount_chain, -- rugpull_total_amount_eth
        COUNT(*) FILTER (WHERE address_role = 'sender') as all_transaction_count, -- rugpull_all_transaction_count
        COUNT(*) as no_of_interaction, -- rugpull_no_of_interaction
        MAX(r.timestamp) as last_interaction_timestamp, -- rugpull_last_interaction_timestamp
        MIN(r.timestamp) as first_interaction_timestamp
    FROM
        rugpull_transactions as r
    LEFT JOIN daily_token_prices_ranked AS rp
        ON rp.address = r.contract_address
        AND rp.rt_timestamp = r.timestamp
        AND rp.rank = 1
    GROUP BY 1
)
SELECT
    wallet_address,
    CASE WHEN total_amount_chain > 1.3e+07 THEN 1.3e+07 ELSE total_amount_chain END as total_amount_chain,
    all_transaction_count,
    no_of_interaction,
    last_interaction_timestamp,
    first_interaction_timestamp,
    (last_interaction_timestamp - first_interaction_timestamp) as first_last_interaction_timestamp_diff
FROM rugpull_features;

-- incremental load
MERGE INTO target_database.rugpull_features
USING (
    WITH existing_lastest_interaction_timestamp AS (
        SELECT max(last_interaction_timestamp) as lastest_interaction_timestamp
        FROM target_database.rugpull_features
    ), -- Fetches the latest block number from the existing table to be the lower limit of the new data
    new_rugpull_transactions AS ( -- Fetches new transactions related to rugpulls
        SELECT
            timestamp,
            wallet_address,
            current_value,
            contract_address,
            address_role
        FROM
            target_database.ethereum_wallet_transactions,
            existing_lastest_interaction_timestamp
        WHERE
            date_partition >= date_format(from_unixtime(lastest_interaction_timestamp), '%Y-%m')
            AND timestamp > lastest_interaction_timestamp -- unix_timestamp
            AND (is_rugpull = TRUE OR to_address_is_rugpull = TRUE)
    ),

    daily_token_prices AS (
    SELECT
        DISTINCT
        tp.address,
        tp.price,
        tp.timestamp
    FROM target_database.features_daily_token_prices as tp
    INNER JOIN new_rugpull_transactions as rt
        ON tp.address = rt.contract_address
        AND date(from_unixtime(tp.timestamp)) <= date(from_unixtime(rt.timestamp))
        AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(rt.timestamp)) <= 7
    ),
    daily_token_prices_ranked AS (
        SELECT
            tp.address,
            CASE WHEN tp.address = 'ETH'
                THEN 1
                ELSE tp.price
            END as price,
            tp.timestamp,
            rt.timestamp AS rt_timestamp,
            ROW_NUMBER() OVER (PARTITION BY rt.contract_address, rt.timestamp ORDER BY ABS(date_diff('second', from_unixtime(tp.timestamp), from_unixtime(rt.timestamp)))) AS rank
        FROM new_rugpull_transactions as rt
        CROSS JOIN daily_token_prices AS tp
        WHERE tp.address = rt.contract_address
            AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(rt.timestamp)) <= 7
    ),
    new_data as (
        SELECT -- Creates history rugpull features for each wallet
            wallet_address,
            COALESCE(SUM(r.current_value * COALESCE(rp.price, CASE WHEN r.contract_address = 'ETH' THEN 1 ELSE 0 END)) FILTER (WHERE address_role = 'sender'), 0) as total_amount_chain, -- rugpull_total_amount_eth
            COUNT(*) FILTER (WHERE address_role = 'sender') as all_transaction_count, -- rugpull_all_transaction_count
            COUNT(*) as no_of_interaction, -- rugpull_no_of_interaction
            MAX(r.timestamp) as last_interaction_timestamp, -- rugpull_last_interaction_timestamp
            MIN(r.timestamp) as first_interaction_timestamp
        FROM
            new_rugpull_transactions as r
        LEFT JOIN daily_token_prices_ranked AS rp
            ON rp.address = r.contract_address
            AND rp.rt_timestamp = r.timestamp
            AND rp.rank = 1
        INNER JOIN (
            SELECT contract_address FROM db_stage_prod.ethereum_tokens_metadata where decimals > 0
            UNION ALL
            -- This query is needed because we don't have the ETH contract on tokens_metadata table
            SELECT 'ETH' as contract_address) as tm
            ON tm.contract_address = r.contract_address
        GROUP BY 1
    ),
    rugpull_features as (
        SELECT
            dtm.wallet_address,
            dtm.total_amount_chain + COALESCE(rf.total_amount_chain, 0) as total_amount_chain,
            dtm.all_transaction_count + COALESCE(rf.all_transaction_count, 0) as all_transaction_count,
            dtm.no_of_interaction + COALESCE(rf.no_of_interaction, 0) as no_of_interaction,
            dtm.last_interaction_timestamp,
            COALESCE(rf.first_interaction_timestamp, dtm.first_interaction_timestamp) as first_interaction_timestamp
        FROM
            new_data dtm
            LEFT JOIN target_database.rugpull_features rf on dtm.wallet_address = rf.wallet_address
    )
    SELECT
        wallet_address,
        CASE WHEN total_amount_chain > 1.3e+07 THEN 1.3e+07 ELSE total_amount_chain END as total_amount_chain,
        all_transaction_count,
        no_of_interaction,
        last_interaction_timestamp,
        first_interaction_timestamp,
        (last_interaction_timestamp - first_interaction_timestamp) as first_last_interaction_timestamp_diff
    FROM rugpull_features
) merged_data
ON merged_data.wallet_address = rugpull_features.wallet_address
WHEN MATCHED THEN
    UPDATE SET
        total_amount_chain = merged_data.total_amount_chain,
        all_transaction_count = merged_data.all_transaction_count,
        no_of_interaction = merged_data.no_of_interaction,
        last_interaction_timestamp = merged_data.last_interaction_timestamp,
        first_interaction_timestamp = merged_data.first_interaction_timestamp,
        first_last_interaction_timestamp_diff = merged_data.first_last_interaction_timestamp_diff
WHEN NOT MATCHED THEN
    INSERT (
        wallet_address,
        total_amount_chain,
        all_transaction_count,
        no_of_interaction,
        last_interaction_timestamp,
        first_interaction_timestamp,
        first_last_interaction_timestamp_diff
    )
    VALUES (
        merged_data.wallet_address,
        merged_data.total_amount_chain,
        merged_data.all_transaction_count,
        merged_data.no_of_interaction,
        merged_data.last_interaction_timestamp,
        merged_data.first_interaction_timestamp,
        merged_data.first_last_interaction_timestamp_diff
    );
