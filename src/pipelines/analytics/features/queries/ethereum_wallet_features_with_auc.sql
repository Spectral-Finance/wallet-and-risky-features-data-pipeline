-- full load
CREATE TABLE db_analytics_prod.ethereum_wallet_features WITH (
    format = 'parquet',
    write_compression = 'SNAPPY',
    location = 's3://data-lakehouse-prod/analytics/ethereum/ethereum_wallet_features',
    table_type = 'ICEBERG',
    is_external = false,
    partitioning = ARRAY['address_partition']
) AS

--INSERT INTO db_analytics_prod.ethereum_wallet_features
WITH ranked_wallet_transactions as (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition
        ORDER BY wt.timestamp, wt.priority,
            CASE
                WHEN wt.transaction_type = 'internal' THEN 999
                WHEN wt.transaction_type = 'erc20' THEN 1000
                ELSE wt.transaction_index
            END,
            CASE WHEN wt.address_role = 'receiver'
              THEN 0
              ELSE 1
            END
    ) as row_num_by_contract,
    ROW_NUMBER() OVER(PARTITION BY wt.wallet_address, wt.address_partition
        ORDER BY wt.timestamp, wt.priority,
            CASE
                WHEN wt.transaction_type = 'internal' THEN 999
                WHEN wt.transaction_type = 'erc20' THEN 1000
                ELSE wt.transaction_index
            END,
            CASE WHEN wt.address_role = 'receiver'
              THEN 0
              ELSE 1
            END
    ) as row_num_by_wallet,
    CASE
        WHEN wt.transaction_type = 'internal' THEN 999
        WHEN wt.transaction_type = 'erc20' THEN 1000
        ELSE wt.transaction_index
    END AS transaction_index_by_transaction_type,
    DENSE_RANK() OVER (PARTITION BY wt.wallet_address, wt.address_partition, wt.hash ORDER BY wt.priority asc) as hash_rank
    FROM db_analytics_prod.ethereum_wallet_transactions AS wt
        where wt.timestamp > filter_value
        and wt.date_partition >= DATE_FORMAT(FROM_UNIXTIME(filter_value), '%Y-%m')
),

daily_token_prices AS (
    SELECT
        DISTINCT
        tp.address,
        tp.price,
        tp.timestamp
    FROM db_analytics_prod.features_daily_token_prices as tp
    INNER JOIN ranked_wallet_transactions as cb
        ON tp.address = cb.contract_address
        AND date(from_unixtime(tp.timestamp)) <= date(from_unixtime(cb.timestamp))
        AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(cb.timestamp)) <= 7
),

daily_token_prices_ranked AS (
    SELECT
        tp.address,
        CASE WHEN tp.address = 'ETH'
            THEN 1
            ELSE tp.price
        END as price,
        tp.timestamp,
        cb.timestamp AS cb_timestamp,
        ROW_NUMBER() OVER (PARTITION BY cb.contract_address, cb.timestamp ORDER BY ABS(date_diff('second', from_unixtime(tp.timestamp), from_unixtime(cb.timestamp)))) AS rank
    FROM ranked_wallet_transactions AS cb
    CROSS JOIN daily_token_prices AS tp
    WHERE tp.address = cb.contract_address
        AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(cb.timestamp)) <= 7
), --select * from daily_token_prices_ranked where rank = 1

coin_balances as (
    SELECT
        wt.wallet_address,
        wt.contract_address,
        wt.token_symbol,
        wt.timestamp,
        wt.block_number,
        wt.hash,
        wt.priority,
        wt.transaction_index_by_transaction_type,
        wt.transaction_type,
        wt.address_role,
        wt.current_value,
        wt.current_value * COALESCE(rp.price, 0) as current_value_in_eth,
        wt.tx_fee,
        CASE
            WHEN ROUND(SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract), 5) = -0.0
            THEN 0.0
            ELSE ROUND(SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract), 5)
        END AS total_balance_by_contract,
        CASE
            WHEN ROUND(SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract), 5) = -0.0
            THEN 0.0
            ELSE ROUND(SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract), 5)
        END AS total_balance_in_eth_by_contract,
        CASE
            WHEN ROUND(SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) END)
            OVER (PARTITION BY wt.wallet_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_wallet), 5) = -0.0
            THEN 0.0
            ELSE ROUND(SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) END)
            OVER (PARTITION BY wt.wallet_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_wallet), 5)
        END AS total_balance_in_eth,
        SUM(CASE WHEN wt.address_role = 'receiver' THEN wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) ELSE 0 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_incoming_value_in_eth,
        SUM(CASE WHEN wt.address_role = 'sender' THEN wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) ELSE 0 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_outgoing_value_in_eth,
        SUM(wt.tx_fee)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS total_tx_fee,
        COUNT(CASE WHEN wt.address_role = 'receiver' AND wt.hash_rank = 1 THEN wt.hash ELSE NULL END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS incoming_transactions_count,
        COUNT(CASE WHEN wt.address_role = 'sender' AND wt.hash_rank = 1 THEN wt.hash ELSE NULL END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS outgoing_transactions_count,
        COUNT(CASE WHEN wt.hash_rank = 1 THEN wt.hash ELSE NULL END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS transactions_count,
        MIN(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS first_transaction_timestamp,
        MAX(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) AS last_transaction_timestamp,
        wt.row_num_by_contract,
        wt.row_num_by_wallet,
        wt.address_partition,
        wt.date_partition
    FROM ranked_wallet_transactions as wt
    LEFT JOIN daily_token_prices_ranked AS rp
        ON rp.address = wt.contract_address
        AND rp.cb_timestamp = wt.timestamp
        AND rp.rank = 1
), --select * from coin_balances --where contract_address = '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da'

-- Getting the min and max balance for the entire wallet and also by contract
coin_balances_with_min_max as (
    SELECT
        cb.wallet_address,
        cb.contract_address,
        cb.token_symbol,
        cb.timestamp,
        -- rp.cb_timestamp as price_timestamp,
        --rp.price,
        cb.hash,
        cb.priority,
        cb.transaction_index_by_transaction_type,
        cb.current_value,
        cb.current_value_in_eth,
        cb.total_balance_by_contract,
        cb.total_balance_in_eth_by_contract,
        cb.total_balance_in_eth,
        COALESCE(MIN(CASE WHEN cb.total_balance_in_eth_by_contract > 0 THEN cb.total_balance_in_eth_by_contract ELSE NULL END) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition), 0) AS min_eth_balance_in_ever_by_contract,
        MAX(cb.total_balance_in_eth_by_contract) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition) AS max_eth_balance_in_ever_by_contract,
        COALESCE(MIN(CASE WHEN cb.total_balance_in_eth > 0 THEN cb.total_balance_in_eth ELSE NULL END) OVER (PARTITION BY cb.wallet_address, cb.address_partition), 0) AS min_eth_balance_in_ever,
        MAX(cb.total_balance_in_eth) OVER (PARTITION BY cb.wallet_address, cb.address_partition) AS max_eth_balance_in_ever,
        cb.total_incoming_value_in_eth,
        cb.total_outgoing_value_in_eth,
        cb.total_tx_fee,
        cb.incoming_transactions_count,
        cb.outgoing_transactions_count,
        cb.transactions_count,
        cb.first_transaction_timestamp,
        cb.last_transaction_timestamp,
        cb.row_num_by_contract,
        cb.row_num_by_wallet,
        cb.address_partition
    FROM coin_balances AS cb
    -- LEFT JOIN daily_token_prices_ranked AS rp
    --     ON rp.address = cb.contract_address
    --     AND rp.cb_timestamp = cb.timestamp
    --     AND rp.rank = 1
    --ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num ASC
), --select * from coin_balances_with_min_max as cb ORDER BY row_num_by_wallet desc
--as cb order by cb.timestamp desc--, cb.transaction_index_by_transaction_type, cb.row_num desc

--select wallet_address, sum(incoming_transactions_count) as incoming_count, sum(outgoing_transactions_count) as outgoing_count from coin_balances_with_min_max where row_num = 1 group by wallet_address --select * from coin_balances_with_min_max where hash = '0xd44e6c82a18151bd148f9a4bea17c21bbf6974f6c1012f21f9f0a07f374fb11d' and contract_address = '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da'
coin_balances_time_deposited_by_contract as (
    SELECT
        cb.*,
        COALESCE(LEAD(cb.timestamp) OVER (PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_contract) - cb.timestamp, 0) AS time_deposited
    FROM coin_balances_with_min_max AS cb
),
coin_balances_with_auc_and_time_in_ever_by_contract AS (
    SELECT
        cb.*,
        SUM(cb.time_deposited * cb.total_balance_in_eth_by_contract) OVER (PARTITION BY cb.wallet_address, contract_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_contract) / 60 / 60 / 24 as total_auc_contract,
        SUM(CASE WHEN cb.total_balance_in_eth_by_contract > 0 THEN cb.time_deposited ELSE 0 END) OVER (PARTITION BY cb.wallet_address, contract_address ORDER BY cb.timestamp, transaction_index_by_transaction_type, cb.row_num_by_contract) AS total_time_in_ever_contract
    FROM coin_balances_time_deposited_by_contract AS cb
), --select * from coin_balances_with_auc_and_time_in_ever_by_contract where contract_address = '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da'
coin_balances_time_deposited_eth as (
    SELECT
        cb.*,
        COALESCE(LEAD(cb.timestamp) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) - cb.timestamp, 0) AS time_deposited
    FROM coin_balances_with_min_max AS cb
),
coin_balances_with_auc_and_time_in_ever_eth AS (
    SELECT
        cb.*,
        SUM(cb.time_deposited * cb.total_balance_in_eth) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) / 60 / 60 / 24 AS total_auc_eth,
        SUM(CASE WHEN cb.total_balance_in_eth > 0 THEN cb.time_deposited ELSE 0 END) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) AS total_time_in_ever_eth
    FROM coin_balances_time_deposited_eth AS cb
), --select * from coin_balances_with_auc_and_time_in_ever_eth order by row_num_by_wallet desc

coin_balances_max_auc_and_time_in_ever as (
    SELECT
        wallet_address,
        MAX(total_auc_eth) AS total_auc_eth,
        MAX(total_time_in_ever_eth) AS total_time_in_ever_eth
    FROM coin_balances_with_auc_and_time_in_ever_eth
    GROUP BY wallet_address
),
latest_transactions AS (
    SELECT
        wallet_address,
        contract_address,
        timestamp as max_timestamp,
        transaction_index_by_transaction_type as max_transaction_index_by_transaction_type
    FROM (
        SELECT
            wallet_address,
            contract_address,
            timestamp,
            transaction_index_by_transaction_type,
            ROW_NUMBER() OVER (PARTITION BY wallet_address, contract_address ORDER BY timestamp DESC, transaction_index_by_transaction_type DESC) as rn
        FROM
            coin_balances_with_min_max
    ) t
    WHERE rn = 1
),
coin_balances_ranked as (
    SELECT
        cb.wallet_address,
        cb.contract_address,
        cb.token_symbol,
        CASE
            WHEN cb.total_balance_by_contract < 0
            THEN 0
            ELSE cb.total_balance_by_contract
        END AS total_balance_by_contract,
        CASE
            WHEN cb.total_balance_in_eth_by_contract < 0
            THEN 0
            ELSE cb.total_balance_in_eth_by_contract
        END AS total_balance_in_eth_by_contract,
        CASE
            WHEN cb.total_balance_in_eth < 0
            THEN 0
            ELSE cb.total_balance_in_eth
        END AS total_balance_in_eth,
        CASE WHEN cb.total_auc_contract < 0
            THEN 0
            ELSE cb.total_auc_contract
        END AS total_auc_contract,
        CASE WHEN auc.total_auc_eth < 0
            THEN 0
            ELSE auc.total_auc_eth
        END AS total_auc_eth,
        cb.total_time_in_ever_contract,
        auc.total_time_in_ever_eth,
        CASE WHEN cb.min_eth_balance_in_ever_by_contract < 0
            THEN 0
            ELSE cb.min_eth_balance_in_ever_by_contract
        END AS min_eth_balance_in_ever_by_contract,
        CASE WHEN cb.max_eth_balance_in_ever_by_contract < cb.min_eth_balance_in_ever_by_contract
            THEN cb.min_eth_balance_in_ever_by_contract
            ELSE cb.max_eth_balance_in_ever_by_contract
        END AS max_eth_balance_in_ever_by_contract,
        CASE WHEN cb.min_eth_balance_in_ever < 0
            THEN 0
            ELSE 0
        END AS min_eth_balance_in_ever,
        CASE WHEN cb.max_eth_balance_in_ever < cb.min_eth_balance_in_ever
            THEN cb.min_eth_balance_in_ever
            ELSE cb.max_eth_balance_in_ever
        END AS max_eth_balance_in_ever,
        cb.total_incoming_value_in_eth,
        cb.total_outgoing_value_in_eth,
        cb.total_tx_fee,
        cb.incoming_transactions_count,
        cb.outgoing_transactions_count,
        cb.transactions_count,
        cb.first_transaction_timestamp,
        cb.last_transaction_timestamp,
        -- CASE
        --     WHEN cb.total_balance_in_eth_by_contract < 0
        --     THEN true
        --     ELSE false
        -- END as has_negative_balance,
        DENSE_RANK() OVER(PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.row_num_by_contract DESC) as rank_by_contract,
        --DENSE_RANK() OVER(PARTITION BY cb.wallet_address ORDER BY cb.row_num_by_wallet DESC) as rank_by_wallet,
        cb.address_partition
    FROM coin_balances_with_auc_and_time_in_ever_by_contract AS cb
    INNER JOIN latest_transactions AS lt
        ON cb.wallet_address = lt.wallet_address
        AND cb.contract_address = lt.contract_address
        AND cb.timestamp = lt.max_timestamp
        AND cb.transaction_index_by_transaction_type = lt.max_transaction_index_by_transaction_type
    INNER JOIN coin_balances_max_auc_and_time_in_ever AS auc
        ON auc.wallet_address = cb.wallet_address
),
coin_balances_final AS (
    SELECT *
    FROM coin_balances_ranked
        WHERE rank_by_contract = 1
), --select * from coin_balances_final as cb

-- Define the contracts
contracts AS (
    SELECT
        wallet_address,
        contract_address,
        MAP(ARRAY[
            'total_balance', 'total_balance_in_eth','total_auc', 'total_time_in_ever', 'min_eth_balance_in_ever', 'max_eth_balance_in_ever', 'total_incoming_value_in_eth', 'total_outgoing_value_in_eth',
            'total_tx_fee', 'incoming_transactions_count', 'outgoing_transactions_count', 'transactions_count', 'first_transaction_timestamp', 'last_transaction_timestamp'],
            ARRAY[
                total_balance_by_contract, total_balance_in_eth_by_contract, total_auc_contract, total_time_in_ever_contract, min_eth_balance_in_ever_by_contract, min_eth_balance_in_ever_by_contract, total_incoming_value_in_eth, total_outgoing_value_in_eth,
                total_tx_fee, incoming_transactions_count, outgoing_transactions_count, transactions_count, first_transaction_timestamp, last_transaction_timestamp]
        ) AS contract_details
      FROM coin_balances_final
),
-- Define the wallet balances
wallet_balances as (
    SELECT
        cb.wallet_address,
        MAX(CASE
            WHEN cb.contract_address = 'ETH'
            THEN cb.total_balance_in_eth_by_contract
            ELSE 0
        END) AS wallet_total_balance_eth_only,
        -- COALESCE(MAX(CASE WHEN cb.rank_by_wallet = 1 THEN cb.total_balance_in_eth ELSE NULL END), 0) AS wallet_total_balance_in_eth_and_erc20,
        MAX(cb.total_balance_in_eth) AS wallet_total_balance_in_eth_and_erc20,
        MAX(cb.total_auc_eth) AS wallet_total_area_in_eth,
        MAX(cb.total_time_in_ever_eth) AS wallet_total_time_in_ever,
        MIN(min_eth_balance_in_ever) AS wallet_min_eth_ever,
        MAX(max_eth_balance_in_ever) AS wallet_max_eth_ever,
        MIN(first_transaction_timestamp) AS wallet_first_tx,
        MAX(last_transaction_timestamp) AS wallet_last_tx,
        SUM(incoming_transactions_count - outgoing_transactions_count) AS wallet_net_incoming_tx,
        SUM(incoming_transactions_count) AS wallet_total_incoming_tx,
        SUM(outgoing_transactions_count) AS wallet_total_outgoing_tx,
        SUM(total_incoming_value_in_eth) AS wallet_incoming_transactions_sum,
        SUM(total_outgoing_value_in_eth) AS wallet_outgoing_transactions_sum,
        AVG(total_incoming_value_in_eth) AS wallet_incoming_transactions_mean,
        AVG(total_outgoing_value_in_eth) AS wallet_outgoing_transactions_mean,
        SUM(total_tx_fee) as misc_total_fees_eth,
        CASE WHEN SUM(outgoing_transactions_count) = 0
            THEN 0
            ELSE SUM(total_tx_fee) / SUM(outgoing_transactions_count)
        END AS misc_avg_total_fees_eth,
        COUNT(cb.contract_address) AS number_of_contracts,
        MAP(ARRAY_AGG(c.contract_address), ARRAY_AGG(c.contract_details)) AS contracts_aggregations,
        current_timestamp(6) as inserted_at,
        current_timestamp(6) as updated_at,
        cb.address_partition
    FROM coin_balances_final as cb
    INNER JOIN contracts as c
        ON c.wallet_address = cb.wallet_address
        AND c.contract_address = cb.contract_address
    GROUP BY
        cb.wallet_address,
        cb.address_partition
        --cb.total_auc_eth,
        --cb.total_time_in_ever_eth
)

select * from wallet_balances

-- incremental load

MERGE INTO db_analytics_prod.ethereum_wallet_features
USING (
    WITH ranked_wallet_transactions as (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition
            ORDER BY wt.timestamp, wt.priority,
                CASE
                    WHEN wt.transaction_type = 'internal' THEN 999
                    WHEN wt.transaction_type = 'erc20' THEN 1000
                    ELSE wt.transaction_index
                END,
                CASE WHEN wt.address_role = 'receiver'
                THEN 0
                ELSE 1
                END
        ) as row_num_by_contract,
        ROW_NUMBER() OVER(PARTITION BY wt.wallet_address, wt.address_partition
            ORDER BY wt.timestamp, wt.priority,
                CASE
                    WHEN wt.transaction_type = 'internal' THEN 999
                    WHEN wt.transaction_type = 'erc20' THEN 1000
                    ELSE wt.transaction_index
                END,
                CASE WHEN wt.address_role = 'receiver'
                THEN 0
                ELSE 1
                END
        ) as row_num_by_wallet,
        CASE
            WHEN wt.transaction_type = 'internal' THEN 999
            WHEN wt.transaction_type = 'erc20' THEN 1000
            ELSE wt.transaction_index
        END AS transaction_index_by_transaction_type,
        DENSE_RANK() OVER (PARTITION BY wt.wallet_address, wt.address_partition, wt.hash ORDER BY wt.priority asc) as hash_rank
        FROM db_analytics_prod.ethereum_wallet_transactions AS wt
            where wt.timestamp > filter_value
            and wt.date_partition >= DATE_FORMAT(FROM_UNIXTIME(filter_value), '%Y-%m')
            and address_partition in chunk
            --and wt.wallet_address in (select wallet_address from db_sandbox_prod.test_set_wallet_addresses) -- only wallets from test se
    ), --select * from ranked_wallet_transactions

    last_coin_balances_and_wallet_balances as (
        SELECT
            wallet_address,
            contract_address,
            address_partition,
            total_balance_in_eth,
            total_auc_eth,
            total_time_in_ever_eth,
            min_eth_balance_in_ever,
            max_eth_balance_in_ever,
            CAST(CAST(json_extract_scalar(json, '$.first_transaction_timestamp') AS double) AS BIGINT) AS first_transaction_timestamp,
            CAST(CAST(json_extract_scalar(json, '$.last_transaction_timestamp') AS double) AS BIGINT) AS last_transaction_timestamp,
            CAST(json_extract_scalar(json, '$.max_eth_balance_in_ever') AS double) AS max_eth_balance_in_ever_by_contract,
            CAST(json_extract_scalar(json, '$.min_eth_balance_in_ever') AS double) AS min_eth_balance_in_ever_by_contract,
            CAST(json_extract_scalar(json, '$.total_balance') AS double) AS total_balance_by_contract,
            CAST(json_extract_scalar(json, '$.total_balance_in_eth') AS double) AS total_balance_in_eth_by_contract,
            CAST(json_extract_scalar(json, '$.total_auc') AS double) AS total_auc_contract,
            CAST(json_extract_scalar(json, '$.total_time_in_ever') AS double) AS total_time_in_ever_contract,
            CAST(json_extract_scalar(json, '$.total_incoming_value_in_eth') AS double) AS total_incoming_value_in_eth,
            CAST(json_extract_scalar(json, '$.total_outgoing_value_in_eth') AS double) AS total_outgoing_value_in_eth,
            CAST(json_extract_scalar(json, '$.total_tx_fee') AS double) AS total_tx_fee,
            CAST(CAST(json_extract_scalar(json, '$.incoming_transactions_count') AS double) AS bigint) AS incoming_transactions_count,
            CAST(CAST(json_extract_scalar(json, '$.outgoing_transactions_count') AS double) AS bigint) AS outgoing_transactions_count,
            CAST(CAST(json_extract_scalar(json, '$.transactions_count') AS double) AS bigint) AS transactions_count,
            ROW_NUMBER() OVER (PARTITION BY wallet_address ORDER BY CAST(CAST(json_extract_scalar(json, '$.last_transaction_timestamp') AS double) AS BIGINT) DESC) AS rn
        FROM (
            SELECT
                wallet_address,
                address_partition,
                wallet_total_balance_in_eth_and_erc20 as total_balance_in_eth,
                wallet_total_area_in_eth as total_auc_eth,
                wallet_total_time_in_ever as total_time_in_ever_eth,
                wallet_min_eth_ever as min_eth_balance_in_ever,
                wallet_max_eth_ever as max_eth_balance_in_ever,
                p.column AS contract_address,
                CAST(p.value AS JSON) AS json
            FROM db_analytics_prod.ethereum_wallet_features as lcb
            CROSS JOIN UNNEST(contracts_aggregations) AS p (column, value)
            WHERE EXISTS (SELECT 1 FROM ranked_wallet_transactions as tb where tb.wallet_address = lcb.wallet_address and tb.address_partition = lcb.address_partition)
        ) t
    ), --select * from last_coin_balances_and_wallet_balances --where contract_address = '0x177d39ac676ed1c67a2b268ad7f1e58826e5b0af'

    --SELECT wallet_address, total_balance_in_eth FROM last_coin_balances_and_wallet_balances WHERE rn = 1
    --select * from  last_coin_balances_and_wallet_balances --where contract_address = '0x15f173b7aca7cd4a01d6f8360e65fb4491d270c1'

    daily_token_prices AS (
        SELECT
            DISTINCT
            tp.address,
            tp.price,
            tp.timestamp
        FROM db_analytics_prod.features_daily_token_prices as tp
        INNER JOIN ranked_wallet_transactions as cb
            ON tp.address = cb.contract_address
            AND date(from_unixtime(tp.timestamp)) <= date(from_unixtime(cb.timestamp))
            AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(cb.timestamp)) <= 7
    ),

    daily_token_prices_ranked AS (
        SELECT
            tp.address,
            CASE WHEN tp.address = 'ETH'
                THEN 1
                ELSE tp.price
            END as price,
            tp.timestamp,
            cb.timestamp AS cb_timestamp,
            ROW_NUMBER() OVER (PARTITION BY cb.contract_address, cb.timestamp ORDER BY ABS(date_diff('second', from_unixtime(tp.timestamp), from_unixtime(cb.timestamp)))) AS rank
        FROM ranked_wallet_transactions AS cb
        CROSS JOIN daily_token_prices AS tp
        WHERE tp.address = cb.contract_address
            --AND date(from_unixtime(tp.timestamp)) <= date(from_unixtime(cb.timestamp))
            AND date_diff('day', from_unixtime(tp.timestamp), from_unixtime(cb.timestamp)) <= 7
    ), --select * from daily_token_prices_ranked where rank = 1

    coin_balances AS (
        SELECT
            wt.wallet_address,
            wt.contract_address,
            wt.token_symbol,
            wt.timestamp,
            wt.hash,
            wt.priority,
            wt.transaction_index_by_transaction_type,
            wt.address_role,
            wt.current_value,
            rp.price,
            wt.current_value * COALESCE(rp.price, 0) as current_value_in_eth,
            wt.tx_fee,
            CAST(CASE WHEN
                ROUND(
                    SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
                    OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract)
                    + COALESCE(lcb.total_balance_by_contract, 0), 5) = -0.0
                THEN 0.0
                ELSE ROUND(
                    SUM(CASE WHEN wt.address_role = 'sender' THEN -wt.current_value - wt.tx_fee ELSE wt.current_value END)
                    OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract)
                    + COALESCE(lcb.total_balance_by_contract, 0), 5)
            END AS DOUBLE) AS total_balance_by_contract,
            CAST(CASE WHEN
                ROUND(
                    SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) END)
                    OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract)
                    + COALESCE(lcb.total_balance_in_eth_by_contract, 0), 5) = -0.0
                THEN 0.0
                ELSE ROUND(
                    SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) END)
                    OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_contract)
                    + COALESCE(lcb.total_balance_in_eth_by_contract, 0), 5)
            END AS DOUBLE) AS total_balance_in_eth_by_contract,
            COALESCE(lcb.min_eth_balance_in_ever_by_contract, 0) AS min_eth_balance_in_ever_by_contract,
            COALESCE(lcb.max_eth_balance_in_ever_by_contract, 0) AS max_eth_balance_in_ever_by_contract,
            CAST(CASE WHEN
                ROUND(
                    SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, 0) END)
                    OVER (PARTITION BY wt.wallet_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_wallet)
                    + COALESCE(lcb_2.total_balance_in_eth, 0), 5) = -0.0
                THEN 0.0
                ELSE ROUND(
                    SUM(CASE WHEN wt.address_role = 'sender' THEN (-wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END)) - wt.tx_fee ELSE wt.current_value * COALESCE(rp.price, 0) END)
                    OVER (PARTITION BY wt.wallet_address, wt.address_partition ORDER BY wt.timestamp, wt.priority, wt.transaction_index_by_transaction_type, wt.row_num_by_wallet)
                    + COALESCE(lcb_2.total_balance_in_eth, 0), 5)
            END AS DOUBLE) AS total_balance_in_eth,
            COALESCE(lcb_2.min_eth_balance_in_ever, 0) AS min_eth_balance_in_ever,
            COALESCE(lcb_2.max_eth_balance_in_ever, 0) AS max_eth_balance_in_ever,
            SUM(CASE WHEN wt.address_role = 'receiver' THEN wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) ELSE 0 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) + COALESCE(lcb.total_incoming_value_in_eth, 0) AS total_incoming_value_in_eth,
            SUM(CASE WHEN wt.address_role = 'sender' THEN wt.current_value * COALESCE(rp.price, CASE WHEN wt.contract_address = 'ETH' THEN 1 ELSE 0 END) ELSE 0 END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) + COALESCE(lcb.total_outgoing_value_in_eth, 0) AS total_outgoing_value_in_eth,
            SUM(wt.tx_fee)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) + COALESCE(lcb.total_tx_fee, 0) AS total_tx_fee,
            COUNT(CASE WHEN wt.address_role = 'receiver' AND wt.hash_rank = 1 THEN wt.hash ELSE NULL END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) + COALESCE(lcb.incoming_transactions_count, 0) AS incoming_transactions_count,
            COUNT(CASE WHEN wt.address_role = 'sender' AND wt.hash_rank = 1 THEN wt.hash ELSE NULL END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) + COALESCE(lcb.outgoing_transactions_count, 0) AS outgoing_transactions_count,
            COUNT(CASE WHEN wt.hash_rank = 1 THEN wt.hash ELSE NULL END)
            OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition) + COALESCE(lcb.transactions_count, 0) AS transactions_count,
            COALESCE(lcb.first_transaction_timestamp, MIN(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition)) AS first_transaction_timestamp,
            COALESCE(
                MAX(wt.timestamp) OVER (PARTITION BY wt.wallet_address, wt.contract_address, wt.address_partition),
                lcb.last_transaction_timestamp
            ) AS last_transaction_timestamp,
            wt.row_num_by_contract,
            wt.row_num_by_wallet,
            wt.address_partition
        FROM ranked_wallet_transactions AS wt
        LEFT JOIN last_coin_balances_and_wallet_balances AS lcb -- left join by wallet_address and contract_address
            ON wt.wallet_address = lcb.wallet_address AND wt.contract_address = lcb.contract_address
        LEFT JOIN (SELECT wallet_address, total_balance_in_eth, min_eth_balance_in_ever, max_eth_balance_in_ever FROM last_coin_balances_and_wallet_balances WHERE rn = 1) AS lcb_2 -- left join only by wallet_address
            ON wt.wallet_address = lcb_2.wallet_address
        LEFT JOIN daily_token_prices_ranked AS rp
            ON rp.address = wt.contract_address
            AND rp.cb_timestamp = wt.timestamp
            AND rp.rank = 1
    ),

    coin_balances_with_min_max as (
        SELECT
            cb.wallet_address,
            cb.contract_address,
            cb.token_symbol,
            cb.timestamp,
            cb.hash,
            cb.transaction_index_by_transaction_type,
            cb.current_value,
            cb.current_value_in_eth,
            cb.total_balance_by_contract,
            cb.total_balance_in_eth_by_contract,
            cb.total_balance_in_eth,
            -- Compare and select the smaller value for min_eth_balance_in_ever_by_contract
            CASE
                WHEN cb.min_eth_balance_in_ever_by_contract IS NULL OR
                    cb.min_eth_balance_in_ever_by_contract > COALESCE(MIN(CASE WHEN cb.total_balance_in_eth_by_contract > 0 THEN cb.total_balance_in_eth_by_contract ELSE NULL END) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition), 0)
                THEN COALESCE(MIN(CASE WHEN cb.total_balance_in_eth_by_contract > 0 THEN cb.total_balance_in_eth_by_contract ELSE NULL END) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition), 0)
                ELSE cb.min_eth_balance_in_ever_by_contract
            END AS min_eth_balance_in_ever_by_contract,
            -- Compare and select the larger value for max_eth_balance_in_ever_by_contract
            CASE
                WHEN cb.max_eth_balance_in_ever_by_contract IS NULL OR
                    cb.max_eth_balance_in_ever_by_contract < MAX(cb.total_balance_in_eth_by_contract) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition)
                THEN MAX(cb.total_balance_in_eth_by_contract) OVER (PARTITION BY cb.wallet_address, cb.contract_address, cb.address_partition)
                ELSE cb.max_eth_balance_in_ever_by_contract
            END AS max_eth_balance_in_ever_by_contract,
            -- Compare and select the smaller value for min_eth_balance_in_ever
            CASE
                WHEN cb.min_eth_balance_in_ever IS NULL OR
                    cb.min_eth_balance_in_ever > COALESCE(MIN(CASE WHEN cb.total_balance_in_eth > 0 THEN cb.total_balance_in_eth ELSE NULL END) OVER (PARTITION BY cb.wallet_address, cb.address_partition), 0)
                THEN COALESCE(MIN(CASE WHEN cb.total_balance_in_eth > 0 THEN cb.total_balance_in_eth ELSE NULL END) OVER (PARTITION BY cb.wallet_address, cb.address_partition), 0)
                ELSE cb.min_eth_balance_in_ever
            END AS min_eth_balance_in_ever,
            -- Compare and select the larger value for max_eth_balance_in_ever
            CASE
                WHEN cb.max_eth_balance_in_ever IS NULL OR
                    cb.max_eth_balance_in_ever < MAX(cb.total_balance_in_eth) OVER (PARTITION BY cb.wallet_address, cb.address_partition)
                THEN MAX(cb.total_balance_in_eth) OVER (PARTITION BY cb.wallet_address, cb.address_partition)
                ELSE cb.max_eth_balance_in_ever
            END AS max_eth_balance_in_ever,
            cb.total_incoming_value_in_eth,
            cb.total_outgoing_value_in_eth,
            cb.total_tx_fee,
            cb.incoming_transactions_count,
            cb.outgoing_transactions_count,
            cb.transactions_count,
            cb.first_transaction_timestamp,
            cb.last_transaction_timestamp,
            cb.row_num_by_contract,
            cb.row_num_by_wallet,
            cb.address_partition
        FROM coin_balances AS cb
    ), --select * from coin_balances_with_min_max --order by row_num desc

    coin_balances_time_deposited_by_contract AS (
        SELECT
            cb.*,
            CASE
                WHEN cb.timestamp = FIRST_VALUE(cb.timestamp) OVER (PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_contract)
                THEN cb.timestamp - COALESCE((SELECT last_transaction_timestamp FROM last_coin_balances_and_wallet_balances WHERE contract_address = cb.contract_address AND wallet_address = cb.wallet_address), cb.timestamp)
                ELSE
                    COALESCE(LEAD(cb.timestamp) OVER (PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_contract) - cb.timestamp, 0)
            END AS time_deposited
        FROM coin_balances_with_min_max AS cb
    ), --select * from coin_balances_time_deposited_by_contract

    coin_balances_with_auc_and_time_in_ever_by_contract AS (
        SELECT
            cb.*,
            coalesce(lcb.total_auc_contract, 0) +
            (SUM(cb.time_deposited * cb.total_balance_in_eth_by_contract) OVER (PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_contract) / 60 / 60 / 24) as total_auc_contract,
            coalesce(lcb.total_time_in_ever_contract, 0) +
            SUM(CASE WHEN cb.total_balance_in_eth_by_contract > 0 THEN cb.time_deposited ELSE 0 END) OVER (PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_contract) AS total_time_in_ever_contract
        FROM coin_balances_time_deposited_by_contract AS cb
        LEFT JOIN last_coin_balances_and_wallet_balances as lcb
            ON cb.wallet_address = lcb.wallet_address
            AND cb.contract_address = lcb.contract_address
        -- LEFT JOIN (
        --     SELECT wallet_address, contract_address, total_balance_in_eth, last_transaction_timestamp, total_auc_contract, total_time_in_ever_contract,
        --         ROW_NUMBER() OVER (PARTITION BY wallet_address, contract_address ORDER BY last_transaction_timestamp DESC) AS rn
        --     FROM last_coin_balances_and_wallet_balances
        -- ) AS lcb
        -- ON cb.wallet_address = lcb.wallet_address
        -- AND cb.contract_address = lcb.contract_address
        -- WHERE lcb.rn = 1 OR lcb.rn IS NULL
    ), --select * from coin_balances_with_auc_and_time_in_ever_by_contract -- where contract_address = '0x15f173b7aca7cd4a01d6f8360e65fb4491d270c1' --where total_auc_contract > 0 and contract_address <> 'ETH' order by timestamp desc

    coin_balances_time_deposited_eth as (
        SELECT
            cb.*,
            COALESCE(LEAD(cb.timestamp) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) - cb.timestamp, 0) AS time_deposited
        FROM coin_balances_with_min_max AS cb
    ), --select * from coin_balances_time_deposited_eth

    -- coin_balances_time_deposited_eth AS (
    --     SELECT
    --         cb.*,
    --         CASE
    --             WHEN cb.timestamp = FIRST_VALUE(cb.timestamp) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) THEN
    --                 cb.timestamp - COALESCE((SELECT MAX(last_transaction_timestamp) FROM last_coin_balances_and_wallet_balances WHERE wallet_address = cb.wallet_address), cb.timestamp)
    --             ELSE
    --                 COALESCE(LEAD(cb.timestamp) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) - cb.timestamp, 0)
    --         END AS time_deposited
    --     FROM coin_balances_with_min_max AS cb
    -- ),

    coin_balances_with_auc_and_time_in_ever_eth AS (
        SELECT
            cb.*,
            coalesce(lcb.total_auc_eth, 0) +
            (SUM(cb.time_deposited * cb.total_balance_in_eth) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, cb.transaction_index_by_transaction_type, cb.row_num_by_wallet) / 60 / 60 / 24) as total_auc_eth,
            coalesce(lcb.total_time_in_ever_eth, 0) +
            SUM(CASE WHEN cb.total_balance_in_eth > 0 THEN cb.time_deposited ELSE 0 END) OVER (PARTITION BY cb.wallet_address ORDER BY cb.timestamp, transaction_index_by_transaction_type, cb.row_num_by_wallet) AS total_time_in_ever_eth
        FROM coin_balances_time_deposited_eth AS cb
        LEFT JOIN (
            SELECT wallet_address, total_balance_in_eth, total_auc_eth, total_time_in_ever_eth,
                ROW_NUMBER() OVER (PARTITION BY wallet_address ORDER BY last_transaction_timestamp DESC) AS rn
            FROM last_coin_balances_and_wallet_balances
        ) AS lcb
        ON cb.wallet_address = lcb.wallet_address
        WHERE lcb.rn = 1 OR lcb.rn IS NULL
        --JOIN last_values_table AS lv ON cb.wallet_address = lv.wallet_address AND cb.contract_address = lv.contract_address
    ), --select * from coin_balances_with_auc_and_time_in_ever_eth order by row_num_by_wallet

    coin_balances_max_auc_and_time_in_ever as (
        SELECT
            wallet_address,
            MAX(total_auc_eth) AS total_auc_eth,
            MAX(total_time_in_ever_eth) AS total_time_in_ever_eth
        FROM coin_balances_with_auc_and_time_in_ever_eth
        GROUP BY wallet_address
    ),
    latest_transactions AS (
        SELECT
            wallet_address,
            contract_address,
            timestamp as max_timestamp,
            transaction_index_by_transaction_type as max_transaction_index
        FROM (
            SELECT
                wallet_address,
                contract_address,
                timestamp,
                transaction_index_by_transaction_type,
                ROW_NUMBER() OVER (PARTITION BY wallet_address, contract_address ORDER BY timestamp DESC, transaction_index_by_transaction_type DESC) as rn
            FROM
                coin_balances_with_min_max
        ) t
        WHERE rn = 1
    ),

    -- Union of coin balances of new and old contracts (old means from the last inserted)
    union_new_and_old_balances as (
        SELECT
            cb.wallet_address,
            cb.contract_address,
            CASE
                WHEN cb.total_balance_by_contract < 0
                THEN 0
                ELSE cb.total_balance_by_contract
            END AS total_balance_by_contract,
            CASE
                WHEN cb.total_balance_in_eth_by_contract < 0
                THEN 0
                ELSE cb.total_balance_in_eth_by_contract
            END AS total_balance_in_eth_by_contract,
            CASE
                WHEN cb.total_balance_in_eth < 0
                THEN 0
                ELSE cb.total_balance_in_eth
            END AS total_balance_in_eth,
            CASE WHEN cb.total_auc_contract < 0
                THEN 0
                ELSE cb.total_auc_contract
            END AS total_auc_contract,
            CASE WHEN auc.total_auc_eth < 0
                THEN 0
                ELSE auc.total_auc_eth
            END AS total_auc_eth,
            cb.total_time_in_ever_contract,
            auc.total_time_in_ever_eth,
            CASE WHEN cb.min_eth_balance_in_ever_by_contract < 0
                THEN 0
                ELSE cb.min_eth_balance_in_ever_by_contract
            END AS min_eth_balance_in_ever_by_contract,
            CASE WHEN cb.max_eth_balance_in_ever_by_contract < cb.min_eth_balance_in_ever_by_contract
                THEN cb.min_eth_balance_in_ever_by_contract
                ELSE cb.max_eth_balance_in_ever_by_contract
            END AS max_eth_balance_in_ever_by_contract,
            CASE WHEN cb.min_eth_balance_in_ever < 0
                THEN 0
                ELSE 0
            END AS min_eth_balance_in_ever,
            CASE WHEN cb.max_eth_balance_in_ever < cb.min_eth_balance_in_ever
                THEN cb.min_eth_balance_in_ever
                ELSE cb.max_eth_balance_in_ever
            END AS max_eth_balance_in_ever,
            cb.total_incoming_value_in_eth,
            cb.total_outgoing_value_in_eth,
            cb.total_tx_fee,
            cb.incoming_transactions_count,
            cb.outgoing_transactions_count,
            cb.transactions_count,
            cb.first_transaction_timestamp,
            cb.last_transaction_timestamp,
            DENSE_RANK() OVER(PARTITION BY cb.wallet_address, cb.contract_address ORDER BY cb.row_num_by_contract DESC) as rank_by_contract,
            cb.address_partition
        FROM coin_balances_with_auc_and_time_in_ever_by_contract AS cb
        INNER JOIN latest_transactions AS lt
            ON cb.wallet_address = lt.wallet_address
            AND cb.contract_address = lt.contract_address
            AND cb.timestamp = lt.max_timestamp
            AND cb.transaction_index_by_transaction_type = lt.max_transaction_index
        INNER JOIN coin_balances_max_auc_and_time_in_ever AS auc
            ON auc.wallet_address = cb.wallet_address

        UNION ALL

        SELECT
            lcb.wallet_address,
            lcb.contract_address,
            lcb.total_balance_by_contract,
            lcb.total_balance_in_eth_by_contract,
            0 total_balance_in_eth,
            lcb.total_auc_contract,
            0 total_auc_eth,
            lcb.total_time_in_ever_contract,
            0 total_time_in_ever_eth,
            lcb.min_eth_balance_in_ever_by_contract,
            lcb.max_eth_balance_in_ever_by_contract,
            0 min_eth_balance_in_ever,
            0 max_eth_balance_in_ever,
            lcb.total_incoming_value_in_eth,
            lcb.total_outgoing_value_in_eth,
            lcb.total_tx_fee,
            lcb.incoming_transactions_count,
            lcb.outgoing_transactions_count,
            lcb.transactions_count,
            lcb.first_transaction_timestamp,
            lcb.last_transaction_timestamp,
            1 rank_by_contract,
            lcb.address_partition
        FROM last_coin_balances_and_wallet_balances AS lcb
        LEFT JOIN coin_balances_with_auc_and_time_in_ever_by_contract AS cb
            ON lcb.wallet_address = cb.wallet_address AND lcb.contract_address = cb.contract_address
        WHERE NOT EXISTS (
            SELECT 1
                FROM coin_balances_with_auc_and_time_in_ever_by_contract AS cb
            WHERE lcb.wallet_address = cb.wallet_address AND lcb.contract_address = cb.contract_address --AND ncb.rank = 1
        )
    ), --select * from union_new_and_old_balances

    -- Define the contracts
    contracts AS (
        SELECT
            wallet_address,
            contract_address,
            MAP(ARRAY[
                'total_balance', 'total_balance_in_eth','total_auc', 'total_time_in_ever', 'min_eth_balance_in_ever', 'max_eth_balance_in_ever', 'total_incoming_value_in_eth', 'total_outgoing_value_in_eth',
                'total_tx_fee', 'incoming_transactions_count', 'outgoing_transactions_count', 'transactions_count', 'first_transaction_timestamp', 'last_transaction_timestamp'],
                ARRAY[
                    total_balance_by_contract, total_balance_in_eth_by_contract, total_auc_contract, total_time_in_ever_contract, min_eth_balance_in_ever_by_contract, min_eth_balance_in_ever_by_contract, total_incoming_value_in_eth, total_outgoing_value_in_eth,
                    total_tx_fee, incoming_transactions_count, outgoing_transactions_count, transactions_count, first_transaction_timestamp, last_transaction_timestamp]
            ) AS contract_details
        FROM union_new_and_old_balances
            WHERE rank_by_contract = 1
    ),--select * from contracts --where contract_address = '0xb3bd49e28f8f832b8d1e246106991e546c323502'

    -- Define the wallet balances
    contracts_aggregations AS (
        SELECT
            c.wallet_address,
            COUNT(c.contract_address) AS number_of_contracts,
            MAP(ARRAY_AGG(c.contract_address), ARRAY_AGG(c.contract_details)) AS contracts_aggregations
        FROM contracts as c
        GROUP BY c.wallet_address
    ),
    wallet_balances as (
        SELECT
            cb.wallet_address,
            MAX(CASE
                WHEN cb.contract_address = 'ETH'
                THEN cb.total_balance_in_eth_by_contract
                ELSE 0
            END) AS wallet_total_balance_eth_only,
            --COALESCE(MAX(CASE WHEN cb.rank_by_wallet = 1 THEN cb.total_balance_in_eth ELSE NULL END), 0) AS wallet_total_balance_in_eth_and_erc20,
            MAX(cb.total_balance_in_eth) AS wallet_total_balance_in_eth_and_erc20,
            MAX(cb.total_auc_eth) AS wallet_total_area_in_eth,
            MAX(cb.total_time_in_ever_eth) AS wallet_total_time_in_ever,
            MIN(min_eth_balance_in_ever) AS wallet_min_eth_ever,
            MAX(max_eth_balance_in_ever) AS wallet_max_eth_ever,
            MIN(first_transaction_timestamp) AS wallet_first_tx,
            MAX(last_transaction_timestamp) AS wallet_last_tx,
            SUM(incoming_transactions_count - outgoing_transactions_count) AS wallet_net_incoming_tx,
            SUM(incoming_transactions_count) AS wallet_total_incoming_tx,
            SUM(outgoing_transactions_count) AS wallet_total_outgoing_tx,
            SUM(total_incoming_value_in_eth) AS wallet_incoming_transactions_sum,
            SUM(total_outgoing_value_in_eth) AS wallet_outgoing_transactions_sum,
            AVG(total_incoming_value_in_eth) AS wallet_incoming_transactions_mean,
            AVG(total_outgoing_value_in_eth) AS wallet_outgoing_transactions_mean,
            SUM(total_tx_fee) as misc_total_fees_eth,
            CASE WHEN SUM(outgoing_transactions_count) = 0
                THEN 0
                ELSE SUM(total_tx_fee) / SUM(outgoing_transactions_count)
            END AS misc_avg_total_fees_eth,
            c.number_of_contracts,
            c.contracts_aggregations,
            current_timestamp(6) as inserted_at,
            current_timestamp(6) as updated_at,
            cb.address_partition
        FROM union_new_and_old_balances as cb
        INNER JOIN contracts_aggregations as c
            ON c.wallet_address = cb.wallet_address
        WHERE cb.rank_by_contract = 1
        GROUP BY
            cb.wallet_address,
            c.contracts_aggregations,
            c.number_of_contracts,
            cb.address_partition
    )

    select * from wallet_balances where address_partition in chunk
    ) wallet_features_updated
ON wallet_features_updated.wallet_address = ethereum_wallet_features.wallet_address
WHEN MATCHED THEN
    UPDATE SET
        wallet_total_balance_eth_only = wallet_features_updated.wallet_total_balance_eth_only,
        wallet_total_balance_in_eth_and_erc20 = wallet_features_updated.wallet_total_balance_in_eth_and_erc20,
        wallet_total_area_in_eth = wallet_features_updated.wallet_total_area_in_eth,
        wallet_total_time_in_ever = wallet_features_updated.wallet_total_time_in_ever,
        wallet_min_eth_ever = wallet_features_updated.wallet_min_eth_ever,
        wallet_max_eth_ever = wallet_features_updated.wallet_max_eth_ever,
        wallet_first_tx = wallet_features_updated.wallet_first_tx,
        wallet_last_tx = wallet_features_updated.wallet_last_tx,
        wallet_net_incoming_tx = wallet_features_updated.wallet_net_incoming_tx,
        wallet_total_incoming_tx = wallet_features_updated.wallet_total_incoming_tx,
        wallet_total_outgoing_tx = wallet_features_updated.wallet_total_outgoing_tx,
        wallet_incoming_transactions_sum = wallet_features_updated.wallet_incoming_transactions_sum,
        wallet_outgoing_transactions_sum = wallet_features_updated.wallet_outgoing_transactions_sum,
        wallet_incoming_transactions_mean = wallet_features_updated.wallet_incoming_transactions_mean,
        wallet_outgoing_transactions_mean = wallet_features_updated.wallet_outgoing_transactions_mean,
        misc_total_fees_eth = wallet_features_updated.misc_total_fees_eth,
        misc_avg_total_fees_eth = wallet_features_updated.misc_avg_total_fees_eth,
        number_of_contracts = wallet_features_updated.number_of_contracts,
        contracts_aggregations = wallet_features_updated.contracts_aggregations,
        updated_at = current_timestamp(6)
WHEN NOT MATCHED THEN
    INSERT (
        wallet_address,
        wallet_total_balance_eth_only,
        wallet_total_balance_in_eth_and_erc20,
        wallet_total_area_in_eth,
        wallet_total_time_in_ever,
        wallet_min_eth_ever,
        wallet_max_eth_ever,
        wallet_first_tx,
        wallet_last_tx,
        wallet_net_incoming_tx,
        wallet_total_incoming_tx,
        wallet_total_outgoing_tx,
        wallet_incoming_transactions_sum,
        wallet_outgoing_transactions_sum,
        wallet_incoming_transactions_mean,
        wallet_outgoing_transactions_mean,
        misc_total_fees_eth,
        misc_avg_total_fees_eth,
        number_of_contracts,
        contracts_aggregations,
        inserted_at,
        updated_at,
        address_partition
    )
    VALUES (
        wallet_features_updated.wallet_address,
        wallet_features_updated.wallet_total_balance_eth_only,
        wallet_features_updated.wallet_total_balance_in_eth_and_erc20,
        wallet_features_updated.wallet_total_area_in_eth,
        wallet_features_updated.wallet_total_time_in_ever,
        wallet_features_updated.wallet_min_eth_ever,
        wallet_features_updated.wallet_max_eth_ever,
        wallet_features_updated.wallet_first_tx,
        wallet_features_updated.wallet_last_tx,
        wallet_features_updated.wallet_net_incoming_tx,
        wallet_features_updated.wallet_total_incoming_tx,
        wallet_features_updated.wallet_total_outgoing_tx,
        wallet_features_updated.wallet_incoming_transactions_sum,
        wallet_features_updated.wallet_outgoing_transactions_sum,
        wallet_features_updated.wallet_incoming_transactions_mean,
        wallet_features_updated.wallet_outgoing_transactions_mean,
        wallet_features_updated.misc_total_fees_eth,
        wallet_features_updated.misc_avg_total_fees_eth,
        wallet_features_updated.number_of_contracts,
        wallet_features_updated.contracts_aggregations,
        current_timestamp(6),
        current_timestamp(6),
        wallet_features_updated.address_partition
    )
