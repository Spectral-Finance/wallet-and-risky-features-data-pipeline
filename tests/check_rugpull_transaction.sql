-- Normal Transactions
-- Check if the transaction is a rugpull transaction

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
    FROM db_stage_prod.ethereum_transactions ts
    LEFT JOIN db_stage_prod.ethereum_blocks as b ON b.number = ts.block_number
        AND b.date_partition = ts.date_partition
    LEFT JOIN db_stage_prod.ethereum_traces tc ON tc.transaction_hash = ts.hash
        AND tc.block_number = ts.block_number
        AND tc.date_partition = ts.date_partition
        AND tc.from_address = ts.from_address
        AND tc.to_address = ts.to_address
    WHERE ts.hash in ('0x7ebe9b9eee2cd4c93564f8e1370ab8859d00950e5ba61fd83cbf61fa9046e65a', '0xe7d18bf2af3d0fd6970d472635188fa1033075b74c699ec04cf943c4f488f603') and ts.date_partition = '2020-06'
),
rugpull_address_list as (
    SELECT
        date("date") as datetime,
        lower(address) as address
    FROM db_analytics_prod.rugpull_transpose_market_data
    CROSS JOIN UNNEST(rugpull_address_list) as t(address)
),
normal_transactions_final AS (
    SELECT DISTINCT
        t.block_number,
        t.timestamp,
        t.hash,
        t.block_hash,
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
            WHEN r_1.address IS NOT NULL THEN true
            ELSE false
        END AS is_rugpull,
        r_1.datetime as is_rugpull_date,
        CASE
            WHEN r_2.address IS NOT NULL THEN true
            ELSE false
        END AS to_address_is_rugpull,
        r_2.datetime as to_address_is_rugpulll_date,
        SUBSTR(t.from_address, 3, 2) AS from_hash_partition,
        SUBSTR(t.to_address, 3, 2) AS to_hash_partition,
        t.date_partition
    FROM normal_transactions_source AS t
    LEFT JOIN db_stage_prod.ethereum_contracts AS c_1 -- when the from address is a contract
        ON c_1.address = t.from_address
        AND c_1.hash_partition = SUBSTR(t.from_address, 3, 2)
    LEFT JOIN db_stage_prod.ethereum_contracts AS c_2 -- when the to address is a contract
        ON c_2.address = t.to_address
        AND c_2.hash_partition = SUBSTR(t.to_address, 3, 2)
    	LEFT JOIN rugpull_address_list as r_1 -- when is rugpull
    	ON r_1.address = t.from_address
    	and ((r_1.datetime < date(from_unixtime(t.timestamp)) and r_1.datetime >= date(from_unixtime(t.timestamp)) - interval '1' day) or (r_1.datetime = date(from_unixtime(t.timestamp))))
    	LEFT JOIN rugpull_address_list as r_2 -- when to_address is rugpull
    	ON r_2.address = t.to_address
    	and ((r_2.datetime < date(from_unixtime(t.timestamp)) and r_2.datetime >= date(from_unixtime(t.timestamp)) - interval '1' day) or (r_2.datetime = date(from_unixtime(t.timestamp))))
)

SELECT CAST(uuid() AS varchar) AS uuid, * FROM normal_transactions_final;
