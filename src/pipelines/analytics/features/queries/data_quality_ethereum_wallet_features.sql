WITH violation_outgoing_tx_negative_counts_check AS (
    SELECT 'outgoing_tx_negative_counts_check' AS constraint_name,
           CASE
               WHEN COUNT(*) > 0 THEN True
               ELSE False
           END AS is_fail
    FROM db_analytics_prod.ethereum_wallet_features
    WHERE wallet_total_outgoing_tx < 0
),
violation_incoming_tx_negative_counts_check AS (
    SELECT 'incoming_tx_negative_counts_check' AS constraint_name,
           CASE
               WHEN COUNT(*) > 0 THEN True
               ELSE False
           END AS is_fail
    FROM db_analytics_prod.ethereum_wallet_features
    WHERE wallet_total_incoming_tx < 0
),
violation_min_eth_ever_check AS (
    SELECT 'zero_min_eth_ever_for_20%_addresses_check' AS constraint_name,
           CASE
               WHEN (CAST(COUNT(CASE WHEN wallet_min_eth_ever = 0 THEN 1 ELSE NULL END) as double) / COUNT(*) * 100) > 20 THEN True
               ELSE False
           END AS is_fail
    FROM db_analytics_prod.ethereum_wallet_features
)

SELECT * FROM violation_outgoing_tx_negative_counts_check
UNION ALL
SELECT * FROM violation_incoming_tx_negative_counts_check
UNION ALL
SELECT * FROM violation_min_eth_ever_check;
