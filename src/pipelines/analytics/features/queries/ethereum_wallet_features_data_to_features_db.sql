SELECT
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
  contracts_aggregations
FROM db_analytics_prod.ethereum_wallet_features
    WHERE wallet_last_tx > last_inserted_timestamp
