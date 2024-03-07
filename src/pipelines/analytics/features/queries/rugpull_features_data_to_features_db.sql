-- latest_modified_data
SELECT
    wallet_address,
    total_amount_chain,
    all_transaction_count,
    no_of_interaction,
    last_interaction_timestamp,
    first_interaction_timestamp
FROM
    db_analytics_prod.rugpull_features
    WHERE last_interaction_timestamp > last_inserted_timestamp
