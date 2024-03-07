-- optimize
optimize db_analytics_prod.table_name rewrite data using bin_pack
where
    address_partition = filter_value ;

-- vacuum
ALTER TABLE
    db_analytics_prod.table_name
SET
    TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '259200');

vacuum db_analytics_prod.table_name
