CREATE OR REPLACE TEMP VIEW universe_btc_address_feature AS
SELECT *
FROM read_parquet($address_feature_glob, hive_partitioning = true);

CREATE OR REPLACE TEMP VIEW universe_btc_script_subject AS
SELECT *
FROM read_parquet($script_subject_glob, hive_partitioning = true);

CREATE OR REPLACE TEMP VIEW universe_btc_source_accounting AS
SELECT *
FROM read_parquet($source_accounting_glob, hive_partitioning = false);
