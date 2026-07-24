-- btc_address_features_v1
-- Same-transaction aggregation key: (block_hash, transaction_hash).
-- Source table identifiers are inserted only after strict dataset validation.
WITH
transaction_io_rows AS (
  SELECT
    tx.block_number,
    tx.block_hash,
    tx.block_timestamp,
    tx.hash AS transaction_hash,
    io.row_kind,
    io.item_index,
    io.spent_transaction_hash,
    io.spent_output_index,
    io.script_hex,
    io.script_type,
    io.addresses,
    SAFE_CAST(io.value AS INT64) AS value_sats
  FROM {{TRANSACTIONS_TABLE}} AS tx
  CROSS JOIN UNNEST(
    ARRAY_CONCAT(
      ARRAY(
        SELECT AS STRUCT
          'output' AS row_kind,
          output.index AS item_index,
          CAST(NULL AS STRING) AS spent_transaction_hash,
          CAST(NULL AS INT64) AS spent_output_index,
          output.script_hex AS script_hex,
          output.type AS script_type,
          output.addresses AS addresses,
          output.value AS value
        FROM UNNEST(tx.outputs) AS output
      ),
      ARRAY(
        SELECT AS STRUCT
          'input' AS row_kind,
          input.index AS item_index,
          input.spent_transaction_hash AS spent_transaction_hash,
          input.spent_output_index AS spent_output_index,
          input.script_hex AS script_hex,
          input.type AS script_type,
          input.addresses AS addresses,
          input.value AS value
        FROM UNNEST(tx.inputs) AS input
      )
    )
  ) AS io
  WHERE tx.block_number <= @cutoff_height
    AND tx.block_timestamp <= @cutoff_time
    AND tx.block_timestamp_month <= DATE_TRUNC(DATE(@cutoff_time), MONTH)
),
outputs_base AS (
  SELECT
    io.block_number,
    io.block_hash,
    io.block_timestamp,
    io.transaction_hash,
    io.item_index AS output_index,
    LOWER(io.script_hex) AS script_hex,
    io.script_type,
    io.addresses,
    io.value_sats
  FROM transaction_io_rows AS io
  WHERE io.row_kind = 'output'
),
inputs_base AS (
  SELECT
    io.block_number,
    io.block_hash,
    io.block_timestamp,
    io.transaction_hash,
    io.item_index AS input_index,
    io.spent_transaction_hash,
    io.spent_output_index,
    io.script_type,
    io.addresses,
    io.value_sats
  FROM transaction_io_rows AS io
  WHERE io.row_kind = 'input'
),
output_address_rows AS (
  SELECT
    block_number,
    block_hash,
    block_timestamp,
    transaction_hash,
    output_index,
    script_type,
    addresses[OFFSET(0)] AS normalized_address,
    value_sats
  FROM outputs_base
  WHERE ARRAY_LENGTH(addresses) = 1
),
input_address_rows AS (
  SELECT
    block_number,
    block_hash,
    block_timestamp,
    transaction_hash,
    input_index,
    script_type,
    addresses[OFFSET(0)] AS normalized_address,
    value_sats
  FROM inputs_base
  WHERE ARRAY_LENGTH(addresses) = 1
),
received_by_transaction AS (
  SELECT
    block_hash,
    transaction_hash,
    normalized_address,
    SUM(value_sats) AS same_tx_received_sats
  FROM output_address_rows
  GROUP BY block_hash, transaction_hash, normalized_address
),
large_transaction_addresses AS (
  SELECT block_hash, transaction_hash, normalized_address
  FROM received_by_transaction
  WHERE same_tx_received_sats >= 10000000000
),
large_counterparties AS (
  SELECT
    left_side.normalized_address,
    COUNT(DISTINCT right_side.normalized_address) AS direct_large_counterparty_count
  FROM large_transaction_addresses AS left_side
  JOIN large_transaction_addresses AS right_side
    USING (block_hash, transaction_hash)
  WHERE left_side.normalized_address != right_side.normalized_address
  GROUP BY left_side.normalized_address
),
output_aggregates AS (
  SELECT
    normalized_address,
    ANY_VALUE(script_type) AS address_type,
    MIN(block_number) AS first_output_height,
    MAX(block_number) AS last_output_height,
    MIN(block_timestamp) AS first_output_time,
    MAX(block_timestamp) AS last_output_time,
    COUNT(*) AS output_count,
    COUNT(DISTINCT CONCAT(block_hash, ':', transaction_hash))
      AS output_transaction_count,
    SUM(value_sats) AS lifetime_received_sats,
    MAX(value_sats) AS max_single_output_sats,
    SUM(IF(block_timestamp >= @window_30d_start, value_sats, 0)) AS inflow_30d_sats,
    SUM(IF(block_timestamp >= @window_90d_start, value_sats, 0)) AS inflow_90d_sats,
    SUM(IF(block_timestamp >= @window_365d_start, value_sats, 0)) AS inflow_365d_sats
  FROM output_address_rows
  GROUP BY normalized_address
),
input_aggregates AS (
  SELECT
    normalized_address,
    MIN(block_number) AS first_input_height,
    MAX(block_number) AS last_input_height,
    MIN(block_timestamp) AS first_input_time,
    MAX(block_timestamp) AS last_input_time,
    COUNT(*) AS spent_output_count,
    COUNT(DISTINCT CONCAT(block_hash, ':', transaction_hash))
      AS input_transaction_count,
    SUM(value_sats) AS lifetime_spent_sats,
    SUM(IF(block_timestamp >= @window_30d_start, value_sats, 0)) AS outflow_30d_sats,
    SUM(IF(block_timestamp >= @window_90d_start, value_sats, 0)) AS outflow_90d_sats,
    SUM(IF(block_timestamp >= @window_365d_start, value_sats, 0)) AS outflow_365d_sats
  FROM input_address_rows
  GROUP BY normalized_address
),
transaction_counts AS (
  SELECT
    normalized_address,
    COUNT(DISTINCT transaction_key) AS transaction_count
  FROM (
    SELECT
      normalized_address,
      CONCAT(block_hash, ':', transaction_hash) AS transaction_key
    FROM output_address_rows
    UNION ALL
    SELECT
      normalized_address,
      CONCAT(block_hash, ':', transaction_hash) AS transaction_key
    FROM input_address_rows
  )
  GROUP BY normalized_address
),
same_tx_maximums AS (
  SELECT normalized_address, MAX(same_tx_received_sats) AS max_same_tx_received_sats
  FROM received_by_transaction
  GROUP BY normalized_address
),
address_features AS (
  SELECT
    LOWER(TO_HEX(SHA256(CAST(CONCAT('bitcoin:', output.normalized_address) AS BYTES))))
      AS address_id,
    output.normalized_address,
    output.address_type,
    LEAST(
      output.first_output_height,
      COALESCE(input.first_input_height, output.first_output_height)
    ) AS first_seen_height,
    GREATEST(
      output.last_output_height,
      COALESCE(input.last_input_height, output.last_output_height)
    ) AS last_seen_height,
    LEAST(
      output.first_output_time,
      COALESCE(input.first_input_time, output.first_output_time)
    ) AS first_seen_time,
    GREATEST(
      output.last_output_time,
      COALESCE(input.last_input_time, output.last_output_time)
    ) AS last_seen_time,
    output.output_count,
    COALESCE(input.spent_output_count, 0) AS spent_output_count,
    transactions.transaction_count,
    output.lifetime_received_sats - COALESCE(input.lifetime_spent_sats, 0)
      AS current_utxo_sats,
    output.lifetime_received_sats,
    COALESCE(input.lifetime_spent_sats, 0) AS lifetime_spent_sats,
    output.max_single_output_sats,
    maximums.max_same_tx_received_sats,
    output.inflow_30d_sats,
    COALESCE(input.outflow_30d_sats, 0) AS outflow_30d_sats,
    output.inflow_30d_sats + COALESCE(input.outflow_30d_sats, 0)
      AS gross_flow_30d_sats,
    output.inflow_90d_sats,
    COALESCE(input.outflow_90d_sats, 0) AS outflow_90d_sats,
    output.inflow_90d_sats + COALESCE(input.outflow_90d_sats, 0)
      AS gross_flow_90d_sats,
    output.inflow_365d_sats + COALESCE(input.outflow_365d_sats, 0)
      AS gross_flow_365d_sats,
    COALESCE(counterparties.direct_large_counterparty_count, 0)
      AS direct_large_counterparty_count
  FROM output_aggregates AS output
  LEFT JOIN input_aggregates AS input USING (normalized_address)
  JOIN transaction_counts AS transactions USING (normalized_address)
  JOIN same_tx_maximums AS maximums USING (normalized_address)
  LEFT JOIN large_counterparties AS counterparties USING (normalized_address)
),
script_subjects AS (
  SELECT
    LOWER(TO_HEX(
      SHA256(
        CONCAT(
          CAST('bitcoin:mainnet' AS BYTES),
          B'\x00',
          FROM_HEX(COALESCE(script_hex, ''))
        )
      )
    )) AS script_id,
    COALESCE(script_hex, '') AS script_hex,
    ANY_VALUE(script_type) AS script_type,
    IF(ARRAY_LENGTH(ANY_VALUE(addresses)) = 1, ANY_VALUE(addresses)[OFFSET(0)], NULL)
      AS normalized_address,
    IF(
      ARRAY_LENGTH(ANY_VALUE(addresses)) = 1,
      LOWER(TO_HEX(
        SHA256(
          CAST(CONCAT('bitcoin:', ANY_VALUE(addresses)[OFFSET(0)]) AS BYTES)
        )
      )),
      NULL
    ) AS address_id,
    ARRAY_LENGTH(ANY_VALUE(addresses)) = 1 AS provider_enrichable
  FROM outputs_base
  GROUP BY outputs_base.script_hex
),
source_accounting AS (
  SELECT
    (SELECT COUNT(*) FROM outputs_base) AS total_output_rows,
    (SELECT COUNT(*) FROM inputs_base) AS total_input_rows,
    (SELECT COUNT(*) FROM script_subjects) AS distinct_script_subjects,
    (SELECT COUNT(*) FROM outputs_base WHERE ARRAY_LENGTH(addresses) = 1)
      AS standard_single_address_rows,
    (SELECT COUNT(*) FROM outputs_base WHERE ARRAY_LENGTH(addresses) = 0)
      AS empty_address_rows,
    (SELECT COUNT(*) FROM outputs_base WHERE ARRAY_LENGTH(addresses) > 1)
      AS multi_address_rows,
    (
      SELECT COUNT(*)
      FROM outputs_base
      WHERE LOWER(COALESCE(script_type, '')) = 'nonstandard'
    )
      AS nonstandard_rows,
    (SELECT COUNT(*) FROM outputs_base WHERE script_hex IS NULL OR script_hex = '')
      AS missing_script_hex_rows,
    (SELECT COUNT(*) FROM inputs_base WHERE value_sats IS NULL)
      AS unmatched_input_rows
)
SELECT
  'script_subject' AS row_kind,
  script_id,
  script_hex,
  script_type,
  normalized_address,
  address_id,
  provider_enrichable,
  CAST(NULL AS STRING) AS feature_version,
  CAST(NULL AS STRING) AS address_type,
  CAST(NULL AS INT64) AS first_seen_height,
  CAST(NULL AS INT64) AS last_seen_height,
  CAST(NULL AS TIMESTAMP) AS first_seen_time,
  CAST(NULL AS TIMESTAMP) AS last_seen_time,
  CAST(NULL AS INT64) AS output_count,
  CAST(NULL AS INT64) AS spent_output_count,
  CAST(NULL AS INT64) AS transaction_count,
  CAST(NULL AS INT64) AS current_utxo_sats,
  CAST(NULL AS INT64) AS lifetime_received_sats,
  CAST(NULL AS INT64) AS lifetime_spent_sats,
  CAST(NULL AS INT64) AS max_single_output_sats,
  CAST(NULL AS INT64) AS max_same_tx_received_sats,
  CAST(NULL AS INT64) AS inflow_30d_sats,
  CAST(NULL AS INT64) AS outflow_30d_sats,
  CAST(NULL AS INT64) AS gross_flow_30d_sats,
  CAST(NULL AS INT64) AS inflow_90d_sats,
  CAST(NULL AS INT64) AS outflow_90d_sats,
  CAST(NULL AS INT64) AS gross_flow_90d_sats,
  CAST(NULL AS INT64) AS gross_flow_365d_sats,
  CAST(NULL AS INT64) AS direct_large_counterparty_count,
  CAST(NULL AS INT64) AS total_output_rows,
  CAST(NULL AS INT64) AS total_input_rows,
  CAST(NULL AS INT64) AS distinct_script_subjects,
  CAST(NULL AS INT64) AS standard_single_address_rows,
  CAST(NULL AS INT64) AS empty_address_rows,
  CAST(NULL AS INT64) AS multi_address_rows,
  CAST(NULL AS INT64) AS nonstandard_rows,
  CAST(NULL AS INT64) AS missing_script_hex_rows,
  CAST(NULL AS INT64) AS unmatched_input_rows
FROM script_subjects
UNION ALL
SELECT
  'address_feature',
  NULL,
  NULL,
  NULL,
  normalized_address,
  address_id,
  TRUE,
  'btc_address_features_v1',
  address_type,
  first_seen_height,
  last_seen_height,
  first_seen_time,
  last_seen_time,
  output_count,
  spent_output_count,
  transaction_count,
  current_utxo_sats,
  lifetime_received_sats,
  lifetime_spent_sats,
  max_single_output_sats,
  max_same_tx_received_sats,
  inflow_30d_sats,
  outflow_30d_sats,
  gross_flow_30d_sats,
  inflow_90d_sats,
  outflow_90d_sats,
  gross_flow_90d_sats,
  gross_flow_365d_sats,
  direct_large_counterparty_count,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
FROM address_features
UNION ALL
SELECT
  'source_accounting',
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  FALSE,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  total_output_rows,
  total_input_rows,
  distinct_script_subjects,
  standard_single_address_rows,
  empty_address_rows,
  multi_address_rows,
  nonstandard_rows,
  missing_script_hex_rows,
  unmatched_input_rows
FROM source_accounting
