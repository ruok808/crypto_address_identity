-- btc_candidate_statistics_v1
-- Aggregate-only census for btc_importance_v1. The source table is referenced
-- once. Historical duplicate transaction ids remain distinct because the
-- same-transaction key includes block_hash and transaction_hash.
WITH
transaction_io AS (
  SELECT
    tx.block_hash,
    tx.hash AS transaction_hash,
    tx.block_timestamp,
    io.row_kind,
    io.addresses,
    io.value AS source_value,
    SAFE_CAST(io.value AS BIGNUMERIC) AS value_sats
  FROM {{TRANSACTIONS_TABLE}} AS tx
  CROSS JOIN UNNEST(
    ARRAY_CONCAT(
      ARRAY(
        SELECT AS STRUCT
          'input' AS row_kind,
          input.addresses AS addresses,
          input.value AS value
        FROM UNNEST(tx.inputs) AS input
      ),
      ARRAY(
        SELECT AS STRUCT
          'output' AS row_kind,
          output.addresses AS addresses,
          output.value AS value
        FROM UNNEST(tx.outputs) AS output
      )
    )
  ) AS io
  WHERE tx.block_number <= @cutoff_height
    AND tx.block_timestamp <= @cutoff_time
    AND tx.block_timestamp_month <= DATE_TRUNC(DATE(@cutoff_time), MONTH)
),
eligible_io AS (
  SELECT
    block_hash,
    transaction_hash,
    block_timestamp,
    row_kind,
    addresses[OFFSET(0)] AS normalized_address,
    source_value,
    value_sats
  FROM transaction_io
  WHERE ARRAY_LENGTH(addresses) = 1
),
quality_counts AS (
  SELECT
    COUNTIF(source_value IS NULL) AS null_value_count,
    COUNTIF(source_value IS NOT NULL AND value_sats IS NULL)
      AS value_cast_failure_count
  FROM eligible_io
),
same_transaction_address_kind AS (
  SELECT
    block_hash,
    transaction_hash,
    normalized_address,
    row_kind,
    MAX(block_timestamp) AS block_timestamp,
    SUM(value_sats) AS same_tx_value_sats
  FROM eligible_io
  WHERE value_sats IS NOT NULL
  -- Required key: block_hash, transaction_hash, normalized_address, row_kind.
  GROUP BY block_hash, transaction_hash, normalized_address, row_kind
),
address_economics AS (
  SELECT
    normalized_address,
    SUM(IF(row_kind = 'output', same_tx_value_sats, 0))
      AS lifetime_received_sats,
    SUM(IF(row_kind = 'input', same_tx_value_sats, 0))
      AS lifetime_spent_sats,
    SUM(IF(row_kind = 'output', same_tx_value_sats, 0))
      - SUM(IF(row_kind = 'input', same_tx_value_sats, 0))
      AS current_utxo_sats,
    MAX(IF(row_kind = 'output', same_tx_value_sats, 0))
      AS max_same_tx_received_sats,
    SUM(
      IF(
        block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 90 DAY),
        same_tx_value_sats,
        0
      )
    ) AS gross_flow_90d_sats,
    MAX(block_timestamp) AS last_seen_time,
    COUNTIF(row_kind = 'output') > 0 AS has_output,
    COUNTIF(row_kind = 'input') > 0 AS has_input
  FROM same_transaction_address_kind
  GROUP BY normalized_address
),
source_quality AS (
  SELECT
    COUNTIF(has_output) AS source_standard_address_count,
    COUNTIF(has_input AND NOT has_output) AS source_input_only_address_count,
    COUNTIF(has_output AND current_utxo_sats < 0)
      AS negative_current_utxo_count,
    MAX(IF(has_output, last_seen_time, NULL)) AS max_observed_activity_time
  FROM address_economics
),
score_components AS (
  SELECT
    *,
    CASE
      WHEN current_utxo_sats >= 100000000000 THEN 25
      WHEN current_utxo_sats >= 10000000000 THEN 20
      WHEN current_utxo_sats >= 1000000000 THEN 12
      WHEN current_utxo_sats >= 100000000 THEN 5
      ELSE 0
    END AS balance_score,
    CASE
      WHEN max_same_tx_received_sats >= 500000000000 THEN 25
      WHEN max_same_tx_received_sats >= 100000000000 THEN 20
      WHEN max_same_tx_received_sats >= 50000000000 THEN 18
      WHEN max_same_tx_received_sats >= 10000000000 THEN 10
      ELSE 0
    END AS same_tx_receipt_score,
    CASE
      WHEN gross_flow_90d_sats >= 1000000000000 THEN 20
      WHEN gross_flow_90d_sats >= 100000000000 THEN 15
      WHEN gross_flow_90d_sats >= 10000000000 THEN 8
      WHEN gross_flow_90d_sats >= 1000000000 THEN 3
      ELSE 0
    END AS gross_90d_score,
    CASE
      WHEN TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 30 THEN 10
      WHEN TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 90 THEN 7
      WHEN TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 365 THEN 3
      ELSE 0
    END AS recency_score,
    current_utxo_sats >= 10000000000 AS p0_utxo,
    max_same_tx_received_sats >= 50000000000 AS p0_same_tx_receive,
    gross_flow_90d_sats >= 100000000000 AS p0_gross_90d,
    lifetime_received_sats >= 1000000000000
      AND TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 365
      AS p0_lifetime_active
  FROM address_economics
  WHERE has_output
),
classified AS (
  SELECT
    *,
    balance_score + same_tx_receipt_score + gross_90d_score + recency_score
      AS chain_importance_score,
    IF(p0_utxo, 1, 0)
      + IF(p0_same_tx_receive, 2, 0)
      + IF(p0_gross_90d, 4, 0)
      + IF(p0_lifetime_active, 8, 0) AS p0_mask
  FROM score_components
),
policy AS (
  SELECT
    *,
    p0_mask != 0 AS is_chain_p0,
    p0_mask = 0 AND chain_importance_score >= 25 AS is_chain_p1,
    p0_mask = 0 AND chain_importance_score BETWEEN 15 AND 24
      AS is_edge_upgrade_frontier,
    current_utxo_sats >= 100000000
      OR max_same_tx_received_sats >= 10000000000
      OR gross_flow_90d_sats >= 1000000000
      AS has_positive_economic_component,
    p0_mask != 0
      OR chain_importance_score >= 15
      OR current_utxo_sats >= 100000000
      OR max_same_tx_received_sats >= 10000000000
      OR gross_flow_90d_sats >= 1000000000
      AS is_coarse_candidate
  FROM classified
),
p0_overlap AS (
  SELECT p0_mask AS mask, COUNT(*) AS address_count
  FROM policy
  GROUP BY p0_mask
),
score_counts AS (
  SELECT chain_importance_score AS score, COUNT(*) AS address_count
  FROM policy
  GROUP BY chain_importance_score
),
aggregate_counts AS (
  SELECT
    COUNTIF(current_utxo_sats >= 100000000) AS utxo_ge_1_btc_count,
    COUNTIF(current_utxo_sats >= 1000000000) AS utxo_ge_10_btc_count,
    COUNTIF(current_utxo_sats >= 10000000000) AS utxo_ge_100_btc_count,
    COUNTIF(current_utxo_sats >= 100000000000) AS utxo_ge_1000_btc_count,
    COUNTIF(max_same_tx_received_sats >= 10000000000)
      AS same_tx_receive_ge_100_btc_count,
    COUNTIF(max_same_tx_received_sats >= 50000000000)
      AS same_tx_receive_ge_500_btc_count,
    COUNTIF(max_same_tx_received_sats >= 100000000000)
      AS same_tx_receive_ge_1000_btc_count,
    COUNTIF(max_same_tx_received_sats >= 500000000000)
      AS same_tx_receive_ge_5000_btc_count,
    COUNTIF(gross_flow_90d_sats >= 1000000000)
      AS gross_90d_ge_10_btc_count,
    COUNTIF(gross_flow_90d_sats >= 10000000000)
      AS gross_90d_ge_100_btc_count,
    COUNTIF(gross_flow_90d_sats >= 100000000000)
      AS gross_90d_ge_1000_btc_count,
    COUNTIF(gross_flow_90d_sats >= 1000000000000)
      AS gross_90d_ge_10000_btc_count,
    COUNTIF(TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 30)
      AS recency_le_30d_count,
    COUNTIF(TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 90)
      AS recency_le_90d_count,
    COUNTIF(TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 365)
      AS recency_le_365d_count,
    COUNTIF(p0_lifetime_active) AS lifetime_ge_10000_active_365d_count,
    COUNTIF(p0_utxo) AS p0_utxo_ge_100_btc_count,
    COUNTIF(p0_same_tx_receive) AS p0_same_tx_receive_ge_500_btc_count,
    COUNTIF(p0_gross_90d) AS p0_gross_90d_ge_1000_btc_count,
    COUNTIF(p0_lifetime_active)
      AS p0_lifetime_ge_10000_active_365d_count,
    COUNTIF(is_chain_p0) AS chain_p0_union_count,
    COUNTIF(is_chain_p1) AS chain_p1_count,
    COUNTIF(is_chain_p0 AND is_chain_p1) AS p0_p1_overlap_count,
    COUNTIF(is_edge_upgrade_frontier) AS edge_upgrade_frontier_count,
    COUNTIF(has_positive_economic_component)
      AS positive_economic_component_count,
    COUNTIF(is_coarse_candidate) AS coarse_candidate_union_count,
    COUNTIF(NOT is_coarse_candidate) AS excluded_source_address_count,
    COUNTIF(current_utxo_sats >= 100000000) AS current_capital_count,
    COUNTIF(max_same_tx_received_sats >= 10000000000)
      AS historical_large_receipt_count,
    COUNTIF(gross_flow_90d_sats >= 1000000000) AS high_turnover_count,
    COUNTIF(
      current_utxo_sats >= 100000000
      AND TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) > 365
    ) AS dormant_holder_count
  FROM policy
)
SELECT
  'btc_candidate_statistics_v1' AS contract_version,
  source_quality.source_standard_address_count,
  source_quality.source_input_only_address_count,
  source_quality.negative_current_utxo_count,
  quality_counts.null_value_count,
  quality_counts.value_cast_failure_count,
  source_quality.max_observed_activity_time,
  @cutoff_height AS source_cutoff_height,
  @cutoff_time AS source_cutoff_time,
  @query_sha256 AS query_sha256,
  @schema_sha256 AS schema_sha256,
  aggregate_counts.*,
  (
    SELECT ARRAY_AGG(STRUCT(mask, address_count) ORDER BY mask)
    FROM p0_overlap
  ) AS p0_overlap_distribution,
  (
    SELECT ARRAY_AGG(STRUCT(score, address_count) ORDER BY score)
    FROM score_counts
  ) AS score_histogram
FROM aggregate_counts
CROSS JOIN source_quality
CROSS JOIN quality_counts
LIMIT 1
