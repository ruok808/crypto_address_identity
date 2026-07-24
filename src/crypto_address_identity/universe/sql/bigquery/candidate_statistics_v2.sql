-- btc_candidate_statistics_v2
-- Aggregate-only census for btc_importance_v2. The final row contains counts,
-- overlap distributions, hashes, and cutoffs only. It never returns an address
-- or transaction identifier.
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
address_transaction AS (
  SELECT
    block_hash,
    transaction_hash,
    normalized_address,
    MAX(block_timestamp) AS block_timestamp,
    SUM(IF(row_kind = 'output', value_sats, 0)) AS tx_received_sats,
    SUM(IF(row_kind = 'input', value_sats, 0)) AS tx_spent_sats,
    SUM(value_sats) AS tx_gross_sats
  FROM eligible_io
  WHERE value_sats IS NOT NULL
  GROUP BY block_hash, transaction_hash, normalized_address
),
address_economics AS (
  SELECT
    normalized_address,
    SUM(tx_received_sats) AS lifetime_received_sats,
    SUM(tx_spent_sats) AS lifetime_spent_sats,
    SUM(tx_received_sats) - SUM(tx_spent_sats) AS current_utxo_sats,
    SUM(
      IF(
        block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 90 DAY),
        tx_gross_sats,
        0
      )
    ) AS gross_flow_90d_sats,
    MAX(block_timestamp) AS last_seen_time,
    MAX(tx_received_sats) AS max_same_tx_received_lifetime_sats,
    MAX(
      IF(
        block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 90 DAY),
        tx_received_sats,
        0
      )
    ) AS max_same_tx_received_90d_sats,
    MAX(
      IF(
        block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 365 DAY),
        tx_received_sats,
        0
      )
    ) AS max_same_tx_received_365d_sats,
    COUNTIF(tx_received_sats >= 50000000000)
      AS same_tx_receive_ge_500_btc_lifetime_count,
    COUNTIF(
      block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 90 DAY)
      AND tx_received_sats >= 50000000000
    ) AS same_tx_receive_ge_500_btc_90d_count,
    COUNTIF(
      block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 365 DAY)
      AND tx_received_sats >= 50000000000
    ) AS same_tx_receive_ge_500_btc_365d_count,
    COUNTIF(
      block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 90 DAY)
    ) AS active_tx_90d_count,
    COUNT(DISTINCT IF(
      block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 90 DAY),
      DATE(block_timestamp),
      NULL
    )) AS active_day_90d_count,
    COUNTIF(
      block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 365 DAY)
    ) AS active_tx_365d_count,
    COUNT(DISTINCT IF(
      block_timestamp >= TIMESTAMP_SUB(@cutoff_time, INTERVAL 365 DAY),
      DATE(block_timestamp),
      NULL
    )) AS active_day_365d_count,
    MAX(
      IF(
        tx_received_sats >= 50000000000,
        block_timestamp,
        NULL
      )
    ) AS last_same_tx_receive_ge_500_btc_time,
    COUNTIF(tx_received_sats > 0) > 0 AS has_output,
    COUNTIF(tx_spent_sats > 0) > 0 AS has_input
  FROM address_transaction
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
derived_features AS (
  SELECT
    *,
    GREATEST(
      gross_flow_90d_sats - max_same_tx_received_90d_sats,
      CAST(0 AS BIGNUMERIC)
    ) AS residual_gross_90d_sats,
    (
      max_same_tx_received_90d_sats >= 50000000000
      AND current_utxo_sats >= 1000000000
      AND current_utxo_sats * 100 >= max_same_tx_received_90d_sats
    ) AS recent_receipt_retained,
    same_tx_receive_ge_500_btc_90d_count >= 2
      AS recent_receipt_repeated,
    (
      max_same_tx_received_90d_sats >= 50000000000
      AND active_tx_90d_count >= 3
      AND active_day_90d_count >= 2
      AND GREATEST(
        gross_flow_90d_sats - max_same_tx_received_90d_sats,
        CAST(0 AS BIGNUMERIC)
      ) >= 50000000000
    ) AS recent_receipt_sustained_activity,
    (
      max_same_tx_received_365d_sats >= 50000000000
      AND current_utxo_sats >= 1000000000
      AND current_utxo_sats * 100 >= max_same_tx_received_365d_sats
    ) AS balanced_receipt_retained,
    (
      max_same_tx_received_lifetime_sats >= 50000000000
      AND current_utxo_sats >= 1000000000
      AND current_utxo_sats * 100 >= max_same_tx_received_lifetime_sats
    ) AS retention_receipt_supported
  FROM address_economics
  WHERE has_output
),
policy_features AS (
  SELECT
    *,
    (
      max_same_tx_received_90d_sats >= 50000000000
      AND (
        recent_receipt_retained
        OR recent_receipt_repeated
        OR recent_receipt_sustained_activity
      )
    ) AS strict_receipt_supported,
    (
      (
        max_same_tx_received_90d_sats >= 50000000000
        AND (
          recent_receipt_retained
          OR recent_receipt_repeated
          OR recent_receipt_sustained_activity
        )
      )
      OR (
        max_same_tx_received_365d_sats >= 50000000000
        AND (
          balanced_receipt_retained
          OR (
            same_tx_receive_ge_500_btc_365d_count >= 2
            AND active_day_365d_count >= 2
          )
        )
      )
    ) AS balanced_receipt_supported,
    current_utxo_sats >= 10000000000 AS p0_utxo,
    (
      residual_gross_90d_sats >= 100000000000
      AND active_tx_90d_count >= 3
      AND active_day_90d_count >= 2
    ) AS p0_sustained_residual_gross,
    (
      lifetime_received_sats >= 1000000000000
      AND TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 90
      AND (
        current_utxo_sats >= 1000000000
        OR (
          residual_gross_90d_sats >= 50000000000
          AND active_tx_90d_count >= 3
          AND active_day_90d_count >= 2
        )
      )
    ) AS p0_lifetime_active_supported,
    CASE
      WHEN current_utxo_sats >= 100000000000 THEN 25
      WHEN current_utxo_sats >= 10000000000 THEN 20
      WHEN current_utxo_sats >= 1000000000 THEN 12
      WHEN current_utxo_sats >= 100000000 THEN 5
      ELSE 0
    END AS balance_score,
    CASE
      WHEN residual_gross_90d_sats >= 1000000000000 THEN 20
      WHEN residual_gross_90d_sats >= 100000000000 THEN 15
      WHEN residual_gross_90d_sats >= 10000000000 THEN 8
      WHEN residual_gross_90d_sats >= 1000000000 THEN 3
      ELSE 0
    END AS residual_gross_score,
    CASE
      WHEN max_same_tx_received_90d_sats >= 50000000000 THEN 10
      WHEN max_same_tx_received_365d_sats >= 50000000000 THEN 5
      ELSE 0
    END AS recent_receipt_score,
    CASE
      WHEN same_tx_receive_ge_500_btc_90d_count >= 2 THEN 12
      WHEN same_tx_receive_ge_500_btc_365d_count >= 2 THEN 7
      ELSE 0
    END AS repeated_receipt_score,
    IF(recent_receipt_retained, 10, 0) AS retained_receipt_score,
    IF(recent_receipt_sustained_activity, 8, 0)
      AS sustained_receipt_score,
    CASE
      WHEN TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 30 THEN 10
      WHEN TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 90 THEN 7
      WHEN TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 365 THEN 3
      ELSE 0
    END AS recency_score
  FROM derived_features
),
classified AS (
  SELECT
    *,
    balance_score + residual_gross_score + recent_receipt_score
      + repeated_receipt_score + retained_receipt_score
      + sustained_receipt_score + recency_score AS v2_chain_score,
    IF(p0_utxo, 1, 0)
      + IF(p0_sustained_residual_gross, 2, 0)
      + IF(strict_receipt_supported, 4, 0)
      + IF(p0_lifetime_active_supported, 8, 0) AS strict_p0_mask,
    IF(p0_utxo, 1, 0)
      + IF(p0_sustained_residual_gross, 2, 0)
      + IF(balanced_receipt_supported, 4, 0)
      + IF(p0_lifetime_active_supported, 8, 0) AS balanced_p0_mask,
    IF(p0_utxo, 1, 0)
      + IF(p0_sustained_residual_gross, 2, 0)
      + IF(retention_receipt_supported, 4, 0)
      + IF(p0_lifetime_active_supported, 8, 0) AS retention_p0_mask,
    IF(recent_receipt_retained, 1, 0)
      + IF(recent_receipt_repeated, 2, 0)
      + IF(recent_receipt_sustained_activity, 4, 0)
      AS receipt_support_mask
  FROM policy_features
),
variant_policy AS (
  SELECT
    *,
    strict_p0_mask != 0 AS strict_is_p0,
    strict_p0_mask = 0 AND v2_chain_score >= 25 AS strict_is_p1,
    strict_p0_mask = 0 AND v2_chain_score BETWEEN 15 AND 24
      AS strict_is_edge,
    (
      strict_p0_mask != 0
      OR v2_chain_score >= 15
      OR current_utxo_sats >= 100000000
      OR residual_gross_90d_sats >= 1000000000
      OR max_same_tx_received_365d_sats >= 50000000000
    ) AS strict_is_coarse,
    balanced_p0_mask != 0 AS balanced_is_p0,
    balanced_p0_mask = 0 AND v2_chain_score >= 25 AS balanced_is_p1,
    balanced_p0_mask = 0 AND v2_chain_score BETWEEN 15 AND 24
      AS balanced_is_edge,
    (
      balanced_p0_mask != 0
      OR v2_chain_score >= 15
      OR current_utxo_sats >= 100000000
      OR residual_gross_90d_sats >= 1000000000
      OR max_same_tx_received_365d_sats >= 50000000000
    ) AS balanced_is_coarse,
    retention_p0_mask != 0 AS retention_is_p0,
    retention_p0_mask = 0 AND v2_chain_score >= 25 AS retention_is_p1,
    retention_p0_mask = 0 AND v2_chain_score BETWEEN 15 AND 24
      AS retention_is_edge,
    (
      retention_p0_mask != 0
      OR v2_chain_score >= 15
      OR current_utxo_sats >= 100000000
      OR residual_gross_90d_sats >= 1000000000
      OR max_same_tx_received_365d_sats >= 50000000000
    ) AS retention_is_coarse
  FROM classified
),
strict_p0_overlap AS (
  SELECT strict_p0_mask AS mask, COUNT(*) AS address_count
  FROM variant_policy
  GROUP BY strict_p0_mask
),
balanced_p0_overlap AS (
  SELECT balanced_p0_mask AS mask, COUNT(*) AS address_count
  FROM variant_policy
  GROUP BY balanced_p0_mask
),
retention_p0_overlap AS (
  SELECT retention_p0_mask AS mask, COUNT(*) AS address_count
  FROM variant_policy
  GROUP BY retention_p0_mask
),
receipt_support_overlap AS (
  SELECT receipt_support_mask AS mask, COUNT(*) AS address_count
  FROM variant_policy
  GROUP BY receipt_support_mask
),
score_counts AS (
  SELECT v2_chain_score AS score, COUNT(*) AS address_count
  FROM variant_policy
  GROUP BY v2_chain_score
),
aggregate_counts AS (
  SELECT
    COUNTIF(current_utxo_sats >= 100000000) AS utxo_ge_1_btc_count,
    COUNTIF(current_utxo_sats >= 1000000000) AS utxo_ge_10_btc_count,
    COUNTIF(current_utxo_sats >= 10000000000) AS utxo_ge_100_btc_count,
    COUNTIF(current_utxo_sats >= 100000000000) AS utxo_ge_1000_btc_count,
    COUNTIF(gross_flow_90d_sats >= 1000000000)
      AS raw_gross_90d_ge_10_btc_count,
    COUNTIF(gross_flow_90d_sats >= 10000000000)
      AS raw_gross_90d_ge_100_btc_count,
    COUNTIF(gross_flow_90d_sats >= 100000000000)
      AS raw_gross_90d_ge_1000_btc_count,
    COUNTIF(gross_flow_90d_sats >= 1000000000000)
      AS raw_gross_90d_ge_10000_btc_count,
    COUNTIF(residual_gross_90d_sats >= 1000000000)
      AS residual_gross_90d_ge_10_btc_count,
    COUNTIF(residual_gross_90d_sats >= 10000000000)
      AS residual_gross_90d_ge_100_btc_count,
    COUNTIF(residual_gross_90d_sats >= 100000000000)
      AS residual_gross_90d_ge_1000_btc_count,
    COUNTIF(residual_gross_90d_sats >= 1000000000000)
      AS residual_gross_90d_ge_10000_btc_count,
    COUNTIF(TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 30)
      AS recency_le_30d_count,
    COUNTIF(TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 90)
      AS recency_le_90d_count,
    COUNTIF(TIMESTAMP_DIFF(@cutoff_time, last_seen_time, DAY) <= 365)
      AS recency_le_365d_count,
    COUNTIF(max_same_tx_received_lifetime_sats >= 50000000000)
      AS lifetime_max_receipt_ge_500_btc_count,
    COUNTIF(max_same_tx_received_lifetime_sats >= 100000000000)
      AS lifetime_max_receipt_ge_1000_btc_count,
    COUNTIF(max_same_tx_received_lifetime_sats >= 500000000000)
      AS lifetime_max_receipt_ge_5000_btc_count,
    COUNTIF(max_same_tx_received_365d_sats >= 50000000000)
      AS max_receipt_365d_ge_500_btc_count,
    COUNTIF(max_same_tx_received_365d_sats >= 100000000000)
      AS max_receipt_365d_ge_1000_btc_count,
    COUNTIF(max_same_tx_received_365d_sats >= 500000000000)
      AS max_receipt_365d_ge_5000_btc_count,
    COUNTIF(max_same_tx_received_90d_sats >= 50000000000)
      AS max_receipt_90d_ge_500_btc_count,
    COUNTIF(max_same_tx_received_90d_sats >= 100000000000)
      AS max_receipt_90d_ge_1000_btc_count,
    COUNTIF(max_same_tx_received_90d_sats >= 500000000000)
      AS max_receipt_90d_ge_5000_btc_count,
    COUNTIF(same_tx_receive_ge_500_btc_lifetime_count >= 1)
      AS receipt_count_lifetime_ge_1_count,
    COUNTIF(same_tx_receive_ge_500_btc_lifetime_count >= 2)
      AS receipt_count_lifetime_ge_2_count,
    COUNTIF(same_tx_receive_ge_500_btc_lifetime_count >= 3)
      AS receipt_count_lifetime_ge_3_count,
    COUNTIF(same_tx_receive_ge_500_btc_365d_count >= 1)
      AS receipt_count_365d_ge_1_count,
    COUNTIF(same_tx_receive_ge_500_btc_365d_count >= 2)
      AS receipt_count_365d_ge_2_count,
    COUNTIF(same_tx_receive_ge_500_btc_365d_count >= 3)
      AS receipt_count_365d_ge_3_count,
    COUNTIF(same_tx_receive_ge_500_btc_90d_count >= 1)
      AS receipt_count_90d_ge_1_count,
    COUNTIF(same_tx_receive_ge_500_btc_90d_count >= 2)
      AS receipt_count_90d_ge_2_count,
    COUNTIF(same_tx_receive_ge_500_btc_90d_count >= 3)
      AS receipt_count_90d_ge_3_count,
    COUNTIF(recent_receipt_retained) AS recent_receipt_retained_count,
    COUNTIF(recent_receipt_repeated) AS recent_receipt_repeated_count,
    COUNTIF(recent_receipt_sustained_activity)
      AS recent_receipt_sustained_activity_count,
    COUNTIF(strict_receipt_supported) AS strict_supported_receipt_count,
    COUNTIF(
      same_tx_receive_ge_500_btc_90d_count = 1
      AND NOT strict_receipt_supported
    ) AS unsupported_recent_singleton_count,
    COUNTIF(
      same_tx_receive_ge_500_btc_lifetime_count = 1
      AND same_tx_receive_ge_500_btc_365d_count = 0
    ) AS stale_lifetime_singleton_count,
    COUNTIF(p0_utxo) AS p0_utxo_count,
    COUNTIF(p0_sustained_residual_gross)
      AS p0_sustained_residual_gross_count,
    COUNTIF(p0_lifetime_active_supported)
      AS p0_lifetime_active_supported_count,
    COUNTIF(strict_receipt_supported) AS strict_p0_receipt_count,
    COUNTIF(strict_is_p0) AS strict_p0_union_count,
    COUNTIF(strict_p0_mask = 4)
      AS strict_incremental_receipt_p0_count,
    COUNTIF(strict_is_p1) AS strict_p1_count,
    COUNTIF(strict_is_p0 AND strict_is_p1) AS strict_p0_p1_overlap_count,
    COUNTIF(strict_is_edge) AS strict_edge_count,
    COUNTIF(strict_is_coarse) AS strict_coarse_count,
    COUNTIF(NOT strict_is_coarse) AS strict_excluded_count,
    COUNTIF(balanced_receipt_supported) AS balanced_p0_receipt_count,
    COUNTIF(balanced_is_p0) AS balanced_p0_union_count,
    COUNTIF(balanced_p0_mask = 4)
      AS balanced_incremental_receipt_p0_count,
    COUNTIF(balanced_is_p1) AS balanced_p1_count,
    COUNTIF(balanced_is_p0 AND balanced_is_p1)
      AS balanced_p0_p1_overlap_count,
    COUNTIF(balanced_is_edge) AS balanced_edge_count,
    COUNTIF(balanced_is_coarse) AS balanced_coarse_count,
    COUNTIF(NOT balanced_is_coarse) AS balanced_excluded_count,
    COUNTIF(retention_receipt_supported) AS retention_p0_receipt_count,
    COUNTIF(retention_is_p0) AS retention_p0_union_count,
    COUNTIF(retention_p0_mask = 4)
      AS retention_incremental_receipt_p0_count,
    COUNTIF(retention_is_p1) AS retention_p1_count,
    COUNTIF(retention_is_p0 AND retention_is_p1)
      AS retention_p0_p1_overlap_count,
    COUNTIF(retention_is_edge) AS retention_edge_count,
    COUNTIF(retention_is_coarse) AS retention_coarse_count,
    COUNTIF(NOT retention_is_coarse) AS retention_excluded_count
  FROM variant_policy
)
SELECT
  'btc_candidate_statistics_v2' AS contract_version,
  'btc_importance_v2' AS policy_version,
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
  aggregate_counts.utxo_ge_1_btc_count,
  aggregate_counts.utxo_ge_10_btc_count,
  aggregate_counts.utxo_ge_100_btc_count,
  aggregate_counts.utxo_ge_1000_btc_count,
  aggregate_counts.raw_gross_90d_ge_10_btc_count,
  aggregate_counts.raw_gross_90d_ge_100_btc_count,
  aggregate_counts.raw_gross_90d_ge_1000_btc_count,
  aggregate_counts.raw_gross_90d_ge_10000_btc_count,
  aggregate_counts.residual_gross_90d_ge_10_btc_count,
  aggregate_counts.residual_gross_90d_ge_100_btc_count,
  aggregate_counts.residual_gross_90d_ge_1000_btc_count,
  aggregate_counts.residual_gross_90d_ge_10000_btc_count,
  aggregate_counts.recency_le_30d_count,
  aggregate_counts.recency_le_90d_count,
  aggregate_counts.recency_le_365d_count,
  aggregate_counts.lifetime_max_receipt_ge_500_btc_count,
  aggregate_counts.lifetime_max_receipt_ge_1000_btc_count,
  aggregate_counts.lifetime_max_receipt_ge_5000_btc_count,
  aggregate_counts.max_receipt_365d_ge_500_btc_count,
  aggregate_counts.max_receipt_365d_ge_1000_btc_count,
  aggregate_counts.max_receipt_365d_ge_5000_btc_count,
  aggregate_counts.max_receipt_90d_ge_500_btc_count,
  aggregate_counts.max_receipt_90d_ge_1000_btc_count,
  aggregate_counts.max_receipt_90d_ge_5000_btc_count,
  aggregate_counts.receipt_count_lifetime_ge_1_count,
  aggregate_counts.receipt_count_lifetime_ge_2_count,
  aggregate_counts.receipt_count_lifetime_ge_3_count,
  aggregate_counts.receipt_count_365d_ge_1_count,
  aggregate_counts.receipt_count_365d_ge_2_count,
  aggregate_counts.receipt_count_365d_ge_3_count,
  aggregate_counts.receipt_count_90d_ge_1_count,
  aggregate_counts.receipt_count_90d_ge_2_count,
  aggregate_counts.receipt_count_90d_ge_3_count,
  aggregate_counts.recent_receipt_retained_count,
  aggregate_counts.recent_receipt_repeated_count,
  aggregate_counts.recent_receipt_sustained_activity_count,
  aggregate_counts.strict_supported_receipt_count,
  aggregate_counts.unsupported_recent_singleton_count,
  aggregate_counts.stale_lifetime_singleton_count,
  (
    SELECT ARRAY_AGG(STRUCT(mask, address_count) ORDER BY mask)
    FROM receipt_support_overlap
  ) AS receipt_support_overlap_distribution,
  (
    SELECT ARRAY_AGG(STRUCT(score, address_count) ORDER BY score)
    FROM score_counts
  ) AS score_histogram,
  STRUCT(
    'V2-S' AS variant,
    aggregate_counts.p0_utxo_count AS p0_utxo_ge_100_btc_count,
    aggregate_counts.p0_sustained_residual_gross_count
      AS p0_sustained_residual_gross_90d_ge_1000_btc_count,
    aggregate_counts.strict_p0_receipt_count AS p0_receipt_rule_count,
    aggregate_counts.p0_lifetime_active_supported_count
      AS p0_lifetime_ge_10000_active_supported_90d_count,
    aggregate_counts.strict_p0_union_count AS chain_p0_union_count,
    aggregate_counts.strict_incremental_receipt_p0_count
      AS incremental_receipt_p0_count,
    (
      SELECT ARRAY_AGG(STRUCT(mask, address_count) ORDER BY mask)
      FROM strict_p0_overlap
    ) AS p0_overlap_distribution,
    aggregate_counts.strict_p1_count AS chain_p1_count,
    aggregate_counts.strict_p0_p1_overlap_count AS p0_p1_overlap_count,
    aggregate_counts.strict_edge_count AS edge_upgrade_frontier_count,
    aggregate_counts.strict_coarse_count AS coarse_candidate_union_count,
    aggregate_counts.strict_excluded_count AS excluded_source_address_count
  ) AS strict_variant,
  STRUCT(
    'V2-B' AS variant,
    aggregate_counts.p0_utxo_count AS p0_utxo_ge_100_btc_count,
    aggregate_counts.p0_sustained_residual_gross_count
      AS p0_sustained_residual_gross_90d_ge_1000_btc_count,
    aggregate_counts.balanced_p0_receipt_count AS p0_receipt_rule_count,
    aggregate_counts.p0_lifetime_active_supported_count
      AS p0_lifetime_ge_10000_active_supported_90d_count,
    aggregate_counts.balanced_p0_union_count AS chain_p0_union_count,
    aggregate_counts.balanced_incremental_receipt_p0_count
      AS incremental_receipt_p0_count,
    (
      SELECT ARRAY_AGG(STRUCT(mask, address_count) ORDER BY mask)
      FROM balanced_p0_overlap
    ) AS p0_overlap_distribution,
    aggregate_counts.balanced_p1_count AS chain_p1_count,
    aggregate_counts.balanced_p0_p1_overlap_count AS p0_p1_overlap_count,
    aggregate_counts.balanced_edge_count AS edge_upgrade_frontier_count,
    aggregate_counts.balanced_coarse_count AS coarse_candidate_union_count,
    aggregate_counts.balanced_excluded_count AS excluded_source_address_count
  ) AS balanced_variant,
  STRUCT(
    'V2-R' AS variant,
    aggregate_counts.p0_utxo_count AS p0_utxo_ge_100_btc_count,
    aggregate_counts.p0_sustained_residual_gross_count
      AS p0_sustained_residual_gross_90d_ge_1000_btc_count,
    aggregate_counts.retention_p0_receipt_count AS p0_receipt_rule_count,
    aggregate_counts.p0_lifetime_active_supported_count
      AS p0_lifetime_ge_10000_active_supported_90d_count,
    aggregate_counts.retention_p0_union_count AS chain_p0_union_count,
    aggregate_counts.retention_incremental_receipt_p0_count
      AS incremental_receipt_p0_count,
    (
      SELECT ARRAY_AGG(STRUCT(mask, address_count) ORDER BY mask)
      FROM retention_p0_overlap
    ) AS p0_overlap_distribution,
    aggregate_counts.retention_p1_count AS chain_p1_count,
    aggregate_counts.retention_p0_p1_overlap_count AS p0_p1_overlap_count,
    aggregate_counts.retention_edge_count AS edge_upgrade_frontier_count,
    aggregate_counts.retention_coarse_count AS coarse_candidate_union_count,
    aggregate_counts.retention_excluded_count AS excluded_source_address_count
  ) AS retention_variant
FROM aggregate_counts
CROSS JOIN source_quality
CROSS JOIN quality_counts
LIMIT 1
