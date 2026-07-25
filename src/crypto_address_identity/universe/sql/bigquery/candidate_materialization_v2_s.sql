-- btc_strict_v2_s_candidate_materialization
-- Address-level Strict V2-S coarse-union rows only. The policy CTEs are
-- injected from the checksum-pinned btc_candidate_statistics_v2 resource.
WITH
{{STRICT_V2_S_POLICY_CTES}}materialization_rows AS (
  SELECT
    normalized_address,
    CASE
      WHEN strict_is_p0 THEN 'p0'
      WHEN strict_is_p1 THEN 'p1'
      WHEN strict_is_edge THEN 'edge'
      ELSE 'coarse_other'
    END AS candidate_tier,
    CASE
      WHEN strict_is_p0 THEN 0
      WHEN strict_is_p1 THEN 1
      WHEN strict_is_edge THEN 2
      ELSE 3
    END AS tier_rank,
    MOD(
      MOD(FARM_FINGERPRINT(normalized_address), 64) + 64,
      64
    ) AS address_bucket,
    v2_chain_score,
    strict_p0_mask,
    receipt_support_mask,
    current_utxo_sats,
    lifetime_received_sats,
    residual_gross_90d_sats,
    max_same_tx_received_lifetime_sats,
    max_same_tx_received_365d_sats,
    max_same_tx_received_90d_sats,
    same_tx_receive_ge_500_btc_90d_count,
    same_tx_receive_ge_500_btc_365d_count,
    active_tx_90d_count,
    active_day_90d_count,
    active_tx_365d_count,
    active_day_365d_count,
    last_seen_time
  FROM variant_policy
  WHERE strict_is_coarse
),
hashed_rows AS (
  SELECT
    *,
    LOWER(TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      'btc_strict_v2_s_candidate_row_v1' AS row_contract_version,
      normalized_address AS normalized_address,
      candidate_tier AS candidate_tier,
      tier_rank AS tier_rank,
      address_bucket AS address_bucket,
      v2_chain_score AS v2_chain_score,
      strict_p0_mask AS strict_p0_mask,
      receipt_support_mask AS receipt_support_mask,
      current_utxo_sats AS current_utxo_sats,
      lifetime_received_sats AS lifetime_received_sats,
      residual_gross_90d_sats AS residual_gross_90d_sats,
      max_same_tx_received_lifetime_sats
        AS max_same_tx_received_lifetime_sats,
      max_same_tx_received_365d_sats
        AS max_same_tx_received_365d_sats,
      max_same_tx_received_90d_sats
        AS max_same_tx_received_90d_sats,
      same_tx_receive_ge_500_btc_90d_count
        AS same_tx_receive_ge_500_btc_90d_count,
      same_tx_receive_ge_500_btc_365d_count
        AS same_tx_receive_ge_500_btc_365d_count,
      active_tx_90d_count AS active_tx_90d_count,
      active_day_90d_count AS active_day_90d_count,
      active_tx_365d_count AS active_tx_365d_count,
      active_day_365d_count AS active_day_365d_count,
      FORMAT_TIMESTAMP(
        '%Y-%m-%dT%H:%M:%E6SZ',
        last_seen_time,
        'UTC'
      ) AS last_seen_time
    ))))) AS candidate_row_sha256
  FROM materialization_rows
)
SELECT
  normalized_address,
  candidate_tier,
  tier_rank,
  address_bucket,
  v2_chain_score,
  strict_p0_mask,
  receipt_support_mask,
  current_utxo_sats,
  lifetime_received_sats,
  residual_gross_90d_sats,
  max_same_tx_received_lifetime_sats,
  max_same_tx_received_365d_sats,
  max_same_tx_received_90d_sats,
  same_tx_receive_ge_500_btc_90d_count,
  same_tx_receive_ge_500_btc_365d_count,
  active_tx_90d_count,
  active_day_90d_count,
  active_tx_365d_count,
  active_day_365d_count,
  last_seen_time,
  candidate_row_sha256
FROM hashed_rows
ORDER BY tier_rank, address_bucket, normalized_address
