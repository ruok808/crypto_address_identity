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
      CAST(
        CONCAT(
          '0x',
          SUBSTR(TO_HEX(SHA256(normalized_address)), 1, 8)
        )
        AS INT64
      ),
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
    LOWER(TO_HEX(SHA256(CONCAT(
      'btc_strict_v2_s_candidate_row_v1',
      CHR(31), normalized_address,
      CHR(31), candidate_tier,
      CHR(31), CAST(tier_rank AS STRING),
      CHR(31), CAST(address_bucket AS STRING),
      CHR(31), CAST(v2_chain_score AS STRING),
      CHR(31), CAST(strict_p0_mask AS STRING),
      CHR(31), CAST(receipt_support_mask AS STRING),
      CHR(31), FORMAT('%.0f', current_utxo_sats),
      CHR(31), FORMAT('%.0f', lifetime_received_sats),
      CHR(31), FORMAT('%.0f', residual_gross_90d_sats),
      CHR(31), FORMAT('%.0f', max_same_tx_received_lifetime_sats),
      CHR(31), FORMAT('%.0f', max_same_tx_received_365d_sats),
      CHR(31), FORMAT('%.0f', max_same_tx_received_90d_sats),
      CHR(31), CAST(
        same_tx_receive_ge_500_btc_90d_count AS STRING
      ),
      CHR(31), CAST(
        same_tx_receive_ge_500_btc_365d_count AS STRING
      ),
      CHR(31), CAST(active_tx_90d_count AS STRING),
      CHR(31), CAST(active_day_90d_count AS STRING),
      CHR(31), CAST(active_tx_365d_count AS STRING),
      CHR(31), CAST(active_day_365d_count AS STRING),
      CHR(31), FORMAT_TIMESTAMP(
        '%Y-%m-%dT%H:%M:%E6SZ',
        last_seen_time,
        'UTC'
      )
    )))) AS candidate_row_sha256
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
