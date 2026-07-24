-- btc_address_scale_v1
-- Exact address scale only. Spent input addresses originate from historical
-- outputs, so a full-history output scan covers the canonical address universe.
WITH output_addresses AS (
  SELECT
    output.addresses[SAFE_OFFSET(0)] AS normalized_address
  FROM {{TRANSACTIONS_TABLE}} AS tx
  CROSS JOIN UNNEST(tx.outputs) AS output
  WHERE tx.block_number <= @cutoff_height
    AND tx.block_timestamp <= @cutoff_time
    AND tx.block_timestamp_month <= DATE_TRUNC(DATE(@cutoff_time), MONTH)
    AND ARRAY_LENGTH(output.addresses) = 1
)
SELECT
  COUNT(DISTINCT normalized_address) AS unique_standard_addresses
FROM output_addresses
