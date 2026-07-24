-- btc_source_checkpoint_v1
WITH recent_blocks AS (
  SELECT
    source_block.number AS block_number,
    source_block.hash AS block_hash,
    source_block.timestamp AS block_timestamp
  FROM {{BLOCKS_TABLE}} AS source_block
  WHERE source_block.timestamp_month BETWEEN
      DATE_TRUNC(DATE_SUB(@as_of_date, INTERVAL 7 DAY), MONTH)
      AND DATE_TRUNC(DATE_SUB(@as_of_date, INTERVAL 1 DAY), MONTH)
    AND DATE(source_block.timestamp) >= DATE_SUB(@as_of_date, INTERVAL 7 DAY)
    AND DATE(source_block.timestamp) < @as_of_date
),
ranked_blocks AS (
  SELECT
    block_number,
    block_hash,
    MAX(block_timestamp) AS block_timestamp,
    ROW_NUMBER() OVER (ORDER BY block_number DESC) AS block_rank
  FROM recent_blocks
  GROUP BY block_number, block_hash
),
recent_outputs AS (
  SELECT output.addresses
  FROM {{TRANSACTIONS_TABLE}} AS tx
  CROSS JOIN UNNEST(tx.outputs) AS output
  WHERE tx.block_timestamp_month BETWEEN
      DATE_TRUNC(DATE_SUB(@as_of_date, INTERVAL 7 DAY), MONTH)
      AND DATE_TRUNC(DATE_SUB(@as_of_date, INTERVAL 1 DAY), MONTH)
    AND DATE(tx.block_timestamp) >= DATE_SUB(@as_of_date, INTERVAL 7 DAY)
    AND DATE(tx.block_timestamp) < @as_of_date
),
checkpoint AS (
  SELECT
    MAX(IF(block_rank = 1, block_number, NULL)) AS latest_height,
    MAX(IF(block_rank = 1, block_hash, NULL)) AS latest_hash,
    MAX(IF(block_rank = 1, block_timestamp, NULL)) AS latest_time,
    MAX(IF(block_rank = @finality_depth + 1, block_number, NULL))
      AS finalized_height,
    MAX(IF(block_rank = @finality_depth + 1, block_hash, NULL))
      AS finalized_hash
  FROM ranked_blocks
)
SELECT
  checkpoint.*,
  (
    SELECT COUNT(*)
    FROM recent_outputs
    WHERE ARRAY_LENGTH(addresses) = 1
      AND STARTS_WITH(LOWER(addresses[OFFSET(0)]), 'bc1p')
  ) AS taproot_address_count
FROM checkpoint
LIMIT 1
