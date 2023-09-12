SELECT "Date" AS date_string
, ISO AS iso
, "Name" AS "name"
, Nickname AS nickname
, Division AS division
, "Status / next fight / Info" AS status_nextfight_info
, "MMA record" AS mma_record
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/recent_signings.parquet'
)