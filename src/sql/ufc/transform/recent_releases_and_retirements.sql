SELECT "Date" AS date_string
, STRPTIME("Date", '%B %-d, %Y')::DATE as "date"
, ISO AS iso
, "Name" AS "name"
, Nickname AS nickname
, Reason AS reason
, Division AS division
, Ref AS ref
, "Endeavor record" AS endeavor_record
, "MMA record" AS mma_record
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/recent_releases_and_retirements.parquet'
)