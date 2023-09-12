SELECT ISO AS iso
, Name AS "name"
, Age AS age
, "Ht." AS height
, Nickname AS nickname
, "Result / next fight / status" AS result_nextfight_status
, "Endeavor record" AS endeavor_record
, "MMA record" AS mma_record
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/debuted_fighters_bantamweights.parquet'
)