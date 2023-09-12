SELECT WC AS weight_class
, Min AS min_weight
, "Upper limit" AS max_weight
, G AS gender
, Champion AS champion
, Flag AS flag
, "Date won" AS date_won_string
, "Days held" AS days_held
, Defenses AS defenses
, "Next Fight / Info" as next_fight_info
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/current_champions_weight_classes_and_status.parquet'
)