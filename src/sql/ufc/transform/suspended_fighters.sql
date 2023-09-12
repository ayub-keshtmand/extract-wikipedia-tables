SELECT ISO as iso
, Name as name
, Nickname as nickname
, Division as division
, "From" as suspended_from_string
, STRPTIME("From", '%B %-d, %Y')::DATE as suspended_from_date
, Duration as duration
, "Tested positive for / Info" as tested_positive_for_info
, "By" as suspended_by
, "Eligible to fight again" as eligible_to_fight_again_string
, TRY_STRPTIME("Eligible to fight again", '%B %-d, %Y')::DATE as eligible_to_fight_again_date
, "Ref." as ref
, Notes as notes
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/suspended_fighters.parquet'
)