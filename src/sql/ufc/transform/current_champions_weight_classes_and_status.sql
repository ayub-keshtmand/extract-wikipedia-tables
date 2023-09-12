SELECT weight_class AS weight_class_abbr
, CASE weight_class
	WHEN 'SW' THEN 'Strawweight'
	WHEN 'FLW' THEN 'Flyweight'
	WHEN 'BW' THEN 'Bantamweight'
	WHEN 'FW' THEN 'Featherweight'
	WHEN 'LW' THEN 'Lightweight'
	WHEN 'WW' THEN 'Welterweight'
	WHEN 'MW' THEN 'Middleweight'
	WHEN 'LHW' THEN 'Light Heavyweight'
	WHEN 'HW' THEN 'Heavyweight'
	END AS weight_class_full
, TRY_CAST(
    SUBSTRING(min_weight, 2, 4)
    AS SMALLINT
) AS min_weight_lbs
, CAST(
    SPLIT_PART(max_weight, ' ', 1)
    AS SMALLINT
) AS max_weight_lbs
, TRY_CAST(
    RTRIM(
        REPLACE(
            SPLIT_PART(min_weight, '>', -1),
            'kg', ''
        )
    )
    AS DECIMAL(4,1)
) AS min_weight_kg
, TRY_CAST(
    RTRIM(
        REPLACE(
            SPLIT_PART(max_weight, ' ', -1),
            'kg', ''
        )
    ) AS DECIMAL(4,1)
) AS max_weight_kg
, CASE WHEN gender = 'M' THEN 'Male' ELSE 'Female' END AS gender
, champion
, flag
, date_won_string
, COALESCE(
    TRY_STRPTIME(date_won_string, '%B %-d, %Y'),
    TRY_STRPTIME(date_won_string, '%b %-d, %Y')
)::DATE AS date_won
, days_held::SMALLINT AS days_held
, defenses
, NULLIF(
	SPLIT_PART(next_fight_info, ' - ', 1)
	, ''
) AS next_fight_event
, NULLIF(
	SPLIT_PART(next_fight_info, ' - ', 2)
	, ''	
) AS next_fight_opponent
FROM read_parquet(
    '<s3_uri>/ufc/clean/<date_parts>/current_champions_weight_classes_and_status.parquet'
)
