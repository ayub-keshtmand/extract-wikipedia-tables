SELECT REPLACE(name, ' *', '') AS name
, age
, SPLIT_PART(height, ' (', 1) AS height_feet_inches
, RTRIM(
	REPLACE(
		SPLIT_PART(height, ' (', 2)
		, 'm)', ''
	)
)::DECIMAL(3, 2) AS height_metres
, result_nextfight_status
, CASE 
	WHEN LEFT(result_nextfight_status, 4) IN ('Win ', 'Loss') THEN 'previous'
	WHEN LEFT(result_nextfight_status, 1) = '(' THEN 'Cancelled'
	WHEN result_nextfight_status IS NULL THEN NULL 
	ELSE 'upcoming'
END AS "type"
, endeavor_record
, extract_wins_from_record(endeavor_record) AS endeavor_record_wins
, extract_losses_from_record(endeavor_record) AS endeavor_record_losses
, extract_draws_from_record(endeavor_record) AS endeavor_record_draws
, extract_nc_from_record(endeavor_record) AS endeavor_record_nc
, mma_record
, extract_wins_from_record(mma_record) AS mma_record_wins
, extract_losses_from_record(mma_record) AS mma_record_losses
, extract_draws_from_record(mma_record) AS mma_record_draws
, extract_nc_from_record(mma_record) AS mma_record_nc
FROM read_parquet(
    '<s3_uri>/ufc/clean/<date_parts>/debuted_fighters_bantamweights.parquet'
)