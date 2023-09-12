{% import 'macros.j2' as macros -%}

SELECT {{ macros.clean_name('name') }} AS name
, age
, {{ macros.select_height_in_feet_and_inches('height') }} AS height_feet_inches
, {{ macros.select_height_in_metres('height') }} AS height_metres
, result_nextfight_status
, {{ macros.select_result_nextfight_status_type }} AS "type"
, endeavor_record
-- , extract_wins_from_record(endeavor_record) AS endeavor_record_wins
-- , extract_losses_from_record(endeavor_record) AS endeavor_record_losses
-- , extract_draws_from_record(endeavor_record) AS endeavor_record_draws
-- , extract_nc_from_record(endeavor_record) AS endeavor_record_nc
-- , mma_record
-- , extract_wins_from_record(mma_record) AS mma_record_wins
-- , extract_losses_from_record(mma_record) AS mma_record_losses
-- , extract_draws_from_record(mma_record) AS mma_record_draws
-- , extract_nc_from_record(mma_record) AS mma_record_nc
FROM read_parquet(
    '<s3_uri>/ufc/clean/<date_parts>/debuted_fighters_bantamweights.parquet'
)