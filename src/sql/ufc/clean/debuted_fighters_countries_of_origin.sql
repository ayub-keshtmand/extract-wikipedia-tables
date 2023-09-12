SELECT ISO AS iso
, FYW AS flyweight
, BW AS bantamweight
, FW AS featherweight
, LW AS lightweight
, WW AS welterweight
, MW AS middleweight
, LHW AS light_heavyweight
, HW AS heavyweight
, WSW AS women_strawweight
, WFYW AS women_flyweight
, WBW AS women_bantamweight
, WFW AS women_featherweight
, Total AS total
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/debuted_fighters_countries_of_origin.parquet'
)
