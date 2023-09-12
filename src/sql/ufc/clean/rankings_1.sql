SELECT Rank AS rank_num
, "Men's pound-for-pound" AS mens_p4p
, Flyweight AS flyweight
, Bantamweight AS bantamweight
, Featherweight AS featherweight
, Lightweight AS lightweight
, Welterweight AS welterweight
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/rankings_1.parquet'
)