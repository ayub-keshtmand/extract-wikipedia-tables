SELECT Rank AS rank_num
, Middleweight AS middleweight
, "Light heavyweight" AS light_heavyweight
, "Women's pound-for-pound" AS womens_p4p
, "Women's strawweight" AS womens_strawweight
, "Women's flyweight" AS womens_flyweight
, "Women's bantamweight" AS womens_bantamweight
FROM read_parquet(
    '<s3_uri>/ufc/extract/<date_parts>/rankings_2.parquet'
)