insert into stl_querytext values(
    '100','6919','19644','1793','0',
    'copy public.catalog_returns from ''s3://fivetran-benchmark/tpcds_1000_dat/catalog_returns/'' region ''us-east-1'' format delimiter ''|'' acceptinvchars compupdate on iam_role ''''                             '
);

insert into stl_querytext values(
    '100','5052','15859','1224','0','-- query12\nSELECT\ni_item_id ,\ni_item_desc ,\ni_category ,\ni_class ,\ni_current_price ,\nSum(ws_ext_sales_price) AS itemrevenue ,\nSum(ws_ext_sales_price)*100/Sum(Sum(ws_ext_sales_price)) OVER (par'
);

insert into stl_querytext values(
    '100','5052','15859','1224','1','tition BY i_class) AS revenueratio\nFROM web_sales ,\nitem ,\ndate_dim\nWHERE ws_item_sk = i_item_sk\nAND i_category IN (''Home'',\n''Men'',\n''Women'')\nAND ws_sold_date_sk = d_date_sk\nAND Cast(d_date AS '
);

insert into stl_querytext values(
    '100','5052','15859','1224','2','DATE) BETWEEN Cast(''2000-05-11'' AS DATE) AND (\nCast(''2000-06-11'' AS DATE))\nGROUP BY i_item_id ,\ni_item_desc ,\ni_category ,\ni_class ,\ni_current_price\nORDER BY i_category ,\ni_class ,\ni_item_id '
);

insert into stl_querytext values(
    '100','5052','15859','1224','3',',\ni_item_desc ,\nrevenueratio\nLIMIT 100;\n'
);

insert into stl_query values(
    '100','1793','default','6919','19644','dev',
    'copy public.catalog_returns from ''s3://fivetran-benchmark/tpcds_1000_dat/catalog_returns/'' region ''us-east-1'' format delimiter ''|'' acceptinvchars compupdate on iam_role '''''
    ,'2018-09-19 13:08:03.419322','2018-09-19 13:46:51.926214','0','0'
);

insert into stl_query values(
    '100','1224','default','5052','15859','dev','-- query12 SELECT i_item_id , i_item_desc , i_category , i_class , i_current_price , Sum(ws_ext_sales_price) AS itemrevenue , Sum(ws_ext_sales_price)*100/Sum(Sum(ws_ext_sales_price)) OVER (partition BY i_class) AS revenueratio FROM web_sales , item , date_dim WHERE ws_item_sk = i_item_sk AND i_category IN (''Home'', ''Men'', ''Women'') AND ws_sold_date_sk = d_date_sk AND Cast(d_date AS DATE) BETWEEN Cast(''2000-05-11'' AS DATE) AND ( Cast(''2000-06-11'' AS DATE)) GROUP BY i_item_id , i_item_desc , i_category , i_class , i_current_price ORDER BY i_category , i_class , i_item_id , i_item_desc , revenueratio LIMIT 100;'
    ,'2018-09-19 11:50:42.221579','2018-09-19 11:51:21.252449','0','0'
)

