-- start query 20 in stream 0 using template query20.tpl 
SELECT
         item_id,
         item_desc,
         category,
         class,
         current_price,
         itemrevenue,
         itemrevenue*100/Sum(itemrevenue) OVER (partition BY class) AS revenueratio
FROM (
SELECT 
         i.i_item_id item_id, 
         i.i_item_desc item_desc, 
         i.i_category category, 
         i.i_class class, 
         i.i_current_price current_price, 
         Sum(c.cs_ext_sales_price) AS itemrevenue 
FROM     catalog_sales c, 
         item i, 
         date_dim d 
WHERE    c.cs_item_sk = i.i_item_sk 
AND      i.i_category IN ('Children', 
                        'Women', 
                        'Electronics') 
AND      c.cs_sold_date_sk = d.d_date_sk 
AND      d.d_date BETWEEN Cast('2001-02-03' AS DATE) AND      ( 
                  Cast('2001-02-03' AS DATE) + INTERVAL '30' day) 
GROUP BY i.i_item_id , 
         i.i_item_desc , 
         i.i_category , 
         i.i_class , 
         i.i_current_price 
)
ORDER BY category , 
         class , 
         item_id , 
         item_desc , 
         revenueratio 
LIMIT 100; 

