-- start query 98 in stream 0 using template query98.tpl 
SELECT
       item_id,
       item_desc,
       category,
       class,
       current_price,
       itemrevenue,
       itemrevenue * 100 / Sum(itemrevenue)
                                         OVER (
                                           PARTITION BY class) AS revenueratio
FROM 
(
SELECT i.i_item_id as item_id, 
       i.i_item_desc as item_desc, 
       i.i_category as category, 
       i.i_class as class, 
       i.i_current_price as current_price, 
       Sum(s.ss_ext_sales_price) AS itemrevenue 
FROM   store_sales s, 
       item i, 
       date_dim d 
WHERE  s.ss_item_sk = i.i_item_sk 
       AND i.i_category IN ( 'Men', 'Home', 'Electronics' ) 
       AND s.ss_sold_date_sk = d.d_date_sk 
       AND d.d_date BETWEEN CAST('2000-05-18' AS DATE) AND ( 
                          CAST('2000-05-18' AS DATE) + INTERVAL '30' DAY ) 
GROUP  BY i.i_item_id, 
          i.i_item_desc, 
          i.i_category, 
          i.i_class, 
          i.i_current_price 
)
ORDER  BY category, 
          class, 
          item_id, 
          item_desc, 
          revenueratio
; 
