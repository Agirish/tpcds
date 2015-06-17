-- start query 89 in stream 0 using template query89.tpl 
SELECT  * 
FROM  (
SELECT 
              category,
              class,
              brand,
              store_name,
              company_name,
              dmoy,
              sum_sales,
              Avg(sum_sales)
                OVER (
                  partition BY category, brand, store_name, company_name
                )
                                  avg_monthly_sales
FROM 
(
SELECT        i.i_category category, 
              i.i_class class, 
              i.i_brand brand, 
              s.s_store_name store_name, 
              s.s_company_name company_name, 
              d.d_moy dmoy, 
              Sum(ss.ss_sales_price) sum_sales 
       FROM   item i, 
              store_sales ss, 
              date_dim d, 
              store s
       WHERE  ss.ss_item_sk = i.i_item_sk 
              AND ss.ss_sold_date_sk = d.d_date_sk 
              AND ss.ss_store_sk = s.s_store_sk 
              AND d.d_year IN ( 2002 ) 
              AND ( ( i.i_category IN ( 'Home', 'Men', 'Sports' ) 
                      AND i.i_class IN ( 'paint', 'accessories', 'fitness' ) ) 
                     OR ( i.i_category IN ( 'Shoes', 'Jewelry', 'Women' ) 
                          AND i.i_class IN ( 'mens', 'pendants', 'swimwear' ) ) ) 
       GROUP  BY i.i_category, 
                 i.i_class, 
                 i.i_brand, 
                 s.s_store_name, 
                 s.s_company_name, 
                 d.d_moy
)) tmp1 
WHERE  CASE 
         WHEN ( avg_monthly_sales <> 0 ) THEN ( 
         Abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ) 
         ELSE NULL 
       END > 0.1 
ORDER  BY sum_sales - avg_monthly_sales, 
          store_name
LIMIT 10; 
