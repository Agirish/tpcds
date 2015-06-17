-- start query 57 in stream 0 using template query57.tpl 
WITH v1 
     AS (
SELECT          category,
                brand,
                name,
                dyear,
                dmoy,
                sum_sales,
                Avg(sum_sales)
                  OVER (
                    partition BY category, brand, name, dyear)
                avg_monthly_sales
                   ,
                Rank()
                  OVER (
                    partition BY category, brand, name
                    ORDER BY dyear, dmoy)                            rn
FROM(
SELECT i.i_category category, 
                i.i_brand brand, 
                cc.cc_name name, 
                d.d_year dyear, 
                d.d_moy dmoy, 
                Sum(cs.cs_sales_price) sum_sales 
         FROM   item i, 
                catalog_sales cs, 
                date_dim d, 
                call_center cc 
         WHERE  cs.cs_item_sk = i.i_item_sk 
                AND cs.cs_sold_date_sk = d.d_date_sk 
                AND cc.cc_call_center_sk = cs.cs_call_center_sk 
                AND ( d.d_year = 2000 
                       OR ( d.d_year = 2000 - 1 
                            AND d.d_moy = 12 ) 
                       OR ( d.d_year = 2000 + 1 
                            AND d.d_moy = 1 ) ) 
         GROUP  BY i.i_category, 
                   i.i_brand, 
                   cc.cc_name, 
                   d.d_year, 
                   d.d_moy)
), 
     v2 
     AS (SELECT v1.brand, 
                v1.dyear, 
                v1.avg_monthly_sales, 
                v1.sum_sales, 
                v1_lag.sum_sales  psum, 
                v1_lead.sum_sales nsum 
         FROM   v1, 
                v1 v1_lag, 
                v1 v1_lead 
         WHERE  v1.category = v1_lag.category 
                AND v1.category = v1_lead.category 
                AND v1.brand = v1_lag.brand 
                AND v1.brand = v1_lead.brand 
                AND v1.name = v1_lag.name 
                AND v1.name = v1_lead.name 
                AND v1.rn = v1_lag.rn + 1 
                AND v1.rn = v1_lead.rn - 1) 
SELECT * 
FROM   v2 
WHERE  dyear = 2000 
       AND avg_monthly_sales > 0 
       AND CASE 
             WHEN avg_monthly_sales > 0 THEN Abs(sum_sales - avg_monthly_sales) 
                                             / 
                                             avg_monthly_sales 
             ELSE NULL 
           END > 0.1 
ORDER  BY sum_sales - avg_monthly_sales, 
          3
LIMIT 10; 
