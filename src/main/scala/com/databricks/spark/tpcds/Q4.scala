/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.tpcds

object Q4 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val year = tpcds.uniformRand(2001, 1998, 2001)
    val select = tpcds.getValue(
      s"""
         | t_s_secyear.customer_id,
         | t_s_secyear.customer_first_name,
         | t_s_secyear.customer_last_name,
         | t_s_secyear.customer_preferred_cust_flag,
         | t_s_secyear.customer_birth_country,
         | t_s_secyear.customer_login,
         | t_s_secyear.customer_email_address""".stripMargin,
      "t_s_secyear.customer_id", "t_s_secyear.customer_first_name",
      "t_s_secyear.customer_last_name", "t_s_secyear.customer_preferred_cust_flag",
      "t_s_secyear.customer_birth_country", "t_s_secyear.customer_login",
      "t_s_secyear.customer_email_address").trim

    TpcdsBenchmark.Query(Seq(
      s"""
         |WITH year_total AS (
         | SELECT c_customer_id customer_id,
         |        c_first_name customer_first_name,
         |        c_last_name customer_last_name,
         |        c_preferred_cust_flag customer_preferred_cust_flag,
         |        c_birth_country customer_birth_country,
         |        c_login customer_login,
         |        c_email_address customer_email_address,
         |        d_year dyear,
         |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
         |        's' sale_type
         | FROM customer, store_sales, date_dim
         | WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
         | GROUP BY c_customer_id,
         |          c_first_name,
         |          c_last_name,
         |          c_preferred_cust_flag,
         |          c_birth_country,
         |          c_login,
         |          c_email_address,
         |          d_year
         | UNION ALL
         | SELECT c_customer_id customer_id,
         |        c_first_name customer_first_name,
         |        c_last_name customer_last_name,
         |        c_preferred_cust_flag customer_preferred_cust_flag,
         |        c_birth_country customer_birth_country,
         |        c_login customer_login,
         |        c_email_address customer_email_address,
         |        d_year dyear,
         |        sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total,
         |        'c' sale_type
         | FROM customer, catalog_sales, date_dim
         | WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk
         | GROUP BY c_customer_id,
         |          c_first_name,
         |          c_last_name,
         |          c_preferred_cust_flag,
         |          c_birth_country,
         |          c_login,
         |          c_email_address,
         |          d_year
         | UNION ALL
         | SELECT c_customer_id customer_id
         |       ,c_first_name customer_first_name
         |       ,c_last_name customer_last_name
         |       ,c_preferred_cust_flag customer_preferred_cust_flag
         |       ,c_birth_country customer_birth_country
         |       ,c_login customer_login
         |       ,c_email_address customer_email_address
         |       ,d_year dyear
         |       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
         |       ,'w' sale_type
         | FROM customer, web_sales, date_dim
         | WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk
         | GROUP BY c_customer_id,
         |          c_first_name,
         |          c_last_name,
         |          c_preferred_cust_flag,
         |          c_birth_country,
         |          c_login,
         |          c_email_address,
         |          d_year)
         | SELECT
         |     $select
         | FROM year_total t_s_firstyear, year_total t_s_secyear, year_total t_c_firstyear,
         |      year_total t_c_secyear, year_total t_w_firstyear, year_total t_w_secyear
         | WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
         |   and t_s_firstyear.customer_id = t_c_secyear.customer_id
         |   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
         |   and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         |   and t_s_firstyear.customer_id = t_w_secyear.customer_id
         |   and t_s_firstyear.sale_type = 's'
         |   and t_c_firstyear.sale_type = 'c'
         |   and t_w_firstyear.sale_type = 'w'
         |   and t_s_secyear.sale_type = 's'
         |   and t_c_secyear.sale_type = 'c'
         |   and t_w_secyear.sale_type = 'w'
         |   and t_s_firstyear.dyear = $year
         |   and t_s_secyear.dyear = $year+1
         |   and t_c_firstyear.dyear = $year
         |   and t_c_secyear.dyear = $year+1
         |   and t_w_firstyear.dyear = $year
         |   and t_w_secyear.dyear = $year+1
         |   and t_s_firstyear.year_total > 0
         |   and t_c_firstyear.year_total > 0
         |   and t_w_firstyear.year_total > 0
         |   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
         |           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
         |   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
         |           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
         | ORDER BY
         |     $select
         | LIMIT 100
     """.stripMargin))
  }
}
