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

object Q11 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val year = tpcds.uniformRand(2001, 1998, 2001)
    val selectone = tpcds.getValue(
      "t_s_secyear.customer_preferred_cust_flag",
      "t_s_secyear.customer_id",
      "t_s_secyear.customer_first_name",
      "t_s_secyear.customer_last_name",
      "t_s_secyear.customer_birth_country",
      "t_s_secyear.customer_login",
      "t_s_secyear.customer_email_address",
      s"""
         | t_s_secyear.customer_id,
         | t_s_secyear.customer_first_name,
         | t_s_secyear.customer_last_name,
         | t_s_secyear.c_preferred_cust_flag,
         | t_s_secyear.c_birth_country,
         | t_s_secyear.c_login,
         | t_s_secyear.c_email_address"
       """.stripMargin)
    TpcdsBenchmark.Query(Seq(
      s"""
         | with year_total as (
         | select c_customer_id customer_id
         |       ,c_first_name customer_first_name
         |       ,c_last_name customer_last_name
         |       ,c_preferred_cust_flag customer_preferred_cust_flag
         |       ,c_birth_country customer_birth_country
         |       ,c_login customer_login
         |       ,c_email_address customer_email_address
         |       ,d_year dyear
         |       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
         |       ,'s' sale_type
         | from customer, store_sales, date_dim
         | where c_customer_sk = ss_customer_sk
         |   and ss_sold_date_sk = d_date_sk
         | group by c_customer_id
         |         ,c_first_name
         |         ,c_last_name
         |         ,d_year
         |         ,c_preferred_cust_flag
         |         ,c_birth_country
         |         ,c_login
         |         ,c_email_address
         |         ,d_year
         | union all
         | select c_customer_id customer_id
         |       ,c_first_name customer_first_name
         |       ,c_last_name customer_last_name
         |       ,c_preferred_cust_flag customer_preferred_cust_flag
         |       ,c_birth_country customer_birth_country
         |       ,c_login customer_login
         |       ,c_email_address customer_email_address
         |       ,d_year dyear
         |       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
         |       ,'w' sale_type
         | from customer, web_sales, date_dim
         | where c_customer_sk = ws_bill_customer_sk
         |   and ws_sold_date_sk = d_date_sk
         | group by
         |    c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country,
         |    c_login, c_email_address, d_year)
         | select
         |     $selectone
         | from year_total t_s_firstyear
         |     ,year_total t_s_secyear
         |     ,year_total t_w_firstyear
         |     ,year_total t_w_secyear
         | where t_s_secyear.customer_id = t_s_firstyear.customer_id
         |         and t_s_firstyear.customer_id = t_w_secyear.customer_id
         |         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         |         and t_s_firstyear.sale_type = 's'
         |         and t_w_firstyear.sale_type = 'w'
         |         and t_s_secyear.sale_type = 's'
         |         and t_w_secyear.sale_type = 'w'
         |         and t_s_firstyear.dyear = $year
         |         and t_s_secyear.dyear = $year+1
         |         and t_w_firstyear.dyear = $year
         |         and t_w_secyear.dyear = $year+1
         |         and t_s_firstyear.year_total > 0
         |         and t_w_firstyear.year_total > 0
         |         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
         |             > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
         | order by $selectone
         | LIMIT 100
     """.stripMargin))
  }
}
