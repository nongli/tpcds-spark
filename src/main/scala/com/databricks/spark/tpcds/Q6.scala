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

// [UNSUPPORTED]: subquery
object Q6 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val month = tpcds.uniformRand(1, 1, 7)
    val year = tpcds.uniformRand(2000, 1998, 2002)
    TpcdsBenchmark.Query(Seq(
      s"""
         | SELECT a.ca_state state, count(*) cnt
         | FROM
         |    customer_address a, customer c, store_sales s, date_dim d, item i
         | WHERE a.ca_address_sk = c.c_current_addr_sk
         | 	AND c.c_customer_sk = s.ss_customer_sk
         | 	AND s.ss_sold_date_sk = d.d_date_sk
         | 	AND s.ss_item_sk = i.i_item_sk
         | 	AND d.d_month_seq =
         | 	     (SELECT distinct (d_month_seq) FROM date_dim 
         |        WHERE d_year = $year AND d_moy = $month)
         | 	AND i.i_current_price > 1.2 *
         |             (SELECT avg(j.i_current_price) FROM item j
         | 	            WHERE j.i_category = i.i_category)
         | GROUP BY a.ca_state
         | HAVING count(*) >= 10
         | ORDER BY cnt LIMIT 100
     """.stripMargin))
  }
}
