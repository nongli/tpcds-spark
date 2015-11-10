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
object Q10 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val year = tpcds.uniformRand(2002, 1999, 2002)
    val month = tpcds.uniformRand(1, 1, 4)

    // TODO
    // define COUNTY = ulist(dist(fips_county,2,1),10);
    val counties = tpcds.toInList(
      Seq("Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"))
    TpcdsBenchmark.Query(Seq(
      s"""
         | select
         |  cd_gender, cd_marital_status, cd_education_status, count(*) cnt1,
         |  cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3,
         |  cd_dep_count, count(*) cnt4, cd_dep_employed_count,  count(*) cnt5,
         |  cd_dep_college_count, count(*) cnt6
         | from
         |  customer c, customer_address ca, customer_demographics
         | where
         |  c.c_current_addr_sk = ca.ca_address_sk and
         |  ca_county in ($counties) and
         |  cd_demo_sk = c.c_current_cdemo_sk and
         |  exists (select * from store_sales, date_dim
         |          where c.c_customer_sk = ss_customer_sk and
         |                ss_sold_date_sk = d_date_sk and
         |                d_year = $year and
         |                d_moy between $month and $month+3) and
         |   (exists (select * from web_sales, date_dim
         |            where c.c_customer_sk = ws_bill_customer_sk and
         |                  ws_sold_date_sk = d_date_sk and
         |                  d_year = $year and
         |                  d_moy between $month AND $month+3) or
         |    exists (select * from catalog_sales, date_dim
         |            where c.c_customer_sk = cs_ship_customer_sk and
         |                  cs_sold_date_sk = d_date_sk and
         |                  d_year = $year and
         |                  d_moy between $month and $month+3))
         | group by cd_gender,
         |          cd_marital_status,
         |          cd_education_status,
         |          cd_purchase_estimate,
         |          cd_credit_rating,
         |          cd_dep_count,
         |          cd_dep_employed_count,
         |          cd_dep_college_count
         | order by cd_gender,
         |          cd_marital_status,
         |          cd_education_status,
         |          cd_purchase_estimate,
         |          cd_credit_rating,
         |          cd_dep_count,
         |          cd_dep_employed_count,
         |          cd_dep_college_count
         |limit 100
     """.stripMargin))
  }
}
