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

import scala.util.Random

// TODO: type out the zips list
object Q8 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val year = tpcds.uniformRand(1998, 1998, 2002)
    val qoy = tpcds.uniformRand(2, 1, 2)
    val zips = tpcds.toInList(Seq.fill(400)(Random.nextInt(99999 - 10000) + 10000))
    TpcdsBenchmark.Query(Seq(
      s"""
         | select s_store_name, sum(ss_net_profit)
         | from store_sales, date_dim, store,
         |     (SELECT ca_zip
         |       from (
         |       (SELECT substr(ca_zip,1,5) ca_zip
         |          FROM customer_address
         |          WHERE substr(ca_zip,1,5) IN ($zips))
         |       INTERSECT
         |       (select ca_zip
         |          FROM
         |            (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
         |              FROM customer_address, customer
         |              WHERE ca_address_sk = c_current_addr_sk and
         |                    c_preferred_cust_flag='Y'
         |              group by ca_zip
         |              having count(*) > 10) A1
         |        )) A2
         |      ) V1
         | where ss_store_sk = s_store_sk
         |  and ss_sold_date_sk = d_date_sk
         |  and d_qoy = $qoy and d_year = $year
         |  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
         | group by s_store_name
         | order by s_store_name LIMIT 100
     """.stripMargin))
  }
}
