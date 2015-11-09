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
object Q9 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val aggcthen = tpcds.getValue("ss_ext_discount_amt", "ss_ext_sales_price",
      "ss_ext_list_price", "ss_ext_tax")
    val aggcelse = tpcds.getValue("ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit")

    // TODO
    // define RC=ulist(random(1, rowcount("store_sales")/5,uniform),5);
    val rc = Seq(74129, 122840, 56580, 10097, 165306)
    TpcdsBenchmark.Query(Seq(
      s"""
         |select case when (select count(*) from store_sales
         |                  where ss_quantity between 1 and 20) > ${rc(0)}
         |            then (select avg($aggcthen) from store_sales
         |                  where ss_quantity between 1 and 20)
         |            else (select avg($aggcelse) from store_sales
         |                  where ss_quantity between 1 and 20) end bucket1 ,
         |       case when (select count(*) from store_sales
         |                  where ss_quantity between 21 and 40) > ${rc(1)}
         |            then (select avg($aggcthen) from store_sales
         |                  where ss_quantity between 21 and 40)
         |            else (select avg($aggcelse) from store_sales
         |                  where ss_quantity between 21 and 40) end bucket2,
         |       case when (select count(*) from store_sales
         |                  where ss_quantity between 41 and 60) > ${rc(2)}
         |            then (select avg($aggcthen) from store_sales
         |                  where ss_quantity between 41 and 60)
         |            else (select avg($aggcelse) from store_sales
         |                  where ss_quantity between 41 and 60) end bucket3,
         |       case when (select count(*) from store_sales
         |                  where ss_quantity between 61 and 80) > ${rc(3)}
         |            then (select avg($aggcthen) from store_sales
         |                  where ss_quantity between 61 and 80)
         |            else (select avg($aggcelse) from store_sales
         |                  where ss_quantity between 61 and 80) end bucket4,
         |       case when (select count(*) from store_sales
         |                  where ss_quantity between 81 and 100) > ${rc(4)}
         |            then (select avg($aggcthen) from store_sales
         |                  where ss_quantity between 81 and 100)
         |            else (select avg($aggcelse) from store_sales
         |                  where ss_quantity between 81 and 100) end bucket5
         |from reason
         |where r_reason_sk = 1
     """.stripMargin))
  }
}
