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

// [UNSUPPORTED]: window functions
// [MODIFICATIONS]: + days --> date_add
object Q12 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val year = tpcds.uniformRand(1999, 1998, 2002)
    // TODO: define SDATE=date([YEAR]+"-01-01",[YEAR]+"-07-01",sales);
    val sdate = s"$year-02-022"
    val categories = tpcds.toInList(tpcds.getList(Seq("Sports", "Books", "Home"), tpcds.getCategory))
    TpcdsBenchmark.Query(Seq(
      s"""
        | select 
        |  i_item_desc, i_category, i_class, i_current_price,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
        |          (partition by i_class) as revenueratio
        | from
        |	web_sales, item, date_dim
        | where
        |	ws_item_sk = i_item_sk
        |  	and i_category in ($categories)
        |  	and ws_sold_date_sk = d_date_sk
        |	and d_date between cast('$sdate' as date)
        |				and date_add(cast('$sdate' as date), 30)
        | group by
        |	i_item_id, i_item_desc, i_category, i_class, i_current_price
        | order by
        |	i_category, i_class, i_item_id, i_item_desc, revenueratio
        | LIMIT 100
      """.stripMargin))
  }
}
