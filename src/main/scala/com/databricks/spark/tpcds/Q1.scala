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
object Q1 {
  def query(tpcds: TpcdsBenchmark): TpcdsBenchmark.Query = {
    val year: String = tpcds.uniformRand(2000, 1998, 2002)
    val aggField = tpcds.getValue("SR_RETURN_AMT", "SR_FEE", "SR_REFUNDED_CASH",
      "SR_RETURN_AMT_INC_TAX", "SR_REVERSED_CHARGE", "SR_STORE_CREDIT", "SR_RETURN_TAX")
    val state: String = "TN"
    // TODO
    // define COUNTY = random(1, rowcount("active_counties", "store"), uniform);
    // define STATE = distmember(fips_county, [COUNTY], 3); -- qualification params ca
    TpcdsBenchmark.Query(Seq(
      s"""
       | WITH customer_total_return AS
       |   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
                   sum($aggField) AS ctr_total_return
       |    FROM store_returns, date_dim
       |    WHERE sr_returned_date_sk = d_date_sk AND d_year = $year
       |    GROUP BY sr_customer_sk, sr_store_sk)
       | SELECT c_customer_id
       |   FROM customer_total_return ctr1, store, customer
       |   WHERE ctr1.ctr_total_return >
       |    (SELECT avg(ctr_total_return)*1.2
       |      FROM customer_total_return ctr2
       |       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
       |   AND s_store_sk = ctr1.ctr_store_sk
       |   AND s_state = '$state'
       |   AND ctr1.ctr_customer_sk = c_customer_sk
       |   ORDER BY c_customer_id LIMIT 100
   """.stripMargin))
  }
}
