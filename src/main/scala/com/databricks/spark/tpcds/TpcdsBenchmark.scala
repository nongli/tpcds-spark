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

import org.apache.spark.sql.SQLContext

import scala.util.Random

object TpcdsBenchmark {
  /**
   * Represents a single TPCDS query. Queries can contain multiple parts and each part should be
   * run in order.
   */
  case class Query(val queries: Seq[String])
}

//
// TODO: should explicit alias make original unresolvable?
// e.g. select col a from t order by col. (order by a works)
case class TpcdsBenchmark(val ctx: SQLContext, val randomize: Boolean = false) {
  /**
    * Returns the query string by index/name
    */
  def getQuery(i: Int): String = {
    i match {
      case 1 => Q1.query(this).queries.head
      case 2 => Q2.query(this).queries.head
      case 3 => Q3.query(this).queries.head
      case 4 => Q4.query(this).queries.head
      case 5 => Q5.query(this).queries.head
      case 6 => Q6.query(this).queries.head
      case 7 => Q7.query(this).queries.head
      case 8 => Q8.query(this).queries.head
      case 9 => Q9.query(this).queries.head
      case 10 => Q10.query(this).queries.head
      case 11 => Q11.query(this).queries.head
      case 12 => Q12.query(this).queries.head
      case 13 => q13()
      case 14 => q14()
      case 15 => q15()
      case 16 => q16()
      case 17 => q17()
      case 18 => q18()
      case 19 => q19()
      case 20 => q20()
      case 21 => q21()
      case 22 => q22()
      case 23 => q23()
      case 24 => q24()
      case 25 => q25()
      case 26 => q26()
      case 27 => q27()
      case 28 => q28()
      case 29 => q29()
      case 30 => q30()
      case 31 => q31()
      case 32 => q32()
      case 33 => q33()
      case 34 => q34()
      case 35 => q35()
      case 36 => q36()
      case 37 => q37()
      case 38 => q38()
      case 39 => q39()
      case 40 => q40()
      case 41 => q41()
      case 42 => q42()
      case 43 => q43()
      case 44 => q44()
      case 45 => q45()
      case 46 => q46()
      case 47 => q47()
      case 48 => q48()
      case 49 => q49()
      case 50 => q50()
      case 51 => q51()
      case 52 => q52()
      case 53 => q53()
      case 54 => q54()
      case 55 => q55()
      case 56 => q56()
      case 57 => q57()
      case 58 => q58()
      case 59 => q59()
      case 60 => q60()
      case 61 => q61()
      case 62 => q62()
      case 63 => q63()
      case 64 => q64()
      case 65 => q65()
      case 66 => q66()
      case 67 => q67()
      case 68 => q68()
      case 69 => q69()
      case 70 => q70()
      case 71 => q71()
      case 72 => q72()
      case 73 => q73()
      case 74 => q74()
      case 75 => q75()
      case 76 => q76()
      case 77 => q77()
      case 78 => q78()
      case 79 => q79()
      case 80 => q80()
      case 81 => q81()
      case 82 => q82()
      case 83 => q83()
      case 84 => q84()
      case 85 => q85()
      case 86 => q86()
      case 87 => q87()
      case 88 => q88()
      case 89 => q89()
      case 90 => q90()
      case 91 => q91()
      case 92 => q92()
      case 93 => q93()
      case 94 => q94()
      case 95 => q95()
      case 96 => q96()
      case 97 => q97()
      case 98 => q98()
      case 99 => q99()
      case _ => throw new RuntimeException("Invalid index: " + i)
    }
  }

  /**
    * Returns all the queries.
    */
  def allQueries(): Seq[String] = {
    (1 to 99).map(getQuery(_))
  }
  
  /**
    * Returns all the queries that are currently supported using hiveql
    */
  def hiveQLSupportedQueries(): Seq[Int] = {
    Seq(3, 4, 7, 11, 13, 15, 17, 19, 21, 25, 26, 28, 29, 31, 34, 37, 40, 42, 43, 46, 47, 48, 49, 51,
      52, 53, 55, 57, 59, 61, 64, 65, 68, 71, 72, 73, 74, 75, 76, 78, 79, 82, 84, 85, 88, 90, 91,
      93, 96, 97)
  }

  /**
    * Returns all the queries that are currently supported using sql dialect.
    */
  // revisit q39
  def supportedQueries(): Seq[Int] = {
    Seq(2, 3, 4, 7, 8, 11, 13, 15, 17, 19, 21, 25, 26, 28, 29, 31, 34, 37, 38, 40, 42, 43, 46, 48,
      52, 55, 59, 61, 64, 65, 66, 68, 71, 72, 73, 74, 75, 76, 78, 79, 82, 84, 85, 87, 88, 90, 91,
      93, 96, 97)
  }

  // Subquery: 1, 6, 9, 14, 23, 24, 30, 33, 35, 41, 45, 54, 56, 58, 60, 69, 81, 83 
  // Window functions 12, 20, 36, 44, 63, 67, 70, 86, 89, 98
  // Alias: 16, 32, 50, 62, 92, 94, 95, 99
  // rollup: 18, 22, 27, 77 
  // Error? 5, 80

  /**
    * Registers all the tables as temp tables in `db`.
    * TODO: non-nullable columns (there many)
    * TODO: support CHAR(N)
    */
  def registerTables(db: Option[String] = None) = {
    val prefix = ""
    ctx.sql(catalog_returns(prefix))
    ctx.sql(catalog_sales(prefix))
    ctx.sql(inventory(prefix))
    ctx.sql(store_returns(prefix))
    ctx.sql(store_sales(prefix))
    ctx.sql(web_returns(prefix))
    ctx.sql(web_sales(prefix))
    
    ctx.sql(call_center(prefix))
    ctx.sql(catalog_page(prefix))
    ctx.sql(customer(prefix))
    ctx.sql(customer_address(prefix))
    ctx.sql(customer_demographics(prefix))
    ctx.sql(date_dim(prefix))
    ctx.sql(household_demographics(prefix))
    ctx.sql(income_band(prefix))
    ctx.sql(item(prefix))
    ctx.sql(promotion(prefix))
    ctx.sql(reason(prefix))
    ctx.sql(ship_mode(prefix))
    ctx.sql(store(prefix))
    ctx.sql(time_dim(prefix))
    ctx.sql(warehouse(prefix))
    ctx.sql(web_page(prefix))
    ctx.sql(web_site(prefix))
  }

  //
  // Fact tables
  //
  def catalog_returns(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}catalog_returns(
        | cr_returned_date_sk BIGINT,
        | cr_returned_time_sk BIGINT,
        | cr_item_sk BIGINT,
        | cr_refunded_customer_sk BIGINT,
        | cr_refunded_cdemo_sk BIGINT,
        | cr_refunded_hdemo_sk BIGINT,
        | cr_refunded_addr_sk BIGINT,
        | cr_returning_customer_sk BIGINT,
        | cr_returning_cdemo_sk BIGINT,
        | cr_returning_hdemo_sk BIGINT,
        | cr_returning_addr_sk BIGINT,
        | cr_call_center_sk BIGINT,
        | cr_catalog_page_sk BIGINT,
        | cr_ship_mode_sk BIGINT,
        | cr_warehouse_sk BIGINT,
        | cr_reason_sk BIGINT,
        | cr_order_number BIGINT,
        | cr_return_quantity INT,
        | cr_return_amount DECIMAL(7,2),
        | cr_return_tax DECIMAL(7,2),
        | cr_return_amt_inc_tax DECIMAL(7,2),
        | cr_fee DECIMAL(7,2),
        | cr_return_ship_cost DECIMAL(7,2),
        | cr_refunded_cash DECIMAL(7,2),
        | cr_reversed_charge DECIMAL(7,2),
        | cr_store_credit DECIMAL(7,2),
        | cr_net_loss DECIMAL(7,2)
        |)""".stripMargin
  }
    
  def catalog_sales(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}catalog_sales(
        | cs_sold_date_sk BIGINT,
        | cs_sold_time_sk BIGINT,
        | cs_ship_date_sk BIGINT,
        | cs_bill_customer_sk BIGINT,
        | cs_bill_cdemo_sk BIGINT,
        | cs_bill_hdemo_sk BIGINT,
        | cs_bill_addr_sk BIGINT,
        | cs_ship_customer_sk BIGINT,
        | cs_ship_cdemo_sk BIGINT,
        | cs_ship_hdemo_sk BIGINT,
        | cs_ship_addr_sk BIGINT,
        | cs_call_center_sk BIGINT,
        | cs_catalog_page_sk BIGINT,
        | cs_ship_mode_sk BIGINT,
        | cs_warehouse_sk BIGINT,
        | cs_item_sk BIGINT,
        | cs_promo_sk BIGINT,
        | cs_order_number BIGINT,
        | cs_quantity BIGINT,
        | cs_wholesale_cost DECIMAL(7,2),
        | cs_list_price DECIMAL(7,2),
        | cs_sales_price DECIMAL(7,2),
        | cs_ext_discount_amt DECIMAL(7,2),
        | cs_ext_sales_price DECIMAL(7,2),
        | cs_ext_wholesale_cost DECIMAL(7,2),
        | cs_ext_list_price DECIMAL(7,2),
        | cs_ext_tax DECIMAL(7,2),
        | cs_coupon_amt DECIMAL(7,2),
        | cs_ext_ship_cost DECIMAL(7,2),
        | cs_net_paid DECIMAL(7,2),
        | cs_net_paid_inc_tax DECIMAL(7,2),
        | cs_net_paid_inc_ship DECIMAL(7,2),
        | cs_net_paid_inc_ship_tax DECIMAL(7,2),
        | cs_net_profit DECIMAL(7,2)
        |)""".stripMargin
  }

  def inventory(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}inventory(
        | inv_date_sk BIGINT,
        | inv_item_sk BIGINT,
        | inv_warehouse_sk BIGINT,
        | inv_quantity_on_hand INT
        |)""".stripMargin
  }
  
  def store_returns(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}store_returns(
        | sr_returned_date_sk BIGINT,
        | sr_return_time_sk BIGINT,
        | sr_item_sk BIGINT,
        | sr_customer_sk BIGINT,
        | sr_cdemo_sk BIGINT,
        | sr_hdemo_sk BIGINT,
        | sr_addr_sk BIGINT,
        | sr_store_sk BIGINT,
        | sr_reason_sk BIGINT,
        | sr_ticket_number BIGINT,
        | sr_return_quantity BIGINT,
        | sr_return_amt DECIMAL(7,2),
        | sr_return_tax DECIMAL(7,2),
        | sr_return_amt_inc_tax DECIMAL(7,2),
        | sr_fee DECIMAL(7,2),
        | sr_return_ship_cost DECIMAL(7,2),
        | sr_refunded_cash DECIMAL(7,2),
        | sr_reversed_charge DECIMAL(7,2),
        | sr_store_credit DECIMAL(7,2),
        | sr_net_loss DECIMAL(7,2)
        |)""".stripMargin
  }
  
  def store_sales(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}store_sales(
        | ss_sold_date_sk BIGINT,
        | ss_sold_time_sk BIGINT,
        | ss_item_sk BIGINT,
        | ss_customer_sk BIGINT,
        | ss_cdemo_sk BIGINT,
        | ss_hdemo_sk BIGINT,
        | ss_addr_sk BIGINT,
        | ss_store_sk BIGINT,
        | ss_promo_sk BIGINT,
        | ss_ticket_number BIGINT,
        | ss_quantity BIGINT,
        | ss_wholesale_cost DECIMAL(7,2),
        | ss_list_price DECIMAL(7,2),
        | ss_sales_price DECIMAL(7,2),
        | ss_ext_discount_amt DECIMAL(7,2),
        | ss_ext_sales_price DECIMAL(7,2),
        | ss_ext_wholesale_cost DECIMAL(7,2),
        | ss_ext_list_price DECIMAL(7,2),
        | ss_ext_tax DECIMAL(7,2),
        | ss_coupon_amt DECIMAL(7,2),
        | ss_net_paid DECIMAL(7,2),
        | ss_net_paid_inc_tax DECIMAL(7,2),
        | ss_net_profit DECIMAL(7,2)
        |)""".stripMargin
  }
  
  def web_returns(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}web_returns(
        | wr_returned_date_sk BIGINT,
        | wr_returned_time_sk BIGINT,
        | wr_item_sk BIGINT,
        | wr_refunded_customer_sk BIGINT,
        | wr_refunded_cdemo_sk BIGINT,
        | wr_refunded_hdemo_sk BIGINT,
        | wr_refunded_addr_sk BIGINT,
        | wr_returning_customer_sk BIGINT,
        | wr_returning_cdemo_sk BIGINT,
        | wr_returning_hdemo_sk BIGINT,
        | wr_returning_addr_sk BIGINT,
        | wr_web_page_sk BIGINT,
        | wr_reason_sk BIGINT,
        | wr_order_number BIGINT,
        | wr_return_quantity INT,
        | wr_return_amt DECIMAL(7,2),
        | wr_return_tax DECIMAL(7,2),
        | wr_return_amt_inc_tax DECIMAL(7,2),
        | wr_fee DECIMAL(7,2),
        | wr_return_ship_cost DECIMAL(7,2),
        | wr_refunded_cash DECIMAL(7,2),
        | wr_reversed_charge DECIMAL(7,2),
        | wr_account_credit DECIMAL(7,2),
        | wr_net_loss DECIMAL(7,2)
        |)""".stripMargin
  }
  
  def web_sales(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}web_sales(
        | ws_sold_date_sk BIGINT,
        | ws_sold_time_sk BIGINT,
        | ws_ship_date_sk BIGINT,
        | ws_item_sk BIGINT,
        | ws_bill_customer_sk BIGINT,
        | ws_bill_cdemo_sk BIGINT,
        | ws_bill_hdemo_sk BIGINT,
        | ws_bill_addr_sk BIGINT,
        | ws_ship_customer_sk BIGINT,
        | ws_ship_cdemo_sk BIGINT,
        | ws_ship_hdemo_sk BIGINT,
        | ws_ship_addr_sk BIGINT,
        | ws_web_page_sk BIGINT,
        | ws_web_site_sk BIGINT,
        | ws_ship_mode_sk BIGINT,
        | ws_warehouse_sk BIGINT,
        | ws_promo_sk BIGINT,
        | ws_order_number BIGINT,
        | ws_quantity BIGINT,
        | ws_wholesale_cost DECIMAL(7,2),
        | ws_list_price DECIMAL(7,2),
        | ws_sales_price DECIMAL(7,2),
        | ws_ext_discount_amt DECIMAL(7,2),
        | ws_ext_sales_price DECIMAL(7,2),
        | ws_ext_wholesale_cost DECIMAL(7,2),
        | ws_ext_list_price DECIMAL(7,2),
        | ws_ext_tax DECIMAL(7,2),
        | ws_coupon_amt DECIMAL(7,2),
        | ws_ext_ship_cost DECIMAL(7,2),
        | ws_net_paid DECIMAL(7,2),
        | ws_net_paid_inc_tax DECIMAL(7,2),
        | ws_net_paid_inc_ship DECIMAL(7,2),
        | ws_net_paid_inc_ship_tax DECIMAL(7,2),
        | ws_net_profit DECIMAL(7,2)
        |)""".stripMargin
  }
    
  //
  // Dimension tables
  //
  
  def call_center(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}call_center(
        | cc_call_center_sk BIGINT,
        | cc_call_center_id STRING,
        | cc_rec_start_date DATE,
        | cc_rec_end_date DATE,
        | cc_closed_date_sk INT,
        | cc_open_date_sk INT,
        | cc_name VARCHAR(50),
        | cc_class VARCHAR(50),
        | cc_employees INT,
        | cc_sq_ft INT,
        | cc_hours STRING,
        | cc_manager VARCHAR(40),
        | cc_mkt_id INT,
        | cc_mkt_class STRING,
        | cc_mkt_desc VARCHAR(100),
        | cc_market_manager VARCHAR(40),
        | cc_division INT,
        | cc_division_name VARCHAR(50),
        | cc_company INT,
        | cc_company_name STRING,
        | cc_street_number STRING,
        | cc_street_name STRING,
        | cc_street_type STRING,
        | cc_suite_number STRING,
        | cc_city VARCHAR(60),
        | cc_county VARCHAR(30),
        | cc_state STRING,
        | cc_zip STRING,
        | cc_country VARCHAR(20),
        | cc_gmt_offset DECIMAL(5,2),
        | cc_tax_percentage DECIMAL(5,2)
        |)""".stripMargin
  }

  def catalog_page(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}catalog_page(
        | cp_catalog_page_sk BIGINT,
        | cp_catalog_page_id STRING,
        | cp_start_date_sk INT,
        | cp_end_date_sk INT,
        | cp_department VARCHAR(50),
        | cp_catalog_number INT,
        | cp_catalog_page_number INT,
        | cp_description VARCHAR(100),
        | cp_type VARCHAR(100)
        |)""".stripMargin
  }

  def customer(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}customer(
        | c_customer_sk BIGINT,
        | c_customer_id STRING,
        | c_current_cdemo_sk BIGINT,
        | c_current_hdemo_sk BIGINT,
        | c_current_addr_sk BIGINT,
        | c_first_shipto_date_sk BIGINT,
        | c_first_sales_date_sk BIGINT,
        | c_salutation STRING,
        | c_first_name STRING,
        | c_last_name STRING,
        | c_preferred_cust_flag STRING,
        | c_birth_day SMALLINT,
        | c_birth_month SMALLINT,
        | c_birth_year SMALLINT,
        | c_birth_country STRING,
        | c_login STRING,
        | c_email_address STRING,
        | c_last_review_date_sk BIGINT
        |)""".stripMargin
  }

  def customer_address(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}customer_address(
        | ca_address_sk BIGINT,
        | ca_address_id STRING,
        | ca_street_number STRING,
        | ca_street_name VARCHAR(60),
        | ca_street_type STRING,
        | ca_suite_number STRING,
        | ca_city VARCHAR(60),
        | ca_county VARCHAR(30),
        | ca_state STRING,
        | ca_zip STRING,
        | ca_country VARCHAR(20),
        | ca_gmt_offset DECIMAL(5,2),
        | ca_location_type STRING
        |)""".stripMargin
  }

  def customer_demographics(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}customer_demographics(
        | cd_demo_sk BIGINT,
        | cd_gender STRING,
        | cd_marital_status STRING,
        | cd_education_status STRING,
        | cd_purchase_estimate INT,
        | cd_credit_rating STRING,
        | cd_dep_count INT,
        | cd_dep_employed_count INT,
        | cd_dep_college_count INT
        |)""".stripMargin
  }

  def date_dim(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}date_dim(
        | d_date_sk BIGINT,
        | d_date_id STRING,
        | d_date DATE,
        | d_month_seq INT,
        | d_week_seq INT,
        | d_quarter_seq INT,
        | d_year INT,
        | d_dow INT,
        | d_moy INT,
        | d_dom INT,
        | d_qoy INT,
        | d_fy_year INT,
        | d_fy_quarter_seq INT,
        | d_fy_week_seq INT,
        | d_day_name STRING,
        | d_quarter_name STRING,
        | d_holiday STRING,
        | d_weekend STRING,
        | d_following_holiday STRING,
        | d_first_dom INT,
        | d_last_dom INT,
        | d_same_day_ly INT,
        | d_same_day_lq INT,
        | d_current_day STRING,
        | d_current_week STRING,
        | d_current_month STRING,
        | d_current_quarter STRING,
        | d_current_year STRING
        |)""".stripMargin
  }

  def household_demographics(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}household_demographics(
        | hd_demo_sk BIGINT,
        | hd_income_band_sk BIGINT,
        | hd_buy_potential STRING,
        | hd_dep_count INT,
        | hd_vehicle_count INT
        |)""".stripMargin
  }

  def income_band(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}income_band(
        | ib_income_band_sk BIGINT,
        | ib_lower_bound INT,
        | ib_upper_bound INT
        |)""".stripMargin
  }

  def item(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}item(
        | i_item_sk BIGINT,
        | i_item_id STRING,
        | i_rec_start_date DATE,
        | i_rec_end_date DATE,
        | i_item_desc STRING,
        | i_current_price DECIMAL(7,2),
        | i_wholesale_cost DECIMAL(7,2),
        | i_brand_id INT,
        | i_brand STRING,
        | i_class_id INT,
        | i_class STRING,
        | i_category_id INT,
        | i_category STRING,
        | i_manufact_id INT,
        | i_manufact STRING,
        | i_size STRING,
        | i_formulation STRING,
        | i_color STRING,
        | i_units STRING,
        | i_container STRING,
        | i_manager_id INT,
        | i_product_name STRING
        |)""".stripMargin
  }

  def promotion(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}promotion(
        | p_promo_sk BIGINT,
        | p_promo_id STRING,
        | p_start_date_sk BIGINT,
        | p_end_date_sk BIGINT,
        | p_item_sk BIGINT,
        | p_cost DECIMAL(15,2),
        | p_response_target INT,
        | p_promo_name STRING,
        | p_channel_dmail STRING,
        | p_channel_email STRING,
        | p_channel_catalog STRING,
        | p_channel_tv STRING,
        | p_channel_radio STRING,
        | p_channel_press STRING,
        | p_channel_event STRING,
        | p_channel_demo STRING,
        | p_channel_details VARCHAR(100),
        | p_purpose STRING,
        | p_discount_active STRING
        |)""".stripMargin
  }

  def reason(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}reason(
        | r_reason_sk BIGINT,
        | r_reason_id STRING,
        | r_reason_desc STRING
        |)""".stripMargin
  }

  def ship_mode(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}ship_mode(
        | sm_ship_mode_sk BIGINT,
        | sm_ship_mode_id STRING,
        | sm_type STRING,
        | sm_code STRING,
        | sm_carrier STRING,
        | sm_contract STRING
        |)""".stripMargin
  }

  def store(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}store(
        | s_store_sk BIGINT,
        | s_store_id STRING,
        | s_rec_start_date DATE,
        | s_rec_end_date DATE,
        | s_closed_date_sk BIGINT,
        | s_store_name VARCHAR(50),
        | s_number_employees INT,
        | s_floor_space INT,
        | s_hours STRING,
        | s_manager VARCHAR(40),
        | s_market_id INT,
        | s_geography_class VARCHAR(100),
        | s_market_desc VARCHAR(100),
        | s_market_manager VARCHAR(40),
        | s_division_id INT,
        | s_division_name VARCHAR(50),
        | s_company_id INT,
        | s_company_name VARCHAR(50),
        | s_street_number VARCHAR(10),
        | s_street_name VARCHAR(60),
        | s_street_type STRING,
        | s_suite_number STRING,
        | s_city VARCHAR(60),
        | s_county VARCHAR(30),
        | s_state STRING,
        | s_zip STRING,
        | s_country VARCHAR(20),
        | s_gmt_offset DECIMAL(5,2),
        | s_tax_percentage DECIMAL(5,2)
        |)""".stripMargin
  }

  def time_dim(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}time_dim(
        | t_time_sk BIGINT,
        | t_time_id STRING,
        | t_time INT,
        | t_hour INT,
        | t_minute INT,
        | t_second INT,
        | t_am_pm STRING,
        | t_shift STRING,
        | t_sub_shift STRING,
        | t_meal_time STRING
        |)""".stripMargin
  }

  def warehouse(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}warehouse(
        | w_warehouse_sk BIGINT,
        | w_warehouse_id STRING,
        | w_warehouse_name VARCHAR(20),
        | w_warehouse_sq_ft INT,
        | w_street_number STRING,
        | w_street_name VARCHAR(60),
        | w_street_type STRING,
        | w_suite_number STRING,
        | w_city VARCHAR(60),
        | w_county VARCHAR(30),
        | w_state STRING,
        | w_zip STRING,
        | w_country VARCHAR(20),
        | w_gmt_offset DECIMAL(5,2)
        |)""".stripMargin
  }

  def web_page(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}web_page(
        | wp_web_page_sk BIGINT,
        | wp_web_page_id STRING,
        | wp_rec_start_date DATE,
        | wp_rec_end_date DATE,
        | wp_creation_date_sk BIGINT,
        | wp_access_date_sk BIGINT,
        | wp_autogen_flag STRING,
        | wp_customer_sk BIGINT,
        | wp_url VARCHAR(100),
        | wp_type STRING,
        | wp_char_count INT,
        | wp_link_count INT,
        | wp_image_count INT,
        | wp_max_ad_count INT
        |)""".stripMargin
  }

  def web_site(prefix: String): String = {
    s"""CREATE TEMPORARY TABLE ${prefix}web_site(
        | wp_web_page_sk BIGINT,
        | wp_web_page_id STRING,
        | wp_rec_start_date DATE,
        | wp_rec_end_date DATE,
        | wp_creation_date_sk BIGINT,
        | wp_access_date_sk BIGINT,
        | wp_autogen_flag STRING,
        | wp_customer_sk BIGINT,
        | wp_url VARCHAR(100),
        | wp_type STRING,
        | wp_char_count INT,
        | wp_link_count INT,
        | wp_image_count INT,
        | wp_max_ad_count INT
        |)""".stripMargin
  }

  /**
    * Returns a random value in [min, max] if randomize is true. Otherwise returns v.
    */
  private[tpcds] def uniformRand(v: Int, min: Int, max: Int): String = {
    require(v >= min)
    require(v <= max)
    require(min <= max)
    if (randomize) {
      val range = max - min + 1
      (min + (Math.random() * range).toInt).toString
    } else {
      v.toString
    }
  }

  /**
    * If randomize, returns a random value in `values`. Otherwise, returns the first value.
    */
  private[tpcds] def getValue[A](values: A*): A = {
    if (randomize) {
      values.toIndexedSeq((Math.random()* values.size).toInt)
    } else {
      values.head
    }
  }

  /** 
   *  If randomize, returns a random state. Otherwise, returns 'v'.
   */
  private def getState(v: String): String = {
    if (randomize) {
      getValue("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
        "IA", "KD", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
        "NJ", "NM", "NY", "NC", "NK", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
        "VT", "VA", "WA", "WV", "WI", "WY")
    } else {
      v
    }
  }

  /** 
   *  If randomize, returns a random category. Otherwise, returns 'v'.
   */
  private[tpcds] def getCategory(v: String): String = {
    if (randomize) {
      getValue("Sports", "Books", "Home", "Electronics", "Jewelry")
    } else {
      v
    }
  }

  /** 
   *  If randomize, returns a random gender. Otherwise, returns 'v'.
   */
  private[tpcds] def getGender(v: String): String = {
    if (randomize) {
      getValue("M", "F")
    } else {
      v
    }
  }

  /** 
   *  If randomize, returns a random education. Otherwise, returns 'v'.
   */
  private[tpcds] def getEducation(v: String): String = {
    if (randomize) {
      getValue("Advanced Degree", "College", "2 yr Degree", "4 yr Degree", "Unknown")
    } else {
      v
    }
  }

  /** 
   *  If randomize, returns a random marital status. Otherwise, returns 'v'.
   */
  private[tpcds] def getMartialStatus(v: String): String = {
    if (randomize) {
      getValue("M", "S", "W", "D")
    } else {
      v
    }
  }

  private def getColor(v: String): String = {
    if (randomize) {
      getValue("blanched", "brown", "burlywood", "burnished", "chiffon", "cornflower", "cyan",
        "deep", "floral", "forest", "frosted", "ghost", "honeydew", "indian", "khaki", "light",
        "medium", "midnight", "orange", "pale", "papaya", "powder",  "purple",  "slate", "snow",
        "spring")
    } else {
      v
    }
  }

  private def getSize(v: String): String = {
    if (randomize) {
      getValue("petite", "small", "medium", "large", "extra large", "N/A")
    } else {
      v
    }
  }

  private def getUnit(v: String): String = {
    if (randomize) {
      getValue("Ounce", "Oz", "Bunch", "Ton", "N/A", "Dozen", "Box", "Pound",
        "Pallet", "Gross", "Cup", "Dram", "Each", "Tbl", "Lb", "Bundle")
    } else {
      v
    }
  }

  private def date(v: String): String = {
    v
  }

  /**
   * If randomize, returns a random list of |v| using p to generate the list.
   * Otherwise, returns v.
   */
  private[tpcds] def getList[A](v: Seq[A], p: (A) => A): Seq[A] = {
    if (randomize) {
      v.map(x => p(v.head))
    } else {
      v
    }
  }

  /**
    * returns v as a sql in-list. e.g. 'v(0)', 'v(1)', ...
    */
  private[tpcds] def toInList(v: Seq[Any]): String = {
    v.map(x => s"'$x'").mkString(", ")
  }

  def q13(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val m1 = getMartialStatus("M")
    val m2 = getMartialStatus("S")
    val m3 = getMartialStatus("W")
    val e1 = getEducation("Advanced Degree")
    val e2 = getEducation("College")
    val e3 = getEducation("2 yr Degree")
    // TODO define STATE= ulist(dist(fips_county, 3, 1), 9);
    val states = Seq("TX", "OH", "TX", "OR", "NM", "KY", "VA", "TX", "MS")
    s"""
       | select avg(ss_quantity)
       |       ,avg(ss_ext_sales_price)
       |       ,avg(ss_ext_wholesale_cost)
       |       ,sum(ss_ext_wholesale_cost)
       | from store_sales
       |     ,store
       |     ,customer_demographics
       |     ,household_demographics
       |     ,customer_address
       |     ,date_dim
       | where s_store_sk = ss_store_sk
       | and  ss_sold_date_sk = d_date_sk and d_year = $year
       | and((ss_hdemo_sk=hd_demo_sk
       |  and cd_demo_sk = ss_cdemo_sk
       |  and cd_marital_status = '$m1'
       |  and cd_education_status = '$e1'
       |  and ss_sales_price between 100.00 and 150.00
       |  and hd_dep_count = 3
       |     )or
       |     (ss_hdemo_sk=hd_demo_sk
       |  and cd_demo_sk = ss_cdemo_sk
       |  and cd_marital_status = '$m2'
       |  and cd_education_status = '$e2'
       |  and ss_sales_price between 50.00 and 100.00
       |  and hd_dep_count = 1
       |     ) or
       |     (ss_hdemo_sk=hd_demo_sk
       |  and cd_demo_sk = ss_cdemo_sk
       |  and cd_marital_status = '$m3'
       |  and cd_education_status = '$e3'
       |  and ss_sales_price between 150.00 and 200.00
       |  and hd_dep_count = 1
       |     ))
       | and((ss_addr_sk = ca_address_sk
       |  and ca_country = 'United States'
       |  and ca_state in ('${states(0)}', '${states(1)}', '${states(2)}')
       |  and ss_net_profit between 100 and 200
       |     ) or
       |     (ss_addr_sk = ca_address_sk
       |  and ca_country = 'United States'
       |  and ca_state in ('${states(3)}', '${states(4)}', '${states(5)}')
       |  and ss_net_profit between 150 and 300
       |     ) or
       |     (ss_addr_sk = ca_address_sk
       |  and ca_country = 'United States'
       |  and ca_state in ('${states(6)}', '${states(7)}', '${states(8)}')
       |  and ss_net_profit between 50 and 250
       |     ))
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q14(): String = {
    val year = uniformRand(1999, 1998, 2000)
    val day = uniformRand(11, 1, 28)
    s"""
       |with  cross_items as
       | (select i_item_sk ss_item_sk
       | from item,
       |    (select iss.i_brand_id brand_id
       |     ,iss.i_class_id class_id
       |     ,iss.i_category_id category_id
       | from store_sales
       |     ,item iss
       |     ,date_dim d1
       | where ss_item_sk = iss.i_item_sk
       |   and ss_sold_date_sk = d1.d_date_sk
       |   and d1.d_year between 1999 AND 1999 + 2
       | intersect
       | select ics.i_brand_id
       |     ,ics.i_class_id
       |     ,ics.i_category_id
       | from catalog_sales
       |     ,item ics
       |     ,date_dim d2
       | where cs_item_sk = ics.i_item_sk
       |   and cs_sold_date_sk = d2.d_date_sk
       |   and d2.d_year between 1999 AND 1999 + 2
       | intersect
       | select iws.i_brand_id
       |     ,iws.i_class_id
       |     ,iws.i_category_id
       | from web_sales
       |     ,item iws
       |     ,date_dim d3
       | where ws_item_sk = iws.i_item_sk
       |   and ws_sold_date_sk = d3.d_date_sk
       |   and d3.d_year between 1999 AND 1999 + 2) x
       | where i_brand_id = brand_id
       |      and i_class_id = class_id
       |      and i_category_id = category_id
       |),
       | avg_sales as
       | (select avg(quantity*list_price) average_sales
       |  from (select ss_quantity quantity
       |             ,ss_list_price list_price
       |       from store_sales
       |           ,date_dim
       |       where ss_sold_date_sk = d_date_sk
       |         and d_year between 1999 and 2001
       |       union all
       |       select cs_quantity quantity
       |             ,cs_list_price list_price
       |       from catalog_sales
       |           ,date_dim
       |       where cs_sold_date_sk = d_date_sk
       |         and d_year between $year and $year + 2
       |       union all
       |       select ws_quantity quantity
       |             ,ws_list_price list_price
       |       from web_sales
       |           ,date_dim
       |       where ws_sold_date_sk = d_date_sk
       |         and d_year between $year and $year + 2) x)
       | select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
       | from(
       |       select 'store' channel, i_brand_id,i_class_id
       |             ,i_category_id,sum(ss_quantity*ss_list_price) sales
       |             , count(*) number_sales
       |       from store_sales
       |           ,item
       |           ,date_dim
       |       where ss_item_sk in (select ss_item_sk from cross_items)
       |         and ss_item_sk = i_item_sk
       |         and ss_sold_date_sk = d_date_sk
       |         and d_year = $year+2
       |         and d_moy = 11
       |       group by i_brand_id,i_class_id,i_category_id
       |       having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
       |       union all
       |       select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
       |       from catalog_sales
       |           ,item
       |           ,date_dim
       |       where cs_item_sk in (select ss_item_sk from cross_items)
       |         and cs_item_sk = i_item_sk
       |         and cs_sold_date_sk = d_date_sk
       |         and d_year = $year+2
       |         and d_moy = 11
       |       group by i_brand_id,i_class_id,i_category_id
       |       having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
       |       union all
       |       select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
       |       from web_sales
       |           ,item
       |           ,date_dim
       |       where ws_item_sk in (select ss_item_sk from cross_items)
       |         and ws_item_sk = i_item_sk
       |         and ws_sold_date_sk = d_date_sk
       |         and d_year = $year+2
       |         and d_moy = 11
       |       group by i_brand_id,i_class_id,i_category_id
       |       having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
       | ) y
       | group by rollup (channel, i_brand_id,i_class_id,i_category_id)
       | order by channel,i_brand_id,i_class_id,i_category_id
       | limit 100;
       |
       | with  cross_items as
       | (select i_item_sk ss_item_sk
       | from item,
       | (select iss.i_brand_id brand_id, iss.i_class_id class_id, iss.i_category_id category_id
       | from store_sales, item iss, date_dim d1
       | where ss_item_sk = iss.i_item_sk
       |   and ss_sold_date_sk = d1.d_date_sk
       |   and d1.d_year between 1999 AND 1999 + 2
       | intersect
       | select ics.i_brand_id, ics.i_class_id, ics.i_category_id
       | from catalog_sales, item ics, date_dim d2
       | where cs_item_sk = ics.i_item_sk
       |   and cs_sold_date_sk = d2.d_date_sk
       |   and d2.d_year between 1999 AND 1999 + 2
       | intersect
       | select iws.i_brand_id, iws.i_class_id, iws.i_category_id
       | from web_sales, item iws, date_dim d3
       | where ws_item_sk = iws.i_item_sk
       |   and ws_sold_date_sk = d3.d_date_sk
       |   and d3.d_year between 1999 AND 1999 + 2) x
       | where i_brand_id = brand_id
       |      and i_class_id = class_id
       |      and i_category_id = category_id
       |),
       | avg_sales as
       |(select avg(quantity*list_price) average_sales
       |  from (select ss_quantity quantity
       |             ,ss_list_price list_price
       |       from store_sales, date_dim
       |       where ss_sold_date_sk = d_date_sk
       |         and d_year between $year and $year + 2
       |       union all
       |       select cs_quantity quantity, cs_list_price list_price
       |       from catalog_sales, date_dim
       |       where cs_sold_date_sk = d_date_sk
       |         and d_year between $year and $year + 2
       |       union all
       |       select ws_quantity quantity, ws_list_price list_price
       |       from web_sales, date_dim
       |       where ws_sold_date_sk = d_date_sk
       |         and d_year between $year and $year + 2) x)
       | select * from
       | (select 'store' channel, i_brand_id,i_class_id,i_category_id
       |        ,sum(ss_quantity*ss_list_price) sales, count(*) number_sales
       | from store_sales
       |     ,item
       |     ,date_dim
       | where ss_item_sk in (select ss_item_sk from cross_items)
       |   and ss_item_sk = i_item_sk
       |   and ss_sold_date_sk = d_date_sk
       |   and d_week_seq = (select d_week_seq
       |                     from date_dim
       |                     where d_year = $year + 1
       |                       and d_moy = 12
       |                       and d_dom = $day)
       | group by i_brand_id,i_class_id,i_category_id
       | having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) this_year,
       | (select 'store' channel, i_brand_id,i_class_id
       |        ,i_category_id, sum(ss_quantity*ss_list_price) sales, count(*) number_sales
       | from store_sales
       |     ,item
       |     ,date_dim
       | where ss_item_sk in (select ss_item_sk from cross_items)
       |   and ss_item_sk = i_item_sk
       |   and ss_sold_date_sk = d_date_sk
       |   and d_week_seq = (select d_week_seq
       |                     from date_dim
       |                     where d_year = $year
       |                       and d_moy = 12
       |                       and d_dom = $day)
       | group by i_brand_id,i_class_id,i_category_id
       | having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) last_year
       | where this_year.i_brand_id= last_year.i_brand_id
       |   and this_year.i_class_id = last_year.i_class_id
       |   and this_year.i_category_id = last_year.i_category_id
       | order by this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
       | limit 100
     """.stripMargin
  }

  def q15(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val qoy = uniformRand(2, 1, 2)
    s"""
       | select ca_zip
       |       ,sum(cs_sales_price)
       | from catalog_sales
       |     ,customer
       |     ,customer_address
       |     ,date_dim
       | where cs_bill_customer_sk = c_customer_sk
       | 	and c_current_addr_sk = ca_address_sk
       | 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
       |                                   '85392', '85460', '80348', '81792')
       | 	      or ca_state in ('CA','WA','GA')
       | 	      or cs_sales_price > 500)
       | 	and cs_sold_date_sk = d_date_sk
       | 	and d_qoy = $qoy and d_year = $year
       | group by ca_zip
       | order by ca_zip
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q16(): String = {
    val year = uniformRand(2002, 1999, 2002)
    val month = uniformRand(2, 2, 5)
    val state = "GA"
    // TODO define STATE = dist(fips_county,3,1);
    // TODO counties
    val counties = toInList(Seq("Williamson County",
      "Williamson County", "Williamson County", "Williamson County", "Williamson County"))

    s"""
       | select
       |   count(distinct cs_order_number) as "order count",
       |   sum(cs_ext_ship_cost) as "total shipping cost",
       |   sum(cs_net_profit) as "total net profit"
       |from
       |   catalog_sales cs1, date_dim, customer_address, call_center
       |where
       |    d_date between '$year-$month-01' and
       |           (cast('$year-$month-01' as date) + 60)
       |and cs1.cs_ship_date_sk = d_date_sk
       |and cs1.cs_ship_addr_sk = ca_address_sk
       |and ca_state = '$state'
       |and cs1.cs_call_center_sk = cc_call_center_sk
       |and cc_county in ($counties)
       |and exists (select *
       |            from catalog_sales cs2
       |            where cs1.cs_order_number = cs2.cs_order_number
       |              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
       |and not exists(select *
       |               from catalog_returns cr1
       |               where cs1.cs_order_number = cr1.cr_order_number)
       |order by count(distinct cs_order_number)
       |limit 100
     """.stripMargin
  }

  def q17(): String = {
    val year = uniformRand(2001, 1998, 2002)
    s"""
       | select i_item_id
       |       ,i_item_desc
       |       ,s_state
       |       ,count(ss_quantity) as store_sales_quantitycount
       |       ,avg(ss_quantity) as store_sales_quantityave
       |       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       |       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       |       ,count(sr_return_quantity) as_store_returns_quantitycount
       |       ,avg(sr_return_quantity) as_store_returns_quantityave
       |       ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
       |       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       |       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       |       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
       |       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
       | from store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2, date_dim d3, store, item
       | where d1.d_quarter_name = '{$year}Q1'
       |   and d1.d_date_sk = ss_sold_date_sk
       |   and i_item_sk = ss_item_sk
       |   and s_store_sk = ss_store_sk
       |   and ss_customer_sk = sr_customer_sk
       |   and ss_item_sk = sr_item_sk
       |   and ss_ticket_number = sr_ticket_number
       |   and sr_returned_date_sk = d2.d_date_sk
       |   and d2.d_quarter_name in ('{$year}Q1','{$year}Q2','{$year}Q3')
       |   and sr_customer_sk = cs_bill_customer_sk
       |   and sr_item_sk = cs_item_sk
       |   and cs_sold_date_sk = d3.d_date_sk
       |   and d3.d_quarter_name in ('{$year}Q1','{$year}Q2','{$year}Q3')
       | group by i_item_id, i_item_desc, s_state
       | order by i_item_id, i_item_desc, s_state
       | limit 100
     """.stripMargin
  }

  // Modifications: numeric(12,2) --> decimal(12,2)
  // [UNSUPPORTED]: rollup
  def q18(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val months = toInList(Seq(1, 6, 8, 9, 12, 2))
    val gender = getGender("F")
    val es = getEducation("Unknown")
    val states = toInList(getList(Seq("MS", "IN", "ND", "OK", "NM", "VA", "MS"), getState))
    s"""
       | select i_item_id,
       |        ca_country,
       |        ca_state,
       |        ca_county,
       |        avg( cast(cs_quantity as decimal(12,2))) agg1,
       |        avg( cast(cs_list_price as decimal(12,2))) agg2,
       |        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
       |        avg( cast(cs_sales_price as decimal(12,2))) agg4,
       |        avg( cast(cs_net_profit as decimal(12,2))) agg5,
       |        avg( cast(c_birth_year as decimal(12,2))) agg6,
       |        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
       | from catalog_sales, customer_demographics cd1,
       |      customer_demographics cd2, customer, customer_address, date_dim, item
       | where cs_sold_date_sk = d_date_sk and
       |       cs_item_sk = i_item_sk and
       |       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       |       cs_bill_customer_sk = c_customer_sk and
       |       cd1.cd_gender = '$gender' and
       |       cd1.cd_education_status = '$es' and
       |       c_current_cdemo_sk = cd2.cd_demo_sk and
       |       c_current_addr_sk = ca_address_sk and
       |       c_birth_month in ($months) and
       |       d_year = $year and
       |       ca_state in ($states)
       | group by rollup (i_item_id, ca_country, ca_state, ca_county)
       | order by ca_country, ca_state, ca_county, i_item_id
       | LIMIT 100
     """.stripMargin
  }

  def q19(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    val manager = 8
    // TODO
    //define MGR_IDX = dist(i_manager_id, 1, 1);
    //define MANAGER=random(distmember(i_manager_id, [MGR_IDX], 2), distmember(i_manager_id, [MGR_IDX], 3),uniform);
    s"""
       | select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
       | 	sum(ss_ext_sales_price) ext_price
       | from date_dim, store_sales, item,customer,customer_address,store
       | where d_date_sk = ss_sold_date_sk
       |   and ss_item_sk = i_item_sk
       |   and i_manager_id=$manager
       |   and d_moy=$month
       |   and d_year=$year
       |   and ss_customer_sk = c_customer_sk
       |   and c_current_addr_sk = ca_address_sk
       |   and substr(ca_zip,1,5) <> substr(s_zip,1,5)
       |   and ss_store_sk = s_store_sk
       | group by i_brand, i_brand_id, i_manufact_id, i_manufact
       | order by ext_price desc, brand, brand_id, i_manufact_id, i_manufact
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: partition
  def q20(): String = {
    val year = uniformRand(1999, 1998, 2002)
    // TODO define SDATE=date([YEAR]+"-01-01",[YEAR]+"-07-01",sales);
    val sdate = s"$year-02-22"
    val categories = toInList(getList(Seq("Sports", "Books", "Home"), getCategory))
    s"""
       |select i_item_desc
       |       ,i_category
       |       ,i_class
       |       ,i_current_price
       |       ,sum(cs_ext_sales_price) as itemrevenue
       |       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
       |           (partition by i_class) as revenueratio
       | from	catalog_sales, item, date_dim
       | where cs_item_sk = i_item_sk
       |   and i_category in ($categories)
       |   and cs_sold_date_sk = d_date_sk
       | and d_date between cast('$sdate' as date)
       | 				and date_add(cast('$sdate' as date), 30)
       | group by i_item_id, i_item_desc, i_category, i_class, i_current_price
       | order by i_category, i_class, i_item_id, i_item_desc, revenueratio
       | limit 100
     """.stripMargin
  }

  // Modifications: "date + 30 days" --> date_add(date, 30)
  def q21(): String = {
    val year = uniformRand(2000, 1998, 2002)
    // TODO define SALES_DATE=date([YEAR]+"-01-31",[YEAR]+"-7-01",sales);
    val sales_date = s"$year-03-11"
    s"""
       | select *
       | from(select w_warehouse_name
       |            ,i_item_id
       |            ,sum(case when (cast(d_date as date) < cast ('$sales_date' as date))
       |	                then inv_quantity_on_hand
       |                      else 0 end) as inv_before
       |            ,sum(case when (cast(d_date as date) >= cast ('$sales_date' as date))
       |                      then inv_quantity_on_hand
       |                      else 0 end) as inv_after
       |   from inventory, warehouse, item, date_dim
       |   where i_current_price between 0.99 and 1.49
       |     and i_item_sk          = inv_item_sk
       |     and inv_warehouse_sk   = w_warehouse_sk
       |     and inv_date_sk    = d_date_sk
       |     and d_date between date_sub(cast('$sales_date' as date), 30)
       |                    and date_add(cast('$sales_date' as date), 30)
       |   group by w_warehouse_name, i_item_id) x
       | where (case when inv_before > 0
       |             then inv_after / inv_before
       |             else null
       |             end) between 2.0/3.0 and 3.0/2.0
       | order by w_warehouse_name, i_item_id
       | limit 100
     """.stripMargin
  }

  // {UNSUPPORTED]: Rollup
  def q22(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       | select i_product_name, i_brand, i_class, i_category, avg(inv_quantity_on_hand) qoh
       |       from inventory, date_dim, item, warehouse
       |       where inv_date_sk=d_date_sk
       |              and inv_item_sk=i_item_sk
       |              and inv_warehouse_sk = w_warehouse_sk
       |              and d_month_seq between $dms and $dms + 11
       |       group by rollup(i_product_name, i_brand, i_class, i_category)
       | order by qoh, i_product_name, i_brand, i_class, i_category
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: Subquery
  def q23(): String = {
    val year = uniformRand(2000, 1998, 2000)
    val month = uniformRand(2, 1, 7)
    val top_percent = uniformRand(50, 0, 100)
    s"""
       | with frequent_ss_items as
       | (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
       |  from store_sales, date_dim, item
       |  where ss_sold_date_sk = d_date_sk
       |    and ss_item_sk = i_item_sk
       |    and d_year in ($year, $year+1, $year+2,$year+3)
       |  group by substr(i_item_desc,1,30),i_item_sk,d_date
       |  having count(*) >4),
       | max_store_sales as
       | (select max(csales) tpcds_cmax
       |  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
       |        from store_sales, customer, date_dim
       |        where ss_customer_sk = c_customer_sk
       |         and ss_sold_date_sk = d_date_sk
       |         and d_year in ($year, $year+1, $year+2,$year+3)
       |        group by c_customer_sk) x),
       | best_ss_customer as
       | (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
       |  from store_sales, customer
       |  where ss_customer_sk = c_customer_sk
       |  group by c_customer_sk
       |  having sum(ss_quantity*ss_sales_price) > ($top_percent/100.0) *
       |    (select * from max_store_sales))
       | select sum(sales)
       | from ((select cs_quantity*cs_list_price sales
       |       from catalog_sales, date_dim
       |       where d_year = $year
       |         and d_moy = $month
       |         and cs_sold_date_sk = d_date_sk
       |         and cs_item_sk in (select item_sk from frequent_ss_items)
       |         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer))
       |      union all
       |      (select ws_quantity*ws_list_price sales
       |       from web_sales, date_dim
       |       where d_year = $year
       |         and d_moy = $month
       |         and ws_sold_date_sk = d_date_sk
       |         and ws_item_sk in (select item_sk from frequent_ss_items)
       |         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))) y
       | limit 100;
       |
       | with frequent_ss_items as
       | (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
       |  from store_sales, date_dim, item
       |  where ss_sold_date_sk = d_date_sk
       |    and ss_item_sk = i_item_sk
       |    and d_year in ($year, $year+1, $year+2,$year+3)
       |  group by substr(i_item_desc,1,30),i_item_sk,d_date
       |  having count(*) > 4),
       | max_store_sales as
       | (select max(csales) tpcds_cmax
       |  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
       |        from store_sales, customer, date_dim
       |        where ss_customer_sk = c_customer_sk
       |         and ss_sold_date_sk = d_date_sk
       |         and d_year in ($year, $year+1, $year+2,$year+3)
       |        group by c_customer_sk) x),
       | best_ss_customer as
       | (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
       |  from store_sales
       |      ,customer
       |  where ss_customer_sk = c_customer_sk
       |  group by c_customer_sk
       |  having sum(ss_quantity*ss_sales_price) > ($top_percent/100.0) *
       |    (select * from max_store_sales))
       | select c_last_name,c_first_name,sales
       | from ((select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
       |        from catalog_sales, customer, date_dim
       |        where d_year = $year
       |         and d_moy = $month
       |         and cs_sold_date_sk = d_date_sk
       |         and cs_item_sk in (select item_sk from frequent_ss_items)
       |         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
       |         and cs_bill_customer_sk = c_customer_sk
       |       group by c_last_name,c_first_name)
       |      union all
       |      (select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
       |       from web_sales, customer, date_dim
       |       where d_year = $year
       |         and d_moy = $month
       |         and ws_sold_date_sk = d_date_sk
       |         and ws_item_sk in (select item_sk from frequent_ss_items)
       |         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
       |         and ws_bill_customer_sk = c_customer_sk
       |       group by c_last_name,c_first_name)) y
       |     order by c_last_name,c_first_name,sales
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: Subquery
  def q24(): String = {
    val market = uniformRand(8, 5, 10)
    val amount = getValue("ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit", "ss_sales_price",
      "ss_ext_sales_price")
    val c1 = getColor("pale")
    val c2 = getColor("chiffon")

    s"""
       |with ssales as
       |(select c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color,
       |        i_current_price, i_manager_id, i_units, i_size, sum($amount) netpaid
       |from store_sales, store_returns, store, item, customer, customer_address
       |where ss_ticket_number = sr_ticket_number
       |  and ss_item_sk = sr_item_sk
       |  and ss_customer_sk = c_customer_sk
       |  and ss_item_sk = i_item_sk
       |  and ss_store_sk = s_store_sk
       |  and c_birth_country = upper(ca_country)
       |  and s_zip = ca_zip
       |and s_market_id=$market
       |group by c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color,
       |         i_current_price, i_manager_id, i_units, i_size)
       |select c_last_name, c_first_name, s_store_name, sum(netpaid) paid
       |from ssales
       |where i_color = '$c1'
       |group by c_last_name, c_first_name, s_store_name
       |having sum(netpaid) > (select 0.05*avg(netpaid) from ssales);
       |
       |with ssales as
       |(select c_last_name
       |      ,c_first_name
       |      ,s_store_name
       |      ,ca_state
       |      ,s_state
       |      ,i_color
       |      ,i_current_price
       |      ,i_manager_id
       |      ,i_units
       |      ,i_size
       |      ,sum($amount) netpaid
       |from store_sales
       |    ,store_returns
       |    ,store
       |    ,item
       |    ,customer
       |    ,customer_address
       |where ss_ticket_number = sr_ticket_number
       |  and ss_item_sk = sr_item_sk
       |  and ss_customer_sk = c_customer_sk
       |  and ss_item_sk = i_item_sk
       |  and ss_store_sk = s_store_sk
       |  and c_birth_country = upper(ca_country)
       |  and s_zip = ca_zip
       |  and s_market_id = $market
       |group by c_last_name
       |        ,c_first_name
       |        ,s_store_name
       |        ,ca_state
       |        ,s_state
       |        ,i_color
       |        ,i_current_price
       |        ,i_manager_id
       |        ,i_units
       |        ,i_size)
       |select c_last_name
       |      ,c_first_name
       |      ,s_store_name
       |      ,sum(netpaid) paid
       |from ssales
       |where i_color = '$c2'
       |group by c_last_name
       |        ,c_first_name
       |        ,s_store_name
       |having sum(netpaid) > (select 0.05*avg(netpaid)
       |                           from ssales)
     """.stripMargin
  }

  def q25(): String = {
    val agg = getValue("sum", "min", "max", "avg", "stddev_samp")
    val year = uniformRand(2001, 1998, 2002)
    val month = uniformRand(4, 1, 9)
    s"""
       |
       | select i_item_id, i_item_desc, s_store_id, s_store_name,
       |    $agg(ss_net_profit) as store_sales_profit,
       |    $agg(sr_net_loss) as store_returns_loss,
       |    $agg(cs_net_profit) as catalog_sales_profit
       | from
       |    store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2, date_dim d3,
       |    store, item
       | where
       |    d1.d_moy = $month
       |    and d1.d_year = $year
       |    and d1.d_date_sk = ss_sold_date_sk
       |    and i_item_sk = ss_item_sk
       |    and s_store_sk = ss_store_sk
       |    and ss_customer_sk = sr_customer_sk
       |    and ss_item_sk = sr_item_sk
       |    and ss_ticket_number = sr_ticket_number
       |    and sr_returned_date_sk = d2.d_date_sk
       |    and d2.d_moy between $month and  10
       |    and d2.d_year = $year
       |    and sr_customer_sk = cs_bill_customer_sk
       |    and sr_item_sk = cs_item_sk
       |    and cs_sold_date_sk = d3.d_date_sk
       |    and d3.d_moy between $month and  10
       |    and d3.d_year = $year
       | group by
       |    i_item_id, i_item_desc, s_store_id, s_store_name
       | order by
       |    i_item_id, i_item_desc, s_store_id, s_store_name
       | limit 100
     """.stripMargin
  }


  def q26(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val gender = getGender("M")
    val maritalStatus = getMartialStatus("S")
    val education = getEducation("College")
    s"""
       | select i_item_id,
       |        avg(cs_quantity) agg1,
       |        avg(cs_list_price) agg2,
       |        avg(cs_coupon_amt) agg3,
       |        avg(cs_sales_price) agg4
       | from catalog_sales, customer_demographics, date_dim, item, promotion
       | where cs_sold_date_sk = d_date_sk and
       |       cs_item_sk = i_item_sk and
       |       cs_bill_cdemo_sk = cd_demo_sk and
       |       cs_promo_sk = p_promo_sk and
       |       cd_gender = '$gender' and
       |       cd_marital_status = '$maritalStatus' and
       |       cd_education_status = '$education' and
       |       (p_channel_email = 'N' or p_channel_event = 'N') and
       |       d_year = $year
       | group by i_item_id
       | order by i_item_id
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: rollup
  def q27(): String = {
    val year = uniformRand(2002, 1998, 2002)
    val gender = getGender("M")
    val maritalStatus = getMartialStatus("S")
    val education = getEducation("College")
    val states = toInList(getList(Seq("TN", "TN", "TN", "TN", "TN", "TN"), getState))
    s"""
       | select i_item_id,
       |        s_state, grouping(s_state) g_state,
       |        avg(ss_quantity) agg1,
       |        avg(ss_list_price) agg2,
       |        avg(ss_coupon_amt) agg3,
       |        avg(ss_sales_price) agg4
       | from store_sales, customer_demographics, date_dim, store, item
       | where ss_sold_date_sk = d_date_sk and
       |       ss_item_sk = i_item_sk and
       |       ss_store_sk = s_store_sk and
       |       ss_cdemo_sk = cd_demo_sk and
       |       cd_gender = '$gender' and
       |       cd_marital_status = '$maritalStatus' and
       |       cd_education_status = '$education' and
       |       d_year = $year and
       |       s_state in ($states)
       | group by rollup (i_item_id, s_state)
       | order by i_item_id
       |         ,s_state
       | limit 100
     """.stripMargin
  }

  def q28(): String = {
    val listPrice = getList[Int](Seq(8, 90, 142, 135, 122, 154), x => uniformRand(x, 0, 190).toInt)
    val coupon = getList[Int](Seq(459, 2323, 12214, 6071, 836, 7326),
      x => uniformRand(x, 0, 18000).toInt)
    val cost = getList[Int](Seq(57, 31, 79, 38, 17, 7), x => uniformRand(x, 0, 80).toInt)
    s"""
       | select *
       | from (select avg(ss_list_price) B1_LP
       |            ,count(ss_list_price) B1_CNT
       |            ,count(distinct ss_list_price) B1_CNTD
       |      from store_sales
       |      where ss_quantity between 0 and 5
       |        and (ss_list_price between ${listPrice(0)} and ${listPrice(0)}+10
       |             or ss_coupon_amt between ${coupon(0)} and ${coupon(0)}+1000
       |             or ss_wholesale_cost between ${cost(0)} and ${cost(0)}+20)) B1,
       |     (select avg(ss_list_price) B2_LP
       |            ,count(ss_list_price) B2_CNT
       |            ,count(distinct ss_list_price) B2_CNTD
       |      from store_sales
       |      where ss_quantity between 6 and 10
       |        and (ss_list_price between ${listPrice(1)} and ${listPrice(1)}+10
       |          or ss_coupon_amt between ${coupon(1)} and ${coupon(1)}+1000
       |          or ss_wholesale_cost between ${cost(1)} and ${cost(1)}+20)) B2,
       |     (select avg(ss_list_price) B3_LP
       |            ,count(ss_list_price) B3_CNT
       |            ,count(distinct ss_list_price) B3_CNTD
       |      from store_sales
       |      where ss_quantity between 11 and 15
       |        and (ss_list_price between ${listPrice(2)} and ${listPrice(2)}+10
       |          or ss_coupon_amt between ${coupon(2)} and ${coupon(2)}+1000
       |          or ss_wholesale_cost between ${cost(2)} and ${cost(2)}+20)) B3,
       |     (select avg(ss_list_price) B4_LP
       |            ,count(ss_list_price) B4_CNT
       |            ,count(distinct ss_list_price) B4_CNTD
       |      from store_sales
       |      where ss_quantity between 16 and 20
       |        and (ss_list_price between ${listPrice(3)} and ${listPrice(3)}+10
       |          or ss_coupon_amt between ${coupon(3)} and ${coupon(3)}+1000
       |          or ss_wholesale_cost between ${cost(3)} and ${cost(3)}+20)) B4,
       |     (select avg(ss_list_price) B5_LP
       |            ,count(ss_list_price) B5_CNT
       |            ,count(distinct ss_list_price) B5_CNTD
       |      from store_sales
       |      where ss_quantity between 21 and 25
       |        and (ss_list_price between ${listPrice(4)} and ${listPrice(4)}+10
       |          or ss_coupon_amt between ${coupon(4)} and ${coupon(4)}+1000
       |          or ss_wholesale_cost between ${cost(4)} and ${cost(4)}+20)) B5,
       |     (select avg(ss_list_price) B6_LP
       |            ,count(ss_list_price) B6_CNT
       |            ,count(distinct ss_list_price) B6_CNTD
       |      from store_sales
       |      where ss_quantity between 26 and 30
       |        and (ss_list_price between ${listPrice(5)} and ${listPrice(5)}+10
       |          or ss_coupon_amt between ${coupon(5)} and ${coupon(5)}+1000
       |          or ss_wholesale_cost between ${cost(5)} and ${cost(5)}+20)) B6
       | limit 100
     """.stripMargin
  }

  def q29(): String = {
    val year = uniformRand(1999, 1998, 2000)
    val month = uniformRand(9, 1, 9)
    val agg = getValue("sum", "min", "max", "avg", "stddev_samp")
    s"""
       | select
       |     i_item_id
       |    ,i_item_desc
       |    ,s_store_id
       |    ,s_store_name
       |    ,$agg(ss_quantity)        as store_sales_quantity
       |    ,$agg(sr_return_quantity) as store_returns_quantity
       |    ,$agg(cs_quantity)        as catalog_sales_quantity
       | from
       |    store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2,
       |    date_dim d3, store, item
       | where
       |     d1.d_moy               = $month
       | and d1.d_year              = $year
       | and d1.d_date_sk           = ss_sold_date_sk
       | and i_item_sk              = ss_item_sk
       | and s_store_sk             = ss_store_sk
       | and ss_customer_sk         = sr_customer_sk
       | and ss_item_sk             = sr_item_sk
       | and ss_ticket_number       = sr_ticket_number
       | and sr_returned_date_sk    = d2.d_date_sk
       | and d2.d_moy               between $month and  $month + 3
       | and d2.d_year              = $year
       | and sr_customer_sk         = cs_bill_customer_sk
       | and sr_item_sk             = cs_item_sk
       | and cs_sold_date_sk        = d3.d_date_sk
       | and d3.d_year              in ($year,$year+1,$year+2)
       | group by
       |    i_item_id, i_item_desc, s_store_id, s_store_name
       | order by
       |    i_item_id, i_item_desc, s_store_id, s_store_name
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q30(): String = {
    val year = uniformRand(2002, 1999, 2002)
    val state = getState("GA")
    s"""
       | with customer_total_return as
       | (select wr_returning_customer_sk as ctr_customer_sk
       |        ,ca_state as ctr_state,
       | 	sum(wr_return_amt) as ctr_total_return
       | from web_returns
       |     ,date_dim
       |     ,customer_address
       | where wr_returned_date_sk = d_date_sk
       |   and d_year = $year
       |   and wr_returning_addr_sk = ca_address_sk
       | group by wr_returning_customer_sk
       |         ,ca_state)
       | select c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       |       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       |       ,c_last_review_date,ctr_total_return
       | from customer_total_return ctr1, customer_address, customer
       | where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
       | 			  from customer_total_return ctr2
       |                  	  where ctr1.ctr_state = ctr2.ctr_state)
       |       and ca_address_sk = c_current_addr_sk
       |       and ca_state = '$state'
       |       and ctr1.ctr_customer_sk = c_customer_sk
       | order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       |                  ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       |                  ,c_last_review_date,ctr_total_return
       | limit 100
     """.stripMargin
  }

  def q31(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val agg = getValue("ss1.ca_county", "ss1.d_year", "web_q1_q2_increase", "store_q1_q2_increase",
      "web_q2_q3_increase", "store_q2_q3_increase")
    s"""
       | with ss as
       | (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales
       | from store_sales,date_dim,customer_address
       | where ss_sold_date_sk = d_date_sk
       |  and ss_addr_sk=ca_address_sk
       | group by ca_county,d_qoy, d_year),
       | ws as
       | (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales
       | from web_sales,date_dim,customer_address
       | where ws_sold_date_sk = d_date_sk
       |  and ws_bill_addr_sk=ca_address_sk
       | group by ca_county,d_qoy, d_year)
       | select
       |        ss1.ca_county
       |       ,ss1.d_year
       |       ,ws2.web_sales/ws1.web_sales web_q1_q2_increase
       |       ,ss2.store_sales/ss1.store_sales store_q1_q2_increase
       |       ,ws3.web_sales/ws2.web_sales web_q2_q3_increase
       |       ,ss3.store_sales/ss2.store_sales store_q2_q3_increase
       | from
       |        ss ss1, ss ss2, ss ss3, ws ws1, ws ws2, ws ws3
       | where
       |    ss1.d_qoy = 1
       |    and ss1.d_year = $year
       |    and ss1.ca_county = ss2.ca_county
       |    and ss2.d_qoy = 2
       |    and ss2.d_year = $year
       | and ss2.ca_county = ss3.ca_county
       |    and ss3.d_qoy = 3
       |    and ss3.d_year = $year
       |    and ss1.ca_county = ws1.ca_county
       |    and ws1.d_qoy = 1
       |    and ws1.d_year = $year
       |    and ws1.ca_county = ws2.ca_county
       |    and ws2.d_qoy = 2
       |    and ws2.d_year = $year
       |    and ws1.ca_county = ws3.ca_county
       |    and ws3.d_qoy = 3
       |    and ws3.d_year = $year
       |    and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end
       |       > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end
       |    and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end
       |       > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end
       | order by $agg
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q32(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val imid = uniformRand(977, 1, 1000)
    // TODO  CSDATE = date([YEAR]+"-01-01",[YEAR]+"-04-01",sales);
    val csdate = s"$year-01-27"
    s"""
       |select sum(cs_ext_discount_amt) as "excess discount amount"
       |from
       |   catalog_sales, item, date_dim
       |where
       |  i_manufact_id = $imid
       |  and i_item_sk = cs_item_sk
       |  and d_date between '$csdate' and (cast('$csdate' as date) + 90 days)
       |  and d_date_sk = cs_sold_date_sk
       |  and cs_ext_discount_amt
       |     > (
       |         select
       |            1.3 * avg(cs_ext_discount_amt)
       |         from
       |            catalog_sales, date_dim
       |         where
       |              cs_item_sk = i_item_sk
       |          and d_date between '$csdate]' and (cast('$csdate' as date) + 90 days)
       |          and d_date_sk = cs_sold_date_sk
       |      )
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: Subquery
  def q33(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val month = uniformRand(5, 1, 7)
    // TODO define COUNTY=random(1, rowcount("active_counties", "store"), uniform);
    // define GMT=distmember(fips_county,[COUNTY], 6);
    val gmt = -5
    val category = getCategory("Electronics")
    s"""
       | with ss as (
       |    select
       |        i_manufact_id,sum(ss_ext_sales_price) total_sales
       |    from
       | 	      store_sales, date_dim, customer_address, item
       |    where
       |        i_manufact_id in (select i_manufact_id
       |                          from item
       |                          where i_category in ('$category'))
       |                            and ss_item_sk = i_item_sk
       |                            and ss_sold_date_sk = d_date_sk
       |                            and d_year = $year
       |                            and d_moy = $month
       |                            and ss_addr_sk = ca_address_sk
       |                            and ca_gmt_offset = $gmt
       |                          group by i_manufact_id), cs as
       |         (select i_manufact_id, sum(cs_ext_sales_price) total_sales
       |          from catalog_sales, date_dim, customer_address, item
       |          where
       |            i_manufact_id in (
       |                select i_manufact_id from item
       |                where
       |                    i_category in ('$category'))
       |                    and cs_item_sk = i_item_sk
       |                    and cs_sold_date_sk = d_date_sk
       |                    and d_year = $year
       |                    and d_moy = $month
       |                    and cs_bill_addr_sk = ca_address_sk
       |                    and ca_gmt_offset = $gmt
       |                group by i_manufact_id),
       | ws as (
       | select i_manufact_id,sum(ws_ext_sales_price) total_sales
       | from
       | 	  web_sales, date_dim, customer_address, item
       | where
       |    i_manufact_id in (select i_manufact_id from item
       |                      where i_category in ('$category'))
       |                          and ws_item_sk = i_item_sk
       |                          and ws_sold_date_sk = d_date_sk
       |                          and d_year = $year
       |                          and d_moy = $month
       |                          and ws_bill_addr_sk = ca_address_sk
       |                          and ca_gmt_offset = $gmt
       |                      group by i_manufact_id)
       | select i_manufact_id ,sum(total_sales) total_sales
       | from  (select * from ss
       |        union all
       |        select * from cs
       |        union all
       |        select * from ws) tmp1
       | group by i_manufact_id
       | order by total_sales
       |limit 100
     """.stripMargin
  }

  def q34(): String = {
    val year = uniformRand(1999, 1998, 2000)
    val bpone = getValue(">10000", "1001-5000", "501-1000")
    val bptwo = getValue("unknown", "0-500", "5001-10000")
    // TODO  define COUNTYNUMBER=ulist(random(1, rowcount("active_counties", "store"), uniform), 8);
    val counties = toInList(Seq("Williamson County", "Williamson County", "Williamson County",
      "Williamson County", "Williamson County", "Williamson County", "Williamson County",
      "Williamson County"))
    s"""
       | select c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number,
       |        cnt
       | FROM
       |   (select ss_ticket_number, ss_customer_sk, count(*) cnt
       |    from store_sales,date_dim,store,household_demographics
       |    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |    and store_sales.ss_store_sk = store.s_store_sk
       |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       |    and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
       |    and (household_demographics.hd_buy_potential = '$bpone' or
       |         household_demographics.hd_buy_potential = '$bptwo')
       |    and household_demographics.hd_vehicle_count > 0
       |    and (case when household_demographics.hd_vehicle_count > 0
       |	then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count
       |	else null
       |	end)  > 1.2
       |    and date_dim.d_year in ($year, $year+1, $year+2)
       |    and store.s_county in ($counties)
       |    group by ss_ticket_number,ss_customer_sk) dn,customer
       |    where ss_customer_sk = c_customer_sk
       |      and cnt between 15 and 20
       |    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q35(): String = {
    val year = uniformRand(2002, 1999, 2002)
    val aggone = getValue("min", "sum", "max", "avg", "stddev_samp")
    val aggtwo = getValue("max", "sum", "min", "avg", "stddev_samp")
    val aggthree = getValue("avg", "sum", "min", "max", "stddev_samp")
    s"""
       | select
       |  ca_state,
       |  cd_gender,
       |  cd_marital_status,
       |  count(*) cnt1,
       |  $aggone(cd_dep_count),
       |  $aggtwo(cd_dep_count),
       |  $aggthree(cd_dep_count),
       |  cd_dep_employed_count,
       |  count(*) cnt2,
       |  $aggone(cd_dep_employed_count),
       |  $aggtwo(cd_dep_employed_count),
       |  $aggthree(cd_dep_employed_count),
       |  cd_dep_college_count,
       |  count(*) cnt3,
       |  $aggone(cd_dep_college_count),
       |  $aggtwo(cd_dep_college_count),
       |  $aggthree(cd_dep_college_count)
       | from
       |  customer c,customer_address ca,customer_demographics
       | where
       |  c.c_current_addr_sk = ca.ca_address_sk and
       |  cd_demo_sk = c.c_current_cdemo_sk and
       |  exists (select *
       |          from store_sales,date_dim
       |          where c.c_customer_sk = ss_customer_sk and
       |                ss_sold_date_sk = d_date_sk and
       |                d_year = $year and
       |                d_qoy < 4) and
       |   (exists (select *
       |            from web_sales,date_dim
       |            where c.c_customer_sk = ws_bill_customer_sk and
       |                  ws_sold_date_sk = d_date_sk and
       |                  d_year = $year and
       |                  d_qoy < 4) or
       |    exists (select *
       |            from catalog_sales,date_dim
       |            where c.c_customer_sk = cs_ship_customer_sk and
       |                  cs_sold_date_sk = d_date_sk and
       |                  d_year = $year and
       |                  d_qoy < 4))
       | group by ca_state,
       |          cd_gender,
       |          cd_marital_status,
       |          cd_dep_count,
       |          cd_dep_employed_count,
       |          cd_dep_college_count
       | order by ca_state,
       |          cd_gender,
       |          cd_marital_status,
       |          cd_dep_count,
       |          cd_dep_employed_count,
       |          cd_dep_college_count
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window function, rollup
  def q36(): String = {
    val year = uniformRand(2001, 1998, 2002)
    // TODO: define STATENUMBER=ulist(random(1, rowcount("active_states", "store"), uniform),8);
    val states = toInList(Seq("TN", "TN", "TN", "TN", "TN", "TN", "TN", "TN"))
    s"""
       | select
       |    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
       |   ,i_category
       |   ,i_class
       |   ,grouping(i_category)+grouping(i_class) as lochierarchy
       |   ,rank() over (
       | 	partition by grouping(i_category)+grouping(i_class),
       | 	case when grouping(i_class) = 0 then i_category end
       | 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
       | from
       |    store_sales, date_dim d1, item, store
       | where
       |    d1.d_year = $year
       |    and d1.d_date_sk = ss_sold_date_sk
       |    and i_item_sk  = ss_item_sk
       |    and s_store_sk  = ss_store_sk
       |    and s_state in ($states)
       | group by rollup(i_category,i_class)
       | order by
       |   lochierarchy desc
       |  ,case when lochierarchy = 0 then i_category end
       |  ,rank_within_parent
       | limit 100
     """.stripMargin
  }

  // Modifications "date + 60 days" --> date_add(date, 60)
  def q37(): String = {
    // TODO INVDATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales);
    val invdate = "2000-02-01"
    val price = uniformRand(68, 10, 78)
    val manufacts = getList[Int](Seq(677, 940, 694, 808), x => uniformRand(x, 667, 1000).toInt)
    s"""
       | select i_item_id, i_item_desc, i_current_price
       | from item, inventory, date_dim, catalog_sales
       | where i_current_price between $price and $price + 30
       | and inv_item_sk = i_item_sk
       | and d_date_sk=inv_date_sk
       | and d_date between cast('$invdate' as date) and date_add(cast('$invdate' as date), 60)
       | and i_manufact_id in (${manufacts(0)},${manufacts(1)},${manufacts(2)},${manufacts(3)})
       | and inv_quantity_on_hand between 100 and 500
       | and cs_item_sk = i_item_sk
       | group by i_item_id,i_item_desc,i_current_price
       | order by i_item_id
       | limit 100
     """.stripMargin
  }

  def q38(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       | select count(*) from (
       |    select distinct c_last_name, c_first_name, d_date
       |    from store_sales, date_dim, customer
       |          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |      and store_sales.ss_customer_sk = customer.c_customer_sk
       |      and d_month_seq between $dms and  $dms + 11
       |  intersect
       |    select distinct c_last_name, c_first_name, d_date
       |    from catalog_sales, date_dim, customer
       |          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
       |      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
       |      and d_month_seq between  $dms and  $dms + 11
       |  intersect
       |    select distinct c_last_name, c_first_name, d_date
       |    from web_sales, date_dim, customer
       |          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
       |      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
       |      and d_month_seq between  $dms and  $dms + 11
       | ) hot_cust
       | limit 100
     """.stripMargin
  }

  def q39(): String = {
    // TODO: multiple queries
    val month = uniformRand(1, 1, 4)
    val year = uniformRand(2001, 1998, 2002)
    s"""
       |with inv as
       |(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       |       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
       | from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       |            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
       |      from inventory, item, warehouse, date_dim
       |      where inv_item_sk = i_item_sk
       |        and inv_warehouse_sk = w_warehouse_sk
       |        and inv_date_sk = d_date_sk
       |        and d_year = $year
       |      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
       | where case mean when 0 then 0 else stdev/mean end > 1)
       |select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
       |        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
       |from inv inv1,inv inv2
       |where inv1.i_item_sk = inv2.i_item_sk
       |  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
       |  and inv1.d_moy=$month
       |  and inv2.d_moy=$month+1
       |order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
       |        ,inv2.d_moy,inv2.mean, inv2.cov;
       |
       |with inv as
       |(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       |       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
       | from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       |            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
       |      from inventory, item, warehouse, date_dim
       |      where inv_item_sk = i_item_sk
       |        and inv_warehouse_sk = w_warehouse_sk
       |        and inv_date_sk = d_date_sk
       |        and d_year = $year
       |      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
       | where case mean when 0 then 0 else stdev/mean end > 1)
       |select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
       |        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
       |from inv inv1,inv inv2
       |where inv1.i_item_sk = inv2.i_item_sk
       |  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
       |  and inv1.d_moy=$month
       |  and inv2.d_moy=$month+1
       |  and inv1.cov > 1.5
       |order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
       |        ,inv2.d_moy,inv2.mean, inv2.cov
       |;
     """.stripMargin
  }

  // MODIFICATIONS "date + 30 days" --> "date_add(date, 30)
  def q40(): String = {
    // TODO define SALES_DATE=date([YEAR]+"-01-31",[YEAR]+"-7-01",sales);
    val sales_date = "2000-03-11"
    s"""
       | select
       |   w_state
       |  ,i_item_id
       |  ,sum(case when (cast(d_date as date) < cast('$sales_date' as date))
       | 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
       |  ,sum(case when (cast(d_date as date) >= cast('$sales_date' as date))
       | 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
       | from
       |   catalog_sales left outer join catalog_returns on
       |       (cs_order_number = cr_order_number
       |        and cs_item_sk = cr_item_sk)
       |  ,warehouse, item, date_dim
       | where
       |     i_current_price between 0.99 and 1.49
       | and i_item_sk          = cs_item_sk
       | and cs_warehouse_sk    = w_warehouse_sk
       | and cs_sold_date_sk    = d_date_sk
       | and d_date between date_sub(cast('$sales_date' as date), 30)
       |                and date_add(cast('$sales_date' as date), 30)
       | group by w_state,i_item_id
       | order by w_state,i_item_id
       | limit 100
     """.stripMargin
  }


  // [UNSUPPORTED]: subquery
  def q41(): String = {
    val manufact = uniformRand(738, 667, 1000)
    val sizes = getList(Seq("medium", "extra large", "N/A", "small", "petite", "large"), getSize)
    val units = getList(Seq("Ounce", "Oz", "Bunch", "Ton", "N/A", "Dozen", "Box", "Pound",
      "Pallet", "Gross", "Cup", "Dram", "Each", "Tbl", "Lb", "Bundle"), getUnit)
    val colors = getList(Seq("powder", "khaki", "brown", "honeydew", "floral", "deep", "light",
      "cornflower", "midnight", "snow", "cyan", "papaya", "orange", "frosted", "forest", "ghost"),
      getColor)

    s"""
       | select distinct(i_product_name)
       | from item i1
       | where i_manufact_id between $manufact and $manufact+40
       |   and (select count(*) as item_cnt
       |        from item
       |        where (i_manufact = i1.i_manufact and
       |        ((i_category = 'Women' and
       |        (i_color = '${colors(0)}' or i_color = '${colors(1)}') and
       |        (i_units = '${units(0)}' or i_units = '${units(1)}') and
       |        (i_size = '${sizes(0)}' or i_size = '${sizes(1)}')
       |        ) or
       |        (i_category = 'Women' and
       |        (i_color = '${colors(2)}' or i_color = '${colors(3)}') and
       |        (i_units = '${units(2)}' or i_units = '${units(3)}') and
       |        (i_size = '${sizes(2)}' or i_size = '${sizes(3)}')
       |        ) or
       |        (i_category = 'Men' and
       |        (i_color = '${colors(4)}' or i_color = '${colors(5)}') and
       |        (i_units = '${units(4)}' or i_units = '${units(5)}') and
       |        (i_size = '${sizes(4)}' or i_size = '${sizes(5)}')
       |        ) or
       |        (i_category = 'Men' and
       |        (i_color = '${colors(6)}' or i_color = '${colors(7)}') and
       |        (i_units = '${units(6)}' or i_units = '${units(7)}') and
       |        (i_size = '${sizes(0)}' or i_size = '${sizes(1)}')
       |        ))) or
       |       (i_manufact = i1.i_manufact and
       |        ((i_category = 'Women' and
       |        (i_color = '${colors(8)}' or i_color = '${colors(9)}') and
       |        (i_units = '${units(8)}' or i_units = '${units(9)}') and
       |        (i_size = '${sizes(0)}' or i_size = '${sizes(1)}')
       |        ) or
       |        (i_category = 'Women' and
       |        (i_color = '${colors(10)}' or i_color = '${colors(11)}') and
       |        (i_units = '${units(10)}' or i_units = '${units(11)}') and
       |        (i_size = '${sizes(2)}' or i_size = '${sizes(3)}')
       |        ) or
       |        (i_category = 'Men' and
       |        (i_color = '${colors(12)}' or i_color = '${colors(13)}') and
       |        (i_units = '${units(12)}' or i_units = '${units(13)}') and
       |        (i_size = '${sizes(4)}' or i_size = '${sizes(5)}')
       |        ) or
       |        (i_category = 'Men' and
       |        (i_color = '${colors(14)}' or i_color = '${colors(15)}') and
       |        (i_units = '${units(14)}' or i_units = '${units(15)}') and
       |        (i_size = '${sizes(0)}' or i_size = '${sizes(1)}')
       |        )))) > 0
       | order by i_product_name
       | limit 100
     """.stripMargin
  }

  def q42(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    s"""
       | select dt.d_year, item.i_category_id, item.i_category, sum(ss_ext_sales_price)
       | from 	date_dim dt, store_sales, item
       | where dt.d_date_sk = store_sales.ss_sold_date_sk
       | 	and store_sales.ss_item_sk = item.i_item_sk
       | 	and item.i_manager_id = 1
       | 	and dt.d_moy=$month
       | 	and dt.d_year=$year
       | group by 	dt.d_year
       | 		,item.i_category_id
       | 		,item.i_category
       | order by       sum(ss_ext_sales_price) desc,dt.d_year
       | 		,item.i_category_id
       | 		,item.i_category
       | limit 100
     """.stripMargin
  }

  def q43(): String = {
    val year = uniformRand(2000, 1998, 2002)
    // TODO:  define COUNTY=random(1, rowcount("active_counties", "store"), uniform);
    // define GMT=distmember(fips_county,[COUNTY], 6);
    val gmt = -5
    s"""
       | select s_store_name, s_store_id,
       |        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
       |        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
       |        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
       |        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
       |        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
       |        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
       |        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
       | from date_dim, store_sales, store
       | where d_date_sk = ss_sold_date_sk and
       |       s_store_sk = ss_store_sk and
       |       s_gmt_offset = $gmt and
       |       d_year = $year
       | group by s_store_name, s_store_id
       | order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,
       |          thu_sales,fri_sales,sat_sales
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions, subquery
  def q44(): String = {
    // TODO define STORE=random(1,rowcount("STORE"),uniform)
    val store = 4
    val nullcols = getValue("ss_addr_sk", "ss_customer_sk",
      "ss_cdemo_sk", "ss_hdemo_sk", "ss_promo_sk")
    s"""
       | select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
       | from(select *
       |     from (select item_sk,rank() over (order by rank_col asc) rnk
       |           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
       |                 from store_sales ss1
       |                 where ss_store_sk = $store
       |                 group by ss_item_sk
       |                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
       |                                                  from store_sales
       |                                                  where ss_store_sk = $store
       |                                                    and $nullcols is null
       |                                                  group by ss_store_sk))V1)V11
       |     where rnk  < 11) asceding,
       |    (select *
       |     from (select item_sk,rank() over (order by rank_col desc) rnk
       |           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
       |                 from store_sales ss1
       |                 where ss_store_sk = $store
       |                 group by ss_item_sk
       |                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
       |                                                  from store_sales
       |                                                  where ss_store_sk = $store
       |                                                    and $nullcols is null
       |                                                  group by ss_store_sk))V2)V21
       |     where rnk  < 11) descending,
       | item i1, item i2
       | where asceding.rnk = descending.rnk
       |   and i1.i_item_sk=asceding.item_sk
       |   and i2.i_item_sk=descending.item_sk
       | order by asceding.rnk
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q45(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val qoy = uniformRand(2, 1, 2)
    val gbobc = getValue("ca_city", "ca_county", "ca_state")
    s"""
       | select ca_zip, $gbobc, sum(ws_sales_price)
       | from web_sales, customer, customer_address, date_dim, item
       | where ws_bill_customer_sk = c_customer_sk
       | 	and c_current_addr_sk = ca_address_sk
       | 	and ws_item_sk = i_item_sk
       | 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
       | 	      or
       | 	      i_item_id in (select i_item_id
       |                             from item
       |                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
       |                             )
       | 	    )
       | 	and ws_sold_date_sk = d_date_sk
       | 	and d_qoy = $qoy and d_year = $year
       | group by ca_zip, $gbobc
       | order by ca_zip, $gbobc
       | limit 100
     """.stripMargin
  }


  def q46(): String = {
    val depcnt = uniformRand(4, 0, 9)
    val year = uniformRand(1999, 1998, 2000)
    val vehcnt = uniformRand(3, -1, 4)
    // TODO define CITYNUMBER = ulist(random(1, rowcount("active_cities", "store"), uniform),5);
    val cities = toInList(Seq("Fairview", "Fairview", "Fairview", "Midway", "Fairview"))
    s"""
       | select c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt,profit
       | from
       |   (select ss_ticket_number
       |          ,ss_customer_sk
       |          ,ca_city bought_city
       |          ,sum(ss_coupon_amt) amt
       |          ,sum(ss_net_profit) profit
       |    from store_sales, date_dim, store, household_demographics, customer_address
       |    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |    and store_sales.ss_store_sk = store.s_store_sk
       |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       |    and store_sales.ss_addr_sk = customer_address.ca_address_sk
       |    and (household_demographics.hd_dep_count = $depcnt or
       |         household_demographics.hd_vehicle_count= $vehcnt)
       |    and date_dim.d_dow in (6,0)
       |    and date_dim.d_year in ($year,$year+1,$year+2)
       |    and store.s_city in ($cities)
       |    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
       |    where ss_customer_sk = c_customer_sk
       |      and customer.c_current_addr_sk = current_addr.ca_address_sk
       |      and current_addr.ca_city <> bought_city
       |  order by c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
       |  limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions, works with hive dialect
  def q47(): String = {
    val year = uniformRand(1999, 1999, 2001)
    val selectone = getValue("v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name",
      "v1.i_category", "v1.i_brand", "v1.i_category, v1.i_brand", "v1.s_store_name",
      "v1.s_company_name", "v1.s_store_name, v1.s_company_name")
    val selecttwo = getValue( ",v1.d_year, v1.d_moy", ",v1.d_year")
    s"""
       | with v1 as(
       | select i_category, i_brand,
       |        s_store_name, s_company_name,
       |        d_year, d_moy,
       |        sum(ss_sales_price) sum_sales,
       |        avg(sum(ss_sales_price)) over
       |          (partition by i_category, i_brand,
       |                     s_store_name, s_company_name, d_year)
       |          avg_monthly_sales,
       |        rank() over
       |          (partition by i_category, i_brand,
       |                     s_store_name, s_company_name
       |           order by d_year, d_moy) rn
       | from item, store_sales, date_dim, store
       | where ss_item_sk = i_item_sk and
       |       ss_sold_date_sk = d_date_sk and
       |       ss_store_sk = s_store_sk and
       |       (
       |         d_year = $year or
       |         ( d_year = $year-1 and d_moy =12) or
       |         ( d_year = $year+1 and d_moy =1)
       |       )
       | group by i_category, i_brand,
       |          s_store_name, s_company_name,
       |          d_year, d_moy),
       | v2 as(
       | select $selectone $selecttwo, v1.avg_monthly_sales
       |        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
       | from v1, v1 v1_lag, v1 v1_lead
       | where v1.i_category = v1_lag.i_category and
       |       v1.i_category = v1_lead.i_category and
       |       v1.i_brand = v1_lag.i_brand and
       |       v1.i_brand = v1_lead.i_brand and
       |       v1.s_store_name = v1_lag.s_store_name and
       |       v1.s_store_name = v1_lead.s_store_name and
       |       v1.s_company_name = v1_lag.s_company_name and
       |       v1.s_company_name = v1_lead.s_company_name and
       |       v1.rn = v1_lag.rn + 1 and
       |       v1.rn = v1_lead.rn - 1)
       | select * from v2
       | where  d_year = $year and
       |        avg_monthly_sales > 0 and
       |        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
       | order by sum_sales - avg_monthly_sales, 3
       | limit 100
     """.stripMargin
  }

  def q48(): String = {
    val martialstatus = getList(Seq("M", "D", "S"), getMartialStatus)
    val education = getList(Seq("4 yr Degree", "2 yr Degree", "College"), getEducation)
    val states = getList(Seq("CO", "OH", "TX", "OR", "MN", "KY", "VA", "CA", "MS"), getState)
    // TODO: year not set in official?
    val year = 1998
    s"""
       | select sum (ss_quantity)
       | from store_sales, store, customer_demographics, customer_address, date_dim
       | where s_store_sk = ss_store_sk
       | and  ss_sold_date_sk = d_date_sk and d_year = $year
       | and
       | (
       |  (
       |   cd_demo_sk = ss_cdemo_sk
       |   and
       |   cd_marital_status = '${martialstatus(0)}'
       |   and
       |   cd_education_status = '${education(0)}'
       |   and
       |   ss_sales_price between 100.00 and 150.00
       |   )
       | or
       |  (
       |  cd_demo_sk = ss_cdemo_sk
       |   and
       |   cd_marital_status = '${martialstatus(0)}'
       |   and
       |   cd_education_status = '${education(0)}'
       |   and
       |   ss_sales_price between 50.00 and 100.00
       |  )
       | or
       | (
       |  cd_demo_sk = ss_cdemo_sk
       |  and
       |   cd_marital_status = '${martialstatus(0)}'
       |   and
       |   cd_education_status = '${education(0)}'
       |   and
       |   ss_sales_price between 150.00 and 200.00
       | )
       | )
       | and
       | (
       |  (
       |  ss_addr_sk = ca_address_sk
       |  and
       |  ca_country = 'United States'
       |  and
       |  ca_state in ('${states(0)}', '${states(1)}', '${states(2)}')
       |  and ss_net_profit between 0 and 2000
       |  )
       | or
       |  (ss_addr_sk = ca_address_sk
       |  and
       |  ca_country = 'United States'
       |  and
       |  ca_state in ('${states(3)}]', '${states(4)}', '${states(5)}')
       |  and ss_net_profit between 150 and 3000
       |  )
       | or
       |  (ss_addr_sk = ca_address_sk
       |  and
       |  ca_country = 'United States'
       |  and
       |  ca_state in ('${states(6)}', '[${states(7)}', '${states(8)}')
       |  and ss_net_profit between 50 and 25000
       |  )
       | )
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions; works in hiveql
  // [MODIFICATIONS Hive] dec->decimal
  def q49(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val month = uniformRand(12, 11, 12)
    s"""
       | select 'web' as channel, web.item, web.return_ratio, web.return_rank, web.currency_rank
       | from (
       | 	select
       |    item, return_ratio, currency_ratio,
       | 	  rank() over (order by return_ratio) as return_rank,
       | 	  rank() over (order by currency_ratio) as currency_rank
       | 	from
       | 	(	select ws.ws_item_sk as item
       | 		,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
       | 		cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
       | 		,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
       | 		cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
       | 		from
       | 		 web_sales ws left outer join web_returns wr
       | 			on (ws.ws_order_number = wr.wr_order_number and
       | 			ws.ws_item_sk = wr.wr_item_sk)
       |        ,date_dim
       | 		where
       | 			wr.wr_return_amt > 10000
       | 			and ws.ws_net_profit > 1
       |                         and ws.ws_net_paid > 0
       |                         and ws.ws_quantity > 0
       |                         and ws_sold_date_sk = d_date_sk
       |                         and d_year = $year
       |                         and d_moy = $month
       | 		group by ws.ws_item_sk
       | 	) in_web
       | ) web
       | where (web.return_rank <= 10 or web.currency_rank <= 10)
       | union
       | select
       |    'catalog' as channel, catalog.item, catalog.return_ratio,
       |    catalog.return_rank, catalog.currency_rank
       | from (
       | 	select
       |    item, return_ratio, currency_ratio,
       | 	  rank() over (order by return_ratio) as return_rank,
       | 	  rank() over (order by currency_ratio) as currency_rank
       | 	from
       | 	(	select
       | 		cs.cs_item_sk as item
       | 		,(cast(sum(coalesce(cr.cr_return_quantity,0)) as decimal(15,4))/
       | 		cast(sum(coalesce(cs.cs_quantity,0)) as decimal(15,4) )) as return_ratio
       | 		,(cast(sum(coalesce(cr.cr_return_amount,0)) as decimal(15,4))/
       | 		cast(sum(coalesce(cs.cs_net_paid,0)) as decimal(15,4) )) as currency_ratio
       | 		from
       | 		catalog_sales cs left outer join catalog_returns cr
       | 			on (cs.cs_order_number = cr.cr_order_number and
       | 			cs.cs_item_sk = cr.cr_item_sk)
       |                ,date_dim
       | 		where
       | 			cr.cr_return_amount > 10000
       | 			and cs.cs_net_profit > 1
       |                         and cs.cs_net_paid > 0
       |                         and cs.cs_quantity > 0
       |                         and cs_sold_date_sk = d_date_sk
       |                         and d_year = $year
       |                         and d_moy = $month
       |                 group by cs.cs_item_sk
       | 	) in_cat
       | ) catalog
       | where (catalog.return_rank <= 10 or catalog.currency_rank <=10)
       | union
       | select
       |    'store' as channel, store.item, store.return_ratio,
       |    store.return_rank, store.currency_rank
       | from (
       | 	select
       |      item, return_ratio, currency_ratio,
       | 	    rank() over (order by return_ratio) as return_rank,
       | 	    rank() over (order by currency_ratio) as currency_rank
       | 	from
       | 	(	select sts.ss_item_sk as item
       | 		,(cast(sum(coalesce(sr.sr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) as decimal(15,4) )) as return_ratio
       | 		,(cast(sum(coalesce(sr.sr_return_amt,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) as decimal(15,4) )) as currency_ratio
       | 		from
       | 		store_sales sts left outer join store_returns sr
       | 			on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
       |                ,date_dim
       | 		where
       | 			sr.sr_return_amt > 10000
       | 			and sts.ss_net_profit > 1
       |                         and sts.ss_net_paid > 0
       |                         and sts.ss_quantity > 0
       |                         and ss_sold_date_sk = d_date_sk
       |                         and d_year = $year
       |                         and d_moy = $month
       | 		group by sts.ss_item_sk
       | 	) in_store
       | ) store
       | where (store.return_rank <= 10 or store.currency_rank <= 10)
       | order by 1,4,5
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED} column labeling?
  def q50(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val month = uniformRand(8, 8, 10)
    s"""
       |select
       |   s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
       |   s_suite_number, s_city, s_county, s_state, s_zip
       |  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
       |  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and
       |                 (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
       |  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and
       |                 (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
       |  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
       |                 (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
       |  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
       |from
       |   store_sales, store_returns, store, date_dim d1, date_dim d2
       |where
       |    d2.d_year = $year
       |and d2.d_moy  = $month
       |and ss_ticket_number = sr_ticket_number
       |and ss_item_sk = sr_item_sk
       |and ss_sold_date_sk   = d1.d_date_sk
       |and sr_returned_date_sk   = d2.d_date_sk
       |and ss_customer_sk = sr_customer_sk
       |and ss_store_sk = s_store_sk
       |group by
       |    s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
       |    s_suite_number, s_city, s_county, s_state, s_zip
       | order by
       |    s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
       |    s_suite_number, s_city, s_county, s_state, s_zip
       | limit 100
     """.stripMargin
  }


  // [UNSUPPORTED]: window functions; works in hiveql
  def q51(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       |WITH web_v1 as (
       |select
       |  ws_item_sk item_sk, d_date,
       |  sum(sum(ws_sales_price))
       |      over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
       |from web_sales
       |    ,date_dim
       |where ws_sold_date_sk=d_date_sk
       |  and d_month_seq between $dms and $dms+11
       |  and ws_item_sk is not NULL
       |group by ws_item_sk, d_date),
       |store_v1 as (
       |select
       |  ss_item_sk item_sk, d_date,
       |  sum(sum(ss_sales_price))
       |      over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
       |from store_sales
       |    ,date_dim
       |where ss_sold_date_sk=d_date_sk
       |  and d_month_seq between $dms and $dms+11
       |  and ss_item_sk is not NULL
       |group by ss_item_sk, d_date)
       |select *
       |from (select item_sk
       |     ,d_date
       |     ,web_sales
       |     ,store_sales
       |     ,max(web_sales)
       |         over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
       |     ,max(store_sales)
       |         over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
       |     from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk
       |                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date
       |                 ,web.cume_sales web_sales
       |                 ,store.cume_sales store_sales
       |           from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk
       |                                                          and web.d_date = store.d_date)
       |          )x )y
       |where web_cumulative > store_cumulative
       |order by item_sk, d_date
       |limit 100
     """.stripMargin
  }

  def q52(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    s"""
       | select dt.d_year
       | 	,item.i_brand_id brand_id
       | 	,item.i_brand brand
       | 	,sum(ss_ext_sales_price) ext_price
       | from date_dim dt, store_sales, item
       | where dt.d_date_sk = store_sales.ss_sold_date_sk
       |    and store_sales.ss_item_sk = item.i_item_sk
       |    and item.i_manager_id = 1
       |    and dt.d_moy=$month
       |    and dt.d_year=$year
       | group by dt.d_year, item.i_brand, item.i_brand_id
       | order by dt.d_year, ext_price desc, brand_id
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED] Window functions; works in hiveql
  def q53(): String = {
    val dms = uniformRand(1200, 1172, 1224)
    s"""
       |select * from
       |  (select i_manufact_id,
       |          sum(ss_sales_price) sum_sales,
       |          avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
       |    from item, store_sales, date_dim, store
       |    where ss_item_sk = i_item_sk and
       |          ss_sold_date_sk = d_date_sk and
       |          ss_store_sk = s_store_sk and
       |          d_month_seq in ($dms,$dms+1,$dms+2,$dms+3,$dms+4,$dms+5,$dms+6,
       |                          $dms+7,$dms+8,$dms+9,$dms+10,$dms+11) and
       |    ((i_category in ('Books','Children','Electronics') and
       |      i_class in ('personal','portable','reference','self-help') and
       |      i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
       |		  'exportiunivamalg #9','scholaramalgamalg #9'))
       |    or
       |    (i_category in ('Women','Music','Men') and
       |     i_class in ('accessories','classical','fragrances','pants') and
       |     i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
       |		'importoamalg #1')))
       |    group by i_manufact_id, d_qoy ) tmp1
       |where case when avg_quarterly_sales > 0
       |	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales
       |	else null end > 0.1
       |order by avg_quarterly_sales,
       |	 sum_sales,
       |	 i_manufact_id
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q54(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val month = uniformRand(12, 6, 12)
    // TODO: define CINDX = random(1,rowcount("categories"),uniform);
    // define CATEGORY = distmember(categories,[CINDX],1);
    // define CLASS = dist(distmember(categories,[CINDX],2),1,1);
    val cl = "maternity"
    val category = "Women"
    s"""
       | with my_customers as (
       | select distinct c_customer_sk
       |        , c_current_addr_sk
       | from
       |        ( select cs_sold_date_sk sold_date_sk,
       |                 cs_bill_customer_sk customer_sk,
       |                 cs_item_sk item_sk
       |          from   catalog_sales
       |          union all
       |          select ws_sold_date_sk sold_date_sk,
       |                 ws_bill_customer_sk customer_sk,
       |                 ws_item_sk item_sk
       |          from   web_sales
       |         ) cs_or_ws_sales,
       |         item,
       |         date_dim,
       |         customer
       | where   sold_date_sk = d_date_sk
       |         and item_sk = i_item_sk
       |         and i_category = '$category'
       |         and i_class = '$cl'
       |         and c_customer_sk = cs_or_ws_sales.customer_sk
       |         and d_moy = $month
       |         and d_year = $year
       | )
       | , my_revenue as (
       | select c_customer_sk,
       |        sum(ss_ext_sales_price) as revenue
       | from   my_customers,
       |        store_sales,
       |        customer_address,
       |        store,
       |        date_dim
       | where  c_current_addr_sk = ca_address_sk
       |        and ca_county = s_county
       |        and ca_state = s_state
       |        and ss_sold_date_sk = d_date_sk
       |        and c_customer_sk = ss_customer_sk
       |        and d_month_seq between (select distinct d_month_seq+1
       |                                 from   date_dim where d_year = $year and d_moy = $month)
       |                           and  (select distinct d_month_seq+3
       |                                 from   date_dim where d_year = $year and d_moy = $month)
       | group by c_customer_sk
       | )
       | , segments as
       | (select cast((revenue/50) as int) as segment
       |  from   my_revenue
       | )
       | select segment, count(*) as num_customers, segment*50 as segment_base
       | from segments
       | group by segment
       | order by segment, num_customers
       | limit 100
     """.stripMargin
  }

  def q55(): String = {
    val year = uniformRand(1999, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    val manager = uniformRand(28, 1, 100)
    s"""
       |select i_brand_id brand_id, i_brand brand,
       | 	sum(ss_ext_sales_price) ext_price
       | from date_dim, store_sales, item
       | where d_date_sk = ss_sold_date_sk
       | 	and ss_item_sk = i_item_sk
       | 	and i_manager_id=$manager
       | 	and d_moy=$month
       | 	and d_year=$year
       | group by i_brand, i_brand_id
       | order by ext_price desc, brand_id
       | limit 100
     """.stripMargin
  }



  // [UNSUPPORTED]: subquery
  def q56(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val month = uniformRand(2, 1, 7)
    // TODO define GMT = dist(fips_county, 6, 1);
    val gmt = -5
    val colors = toInList(getList(Seq("slate", "blanched", "burnished"), getColor))
    s"""
       | with ss as (
       | select i_item_id,sum(ss_ext_sales_price) total_sales
       | from
       | 	  store_sales, date_dim, customer_address, item
       | where
       |    i_item_id in (select i_item_id from item where i_color in ($colors))
       | and     ss_item_sk              = i_item_sk
       | and     ss_sold_date_sk         = d_date_sk
       | and     d_year                  = $year
       | and     d_moy                   = $month
       | and     ss_addr_sk              = ca_address_sk
       | and     ca_gmt_offset           = $gmt
       | group by i_item_id),
       | cs as (
       | select i_item_id,sum(cs_ext_sales_price) total_sales
       | from
       | 	  catalog_sales, date_dim, customer_address, item
       | where
       |    i_item_id in (select i_item_id from item where i_color in ($colors))
       | and     cs_item_sk              = i_item_sk
       | and     cs_sold_date_sk         = d_date_sk
       | and     d_year                  = $year
       | and     d_moy                   = $month
       | and     cs_bill_addr_sk         = ca_address_sk
       | and     ca_gmt_offset           = $gmt
       | group by i_item_id),
       | ws as (
       | select i_item_id,sum(ws_ext_sales_price) total_sales
       | from
       | 	  web_sales, date_dim, customer_address, item
       | where
       |    i_item_id in (select i_item_id from item where i_color in ($colors))
       | and     ws_item_sk              = i_item_sk
       | and     ws_sold_date_sk         = d_date_sk
       | and     d_year                  = $year 
       | and     d_moy                   = $month
       | and     ws_bill_addr_sk         = ca_address_sk
       | and     ca_gmt_offset           = $gmt
       | group by i_item_id)
       | select i_item_id ,sum(total_sales) total_sales
       | from  (select * from ss
       |        union all
       |        select * from cs
       |        union all
       |        select * from ws) tmp1
       | group by i_item_id
       | order by total_sales
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions, works in hiveql
  def q57(): String = {
    val year = uniformRand(1999, 1999, 2001)
    val selectone = getValue("v1.i_category, v1.i_brand, v1.cc_name", "v1.i_category", "v1.i_brand",
      "v1.i_category, v1.i_brand", "v1.cc_name")
    val selecttwo = getValue(",v1.d_year, v1.d_moy", ",v1.d_year")
    s"""
       | with v1 as(
       | select i_category, i_brand,
       |        cc_name,
       |        d_year, d_moy,
       |        sum(cs_sales_price) sum_sales,
       |        avg(sum(cs_sales_price)) over
       |          (partition by i_category, i_brand, cc_name, d_year)
       |          avg_monthly_sales,
       |        rank() over
       |          (partition by i_category, i_brand, cc_name
       |           order by d_year, d_moy) rn
       | from item, catalog_sales, date_dim, call_center
       | where cs_item_sk = i_item_sk and
       |       cs_sold_date_sk = d_date_sk and
       |       cc_call_center_sk= cs_call_center_sk and
       |       (
       |         d_year = $year or
       |         ( d_year = $year-1 and d_moy =12) or
       |         ( d_year = $year+1 and d_moy =1)
       |       )
       | group by i_category, i_brand,
       |          cc_name , d_year, d_moy),
       | v2 as(
       | select $selectone
       |        $selecttwo
       |        ,v1.avg_monthly_sales
       |        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
       | from v1, v1 v1_lag, v1 v1_lead
       | where v1.i_category = v1_lag.i_category and
       |       v1.i_category = v1_lead.i_category and
       |       v1.i_brand = v1_lag.i_brand and
       |       v1.i_brand = v1_lead.i_brand and
       |       v1. cc_name = v1_lag. cc_name and
       |       v1. cc_name = v1_lead. cc_name and
       |       v1.rn = v1_lag.rn + 1 and
       |       v1.rn = v1_lead.rn - 1)
       | select * from v2
       | where  d_year = $year and
       |        avg_monthly_sales > 0 and
       |        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
       | order by sum_sales - avg_monthly_sales, 3
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q58(): String = {
    // TODO: define SALES_DATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales)
    val salesdate = "2000-01-03"
    s"""
       | with ss_items as
       | (select i_item_id item_id, sum(ss_ext_sales_price) ss_item_rev
       | from store_sales, item, date_dim
       | where ss_item_sk = i_item_sk
       |   and d_date in (select d_date
       |                  from date_dim
       |                  where d_week_seq = (select d_week_seq
       |                                      from date_dim
       |                                      where d_date = '$salesdate'))
       |   and ss_sold_date_sk   = d_date_sk
       | group by i_item_id),
       | cs_items as
       | (select i_item_id item_id
       |        ,sum(cs_ext_sales_price) cs_item_rev
       |  from catalog_sales, item, date_dim
       | where cs_item_sk = i_item_sk
       |  and  d_date in (select d_date
       |                  from date_dim
       |                  where d_week_seq = (select d_week_seq
       |                                      from date_dim
       |                                      where d_date = '$salesdate'))
       |  and  cs_sold_date_sk = d_date_sk
       | group by i_item_id),
       | ws_items as
       | (select i_item_id item_id, sum(ws_ext_sales_price) ws_item_rev
       |  from web_sales, item, date_dim
       | where ws_item_sk = i_item_sk
       |  and  d_date in (select d_date
       |                  from date_dim
       |                  where d_week_seq =(select d_week_seq
       |                                     from date_dim
       |                                     where d_date = '$salesdate'))
       |  and ws_sold_date_sk   = d_date_sk
       | group by i_item_id)
       | select ss_items.item_id
       |       ,ss_item_rev
       |       ,ss_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ss_dev
       |       ,cs_item_rev
       |       ,cs_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 cs_dev
       |       ,ws_item_rev
       |       ,ws_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ws_dev
       |       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
       | from ss_items,cs_items,ws_items
       | where ss_items.item_id=cs_items.item_id
       |   and ss_items.item_id=ws_items.item_id
       |   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
       |   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
       |   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
       |   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
       |   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
       |   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
       | order by item_id, ss_item_rev
       | limit 100
     """.stripMargin
  }

  def q59(): String = {
    val dms = uniformRand(1212, 1176, 1212)
    s"""
       | with wss as
       | (select d_week_seq,
       |        ss_store_sk,
       |        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
       |        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
       |        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
       |        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
       |        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
       |        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
       |        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
       | from store_sales,date_dim
       | where d_date_sk = ss_sold_date_sk
       | group by d_week_seq,ss_store_sk
       | )
       | select  s_store_name1,s_store_id1,d_week_seq1
       |       ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
       |       ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
       |       ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
       | from
       | (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
       |        ,s_store_id s_store_id1,sun_sales sun_sales1
       |        ,mon_sales mon_sales1,tue_sales tue_sales1
       |        ,wed_sales wed_sales1,thu_sales thu_sales1
       |        ,fri_sales fri_sales1,sat_sales sat_sales1
       |  from wss,store,date_dim d
       |  where d.d_week_seq = wss.d_week_seq and
       |        ss_store_sk = s_store_sk and
       |        d_month_seq between $dms and $dms + 11) y,
       | (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2
       |        ,s_store_id s_store_id2,sun_sales sun_sales2
       |        ,mon_sales mon_sales2,tue_sales tue_sales2
       |        ,wed_sales wed_sales2,thu_sales thu_sales2
       |        ,fri_sales fri_sales2,sat_sales sat_sales2
       |  from wss,store,date_dim d
       |  where d.d_week_seq = wss.d_week_seq and
       |        ss_store_sk = s_store_sk and
       |        d_month_seq between $dms+ 12 and $dms + 23) x
       | where s_store_id1=s_store_id2
       |   and d_week_seq1=d_week_seq2-52
       | order by s_store_name1,s_store_id1,d_week_seq1
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q60(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val month = uniformRand(9, 8, 10)
    // TODO: GMT = dist(fips_county, 6, 1);
    val gmt = -5
    val category = getValue("Music", "Children", "Men", "Jewelry", "Shoes")
    s"""
       | with ss as (
       |    select i_item_id,sum(ss_ext_sales_price) total_sales
       |    from store_sales, date_dim, customer_address, item
       |    where
       |        i_item_id in (select i_item_id from item where i_category in ('$category'))
       |    and     ss_item_sk              = i_item_sk
       |    and     ss_sold_date_sk         = d_date_sk
       |    and     d_year                  = $year
       |    and     d_moy                   = $month
       |    and     ss_addr_sk              = ca_address_sk
       |    and     ca_gmt_offset           = $gmt
       |    group by i_item_id),
       |  cs as (
       |    select i_item_id,sum(cs_ext_sales_price) total_sales
       |    from catalog_sales, date_dim, customer_address, item
       |    where
       |        i_item_id in (select i_item_id from item where i_category in ('$category'))
       |    and     cs_item_sk              = i_item_sk
       |    and     cs_sold_date_sk         = d_date_sk
       |    and     d_year                  = $year
       |    and     d_moy                   = $month
       |    and     cs_bill_addr_sk         = ca_address_sk
       |    and     ca_gmt_offset           = $gmt
       |    group by i_item_id),
       |  ws as (
       |    select i_item_id,sum(ws_ext_sales_price) total_sales
       |    from web_sales, date_dim, customer_address, item
       |    where
       |        i_item_id in (select i_item_id from item where i_category in ('$category'))
       |    and     ws_item_sk              = i_item_sk
       |    and     ws_sold_date_sk         = d_date_sk
       |    and     d_year                  = $year
       |    and     d_moy                   = $month
       |    and     ws_bill_addr_sk         = ca_address_sk
       |    and     ca_gmt_offset           = $gmt
       |    group by i_item_id)
       | select i_item_id, sum(total_sales) total_sales
       | from  (select * from ss
       |        union all
       |        select * from cs
       |        union all
       |        select * from ws) tmp1
       | group by i_item_id
       | order by i_item_id, total_sales
       | limit 100
     """.stripMargin
  }

  def q61(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    val category = getValue("Jewelry", "Books", "Electronics", "Sports")
    // TODO
    val gmt = -5
    s"""
       |select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
       |from
       |  (select sum(ss_ext_sales_price) promotions
       |    from  store_sales, store, promotion, date_dim, customer, customer_address, item
       |    where ss_sold_date_sk = d_date_sk
       |    and   ss_store_sk = s_store_sk
       |    and   ss_promo_sk = p_promo_sk
       |    and   ss_customer_sk= c_customer_sk
       |    and   ca_address_sk = c_current_addr_sk
       |    and   ss_item_sk = i_item_sk
       |    and   ca_gmt_offset = $gmt
       |    and   i_category = '$category'
       |    and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
       |    and   s_gmt_offset = $gmt
       |    and   d_year = $year
       |    and   d_moy  = $month) promotional_sales,
       |  (select sum(ss_ext_sales_price) total
       |    from  store_sales, store, date_dim, customer, customer_address, item
       |    where ss_sold_date_sk = d_date_sk
       |    and   ss_store_sk = s_store_sk
       |    and   ss_customer_sk= c_customer_sk
       |    and   ca_address_sk = c_current_addr_sk
       |    and   ss_item_sk = i_item_sk
       |    and   ca_gmt_offset = $gmt
       |    and   i_category = '$category'
       |    and   s_gmt_offset = $gmt
       |    and   d_year = $year
       |    and   d_moy  = $month) all_sales
       |order by promotions, total
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED] literal alias
  def q62(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       | select
       |   substr(w_warehouse_name,1,20)
       |  ,sm_type
       |  ,web_name
       |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
       |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
       |                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
       |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
       |                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
       |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
       |                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
       |  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
       |from
       |   web_sales, warehouse, ship_mode, web_site, date_dim
       |where
       |    d_month_seq between $dms and $dms + 11
       |and ws_ship_date_sk   = d_date_sk
       |and ws_warehouse_sk   = w_warehouse_sk
       |and ws_ship_mode_sk   = sm_ship_mode_sk
       |and ws_web_site_sk    = web_site_sk
       |group by
       |   substr(w_warehouse_name,1,20), sm_type, web_name
       |order by
       |   substr(w_warehouse_name,1,20), sm_type, web_name
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions works in hiveql
  def q63(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       |select *
       |from (select i_manager_id
       |             ,sum(ss_sales_price) sum_sales
       |             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
       |      from item
       |          ,store_sales
       |          ,date_dim
       |          ,store
       |      where ss_item_sk = i_item_sk
       |        and ss_sold_date_sk = d_date_sk
       |        and ss_store_sk = s_store_sk
       |        and d_month_seq in ($dms,$dms+1,$dms+2,$dms+3,$dms+4,$dms+5,$dms+6,$dms+7,
       |                            $dms+8,$dms+9,$dms+10,$dms+11)
       |        and ((    i_category in ('Books','Children','Electronics')
       |              and i_class in ('personal','portable','refernece','self-help')
       |              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
       |		                  'exportiunivamalg #9','scholaramalgamalg #9'))
       |           or(    i_category in ('Women','Music','Men')
       |              and i_class in ('accessories','classical','fragrances','pants')
       |              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
       |		                 'importoamalg #1')))
       |group by i_manager_id, d_moy) tmp1
       |where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
       |order by i_manager_id
       |        ,avg_monthly_sales
       |        ,sum_sales
       |limit 100
     """.stripMargin
  }

  def q64(): String = {
    val year = uniformRand(1999, 1999, 2001)
    val price = uniformRand(64, 0, 85)
    val colors = toInList(getList(
      Seq("purple", "burlywood", "indian", "spring", "floral", "medium"), getColor))
    s"""
       |with cs_ui as
       | (select cs_item_sk
       |        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
       |  from catalog_sales
       |      ,catalog_returns
       |  where cs_item_sk = cr_item_sk
       |    and cs_order_number = cr_order_number
       |  group by cs_item_sk
       |  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
       |cross_sales as
       | (select i_product_name product_name
       |     ,i_item_sk item_sk
       |     ,s_store_name store_name
       |     ,s_zip store_zip
       |     ,ad1.ca_street_number b_street_number
       |     ,ad1.ca_street_name b_streen_name
       |     ,ad1.ca_city b_city
       |     ,ad1.ca_zip b_zip
       |     ,ad2.ca_street_number c_street_number
       |     ,ad2.ca_street_name c_street_name
       |     ,ad2.ca_city c_city
       |     ,ad2.ca_zip c_zip
       |     ,d1.d_year as syear
       |     ,d2.d_year as fsyear
       |     ,d3.d_year s2year
       |     ,count(*) cnt
       |     ,sum(ss_wholesale_cost) s1
       |     ,sum(ss_list_price) s2
       |     ,sum(ss_coupon_amt) s3
       |  FROM store_sales, store_returns, cs_ui, date_dim d1, date_dim d2, date_dim d3,
       |       store, customer, customer_demographics cd1, customer_demographics cd2,
       |       promotion, household_demographics hd1, household_demographics hd2,
       |       customer_address ad1, customer_address ad2, income_band ib1, income_band ib2, item
       |  WHERE  ss_store_sk = s_store_sk AND
       |         ss_sold_date_sk = d1.d_date_sk AND
       |         ss_customer_sk = c_customer_sk AND
       |         ss_cdemo_sk= cd1.cd_demo_sk AND
       |         ss_hdemo_sk = hd1.hd_demo_sk AND
       |         ss_addr_sk = ad1.ca_address_sk and
       |         ss_item_sk = i_item_sk and
       |         ss_item_sk = sr_item_sk and
       |         ss_ticket_number = sr_ticket_number and
       |         ss_item_sk = cs_ui.cs_item_sk and
       |         c_current_cdemo_sk = cd2.cd_demo_sk AND
       |         c_current_hdemo_sk = hd2.hd_demo_sk AND
       |         c_current_addr_sk = ad2.ca_address_sk and
       |         c_first_sales_date_sk = d2.d_date_sk and
       |         c_first_shipto_date_sk = d3.d_date_sk and
       |         ss_promo_sk = p_promo_sk and
       |         hd1.hd_income_band_sk = ib1.ib_income_band_sk and
       |         hd2.hd_income_band_sk = ib2.ib_income_band_sk and
       |         cd1.cd_marital_status <> cd2.cd_marital_status and
       |         i_color in ($colors) and
       |         i_current_price between $price and $price + 10 and
       |         i_current_price between $price + 1 and $price + 15
       |group by i_product_name
       |       ,i_item_sk
       |       ,s_store_name
       |       ,s_zip
       |       ,ad1.ca_street_number
       |       ,ad1.ca_street_name
       |       ,ad1.ca_city
       |       ,ad1.ca_zip
       |       ,ad2.ca_street_number
       |       ,ad2.ca_street_name
       |       ,ad2.ca_city
       |       ,ad2.ca_zip
       |       ,d1.d_year
       |       ,d2.d_year
       |       ,d3.d_year
       |)
       |select cs1.product_name
       |     ,cs1.store_name
       |     ,cs1.store_zip
       |     ,cs1.b_street_number
       |     ,cs1.b_streen_name
       |     ,cs1.b_city
       |     ,cs1.b_zip
       |     ,cs1.c_street_number
       |     ,cs1.c_street_name
       |     ,cs1.c_city
       |     ,cs1.c_zip
       |     ,cs1.syear
       |     ,cs1.cnt
       |     ,cs1.s1
       |     ,cs1.s2
       |     ,cs1.s3
       |     ,cs2.s1
       |     ,cs2.s2
       |     ,cs2.s3
       |     ,cs2.syear
       |     ,cs2.cnt
       |from cross_sales cs1,cross_sales cs2
       |where cs1.item_sk=cs2.item_sk and
       |     cs1.syear = $year and
       |     cs2.syear = $year + 1 and
       |     cs2.cnt <= cs1.cnt and
       |     cs1.store_name = cs2.store_name and
       |     cs1.store_zip = cs2.store_zip
       |order by cs1.product_name, cs1.store_name, cs2.cnt
     """.stripMargin
  }

  def q65(): String = {
    val dms = uniformRand(1176, 1176, 1224)
    s"""
       | select
       |	  s_store_name, i_item_desc, sc.revenue, i_current_price,	i_wholesale_cost,	i_brand
       | from store, item,
       |     (select ss_store_sk, avg(revenue) as ave
       | 	from
       | 	    (select  ss_store_sk, ss_item_sk,
       | 		     sum(ss_sales_price) as revenue
       | 		from store_sales, date_dim
       | 		where ss_sold_date_sk = d_date_sk and d_month_seq between $dms and $dms+11
       | 		group by ss_store_sk, ss_item_sk) sa
       | 	group by ss_store_sk) sb,
       |     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
       | 	from store_sales, date_dim
       | 	where ss_sold_date_sk = d_date_sk and d_month_seq between $dms and $dms+11
       | 	group by ss_store_sk, ss_item_sk) sc
       | where sb.ss_store_sk = sc.ss_store_sk and
       |       sc.revenue <= 0.1 * sb.ave and
       |       s_store_sk = sc.ss_store_sk and
       |       i_item_sk = sc.ss_item_sk
       | order by s_store_name, i_item_desc
       | limit 100
     """.stripMargin
  }

  // [MODIFICATIONS]: || --> concat
  // Doesn't work in hiveql
  def q66(): String = {
    val year = uniformRand(2001, 1998, 2002)
    val time = uniformRand(30838, 1, 57597)
    val salesone = getValue("ws_ext_sales_price", "ws_sales_price", "ws_ext_list_price")
    val salestwo = getValue("cs_sales_price", "cs_ext_sales_price", "cs_ext_list_price")
    val netone = getValue("ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship",
      "ws_net_paid_inc_ship_tax", "ws_net_profit")
    val nettwo = getValue("cs_net_paid_inc_tax", "cs_net_paid", "cs_net_paid_inc_ship",
      "cs_net_paid_inc_ship_tax", "cs_net_profit")
    // TODO SMC = ulist(dist(ship_mode_carrier, 1, 1),2);
    val smc = Seq("DHL", "BARIAN")
    s"""
       | select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country,
       |    ship_carriers, year
       | 	  ,sum(jan_sales) as jan_sales
       | 	  ,sum(feb_sales) as feb_sales
       | 	  ,sum(mar_sales) as mar_sales
       | 	  ,sum(apr_sales) as apr_sales
       | 	  ,sum(may_sales) as may_sales
       | 	  ,sum(jun_sales) as jun_sales
       | 	  ,sum(jul_sales) as jul_sales
       | 	  ,sum(aug_sales) as aug_sales
       | 	  ,sum(sep_sales) as sep_sales
       | 	  ,sum(oct_sales) as oct_sales
       | 	  ,sum(nov_sales) as nov_sales
       | 	  ,sum(dec_sales) as dec_sales
       | 	  ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
       | 	  ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
       | 	  ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
       | 	  ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
       | 	  ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
       | 	  ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
       | 	  ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
       | 	  ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
       | 	  ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
       | 	  ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
       | 	  ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
       | 	  ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
       | 	  ,sum(jan_net) as jan_net
       | 	  ,sum(feb_net) as feb_net
       | 	  ,sum(mar_net) as mar_net
       | 	  ,sum(apr_net) as apr_net
       | 	  ,sum(may_net) as may_net
       | 	  ,sum(jun_net) as jun_net
       | 	  ,sum(jul_net) as jul_net
       | 	  ,sum(aug_net) as aug_net
       | 	  ,sum(sep_net) as sep_net
       | 	  ,sum(oct_net) as oct_net
       | 	  ,sum(nov_net) as nov_net
       | 	  ,sum(dec_net) as dec_net
       | from (
       |    (select
       | 	    w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country
       | 	    ,concat('${smc(0)}', ',', '${smc(1)}') as ship_carriers
       |      ,d_year as year
       | 	    ,sum(case when d_moy = 1 then $salesone * ws_quantity else 0 end) as jan_sales
       | 	    ,sum(case when d_moy = 2 then $salesone * ws_quantity else 0 end) as feb_sales
       | 	    ,sum(case when d_moy = 3 then $salesone * ws_quantity else 0 end) as mar_sales
       | 	    ,sum(case when d_moy = 4 then $salesone * ws_quantity else 0 end) as apr_sales
       | 	    ,sum(case when d_moy = 5 then $salesone * ws_quantity else 0 end) as may_sales
       | 	    ,sum(case when d_moy = 6 then $salesone * ws_quantity else 0 end) as jun_sales
       | 	    ,sum(case when d_moy = 7 then $salesone * ws_quantity else 0 end) as jul_sales
       | 	    ,sum(case when d_moy = 8 then $salesone * ws_quantity else 0 end) as aug_sales
       | 	    ,sum(case when d_moy = 9 then $salesone * ws_quantity else 0 end) as sep_sales
       | 	    ,sum(case when d_moy = 10 then $salesone * ws_quantity else 0 end) as oct_sales
       | 	    ,sum(case when d_moy = 11 then $salesone * ws_quantity else 0 end) as nov_sales
       | 	    ,sum(case when d_moy = 12 then $salesone * ws_quantity else 0 end) as dec_sales
       | 	    ,sum(case when d_moy = 1 then $netone  * ws_quantity else 0 end) as jan_net
       | 	    ,sum(case when d_moy = 2 then $netone  * ws_quantity else 0 end) as feb_net
       | 	    ,sum(case when d_moy = 3 then $netone  * ws_quantity else 0 end) as mar_net
       | 	    ,sum(case when d_moy = 4 then $netone  * ws_quantity else 0 end) as apr_net
       | 	    ,sum(case when d_moy = 5 then $netone  * ws_quantity else 0 end) as may_net
       | 	    ,sum(case when d_moy = 6 then $netone  * ws_quantity else 0 end) as jun_net
       | 	    ,sum(case when d_moy = 7 then $netone  * ws_quantity else 0 end) as jul_net
       | 	    ,sum(case when d_moy = 8 then $netone  * ws_quantity else 0 end) as aug_net
       | 	    ,sum(case when d_moy = 9 then $netone  * ws_quantity else 0 end) as sep_net
       | 	    ,sum(case when d_moy = 10 then $netone  * ws_quantity else 0 end) as oct_net
       | 	    ,sum(case when d_moy = 11 then $netone  * ws_quantity else 0 end) as nov_net
       | 	    ,sum(case when d_moy = 12 then $netone  * ws_quantity else 0 end) as dec_net
       |    from
       |      web_sales, warehouse, date_dim, time_dim, ship_mode
       |    where
       |      ws_warehouse_sk =  w_warehouse_sk
       |      and ws_sold_date_sk = d_date_sk
       |      and ws_sold_time_sk = t_time_sk
       | 	    and ws_ship_mode_sk = sm_ship_mode_sk
       |      and d_year = $year
       | 	    and t_time between $time and $time+28800
       | 	    and sm_carrier in ('${smc(0)}','${smc(1)}')
       |   group by
       |      w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year)
       | union all
       |    (select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country
       | 	    ,concat('${smc(0)}', ',', '${smc(1)}') as ship_carriers
       |      ,d_year as year
       | 	    ,sum(case when d_moy = 1 then $salestwo * cs_quantity else 0 end) as jan_sales
       | 	    ,sum(case when d_moy = 2 then $salestwo * cs_quantity else 0 end) as feb_sales
       | 	    ,sum(case when d_moy = 3 then $salestwo * cs_quantity else 0 end) as mar_sales
       | 	    ,sum(case when d_moy = 4 then $salestwo * cs_quantity else 0 end) as apr_sales
       | 	    ,sum(case when d_moy = 5 then $salestwo * cs_quantity else 0 end) as may_sales
       | 	    ,sum(case when d_moy = 6 then $salestwo * cs_quantity else 0 end) as jun_sales
       | 	    ,sum(case when d_moy = 7 then $salestwo * cs_quantity else 0 end) as jul_sales
       | 	    ,sum(case when d_moy = 8 then $salestwo * cs_quantity else 0 end) as aug_sales
       | 	    ,sum(case when d_moy = 9 then $salestwo * cs_quantity else 0 end) as sep_sales
       | 	    ,sum(case when d_moy = 10 then $salestwo * cs_quantity else 0 end) as oct_sales
       | 	    ,sum(case when d_moy = 11 then $salestwo * cs_quantity else 0 end) as nov_sales
       | 	    ,sum(case when d_moy = 12 then $salestwo * cs_quantity else 0 end) as dec_sales
       | 	    ,sum(case when d_moy = 1 then $nettwo * cs_quantity else 0 end) as jan_net
       | 	    ,sum(case when d_moy = 2 then $nettwo * cs_quantity else 0 end) as feb_net
       | 	    ,sum(case when d_moy = 3 then $nettwo * cs_quantity else 0 end) as mar_net
       | 	    ,sum(case when d_moy = 4 then $nettwo * cs_quantity else 0 end) as apr_net
       | 	    ,sum(case when d_moy = 5 then $nettwo * cs_quantity else 0 end) as may_net
       | 	    ,sum(case when d_moy = 6 then $nettwo * cs_quantity else 0 end) as jun_net
       | 	    ,sum(case when d_moy = 7 then $nettwo * cs_quantity else 0 end) as jul_net
       | 	    ,sum(case when d_moy = 8 then $nettwo * cs_quantity else 0 end) as aug_net
       | 	    ,sum(case when d_moy = 9 then $nettwo * cs_quantity else 0 end) as sep_net
       | 	    ,sum(case when d_moy = 10	then $nettwo * cs_quantity else 0 end) as oct_net
       | 	    ,sum(case when d_moy = 11	then $nettwo * cs_quantity else 0 end) as nov_net
       | 	    ,sum(case when d_moy = 12 then $nettwo * cs_quantity else 0 end) as dec_net
       |     from
       |        catalog_sales, warehouse, date_dim, time_dim, ship_mode
       |     where
       |        cs_warehouse_sk =  w_warehouse_sk
       |        and cs_sold_date_sk = d_date_sk
       |        and cs_sold_time_sk = t_time_sk
       | 	      and cs_ship_mode_sk = sm_ship_mode_sk
       |        and d_year = $year
       | 	      and t_time between $time AND $time+28800
       | 	      and sm_carrier in ('${smc(0)}','${smc(1)}')
       |     group by
       |        w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
       |     )
       | ) x
       | group by
       |    w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country,
       |    ship_carriers, year
       | order by w_warehouse_name
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions, rollup
  def q67(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       |select * from
       |    (select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id,
       |            sumsales, rank() over (partition by i_category order by sumsales desc) rk
       |     from
       |        (select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,
       |                s_store_id, sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
       |         from store_sales, date_dim, store, item
       |       where  ss_sold_date_sk=d_date_sk
       |          and ss_item_sk=i_item_sk
       |          and ss_store_sk = s_store_sk
       |          and d_month_seq between $dms and $dms+11
       |       group by rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy,
       |                       d_moy,s_store_id))dw1) dw2
       |where rk <= 100
       |order by
       |  i_category, i_class, i_brand, i_product_name, d_year,
       |  d_qoy, d_moy, s_store_id, sumsales, rk
       |limit 100
     """.stripMargin
  }

  def q68(): String = {
    val year = uniformRand(1999, 1998, 2000)
    val depcnt = uniformRand(4, 0, 9)
    val vehcnt = uniformRand(3, -1, 4)
    // TODO CITYNUMBER = ulist(random(1, rowcount("active_cities", "store"), uniform), 2);
    val citya = "Midway"
    val cityb = "Fairview"
    s"""
       | select
       |    c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, extended_price,
       |    extended_tax, list_price
       | from (select
       |        ss_ticket_number, ss_customer_sk, ca_city bought_city,
       |        sum(ss_ext_sales_price) extended_price,
       |        sum(ss_ext_list_price) list_price,
       |        sum(ss_ext_tax) extended_tax
       |     from store_sales, date_dim, store, household_demographics, customer_address
       |     where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |        and store_sales.ss_store_sk = store.s_store_sk
       |        and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       |        and store_sales.ss_addr_sk = customer_address.ca_address_sk
       |        and date_dim.d_dom between 1 and 2
       |        and (household_demographics.hd_dep_count = $depcnt or
       |             household_demographics.hd_vehicle_count = $vehcnt)
       |        and date_dim.d_year in ($year,$year+1,$year+2)
       |        and store.s_city in ('$citya','$cityb')
       |     group by ss_ticket_number, ss_customer_sk, ss_addr_sk,ca_city) dn,
       |    customer,
       |    customer_address current_addr
       | where ss_customer_sk = c_customer_sk
       |   and customer.c_current_addr_sk = current_addr.ca_address_sk
       |   and current_addr.ca_city <> bought_city
       | order by c_last_name, ss_ticket_number
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q69(): String = {
    val year = uniformRand(2001, 1999, 2004)
    val month = uniformRand(4, 1, 4)
    val states = toInList(getList(Seq("KY", "GA", "NM"), getState))
    s"""
       | select
       |    cd_gender, cd_marital_status, cd_education_status, count(*) cnt1,
       |    cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3
       | from
       |    customer c,customer_address ca,customer_demographics
       | where
       |    c.c_current_addr_sk = ca.ca_address_sk and
       |    ca_state in ($states) and
       |    cd_demo_sk = c.c_current_cdemo_sk and
       |    exists (select * from store_sales, date_dim
       |            where c.c_customer_sk = ss_customer_sk and
       |                ss_sold_date_sk = d_date_sk and
       |                d_year = $year and
       |                d_moy between $month and $month+2) and
       |   (not exists (select * from web_sales, date_dim
       |                where c.c_customer_sk = ws_bill_customer_sk and
       |                    ws_sold_date_sk = d_date_sk and
       |                    d_year = $year and
       |                    d_moy between $month and $month+2) and
       |    not exists (select * from catalog_sales, date_dim
       |                where c.c_customer_sk = cs_ship_customer_sk and
       |                    cs_sold_date_sk = d_date_sk and
       |                    d_year = $year and
       |                    d_moy between $month and $month+2))
       | group by cd_gender, cd_marital_status, cd_education_status,
       |          cd_purchase_estimate, cd_credit_rating
       | order by cd_gender, cd_marital_status, cd_education_status,
       |          cd_purchase_estimate, cd_credit_rating
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions, rollup
  def q70(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       | select
       |    sum(ss_net_profit) as total_sum, s_state, s_county
       |   ,grouping(s_state)+grouping(s_county) as lochierarchy
       |   ,rank() over (
       | 	    partition by grouping(s_state)+grouping(s_county),
       | 	    case when grouping(s_county) = 0 then s_state end
       | 	    order by sum(ss_net_profit) desc) as rank_within_parent
       | from
       |    store_sales, date_dim d1, store
       | where
       |    d1.d_month_seq between $dms and $dms+11
       | and d1.d_date_sk = ss_sold_date_sk
       | and s_store_sk  = ss_store_sk
       | and s_state in
       |    (select s_state from
       |        (select s_state as s_state,
       | 			      rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
       |         from store_sales, store, date_dim
       |         where  d_month_seq between $dms and $dms+11
       | 			   and d_date_sk = ss_sold_date_sk
       | 			   and s_store_sk  = ss_store_sk
       |         group by s_state) tmp1
       |     where ranking <= 5)
       | group by rollup(s_state,s_county)
       | order by
       |   lochierarchy desc
       |  ,case when lochierarchy = 0 then s_state end
       |  ,rank_within_parent
       | limit 100
     """.stripMargin
  }

  def q71(): String = {
    val year = uniformRand(1999, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    s"""
       | select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
       | 	  sum(ext_price) ext_price
       | from item,
       |    (select
       |        ws_ext_sales_price as ext_price,
       |        ws_sold_date_sk as sold_date_sk,
       |        ws_item_sk as sold_item_sk,
       |        ws_sold_time_sk as time_sk
       |     from web_sales, date_dim
       |     where d_date_sk = ws_sold_date_sk
       |        and d_moy=$month
       |        and d_year=$year
       |     union all
       |     select
       |        cs_ext_sales_price as ext_price,
       |        cs_sold_date_sk as sold_date_sk,
       |        cs_item_sk as sold_item_sk,
       |        cs_sold_time_sk as time_sk
       |      from catalog_sales, date_dim
       |      where d_date_sk = cs_sold_date_sk
       |          and d_moy=$month
       |          and d_year=$year
       |     union all
       |     select
       |        ss_ext_sales_price as ext_price,
       |        ss_sold_date_sk as sold_date_sk,
       |        ss_item_sk as sold_item_sk,
       |        ss_sold_time_sk as time_sk
       |     from store_sales,date_dim
       |     where d_date_sk = ss_sold_date_sk
       |        and d_moy=$month
       |        and d_year=$year
       |     ) as tmp, time_dim
       | where
       |   sold_item_sk = i_item_sk
       |   and i_manager_id=1
       |   and time_sk = t_time_sk
       |   and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
       | group by i_brand, i_brand_id,t_hour,t_minute
       | order by ext_price desc, brand_id
     """.stripMargin
  }

  // [MODIFICATIONS]: date + 5 --> date_add(date, 5)
  def q72(): String = {
    val year = uniformRand(1999, 1998, 2002)
    val bp = getValue(">10000", "1001-5000", "501-1000")
    val ms = getMartialStatus("D")
    s"""
       |select i_item_desc
       |      ,w_warehouse_name
       |      ,d1.d_week_seq
       |      ,count(case when p_promo_sk is null then 1 else 0 end) no_promo
       |      ,count(case when p_promo_sk is not null then 1 else 0 end) promo
       |      ,count(*) total_cnt
       |from catalog_sales
       |join inventory on (cs_item_sk = inv_item_sk)
       |join warehouse on (w_warehouse_sk=inv_warehouse_sk)
       |join item on (i_item_sk = cs_item_sk)
       |join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
       |join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
       |join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
       |join date_dim d2 on (inv_date_sk = d2.d_date_sk)
       |join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
       |left outer join promotion on (cs_promo_sk=p_promo_sk)
       |left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
       |where d1.d_week_seq = d2.d_week_seq
       |  and inv_quantity_on_hand < cs_quantity
       |  and d3.d_date > date_add(d1.d_date, 5)
       |  and hd_buy_potential = '$bp'
       |  and d1.d_year = $year
       |  and hd_buy_potential = '$bp'
       |  and cd_marital_status = '$ms'
       |  and d1.d_year = $year
       |group by i_item_desc,w_warehouse_name,d1.d_week_seq
       |order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
       |limit 100
     """.stripMargin
  }

  def q73(): String = {
    val year = uniformRand(1999, 1998, 2000)
    val bpone = getValue(">10000", "1001-5000", "501-1000")
    val bptwo = getValue("unknown", "0-500", "5001-10000")
    // TODO COUNTYNUMBER=ulist(random(1, rowcount("active_counties", "store"), uniform),8);
    val counties = toInList(Seq("Williamson County",
      "Franklin Parish", "Bronx County", "Orange County"))
    s"""
       | select
       |    c_last_name, c_first_name, c_salutation, c_preferred_cust_flag,
       |    ss_ticket_number, cnt from
       |   (select ss_ticket_number, ss_customer_sk, count(*) cnt
       |    from store_sales,date_dim,store,household_demographics
       |    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |    and store_sales.ss_store_sk = store.s_store_sk
       |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       |    and date_dim.d_dom between 1 and 2
       |    and (household_demographics.hd_buy_potential = '$bpone' or
       |         household_demographics.hd_buy_potential = '$bptwo')
       |    and household_demographics.hd_vehicle_count > 0
       |    and case when household_demographics.hd_vehicle_count > 0 then
       |             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
       |    and date_dim.d_year in ($year,$year+1,$year+2)
       |    and store.s_county in ($counties)
       |    group by ss_ticket_number,ss_customer_sk) dj,customer
       |    where ss_customer_sk = c_customer_sk
       |      and cnt between 1 and 5
       |    order by cnt desc
     """.stripMargin
  }

  def q74(): String = {
    val year = uniformRand(2001, 1998, 2001)
    val aggone = getValue("sum", "min", "max", "avg", "stddev_samp")
    // define ORDERC=ulist(random(1,3,uniform),3); -- for qualification 1 1 1
    val orderc = Seq(1, 1, 1)
    s"""
       | with year_total as (
       | select
       |    c_customer_id customer_id, c_first_name customer_first_name,
       |    c_last_name customer_last_name, d_year as year,
       |    $aggone(ss_net_paid) year_total, 's' sale_type
       | from
       |    customer, store_sales, date_dim
       | where c_customer_sk = ss_customer_sk
       |    and ss_sold_date_sk = d_date_sk
       |    and d_year in ($year,$year+1)
       | group by
       |    c_customer_id, c_first_name, c_last_name, d_year
       | union all
       | select
       |    c_customer_id customer_id, c_first_name customer_first_name,
       |    c_last_name customer_last_name, d_year as year,
       |    $aggone(ws_net_paid) year_total, 'w' sale_type
       | from
       |    customer, web_sales, date_dim
       | where c_customer_sk = ws_bill_customer_sk
       |    and ws_sold_date_sk = d_date_sk
       |    and d_year in ($year,$year+1)
       | group by
       |    c_customer_id, c_first_name, c_last_name, d_year)
       | select
       |    t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
       | from
       |    year_total t_s_firstyear, year_total t_s_secyear,
       |    year_total t_w_firstyear, year_total t_w_secyear
       | where t_s_secyear.customer_id = t_s_firstyear.customer_id
       |    and t_s_firstyear.customer_id = t_w_secyear.customer_id
       |    and t_s_firstyear.customer_id = t_w_firstyear.customer_id
       |    and t_s_firstyear.sale_type = 's'
       |    and t_w_firstyear.sale_type = 'w'
       |    and t_s_secyear.sale_type = 's'
       |    and t_w_secyear.sale_type = 'w'
       |    and t_s_firstyear.year = $year
       |    and t_s_secyear.year = $year+1
       |    and t_w_firstyear.year = $year
       |    and t_w_secyear.year = $year+1
       |    and t_s_firstyear.year_total > 0
       |    and t_w_firstyear.year_total > 0
       |    and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
       |      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
       | order by ${orderc(0)}, ${orderc(1)}, ${orderc(2)}
       |limit 100
     """.stripMargin
  }

  def q75(): String = {
    val category = getCategory("Books")
    val year = uniformRand(2002, 1999, 2002)
    s"""
       |WITH all_sales AS (
       |    SELECT
       |        d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
       |        SUM(sales_cnt) AS sales_cnt, SUM(sales_amt) AS sales_amt
       |    FROM (
       |        SELECT
       |            d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
       |            cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt,
       |            cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
       |        FROM catalog_sales
       |        JOIN item ON i_item_sk=cs_item_sk
       |        JOIN date_dim ON d_date_sk=cs_sold_date_sk
       |        LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number
       |                                      AND cs_item_sk=cr_item_sk)
       |        WHERE i_category='$category'
       |        UNION
       |        SELECT
       |            d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
       |             ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt,
       |             ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
       |        FROM store_sales
       |        JOIN item ON i_item_sk=ss_item_sk
       |        JOIN date_dim ON d_date_sk=ss_sold_date_sk
       |        LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number
       |                                    AND ss_item_sk=sr_item_sk)
       |        WHERE i_category='$category'
       |        UNION
       |        SELECT
       |            d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
       |            ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt,
       |            ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
       |        FROM web_sales
       |        JOIN item ON i_item_sk=ws_item_sk
       |        JOIN date_dim ON d_date_sk=ws_sold_date_sk
       |        LEFT JOIN web_returns ON (ws_order_number=wr_order_number
       |                                  AND ws_item_sk=wr_item_sk)
       |        WHERE i_category='$category') sales_detail
       |    GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)
       | SELECT
       |    prev_yr.d_year AS prev_year, curr_yr.d_year AS year, curr_yr.i_brand_id,
       |    curr_yr.i_class_id, curr_yr.i_category_id, curr_yr.i_manufact_id,
       |    prev_yr.sales_cnt AS prev_yr_cnt, curr_yr.sales_cnt AS curr_yr_cnt,
       |    curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff,
       |    curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
       | FROM all_sales curr_yr, all_sales prev_yr
       | WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
       |   AND curr_yr.i_class_id=prev_yr.i_class_id
       |   AND curr_yr.i_category_id=prev_yr.i_category_id
       |   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
       |   AND curr_yr.d_year=$year
       |   AND prev_yr.d_year=$year-1
       |   AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
       | ORDER BY sales_cnt_diff
       | LIMIT 100
     """.stripMargin
  }

  def q76(): String = {
    val cs = getValue("cs_ship_addr_sk", "cs_bill_customer_sk", "cs_bill_hdemo_sk",
      "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_ship_cdemo_sk", "cs_ship_hdemo_sk",
      "cs_ship_mode_sk", "cs_warehouse_sk", "cs_promo_sk")
    val ss = getValue("ss_store_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk",
      "ss_promo_sk")
    val ws = getValue("ws_ship_customer_sk", "ws_bill_customer_sk", "ws_bill_hdemo_sk",
      "ws_bill_addr_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk",
      "ws_web_page_sk", "ws_web_site_sk", "ws_ship_mode_sk", "ws_warehouse_sk", "ws_promo_sk")
    s"""
       | SELECT
       |    channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt,
       |    SUM(ext_sales_price) sales_amt
       | FROM(
       |    SELECT
       |        'store' as channel, '$ss' col_name, d_year, d_qoy, i_category,
       |        ss_ext_sales_price ext_sales_price
       |    FROM store_sales, item, date_dim
       |    WHERE $ss IS NULL
       |      AND ss_sold_date_sk=d_date_sk
       |      AND ss_item_sk=i_item_sk
       |    UNION ALL
       |    SELECT
       |        'web' as channel, '$ws' col_name, d_year, d_qoy, i_category,
       |        ws_ext_sales_price ext_sales_price
       |    FROM web_sales, item, date_dim
       |    WHERE $ws IS NULL
       |      AND ws_sold_date_sk=d_date_sk
       |      AND ws_item_sk=i_item_sk
       |    UNION ALL
       |    SELECT
       |        'catalog' as channel, '$cs' col_name, d_year, d_qoy, i_category,
       |        cs_ext_sales_price ext_sales_price
       |    FROM catalog_sales, item, date_dim
       |    WHERE $cs IS NULL
       |      AND cs_sold_date_sk=d_date_sk
       |      AND cs_item_sk=i_item_sk) foo
       |GROUP BY channel, col_name, d_year, d_qoy, i_category
       |ORDER BY channel, col_name, d_year, d_qoy, i_category
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: rollup
  // [MODIFICATIONS]: + 30 days --> date_add(, 30)
  def q77(): String = {
    // TODO SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
    val salesdate = "2000-08-23"
    s"""
       | with ss as
       | (select s_store_sk, sum(ss_ext_sales_price) as sales, sum(ss_net_profit) as profit
       |  from store_sales, date_dim, store
       |  where ss_sold_date_sk = d_date_sk
       |    and d_date between cast('$salesdate' as date) and
       |                       date_add(cast('$salesdate' as date), 30)
       |    and ss_store_sk = s_store_sk
       |  group by s_store_sk),
       | sr as
       | (select s_store_sk, sum(sr_return_amt) as returns, sum(sr_net_loss) as profit_loss
       | from store_returns, date_dim, store
       | where sr_returned_date_sk = d_date_sk
       |    and d_date between cast('$salesdate' as date) and
       |                       date_add(cast('$salesdate' as date), 30)
       |    and sr_store_sk = s_store_sk
       | group by s_store_sk),
       | cs as
       | (select cs_call_center_sk, sum(cs_ext_sales_price) as sales, sum(cs_net_profit) as profit
       | from catalog_sales, date_dim
       | where cs_sold_date_sk = d_date_sk
       |    and d_date between cast('$salesdate' as date) and
       |                       date_add(cast('$salesdate' as date), 30)
       | group by cs_call_center_sk),
       | cr as
       | (select sum(cr_return_amount) as returns, sum(cr_net_loss) as profit_loss
       | from catalog_returns, date_dim
       | where cr_returned_date_sk = d_date_sk
       |    and d_date between cast('$salesdate]' as date) and
       |                       date_add(cast('$salesdate' as date), 30)),
       | ws as
       | (select wp_web_page_sk, sum(ws_ext_sales_price) as sales, sum(ws_net_profit) as profit
       | from web_sales, date_dim, web_page
       | where ws_sold_date_sk = d_date_sk
       |    and d_date between cast('$salesdate' as date) and
       |                       date_add(cast('$salesdate' as date), 30)
       |    and ws_web_page_sk = wp_web_page_sk
       | group by wp_web_page_sk),
       | wr as
       | (select wp_web_page_sk, sum(wr_return_amt) as returns, sum(wr_net_loss) as profit_loss
       | from web_returns, date_dim, web_page
       | where wr_returned_date_sk = d_date_sk
       |       and d_date between cast('$salesdate' as date) and
       |                          date_add(cast('$salesdate' as date), 30)
       |       and wr_web_page_sk = wp_web_page_sk
       | group by wp_web_page_sk)
       | select channel, id, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit
       | from
       | (select
       |    'store channel' as channel, ss.s_store_sk as id, sales,
       |    coalesce(returns, 0) as returns, (profit - coalesce(profit_loss,0)) as profit
       | from ss left join sr
       |      on  ss.s_store_sk = sr.s_store_sk
       | union all
       | select
       |    'catalog channel' as channel, cs_call_center_sk as id, sales,
       |    returns, (profit - profit_loss) as profit
       | from cs, cr
       | union all
       | select
       |    'web channel' as channel, ws.wp_web_page_sk as id, sales,
       |    coalesce(returns, 0) returns, (profit - coalesce(profit_loss,0)) as profit
       | from   ws left join wr
       |        on  ws.wp_web_page_sk = wr.wp_web_page_sk
       | ) x
       | group by rollup(channel, id)
       | order by channel, id
       | limit 100
     """.stripMargin
  }

  def q78(): String = {
    val year = uniformRand(2000, 1998, 2002)
    s"""
       |with ws as
       |  (select d_year AS ws_sold_year, ws_item_sk,
       |    ws_bill_customer_sk ws_customer_sk,
       |    sum(ws_quantity) ws_qty,
       |    sum(ws_wholesale_cost) ws_wc,
       |    sum(ws_sales_price) ws_sp
       |   from web_sales
       |   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
       |   join date_dim on ws_sold_date_sk = d_date_sk
       |   where wr_order_number is null
       |   group by d_year, ws_item_sk, ws_bill_customer_sk
       |   ),
       |cs as
       |  (select d_year AS cs_sold_year, cs_item_sk,
       |    cs_bill_customer_sk cs_customer_sk,
       |    sum(cs_quantity) cs_qty,
       |    sum(cs_wholesale_cost) cs_wc,
       |    sum(cs_sales_price) cs_sp
       |   from catalog_sales
       |   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
       |   join date_dim on cs_sold_date_sk = d_date_sk
       |   where cr_order_number is null
       |   group by d_year, cs_item_sk, cs_bill_customer_sk
       |   ),
       |ss as
       |  (select d_year AS ss_sold_year, ss_item_sk,
       |    ss_customer_sk,
       |    sum(ss_quantity) ss_qty,
       |    sum(ss_wholesale_cost) ss_wc,
       |    sum(ss_sales_price) ss_sp
       |   from store_sales
       |   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
       |   join date_dim on ss_sold_date_sk = d_date_sk
       |   where sr_ticket_number is null
       |   group by d_year, ss_item_sk, ss_customer_sk
       |   )
       |select
       |  round(ss_qty/(coalesce(ws_qty+cs_qty,1)),2) ratio,
       |  ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
       |  coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
       |  coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
       |  coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
       |from ss
       |left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)
       |left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=cs_item_sk and cs_customer_sk=ss_customer_sk)
       |where coalesce(ws_qty,0)>0 and coalesce(cs_qty, 0)>0 and ss_sold_year=$year
       |order by
       |  ratio,
       |  ss_qty desc, ss_wc desc, ss_sp desc,
       |  other_chan_qty,
       |  other_chan_wholesale_cost,
       |  other_chan_sales_price,
       |  round(ss_qty/(coalesce(ws_qty+cs_qty,1)),2)
       | limit 100
     """.stripMargin
  }


  def q79(): String = {
    val year = uniformRand(1999, 1998, 2000)
    val depcnt = uniformRand(6, 0, 9)
    val vehcnt = uniformRand(2, -1, 4)
    s"""
       | select
       |  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
       |  from
       |   (select ss_ticket_number
       |          ,ss_customer_sk
       |          ,store.s_city
       |          ,sum(ss_coupon_amt) amt
       |          ,sum(ss_net_profit) profit
       |    from store_sales,date_dim,store,household_demographics
       |    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |    and store_sales.ss_store_sk = store.s_store_sk
       |    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
       |    and (household_demographics.hd_dep_count = $depcnt or
       |        household_demographics.hd_vehicle_count > $vehcnt)
       |    and date_dim.d_dow = 1
       |    and date_dim.d_year in ($year,$year+1,$year+2)
       |    and store.s_number_employees between 200 and 295
       |    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
       |    where ss_customer_sk = c_customer_sk
       | order by c_last_name,c_first_name,substr(s_city,1,30), profit
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: rollup
  // [MODIFICATIONS]:  + N days --> date_add
  // [MODIFICATIONS]: || --> concat()
  def q80(): String = {
    // TODO define SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
    val salesdate = "2000-08-23"
    s"""
       | with ssr as
       | (select  s_store_id as store_id,
       |          sum(ss_ext_sales_price) as sales,
       |          sum(coalesce(sr_return_amt, 0)) as returns,
       |          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
       |  from store_sales left outer join store_returns on
       |         (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
       |     date_dim, store, item, promotion
       | where ss_sold_date_sk = d_date_sk
       |       and d_date between cast('$salesdate' as date)
       |                  and date_add(cast('$salesdate' as date), 30)
       |       and ss_store_sk = s_store_sk
       |       and ss_item_sk = i_item_sk
       |       and i_current_price > 50
       |       and ss_promo_sk = p_promo_sk
       |       and p_channel_tv = 'N'
       | group by s_store_id),
       | csr as
       | (select  cp_catalog_page_id as catalog_page_id,
       |          sum(cs_ext_sales_price) as sales,
       |          sum(coalesce(cr_return_amount, 0)) as returns,
       |          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
       |  from catalog_sales left outer join catalog_returns on
       |         (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
       |     date_dim, catalog_page, item, promotion
       | where cs_sold_date_sk = d_date_sk
       |       and d_date between cast('$salesdate' as date)
       |                  and date_add(cast('$salesdate' as date), 30)
       |        and cs_catalog_page_sk = cp_catalog_page_sk
       |       and cs_item_sk = i_item_sk
       |       and i_current_price > 50
       |       and cs_promo_sk = p_promo_sk
       |       and p_channel_tv = 'N'
       | group by cp_catalog_page_id),
       | wsr as
       | (select  web_site_id,
       |          sum(ws_ext_sales_price) as sales,
       |          sum(coalesce(wr_return_amt, 0)) as returns,
       |          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
       |  from web_sales left outer join web_returns on
       |         (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
       |     date_dim, web_site, item, promotion
       | where ws_sold_date_sk = d_date_sk
       |       and d_date between cast('$salesdate' as date)
       |                  and date_add(cast('$salesdate' as date), 30)
       |        and ws_web_site_sk = web_site_sk
       |       and ws_item_sk = i_item_sk
       |       and i_current_price > 50
       |       and ws_promo_sk = p_promo_sk
       |       and p_channel_tv = 'N'
       | group by web_site_id)
       | select channel, id, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit
       | from (select
       |        'store channel' as channel, concat('store', store_id) as id, sales, returns, profit
       |      from ssr
       |      union all
       |      select
       |        'catalog channel' as channel, concat('catalog_page', catalog_page_id) as id,
       |        sales, returns, profit
       |      from csr
       |      union all
       |      select
       |        'web channel' as channel, concat('web_site', web_site_id) as id, sales, returns, profit
       |      from  wsr) x
       | group by rollup (channel, id)
       | order by channel, id
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q81(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val state = getState("GA")
    s"""
       | with customer_total_return as
       | (select
       |    cr_returning_customer_sk as ctr_customer_sk, ca_state as ctr_state,
            sum(cr_return_amt_inc_tax) as ctr_total_return
       | from catalog_returns, date_dim, customer_address
       | where cr_returned_date_sk = d_date_sk
       |   and d_year = $year
       |   and cr_returning_addr_sk = ca_address_sk
       | group by cr_returning_customer_sk, ca_state )
       | select
       |    c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name,
       |    ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,
       |    ca_gmt_offset,ca_location_type,ctr_total_return
       | from customer_total_return ctr1, customer_address, customer
       | where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
       | 			  from customer_total_return ctr2
       |                  	  where ctr1.ctr_state = ctr2.ctr_state)
       |       and ca_address_sk = c_current_addr_sk
       |       and ca_state = '$state'
       |       and ctr1.ctr_customer_sk = c_customer_sk
       | order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
       |                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
       |                  ,ca_location_type,ctr_total_return
       | limit 100
     """.stripMargin
  }

  def q82(): String = {
    val price = uniformRand(62, 0, 90)
    val manufact_ids =
      getList[Int](Seq(129, 270, 821, 423), x => uniformRand(x, 1, 1000).toInt).mkString(", ")
    // TODO INVDATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales);
    val invdate = "2000-05-25"
    s"""
       | select i_item_id, i_item_desc, i_current_price
       | from item, inventory, date_dim, store_sales
       | where i_current_price between $price and $price+30
       | and inv_item_sk = i_item_sk
       | and d_date_sk=inv_date_sk
       | and d_date between cast('$invdate' as date) and date_add(cast('$invdate' as date), 60)
       | and i_manufact_id in ($manufact_ids)
       | and inv_quantity_on_hand between 100 and 500
       | and ss_item_sk = i_item_sk
       | group by i_item_id,i_item_desc,i_current_price
       | order by i_item_id
       | limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: subquery
  def q83(): String = {
    // TODO  RETURNED_DATE_ONE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales);
    val returned_dates = toInList(Seq("2000-06-30", "2000-09-27", "2000-11-17"))
    s"""
       | with sr_items as
       |  (select i_item_id item_id, sum(sr_return_quantity) sr_item_qty
       |   from store_returns, item, date_dim
       |   where sr_item_sk = i_item_sk
       |      and  d_date in (select d_date	from date_dim	where d_week_seq in
       |		      (select d_week_seq	from date_dim where d_date in ($returned_dates)))
       |      and sr_returned_date_sk   = d_date_sk
       |   group by i_item_id),
       | cr_items as
       |  (select i_item_id item_id, sum(cr_return_quantity) cr_item_qty
       |  from catalog_returns, item, date_dim
       |  where cr_item_sk = i_item_sk
       |      and d_date in (select d_date from date_dim where d_week_seq in
       |		      (select d_week_seq from date_dim where d_date in ($returned_dates)))
       |      and cr_returned_date_sk   = d_date_sk
       |      group by i_item_id),
       | wr_items as
       |  (select i_item_id item_id, sum(wr_return_quantity) wr_item_qty
       |  from web_returns, item, date_dim
       |  where wr_item_sk = i_item_sk and d_date in
       |      (select d_date	from date_dim	where d_week_seq in
       |		      (select d_week_seq from date_dim where d_date in ($returned_dates)))
       |    and wr_returned_date_sk = d_date_sk
       |  group by i_item_id)
       | select sr_items.item_id
       |       ,sr_item_qty
       |       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       |       ,cr_item_qty
       |       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       |       ,wr_item_qty
       |       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       |       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
       | from sr_items, cr_items, wr_items
       | where sr_items.item_id=cr_items.item_id
       |   and sr_items.item_id=wr_items.item_id
       | order by sr_items.item_id, sr_item_qty
       | limit 100
     """.stripMargin
  }


  // [MODIFICATIONS]: || --> concat
  def q84(): String = {
    // TODO define CITY = dist(cities, 1, large);
    val city = "Edgewood"
    val income = uniformRand(38128, 0, 70000)
    s"""
       | select c_customer_id as customer_id
       |       ,concat(c_last_name, ', ', c_first_name) as customername
       | from customer
       |     ,customer_address
       |     ,customer_demographics
       |     ,household_demographics
       |     ,income_band
       |     ,store_returns
       | where ca_city	        =  '$city'
       |   and c_current_addr_sk = ca_address_sk
       |   and ib_lower_bound   >=  $income
       |   and ib_upper_bound   <=  $income + 50000
       |   and ib_income_band_sk = hd_income_band_sk
       |   and cd_demo_sk = c_current_cdemo_sk
       |   and hd_demo_sk = c_current_hdemo_sk
       |   and sr_cdemo_sk = cd_demo_sk
       | order by c_customer_id
       | limit 100
     """.stripMargin
  }

  def q85(): String = {
    val year = uniformRand(2000, 1998, 2002)
    val ms = getList(Seq("M", "S", "W"), getMartialStatus)
    val es = getList(Seq("Advanced Degree", "College", "2 yr Degree"), getEducation)
    val states = getList(Seq("IN", "OH", "NJ", "WI", "CT", "KY", "LA", "IA", "AR"), getState)
    s"""
       | select
       |    substr(r_reason_desc,1,20), avg(ws_quantity), avg(wr_refunded_cash), avg(wr_fee)
       | from web_sales, web_returns, web_page, customer_demographics cd1,
       |      customer_demographics cd2, customer_address, date_dim, reason
       | where ws_web_page_sk = wp_web_page_sk
       |   and ws_item_sk = wr_item_sk
       |   and ws_order_number = wr_order_number
       |   and ws_sold_date_sk = d_date_sk and d_year = $year
       |   and cd1.cd_demo_sk = wr_refunded_cdemo_sk
       |   and cd2.cd_demo_sk = wr_returning_cdemo_sk
       |   and ca_address_sk = wr_refunded_addr_sk
       |   and r_reason_sk = wr_reason_sk
       |   and
       |   (
       |    (
       |     cd1.cd_marital_status = '${ms(0)}'
       |     and
       |     cd1.cd_marital_status = cd2.cd_marital_status
       |     and
       |     cd1.cd_education_status = '${es(0)}'
       |     and
       |     cd1.cd_education_status = cd2.cd_education_status
       |     and
       |     ws_sales_price between 100.00 and 150.00
       |    )
       |   or
       |    (
       |     cd1.cd_marital_status = '${ms(1)}'
       |     and
       |     cd1.cd_marital_status = cd2.cd_marital_status
       |     and
       |     cd1.cd_education_status = '${es(1)}'
       |     and
       |     cd1.cd_education_status = cd2.cd_education_status
       |     and
       |     ws_sales_price between 50.00 and 100.00
       |    )
       |   or
       |    (
       |     cd1.cd_marital_status = '${ms(2)}'
       |     and
       |     cd1.cd_marital_status = cd2.cd_marital_status
       |     and
       |     cd1.cd_education_status = '${es(2)}'
       |     and
       |     cd1.cd_education_status = cd2.cd_education_status
       |     and
       |     ws_sales_price between 150.00 and 200.00
       |    )
       |   )
       |   and
       |   (
       |    (
       |     ca_country = 'United States'
       |     and
       |     ca_state in ('${states(0)}', '${states(1)}', '${states(2)}')
       |     and ws_net_profit between 100 and 200
       |    )
       |    or
       |    (
       |     ca_country = 'United States'
       |     and
       |     ca_state in ('${states(3)}', '${states(4)}', '${states(5)}')
       |     and ws_net_profit between 150 and 300
       |    )
       |    or
       |    (
       |     ca_country = 'United States'
       |     and
       |     ca_state in ('${states(6)}', '${states(7)}', '${states(8)}')
       |     and ws_net_profit between 50 and 250
       |    )
       |   )
       | group by r_reason_desc
       | order by substr(r_reason_desc,1,20)
       |        ,avg(ws_quantity)
       |        ,avg(wr_refunded_cash)
       |        ,avg(wr_fee)
       | limit 100
     """.stripMargin
  }

  // {UNSUPPORTED]: window functions
  // [UNSUPPORTED]: rollup
  def q86(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       | select sum(ws_net_paid) as total_sum, i_category, i_class,
       |  grouping(i_category)+grouping(i_class) as lochierarchy,
       |  rank() over (
       | 	    partition by grouping(i_category)+grouping(i_class),
       | 	    case when grouping(i_class) = 0 then i_category end
       | 	    order by sum(ws_net_paid) desc) as rank_within_parent
       | from
       |    web_sales, date_dim d1, item
       | where
       |    d1.d_month_seq between $dms and $dms+11
       | and d1.d_date_sk = ws_sold_date_sk
       | and i_item_sk  = ws_item_sk
       | group by rollup(i_category,i_class)
       | order by
       |   lochierarchy desc,
       |   case when lochierarchy = 0 then i_category end,
       |   rank_within_parent
       | limit 100
     """.stripMargin
  }

  def q87(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       | select count(*)
       | from ((select distinct c_last_name, c_first_name, d_date
       |       from store_sales, date_dim, customer
       |       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
       |         and store_sales.ss_customer_sk = customer.c_customer_sk
       |         and d_month_seq between $dms and $dms+11)
       |       except
       |      (select distinct c_last_name, c_first_name, d_date
       |       from catalog_sales, date_dim, customer
       |       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
       |         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
       |         and d_month_seq between $dms and $dms+11)
       |       except
       |      (select distinct c_last_name, c_first_name, d_date
       |       from web_sales, date_dim, customer
       |       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
       |         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
       |         and d_month_seq between $dms and $dms+11)
       |) cool_cust
     """.stripMargin
  }

  def q88(): String = {
    val hour = getList[Int](Seq(4, 2, 0), x => uniformRand(x, -1, 4).toInt)
    s"""
       | select  *
       | from
       |   (select count(*) h8_30_to_9
       |    from store_sales, household_demographics , time_dim, store
       |    where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 8
       |     and time_dim.t_minute >= 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s1,
       |   (select count(*) h9_to_9_30
       |    from store_sales, household_demographics , time_dim, store
       |    where ss_sold_time_sk = time_dim.t_time_sk
       |      and ss_hdemo_sk = household_demographics.hd_demo_sk
       |      and ss_store_sk = s_store_sk
       |      and time_dim.t_hour = 9
       |      and time_dim.t_minute < 30
       |      and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |      and store.s_store_name = 'ese') s2,
       | (select count(*) h9_30_to_10
       | from store_sales, household_demographics , time_dim, store
       | where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 9
       |     and time_dim.t_minute >= 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s3,
       | (select count(*) h10_to_10_30
       | from store_sales, household_demographics , time_dim, store
       | where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 10
       |     and time_dim.t_minute < 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s4,
       | (select count(*) h10_30_to_11
       | from store_sales, household_demographics , time_dim, store
       | where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 10
       |     and time_dim.t_minute >= 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s5,
       | (select count(*) h11_to_11_30
       | from store_sales, household_demographics , time_dim, store
       | where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 11
       |     and time_dim.t_minute < 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s6,
       | (select count(*) h11_30_to_12
       | from store_sales, household_demographics , time_dim, store
       | where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 11
       |     and time_dim.t_minute >= 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s7,
       | (select count(*) h12_to_12_30
       | from store_sales, household_demographics , time_dim, store
       | where ss_sold_time_sk = time_dim.t_time_sk
       |     and ss_hdemo_sk = household_demographics.hd_demo_sk
       |     and ss_store_sk = s_store_sk
       |     and time_dim.t_hour = 12
       |     and time_dim.t_minute < 30
       |     and ((household_demographics.hd_dep_count = ${hour(0)} and household_demographics.hd_vehicle_count<=${hour(0)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(1)} and household_demographics.hd_vehicle_count<=${hour(1)}+2) or
       |          (household_demographics.hd_dep_count = ${hour(2)} and household_demographics.hd_vehicle_count<=${hour(2)}+2))
       |     and store.s_store_name = 'ese') s8
     """.stripMargin
  }

  // [UNSUPPORTED]: Window functions, works in hiveql
  def q89(): String = {
    val year = uniformRand(1999, 1998, 2002)
    val classes1 = toInList(Seq("computers", "stereo", "football"))
    val cats1 = toInList(Seq("Books", "Electronics", "Sports"))
    val classes2 = toInList(Seq("shirts", "bridal", "dresses"))
    val cats2 = toInList(Seq("Men", "Jewelry", "Women"))
    s"""
       | select *
       | from(
       | select i_category, i_class, i_brand,
       |       s_store_name, s_company_name,
       |       d_moy,
       |       sum(ss_sales_price) sum_sales,
       |       avg(sum(ss_sales_price)) over
       |         (partition by i_category, i_brand, s_store_name, s_company_name)
       |         avg_monthly_sales
       | from item, store_sales, date_dim, store
       | where ss_item_sk = i_item_sk and
       |      ss_sold_date_sk = d_date_sk and
       |      ss_store_sk = s_store_sk and
       |      d_year in ($year) and
       |        ((i_category in ($cats1) and i_class in ($classes1)
       |         )
       |      or (i_category in ($cats2) and i_class in ($classes2)
       |        ))
       | group by i_category, i_class, i_brand,
       |         s_store_name, s_company_name, d_moy) tmp1
       | where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
       | order by sum_sales - avg_monthly_sales, s_store_name
       | limit 100
     """.stripMargin
  }

  def q90(): String = {
    val depcnt = uniformRand(6, 0, 9)
    val hourpm = uniformRand(19, 13, 21)
    val houram = uniformRand(8, 6, 12)
    s"""
       | select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
       | from ( select count(*) amc
       |       from web_sales, household_demographics , time_dim, web_page
       |       where ws_sold_time_sk = time_dim.t_time_sk
       |         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
       |         and ws_web_page_sk = web_page.wp_web_page_sk
       |         and time_dim.t_hour between $houram and $houram+1
       |         and household_demographics.hd_dep_count = $depcnt
       |         and web_page.wp_char_count between 5000 and 5200) at,
       |      ( select count(*) pmc
       |       from web_sales, household_demographics , time_dim, web_page
       |       where ws_sold_time_sk = time_dim.t_time_sk
       |         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
       |         and ws_web_page_sk = web_page.wp_web_page_sk
       |         and time_dim.t_hour between $hourpm and $hourpm+1
       |         and household_demographics.hd_dep_count = $depcnt
       |         and web_page.wp_char_count between 5000 and 5200) pt
       | order by am_pm_ratio
       | limit 100
     """.stripMargin
  }

  def q91(): String = {
    val year = uniformRand(1998, 1998, 2002)
    val month = uniformRand(11, 11, 12)
    val gmt = uniformRand(-7, -7, -6)
    val buy = getValue("Unknown", "1001-5000", ">10000", "501-1000", "0-500", "5001-10000")
    s"""
       | select
       |        cc_call_center_id Call_Center, cc_name Call_Center_Name, cc_manager Manager,
       |        sum(cr_net_loss) Returns_Loss
       | from
       |        call_center, catalog_returns, date_dim, customer, customer_address,
       |        customer_demographics, household_demographics
       | where
       |        cr_call_center_sk       = cc_call_center_sk
       | and     cr_returned_date_sk     = d_date_sk
       | and     cr_returning_customer_sk= c_customer_sk
       | and     cd_demo_sk              = c_current_cdemo_sk
       | and     hd_demo_sk              = c_current_hdemo_sk
       | and     ca_address_sk           = c_current_addr_sk
       | and     d_year                  = $year
       | and     d_moy                   = $month
       | and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
       |        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
       | and     hd_buy_potential like '$buy%'
       | and     ca_gmt_offset           = $gmt
       | group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
       | order by sum(cr_net_loss) desc
     """.stripMargin
  }


  // [UNSUPPORTED]: Column alias string
  // [UNSUPPORTED]: subquery
  // [MODIFICATIONS]: date_add
  def q92(): String = {
    val imid = uniformRand(350, 1, 1000)
    // TODO: Define WSDATE = date([YEAR]+"-01-01",[YEAR]+"-04-01",sales)
    val wsdate = "2000-01-27"
    s"""
       |select sum(ws_ext_discount_amt) as "Excess Discount Amount"
       |from web_sales, item, date_dim
       |where i_manufact_id = $imid
       |and i_item_sk = ws_item_sk
       |and d_date between '$wsdate' and date_add(cast('$wsdate' as date), 90)
       |and d_date_sk = ws_sold_date_sk
       |and ws_ext_discount_amt >
       |    (
       |      SELECT 1.3 * avg(ws_ext_discount_amt)
       |      FROM web_sales, date_dim
       |      WHERE ws_item_sk = i_item_sk
       |        and d_date between '$wsdate' and date_add(cast('$wsdate' as date), 90)
       |        and d_date_sk = ws_sold_date_sk
       |    )
       |order by sum(ws_ext_discount_amt)
       |limit 100
     """.stripMargin
  }

  def q93(): String = {
    val reason = "reason 28"
    s"""
       |select ss_customer_sk, sum(act_sales) sumsales
       |from (select
       |        ss_item_sk, ss_ticket_number, ss_customer_sk,
       |        case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
       |                                                 else (ss_quantity*ss_sales_price) end act_sales
       |      from store_sales
       |      left outer join store_returns
       |      on (sr_item_sk = ss_item_sk and sr_ticket_number = ss_ticket_number),
       |      reason
       |      where sr_reason_sk = r_reason_sk and r_reason_desc = '$reason') t
       |group by ss_customer_sk
       |order by sumsales, ss_customer_sk
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: Column alias string
  // [UNSUPPORTED]: subquery
  // [MODIFICATIONS]: date_add
  def q94(): String = {
    val year = uniformRand(1999, 1999, 2002)
    val month = uniformRand(2, 2, 5)
    val state = getState("IL")
    s"""
       |select
       |   count(distinct ws_order_number) as "order count"
       |  ,sum(ws_ext_ship_cost) as "total shipping cost"
       |  ,sum(ws_net_profit) as "total net profit"
       |from
       |   web_sales ws1, date_dim, customer_address, web_site
       |where
       |    d_date between '$year-$month-01' and
       |           date_add(cast('$year-$month]-01' as date), 60)
       |and ws1.ws_ship_date_sk = d_date_sk
       |and ws1.ws_ship_addr_sk = ca_address_sk
       |and ca_state = '$state'
       |and ws1.ws_web_site_sk = web_site_sk
       |and web_company_name = 'pri'
       |and exists (select *
       |            from web_sales ws2
       |            where ws1.ws_order_number = ws2.ws_order_number
       |              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
       |and not exists(select *
       |               from web_returns wr1
       |               where ws1.ws_order_number = wr1.wr_order_number)
       |order by count(distinct ws_order_number)
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: Column alias string
  // [UNSUPPORTED]: subquery
  // [MODIFICATIONS]: date_add
  def q95(): String = {
    val state = getState("IL")
    val year = uniformRand(1999, 1999, 2002)
    val month = uniformRand(2, 2, 5)
    s"""
       |with ws_wh as
       |(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
       | from web_sales ws1,web_sales ws2
       | where ws1.ws_order_number = ws2.ws_order_number
       |   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
       |select
       |   count(distinct ws_order_number) as "order count"
       |  ,sum(ws_ext_ship_cost) as "total shipping cost"
       |  ,sum(ws_net_profit) as "total net profit"
       |from
       |   web_sales ws1, date_dim, customer_address, web_site
       |where
       |    d_date between '$year-$month-01' and
       |           date_add(cast('$year-$month-01' as date), 60)
       |and ws1.ws_ship_date_sk = d_date_sk
       |and ws1.ws_ship_addr_sk = ca_address_sk
       |and ca_state = '$state'
       |and ws1.ws_web_site_sk = web_site_sk
       |and web_company_name = 'pri'
       |and ws1.ws_order_number in (select ws_order_number
       |                            from ws_wh)
       |and ws1.ws_order_number in (select wr_order_number
       |                            from web_returns,ws_wh
       |                            where wr_order_number = ws_wh.ws_order_number)
       |order by count(distinct ws_order_number)
       |limit 100
     """.stripMargin
  }

  def q96(): String = {
    val hour = getValue(20, 15, 16, 8)
    val depcnt = uniformRand(7, 0, 9)
    s"""
       |select count(*)
       |from store_sales, household_demographics, time_dim, store
       |where ss_sold_time_sk = time_dim.t_time_sk
       |    and ss_hdemo_sk = household_demographics.hd_demo_sk
       |    and ss_store_sk = s_store_sk
       |    and time_dim.t_hour = $hour
       |    and time_dim.t_minute >= 30
       |    and household_demographics.hd_dep_count = $depcnt
       |    and store.s_store_name = 'ese'
       |order by count(*)
       |limit 100
     """.stripMargin
  }

  def q97(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       |with ssci as (
       |select ss_customer_sk customer_sk, ss_item_sk item_sk
       |from store_sales,date_dim
       |where ss_sold_date_sk = d_date_sk
       |  and d_month_seq between $dms and $dms + 11
       |group by ss_customer_sk, ss_item_sk),
       |csci as(
       | select cs_bill_customer_sk customer_sk, cs_item_sk item_sk
       |from catalog_sales,date_dim
       |where cs_sold_date_sk = d_date_sk
       |  and d_month_seq between $dms and $dms + 11
       |group by cs_bill_customer_sk, cs_item_sk)
       |select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
       |      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
       |      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
       |from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
       |                               and ssci.item_sk = csci.item_sk)
       |limit 100
     """.stripMargin
  }

  // [UNSUPPORTED]: window functions
  // [MODIFICATIONS]: date_add
  def q98(): String = {
    // TODO: Define SDATE=date([YEAR]+"-01-01",[YEAR]+"-07-01",sales);
    val sdate = "1999-02-22"
    val categories = toInList(getList(Seq("Sports", "Books", "Home"), getCategory))
    s"""
       |select i_item_desc
       |      ,i_category
       |      ,i_class
       |      ,i_current_price
       |      ,sum(ss_ext_sales_price) as itemrevenue
       |      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
       |          (partition by i_class) as revenueratio
       |from
       |	store_sales, item, date_dim
       |where
       |	ss_item_sk = i_item_sk
       |  	and i_category in ($categories)
       |  	and ss_sold_date_sk = d_date_sk
       |	and d_date between cast('$sdate' as date)
       |				and date_add(cast('$sdate' as date), 30)
       |group by
       |	i_item_id, i_item_desc, i_category, i_class, i_current_price
       |order by
       |	i_category, i_class, i_item_id, i_item_desc, revenueratio
     """.stripMargin
  }

  // [UNSUPPORTED]: Column alias string
  def q99(): String = {
    val dms = uniformRand(1200, 1176, 1224)
    s"""
       |select
       |   substr(w_warehouse_name,1,20)
       |  ,sm_type
       |  ,cc_name
       |  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
       |  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and
       |                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
       |  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and
       |                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
       |  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
       |                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
       |  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
       |from
       |   catalog_sales, warehouse, ship_mode, call_center, date_dim
       |where
       |    d_month_seq between $dms and $dms + 11
       |and cs_ship_date_sk   = d_date_sk
       |and cs_warehouse_sk   = w_warehouse_sk
       |and cs_ship_mode_sk   = sm_ship_mode_sk
       |and cs_call_center_sk = cc_call_center_sk
       |group by
       |   substr(w_warehouse_name,1,20)
       |  ,sm_type
       |  ,cc_name
       |order by substr(w_warehouse_name,1,20), sm_type, cc_name
       |limit 100
     """.stripMargin
  }
}
