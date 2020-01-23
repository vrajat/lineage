/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package io.tokern.lineage.sqlplanner.planner;

import io.tokern.lineage.sqlplanner.QanException;

import java.sql.Types;
import java.util.Arrays;

public class Tpcds extends MartSchema {
  public Tpcds(String name) {
    super(name);
  }

  public void addTables() throws QanException {
    MartTable customer_demographics = new MartTable(this, "CUSTOMER_DEMOGRAPHICS",
        Arrays.asList(new MartColumn("cd_demo_sk", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("cd_marital_status", Types.VARCHAR),
            new MartColumn("cd_education_status", Types.VARCHAR),
            new MartColumn("cd_purchase_estimate", Types.INTEGER),
            new MartColumn("cd_credit_rating", Types.VARCHAR),
            new MartColumn("cd_dep_count", Types.INTEGER),
            new MartColumn("cd_dep_employed_count", Types.INTEGER),
            new MartColumn("cd_dep_college_count", Types.INTEGER)
        ));

    this.addTable(customer_demographics);

    MartTable date_dim = new MartTable(this, "DATE_DIM",
        Arrays.asList(
            new MartColumn("d_date_sk", Types.INTEGER),
            new MartColumn("d_date_id", Types.VARCHAR),
            new MartColumn("d_date", Types.DATE),
            new MartColumn("d_month_seq", Types.INTEGER),
            new MartColumn("d_week_seq", Types.INTEGER),
            new MartColumn("d_quarter_seq", Types.INTEGER),
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_dow", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("d_dom", Types.INTEGER),
            new MartColumn("d_qoy", Types.INTEGER),
            new MartColumn("d_fy_year", Types.INTEGER),
            new MartColumn("d_fy_quarter_seq", Types.INTEGER),
            new MartColumn("d_fy_week_seq", Types.INTEGER),
            new MartColumn("d_day_name", Types.VARCHAR),
            new MartColumn("d_quarter_name", Types.VARCHAR),
            new MartColumn("d_holiday", Types.VARCHAR),
            new MartColumn("d_weekend", Types.VARCHAR),
            new MartColumn("d_following_holiday", Types.VARCHAR),
            new MartColumn("d_first_dom", Types.INTEGER),
            new MartColumn("d_last_dom", Types.INTEGER),
            new MartColumn("d_same_day_ly", Types.INTEGER),
            new MartColumn("d_same_day_lq", Types.INTEGER),
            new MartColumn("d_current_day", Types.VARCHAR),
            new MartColumn("d_current_week", Types.VARCHAR),
            new MartColumn("d_current_month", Types.VARCHAR),
            new MartColumn("d_current_quarter", Types.VARCHAR),
            new MartColumn("d_current_year", Types.VARCHAR)
        ));

    this.addTable(date_dim);

    MartTable time_dim = new MartTable(this, "TIME_DIM",
        Arrays.asList(
            new MartColumn("t_time_sk", Types.INTEGER),
            new MartColumn("t_time_id", Types.VARCHAR),
            new MartColumn("t_time", Types.INTEGER),
            new MartColumn("t_hour", Types.INTEGER),
            new MartColumn("t_minute", Types.INTEGER),
            new MartColumn("t_second", Types.INTEGER),
            new MartColumn("t_am_pm", Types.VARCHAR),
            new MartColumn("t_shift", Types.VARCHAR),
            new MartColumn("t_sub_shift", Types.VARCHAR),
            new MartColumn("t_meal_time", Types.VARCHAR)
        ));

    this.addTable(time_dim);

    MartTable item = new MartTable(this, "ITEM", Arrays.asList(
        new MartColumn("i_item_sk", Types.INTEGER),
        new MartColumn("i_item_id", Types.VARCHAR),
        new MartColumn("i_rec_start_date", Types.DATE),
        new MartColumn("i_rec_end_date", Types.DATE),
        new MartColumn("i_item_desc", Types.VARCHAR),
        new MartColumn("i_current_price", Types.DOUBLE),
        new MartColumn("i_wholesale_cost", Types.DOUBLE),
        new MartColumn("i_brand_id", Types.INTEGER),
        new MartColumn("i_brand", Types.VARCHAR),
        new MartColumn("i_class_id", Types.INTEGER),
        new MartColumn("i_class", Types.VARCHAR),
        new MartColumn("i_category_id", Types.INTEGER),
        new MartColumn("i_category", Types.VARCHAR),
        new MartColumn("i_manufact_id", Types.INTEGER),
        new MartColumn("i_manufact", Types.VARCHAR),
        new MartColumn("i_size", Types.VARCHAR),
        new MartColumn("i_formulation", Types.VARCHAR),
        new MartColumn("i_color", Types.VARCHAR),
        new MartColumn("i_units", Types.VARCHAR),
        new MartColumn("i_container", Types.VARCHAR),
        new MartColumn("i_manager_id", Types.INTEGER),
        new MartColumn("i_product_name", Types.VARCHAR)
    ), 100.0, "i_item_id");

    this.addTable(item);

    MartTable customer = new MartTable(this, "CUSTOMER", Arrays.asList(
        new MartColumn("c_customer_sk", Types.INTEGER),
        new MartColumn("c_customer_id", Types.VARCHAR),
        new MartColumn("c_current_cdemo_sk", Types.INTEGER),
        new MartColumn("c_current_hdemo_sk", Types.INTEGER),
        new MartColumn("c_current_addr_sk", Types.INTEGER),
        new MartColumn("c_first_shipto_date_sk", Types.INTEGER),
        new MartColumn("c_first_sales_date_sk", Types.INTEGER),
        new MartColumn("c_salutation", Types.VARCHAR),
        new MartColumn("c_first_name", Types.VARCHAR),
        new MartColumn("c_last_name", Types.VARCHAR),
        new MartColumn("c_preferred_cust_flag", Types.VARCHAR),
        new MartColumn("c_birth_day", Types.INTEGER),
        new MartColumn("c_birth_month", Types.INTEGER),
        new MartColumn("c_birth_year", Types.INTEGER),
        new MartColumn("c_birth_country", Types.VARCHAR),
        new MartColumn("c_login", Types.VARCHAR),
        new MartColumn("c_email_address", Types.VARCHAR),
        new MartColumn("c_last_review_date", Types.VARCHAR)
    ));

    this.addTable(customer);

    MartTable web_returns = new MartTable(this, "WEB_RETURNS",
        Arrays.asList(
            new MartColumn("wr_returned_date_sk", Types.INTEGER),
            new MartColumn("wr_returned_time_sk", Types.INTEGER),
            new MartColumn("wr_item_sk", Types.INTEGER),
            new MartColumn("wr_refunded_customer_sk", Types.INTEGER),
            new MartColumn("wr_refunded_cdemo_sk", Types.INTEGER),
            new MartColumn("wr_refunded_hdemo_sk", Types.INTEGER),
            new MartColumn("wr_refunded_addr_sk", Types.INTEGER),
            new MartColumn("wr_returning_customer_sk", Types.INTEGER),
            new MartColumn("wr_returning_cdemo_sk", Types.INTEGER),
            new MartColumn("wr_returning_hdemo_sk", Types.INTEGER),
            new MartColumn("wr_returning_addr_sk", Types.INTEGER),
            new MartColumn("wr_web_page_sk", Types.INTEGER),
            new MartColumn("wr_reason_sk", Types.INTEGER),
            new MartColumn("wr_order_number", Types.INTEGER),
            new MartColumn("wr_return_quantity", Types.INTEGER),
            new MartColumn("wr_return_amt", Types.DOUBLE),
            new MartColumn("wr_return_tax", Types.DOUBLE),
            new MartColumn("wr_return_amt_inc_tax", Types.DOUBLE),
            new MartColumn("wr_fee", Types.DOUBLE),
            new MartColumn("wr_return_ship_cost", Types.DOUBLE),
            new MartColumn("wr_refunded_cash", Types.DOUBLE),
            new MartColumn("wr_reversed_charge", Types.DOUBLE),
            new MartColumn("wr_account_credit", Types.DOUBLE),
            new MartColumn("wr_net_loss", Types.DOUBLE)
        ));

    this.addTable(web_returns);

    MartTable web_returns_cube = new MartTable(this, "WEB_RETURNS_CUBE",
        Arrays.asList(
            new MartColumn("i_item_id", Types.VARCHAR),
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_qoy", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("d_date", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("cd_marital_status", Types.VARCHAR),
            new MartColumn("cd_education_status", Types.VARCHAR),
            new MartColumn("grouping_id", Types.VARCHAR),
            new MartColumn("total_net_loss", Types.DOUBLE)
        ));

    this.addTable(web_returns_cube);

    MartTable store_sales = new MartTable(this, "STORE_SALES",
        Arrays.asList(
            new MartColumn("ss_sold_date_sk", Types.INTEGER),
            new MartColumn("ss_sold_time_sk", Types.INTEGER),
            new MartColumn("ss_item_sk", Types.INTEGER),
            new MartColumn("ss_customer_sk", Types.INTEGER),
            new MartColumn("ss_cdemo_sk", Types.INTEGER),
            new MartColumn("ss_hdemo_sk", Types.INTEGER),
            new MartColumn("ss_addr_sk", Types.INTEGER),
            new MartColumn("ss_store_sk", Types.INTEGER),
            new MartColumn("ss_promo_sk", Types.INTEGER),
            new MartColumn("ss_ticket_number", Types.INTEGER),
            new MartColumn("ss_quantity", Types.INTEGER),
            new MartColumn("ss_wholesale_cost", Types.DOUBLE),
            new MartColumn("ss_list_price", Types.DOUBLE),
            new MartColumn("ss_sales_price", Types.DOUBLE),
            new MartColumn("ss_ext_discount_amt", Types.DOUBLE),
            new MartColumn("ss_ext_sales_price", Types.DOUBLE),
            new MartColumn("ss_ext_wholesale_cost", Types.DOUBLE),
            new MartColumn("ss_ext_list_price", Types.DOUBLE),
            new MartColumn("ss_ext_tax", Types.DOUBLE),
            new MartColumn("ss_coupon_amt", Types.DOUBLE),
            new MartColumn("ss_net_paid", Types.DOUBLE),
            new MartColumn("ss_net_paid_inc_tax", Types.DOUBLE),
            new MartColumn("ss_net_profit", Types.DOUBLE)
        ));

    this.addTable(store_sales);

    MartTable web_site = new MartTable(this, "WEB_SITE", Arrays.asList(
        new MartColumn("web_site_sk", Types.INTEGER),
        new MartColumn("web_site_id", Types.CHAR),
        new MartColumn("web_rec_start_date", Types.DATE),
        new MartColumn("web_rec_end_data", Types.DATE),
        new MartColumn("web_name", Types.VARCHAR),
        new MartColumn("web_open_date_sk", Types.INTEGER),
        new MartColumn("web_close_date_sk", Types.INTEGER),
        new MartColumn("web_class", Types.VARCHAR),
        new MartColumn("web_manager", Types.VARCHAR),
        new MartColumn("web_mkt_id", Types.INTEGER),
        new MartColumn("web_mkt_class", Types.VARCHAR),
        new MartColumn("web_mkt_desc", Types.VARCHAR),
        new MartColumn("web_market_manager", Types.VARCHAR),
        new MartColumn("web_company_id", Types.INTEGER),
        new MartColumn("web_company_name", Types.CHAR),
        new MartColumn("web_street_number", Types.CHAR),
        new MartColumn("web_street_name", Types.VARCHAR),
        new MartColumn("web_street_type", Types.CHAR),
        new MartColumn("web_suite_number", Types.CHAR),
        new MartColumn("web_city", Types.VARCHAR),
        new MartColumn("web_county", Types.VARCHAR),
        new MartColumn("web_state", Types.CHAR),
        new MartColumn("web_zip", Types.CHAR),
        new MartColumn("web_country", Types.VARCHAR),
        new MartColumn("web_gmt_offset", Types.DECIMAL),
        new MartColumn("web_tax_percentage", Types.DECIMAL)
    ));

    this.addTable(web_site);

    MartTable store_sales_cube = new MartTable(this, "STORE_SALES_CUBE",
        Arrays.asList(
            new MartColumn("i_item_id", Types.VARCHAR),
            new MartColumn("c_customer_id", Types.VARCHAR),
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_qoy", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("d_date", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("cd_marital_status", Types.VARCHAR),
            new MartColumn("cd_education_status", Types.VARCHAR),
            new MartColumn("grouping_id", Types.VARCHAR),
            new MartColumn("sum_sales_price", Types.DOUBLE),
            new MartColumn("sum_extended_sales_price", Types.DOUBLE)
        ));

    this.addTable(store_sales_cube);

    MartTable store_sales_cube_partial = new MartTable(this, "STORE_SALES_CUBE_PARTIAL",
        Arrays.asList(
            new MartColumn("i_item_id", Types.VARCHAR),
            new MartColumn("c_customer_id", Types.VARCHAR),
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("d_dom", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("cd_marital_status", Types.VARCHAR),
            new MartColumn("cd_education_status", Types.VARCHAR),
            new MartColumn("grouping_id", Types.VARCHAR),
            new MartColumn("sum_sales_price", Types.DOUBLE),
            new MartColumn("sum_extended_sales_price", Types.DOUBLE)
        ));

    this.addTable(store_sales_cube_partial);

    MartTable store_sales_cube_daily = new MartTable(this, "STORE_SALES_CUBE_DAILY",
        Arrays.asList(
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("d_dom", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("grouping_id", Types.VARCHAR),
            new MartColumn("sum_sales_price", Types.DOUBLE),
            new MartColumn("sum_extended_sales_price", Types.DOUBLE)
        ));

    this.addTable(store_sales_cube_daily);

    MartTable store_sales_cube_weekly = new MartTable(this, "STORE_SALES_CUBE_WEEKLY",
        Arrays.asList(
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("d_week_seq", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("grouping_id", Types.VARCHAR),
            new MartColumn("sum_sales_price", Types.DOUBLE),
            new MartColumn("sum_extended_sales_price", Types.DOUBLE)
        ));

    this.addTable(store_sales_cube_weekly);

    MartTable store_sales_cube_monthly = new MartTable(this, "STORE_SALES_CUBE_MONTHLY",
        Arrays.asList(
            new MartColumn("d_year", Types.INTEGER),
            new MartColumn("d_moy", Types.INTEGER),
            new MartColumn("cd_gender", Types.VARCHAR),
            new MartColumn("grouping_id", Types.VARCHAR),
            new MartColumn("sum_sales_price", Types.DOUBLE),
            new MartColumn("sum_extended_sales_price", Types.DOUBLE)
        ));

    this.addTable(store_sales_cube_monthly);

    MartTable web_site_partition = new MartTable(this, "WEB_SITE_PARTITION",
        Arrays.asList(
            new MartColumn("web_site_sk", Types.INTEGER),
            new MartColumn("web_rec_start_date", Types.DATE),
            new MartColumn("web_county", Types.VARCHAR),
            new MartColumn("web_tax_percentage", Types.DECIMAL)
        ));

    this.addTable(web_site_partition);
  }
}
