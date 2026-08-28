# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VIEW retail_q.retail_semantic.retail_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC
# MAGIC source: retail_q.retail_gold.fact_sales
# MAGIC comment: Retail metrics for analyzing sales transactions, revenue, and product performance
# MAGIC joins:
# MAGIC   - name: product
# MAGIC     source: retail_q.retail_gold.dim_product
# MAGIC     on: source.product_id = product.product_id
# MAGIC   - name: calendar
# MAGIC     source: retail_q.retail_gold.dim_calendar
# MAGIC     on: source.transaction_date = calendar.date
# MAGIC   - name: customer
# MAGIC     source: retail_q.retail_gold.dim_customer
# MAGIC     on: source.customer_id = customer.customer_id
# MAGIC
# MAGIC     
# MAGIC dimensions:
# MAGIC   - name: Transaction Date
# MAGIC     expr: calendar.date
# MAGIC     display_name: Transaction Date
# MAGIC     comment: Date when the transaction occurred
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC     synonyms:
# MAGIC       - date
# MAGIC       - sale date
# MAGIC       - transaction day
# MAGIC   - name: Year
# MAGIC     expr: calendar.year
# MAGIC     display_name: Year
# MAGIC     comment: Year of the transaction
# MAGIC   - name: Quarter
# MAGIC     expr: calendar.quarter
# MAGIC     display_name: Quarter
# MAGIC     comment: Quarter of the transaction
# MAGIC   - name: Month Name
# MAGIC     expr: calendar.month_name
# MAGIC     display_name: Month
# MAGIC     comment: Month name of the transaction
# MAGIC     synonyms:
# MAGIC       - month
# MAGIC   - name: Product Category
# MAGIC     expr: product.category
# MAGIC     display_name: Product Category
# MAGIC     comment: Main product category
# MAGIC     synonyms:
# MAGIC       - category
# MAGIC   - name: Product Brand
# MAGIC     expr: product.brand
# MAGIC     display_name: Brand
# MAGIC     comment: Product brand name
# MAGIC     synonyms:
# MAGIC       - brand
# MAGIC   - name: Payment Mode
# MAGIC     expr: source.payment_mode
# MAGIC     display_name: Payment Mode
# MAGIC     comment: Method of payment used for transaction
# MAGIC     synonyms:
# MAGIC       - payment method
# MAGIC       - payment type
# MAGIC   - name: Sales Channel
# MAGIC     expr: source.sales_channel
# MAGIC     display_name: Sales Channel
# MAGIC     comment: Channel through which sale was made
# MAGIC     synonyms:
# MAGIC       - channel
# MAGIC   - name: Stage Name
# MAGIC     expr: source.stage_name
# MAGIC     display_name: Opportunity Stage
# MAGIC     comment: Sales opportunity stage
# MAGIC   - name: Customer Type
# MAGIC     expr: customer.customer_type
# MAGIC     display_name: Customer Type
# MAGIC     comment: Type or category of customer
# MAGIC     synonyms:
# MAGIC       - customer segment
# MAGIC       - customer category
# MAGIC   - name: Customer Name
# MAGIC     expr: customer.customer_name
# MAGIC     display_name: Customer Name
# MAGIC     comment: Name of the customer
# MAGIC     synonyms:
# MAGIC       - customer
# MAGIC   - name: Billing City
# MAGIC     expr: customer.billing_city
# MAGIC     display_name: City
# MAGIC     comment: Customer billing city
# MAGIC     synonyms:
# MAGIC       - city
# MAGIC       - customer city
# MAGIC   - name: Billing State
# MAGIC     expr: customer.billing_state
# MAGIC     display_name: State
# MAGIC     comment: Customer billing state
# MAGIC     synonyms:
# MAGIC       - state
# MAGIC       - customer state
# MAGIC   - name: Billing Country
# MAGIC     expr: customer.billing_country
# MAGIC     display_name: Country
# MAGIC     comment: Customer billing country
# MAGIC     synonyms:
# MAGIC       - country
# MAGIC       - customer country
# MAGIC   - name: Industry
# MAGIC     expr: customer.industry
# MAGIC     display_name: Industry
# MAGIC     comment: Customer industry sector
# MAGIC     synonyms:
# MAGIC       - customer industry
# MAGIC       - sector
# MAGIC measures:
# MAGIC   - name: Transaction Count
# MAGIC     expr: COUNT(1)
# MAGIC     display_name: Transaction Count
# MAGIC     comment: Total number of transactions
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC     synonyms:
# MAGIC       - transactions
# MAGIC       - count
# MAGIC       - order count
# MAGIC   - name: Total Revenue
# MAGIC     expr: SUM(amount)
# MAGIC     display_name: Total Revenue
# MAGIC     comment: Sum of all transaction amounts
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: USD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - revenue
# MAGIC       - sales
# MAGIC       - total sales
# MAGIC   - name: Total Quantity Sold
# MAGIC     expr: SUM(quantity)
# MAGIC     display_name: Total Quantity
# MAGIC     comment: Total number of items sold
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC     synonyms:
# MAGIC       - quantity
# MAGIC       - units sold
# MAGIC   - name: Total Discount
# MAGIC     expr: SUM(discount_amount)
# MAGIC     display_name: Total Discount
# MAGIC     comment: Sum of all discount amounts
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: USD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - discounts
# MAGIC   - name: Average Transaction Value
# MAGIC     expr: SUM(amount) / COUNT(1)
# MAGIC     display_name: Avg Transaction Value
# MAGIC     comment: Average revenue per transaction
# MAGIC     format:
# MAGIC       type: currency
# MAGIC       currency_code: USD
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC     synonyms:
# MAGIC       - avg revenue
# MAGIC       - average order value
# MAGIC   - name: Unique Customers
# MAGIC     expr: COUNT(DISTINCT customer.customer_id)
# MAGIC     display_name: Unique Customers
# MAGIC     comment: Count of distinct customers
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC     synonyms:
# MAGIC       - customer count
# MAGIC       - distinct customers
# MAGIC $$