----------------------------
-- ORDERS SPARK DECLARATIVE PIPELINE
-- Data Quality Expectations
----------------------------

-- 1. Select the 'Run pipeline' button to run the pipeline. While the pipeline is running,  proceed to step 2.


-- 2. While the pipeline is executing explore the second query below and examine the 'CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo3' query. 
-- Notice that it includes 3 data quality expectations applied as data is ingested into the orders_silver_demo3 table.

  -- NOTE: We have added expectations that will perform the warn and drop actions.

  -- a. In notification we expect a 'Y' or an 'x'. (Column only contains Y or N values).

  -- b. In order_timestamp we expect dates after '2021-12-26'. (Column contains dates on 2021-12-25)

  -- c. In customer_id the values can't be null or the pipeline will fail (this will pass in this demonstration)

------------------------------------------------------------------
-- PIPELINE CODE
------------------------------------------------------------------
-- Create the bronze streaming table in your labuser.1_bronze_db schema (database) and ingest the JSON files
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze_demo3
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
    "${source}/orders",  -- Uses the source configuration variable set in the pipeline settings
    format => 'JSON'
);


-- Create the silver streaming table in your labuser.2_silver_db schema (database) with data expectations
CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo3
  (
    -- Check for a 'Y' or 'x' in the notifications column, returns a warning
    CONSTRAINT valid_notifications EXPECT (notifications IN ('Y','x')),
    -- Drop row if not a valid date (set to 2021-12-26)
    CONSTRAINT valid_date EXPECT (order_timestamp > "2021-12-26") ON VIOLATION DROP ROW,
    -- Fail pipeline if null
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications 
FROM STREAM 1_bronze_db.orders_bronze_demo3; 


-- Create the materialized view aggregation from the orders_silver_demo3 with the summarization
CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date_demo3 
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM 2_silver_db.orders_silver_demo3   
GROUP BY date(order_timestamp);
------------------------------------------------------------------

-- 3. After the pipeline completes, explore the 'Pipeline graph' on the right. 
-- Observe that it’s the same pipeline built in the previous demonstration with the single 00.json file. 
-- It creates the orders bronze and silver tables, and the materialized view summarizing the silver streaming table. In the pipeline you should see that:
    -- a. It shows that 174 rows were read into the bronze table as expected 
    -- b. Only 148 rows were read into the silver table (the table with constraints)


-- 4. In the bottom window make sure you are in the 'Tables' tab. 
      -- Then select the 'orders_silver_demo3' table.
      -- Then select 'Table metrics'. 
  -- Note the following:

  -- a. The 'Output records' column shows 148 rows were written to the table. This is the number of rows that passed the drop expectation.

  -- b. The 'Expectations' column shows 3 expectations are set for the streaming table.
  
  -- c. The 'Dropped records' column shows that 26 rows were dropped. This is the number of rows that failed the drop expectation, or 14.9% failed (were dropped). 

  -- d. Select the resulting expectations in the 'Expectations' column. The popup window displays the following:
    -- valid_notifications	ALLOW	  22.4%    39 (warning)
    -- valid_date	          DROP	  14.9%    26 (completely dropped)

  -- e. Note, the constraints will show information for each specific run. To view the overall information you will have to query the event log. We will look at that later.


-- 5. Return back to the notebook '3 - Adding Data Quality Expectations' and close this tab.