-- CREATING TABLES in the dataset

-- Creating Geolocation table 

CREATE TABLE olist_geolocation_dataset(geolocation_zip_code_prefix INT,
									    geolocation_lat	FLOAT,
										geolocation_lng	FLOAT,
										geolocation_city TEXT,
										geolocation_state TEXT); -- Table created

SELECT *
FROM olist_geolocation_dataset; -- Table confirmed

COPY olist_geolocation_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER; -- Files copied from a CSV
-- FILE

SELECT *
FROM olist_geolocation_dataset; -- Contents confirmed


-- Creating Order_review table
CREATE TABLE olist_order_reviews_dataset(review_id TEXT,	
										  order_id TEXT,
										  review_score INT,
										  review_comment_title TEXT,
										  review_comment_message TEXT,
										  review_creation_date TIMESTAMP,
										  review_answer_timestamp TIMESTAMP
);-- Table created

SELECT *
FROM olist_order_reviews_dataset; -- Table confirmed


COPY olist_order_reviews_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER; -- contents copied from 
-- a CSV FILE


-- Creating Products table
CREATE TABLE olist_products_dataset(product_id TEXT PRIMARY KEY,
									 product_category_name TEXT,
									 product_name_lenght INT,
									 product_description_lenght	INT,
									 product_photos_qty	INT,
									 product_weight_g INT,
									 product_length_cm INT,
									 product_height_cm	INT,
									 product_width_cm INT
); -- Table Created

COPY olist_products_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_products_dataset.csv' DELIMITER ',' CSV HEADER; -- contents copied from a 
--CSV FILE

SELECT *
FROM olist_products_dataset; -- Table and its content confirmed


-- Creating Sellers table
CREATE TABLE olist_sellers_dataset(seller_id TEXT PRIMARY KEY,
									seller_zip_code_prefix INT,
									seller_city TEXT,
									seller_state TEXT
); -- Table Created

COPY olist_sellers_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER; -- Contents copied from a CSV
-- FILE

SELECT *
FROM olist_sellers_dataset; -- Table and contents cofirmed

-- Creating Product Category Name Translation Table
CREATE TABLE product_category_name_translation(product_category_name TEXT,
												product_category_name_english TEXT
); -- Table created

COPY product_category_name_translation
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\product_category_name_translation.csv' DELIMITER ',' CSV HEADER; -- contents copied
-- from a CSV FILE

SELECT *
FROM product_category_name_translation; -- Table and its content confirmed
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------

-- DATA CLEANING ----------------------------------------------------------------------------------------------------------------------

-- Checking for missing values 
SELECT                   -- This can be used to check null values but takes much time since this needs to be used for each column in each table.
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_values
FROM olist_customers_dataset;

-- Check for null & duplicate values
-- For better efficiency, only check for null & duplicate values in tables without Unique Identifier(Primary key)

-- Checking for missing values in all columns at once 
DO $$
DECLARE
    col_name TEXT;
    total_rows INT;
    missing_values INT;
BEGIN
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'olist_customers_dataset'
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) AS total_rows, 
                    SUM(CASE WHEN %I IS NULL THEN 1 ELSE 0 END) AS missing_values 
             FROM olist_customers_dataset;',
            col_name
        )
        INTO total_rows, missing_values;

        RAISE NOTICE 'Column: %, Total Rows: %, Missing Values: %', col_name, total_rows, missing_values;
    END LOOP;
END $$; -- We have 0 missing values thanks to the presence of a primary key


-- Checking for Duplicate Values in all columns at once
DO $$
DECLARE
    col_name TEXT;
    duplicate_count INT;
BEGIN
    -- Loop through each column in the specified table
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'olist_customers_dataset' 
    LOOP
        -- Execute dynamic SQL to count duplicates for each column
        EXECUTE format(
            'SELECT COUNT(*) - COUNT(DISTINCT %I) AS duplicate_count
             FROM olist_customers_dataset', 
            col_name
        )
        INTO duplicate_count;

        -- Raise a notice for columns with duplicates
        IF duplicate_count > 0 THEN
            RAISE NOTICE 'Column: %, Duplicates: %', col_name, duplicate_count;
        ELSE
            RAISE NOTICE 'Column: % has no duplicates.', col_name;
        END IF;
    END LOOP;
END $$; -- There are duplicates in columns with location data which is normal and expected since many customer are from the same state, city
-- or even zip code


-- Olist_geolocation_dataset
-- Missing values
DO $$
DECLARE
	col_name TEXT;
	total_rows INT;
	missing_values INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_geolocation_dataset'
	LOOP
		EXECUTE format(
				'SELECT COUNT(*) AS total_rows,
						SUM(CASE WHEN %I IS NULL THEN 1 ELSE 0 END) AS missing_values
				 FROM olist_geolocation_dataset;',
				 col_name
		)
		INTO total_rows, missing_values;

		RAISE NOTICE 'Column: %, Total rows: %, Missing values: %', col_name, total_rows, missing_values;
	END LOOP;
END $$;

-------------------
-- Duplicate Values
DO $$
DECLARE
    col_name TEXT;
    duplicate_count INT;
BEGIN
    -- Loop through each column in the specified table
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'olist_geolocation_dataset' 
    LOOP
        -- Execute dynamic SQL to count duplicates for each column
        EXECUTE format(
            'SELECT COUNT(*) - COUNT(DISTINCT %I) AS duplicate_count
             FROM olist_geolocation_dataset', 
            col_name
        )
        INTO duplicate_count;

        -- Raise a notice for columns with duplicates
        IF duplicate_count > 0 THEN
            RAISE NOTICE 'Column: %, Duplicates: %', col_name, duplicate_count;
        ELSE
            RAISE NOTICE 'Column: % has no duplicates.', col_name;
        END IF;
    END LOOP;
END $$;


-- Olist_order_items_dataset
-- Missing Value

DO $$
DECLARE
	col_name TEXT;
	total_rows INT;
	missing_values INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_order_items_dataset'
	LOOP
		-- Execute a dynamic query to count missing values
		EXECUTE format('SELECT COUNT(*) AS total_rows,
						SUM(CASE WHEN %I IS NULL THEN 1 ELSE 0 END) AS missing_values
						FROM olist_order_items_dataset;',
						col_name
		)
		INTO total_rows, missing_values;
		RAISE NOTICE 'Column: %, Total Rows: %, Missing Values: %', col_name, total_rows, missing_values;
	END LOOP;
END $$;

-- Duplicate count

DO $$
DECLARE
	col_name TEXT;
	duplicate_count INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_order_items_dataset'
	LOOP
		EXECUTE format('SELECT COUNT(*) - COUNT(DISTINCT %I) AS duplicate_count
						FROM olist_order_items_dataset',
						col_name
		)
		INTO duplicate_count;
		IF duplicate_count > 0 THEN
			RAISE NOTICE 'Column: %, Duplicate Count: %', col_name, duplicate_count;
		ELSE
			RAISE NOTICE 'Column: % has no duplicate', col_name;
		END IF;
	END LOOP;
END $$; -- This reveals the count of duplicates in each column

SELECT order_id, COUNT(*) AS row_count
FROM olist_order_items_dataset
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC; -- For further investigation into the duplications.
						  -- This reveals the duplication is due to the nature of the dataset
						  -- One order(order_id) can have more than one item in it, so the 'order_id' is mentioned each time since this table
						  -- focuses on the items in each order

SELECT *
FROM olist_order_items_dataset
WHERE order_id = '8272b63d03f5f79c56e9e4120aec44ef'; -- This revealed for example by looking specifically at one of the order_id with 
													  -- duplicates that it is indeed due to the multiple items within a single order as 
													  -- predicted.
													  
													  -- This is evident in the numbering of the 'order_item_id' which in this example  
													  -- is numbered from 1 to 21, meaning, this order has 21 items in it.

-- Olist Order Payments
-- Missing Values		

DO $$
DECLARE
	col_name TEXT;
	total_rows INT;
	missing_values INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_order_payments_dataset'
	LOOP
		EXECUTE format(' SELECT COUNT(*) AS total_rows,
						SUM(CASE WHEN %I IS NULL THEN 1 ELSE 0 END) AS missing_values
						FROM olist_order_payments_dataset;',
						col_name
		)
		INTO total_rows, missing_values;
		RAISE NOTICE 'Columns: %, Total Rows: %, Missing Values: %', col_name, total_rows, missing_values;
	END LOOP;
END $$;

-- Duplicate Values

DO $$
DECLARE
	col_name TEXT;
	duplicate_count INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_order_payments_dataset'
	LOOP
		EXECUTE format(' SELECT (COUNT(*) - COUNT(DISTINCT %I)) AS duplicate_count
						FROM olist_order_payments_dataset;',
						col_name
		)
		INTO duplicate_count;
		IF duplicate_count > 0 THEN
			RAISE NOTICE 'Column: %, Duplicate Count: %', col_name, duplicate_count;
		ELSE
			RAISE NOTICE 'Column: % has no duplicates', col_name;
		END IF;
	END LOOP;
END $$;

-- The above Dynamic query shows all the column in Order_payments table have duplicates
-- 1. Payment sequential: This is expected since the sequencing is restarted for each order e.g 1,2,3. so there is multiple 1s, 2s etc.
-- 2. Payment type: Since the same types are payment types are used, duplicates are the norms
-- 3. Payment Installments: Relates to how many times a single Order was payed for. 1,2 etc. times. for each order.
-- 4. Payment Value: Since an order can be payed for in installments, payment value is bound to occur more than once for the same order.
-- 5. Order ID: This is a foreign key from Orders table and duplication is allowed coupled with the fact that installmental payments 
-- 	  were made and the same order was payed for multiple times in many occations.

-- Observations on Duplicate Columns in olist_order_payments_dataset
-- payment_sequential:
-- Explanation: This column represents the sequence of payments for each order, starting from 1. As the sequence resets for 

-- payment_type:
-- Explanation: This column indicates the payment method (e.g., credit card, voucher). Since payment methods are reused across multiple orders, duplicates are normal.
-- every order_id, duplicates are expected (e.g., multiple rows with 1, 2, etc., across different orders).

-- Payment_installments:
-- Explanation: This column shows the number of installments for which an order is paid. Repetition of common installment counts 
-- (e.g., 1, 2) is expected due to multiple orders opting for similar installment plans.

-- Payment_value:
-- Explanation: This represents the payment amount in a single transaction. For orders paid in installments, the same payment_value 
-- may appear multiple times for a single order. Additionally, similar amounts might appear across different orders due to price 
-- standardization.

-- Order_id:
-- Explanation: As a foreign key from the orders table, order_id duplication is valid. It occurs because a single order can involve 
-- multiple payment transactions due to installment payments or split payments across different methods.

-- Additional Notes
-- The presence of duplicates across these columns is expected behavior based on the dataset's design.
-- These duplicates reflect the multi-faceted nature of payment processing (e.g., installments, multiple payment methods) and do not indicate data issues.


SELECT *
FROM olist_order_payments_dataset
WHERE order_id = 'fa65dad1b0e818e3ccc5cb0e39231352'
ORDER BY payment_sequential ASC; -- With these query, we can see payment sequential act as a kind of unique identifier for this single
-- order_id. It shows that thought, the order_id is repeated, it is completely normal as it was paid for in 29 sequential order hence
-- the repetition of the same.

------------------------------------------------------------------------------------------------------------------

-- Order Reviews Table

SELECT *
FROM olist_order_reviews_dataset; -- To understand the contents and structure of the table

-- Check for Missing values
 
DO $$
DECLARE
	col_name TEXT;
	total_rows INT;
	missing_values INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_order_reviews_dataset'
	LOOP
		EXECUTE format(' SELECT COUNT(*) AS total_rows,
						SUM(CASE WHEN %I IS NULL THEN 1 ELSE 0 END) AS missing_values
						FROM olist_order_reviews_dataset;',
						col_name
		)
		INTO total_rows, missing_values;
		RAISE NOTICE 'Column: %, Total Rows: %, Missing Values: %', col_name, total_rows, missing_values;
	END LOOP;
END $$;

-- Check for DUPLICATE VALUES

DO $$
DECLARE
	col_name TEXT;
	duplicate_count INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_order_reviews_dataset'
	LOOP
		EXECUTE format(' SELECT (COUNT(*) - COUNT(DISTINCT %I)) AS duplicate_count
						 FROM olist_order_reviews_dataset',
						 col_name
		)
		INTO duplicate_count;
		IF duplicate_count > 0 THEN
			RAISE NOTICE 'Column: %, Duplicate Count: %', col_name, duplicate_count;
		ELSE
			RAISE NOTICE 'Column: % has no duplicates', col_name;
		END IF;
	END LOOP;
END $$;

-- After running the above Dynamic queries, it reavealed 

-- Missing values in the "review message" & "review header" columns
-- which are expected since some customer did not write any review and some wrote review but didn't write review header

-- Duplicate Values
-- All the columns have duplicate values and most or all of them are expected even order_id column since it is a foreign key from 
-- order table.


SELECT order_id, COUNT(review_comment_message) AS "Reviews Counts"
FROM olist_order_reviews_dataset
GROUP BY order_id
ORDER BY "Reviews Counts" DESC; -- To view total review per order-id

SELECT *
FROM olist_order_reviews_dataset
WHERE order_id = '8e17072ec97ce29f0e1f111e598b0c85'; -- Sampling with 1 order_id that has multiple reviews to understand the
														-- nature of the multiple reviews

-- The last Query confirmed that the multiple reviews per order_id weren't duplicates but different reviews for the different
-- items in the same order, for example


"1. Delivered the wrong product."
-- Interpretation:
-- This indicates a basic fulfillment issue where the customer received an item different from what they ordered.
-- Implication: Such errors damage customer trust and satisfaction and may indicate problems in the picking, packaging, or inventory process.

"2. I bought 3 units of the product, but 2 units came that do not match what I bought. Therefore, my opinion is negative regarding this seller because they did not fulfill what was promised in the sale."
-- Interpretation:
-- The customer received fewer items than ordered, and the items delivered were incorrect.
-- Implication: This reflects a combination of quantity discrepancy and incorrect product delivery, suggesting deeper operational or logistical problems.

"3. Although it was delivered on time, they did not send the product I purchased."
-- Interpretation:
-- The delivery was punctual, but the content was incorrect (wrong product).
-- Implication: Timely delivery is overshadowed by the fundamental issue of incorrect fulfillment, showing that accuracy is as critical as speed.

"Common Themes
Incorrect Product Delivery: All three reviews highlight issues with customers receiving products different from what they ordered.
Quantity and Quality Discrepancies: There are mismatches in the ordered vs. received items, both in terms of type and quantity.
Customer Dissatisfaction: The tone of these comments reflects frustration and disappointment with the seller's reliability.
Actionable Insights
Improve Order Accuracy:

Audit the order fulfillment process to reduce errors in picking, packing, and shipping.
Implement quality checks at multiple stages.
Enhance Communication:

Inform customers immediately if an item is unavailable or substitutions are necessary, and seek approval before proceeding.
Customer Recovery Strategy:

Offer refunds, replacements, or discounts to affected customers to mitigate dissatisfaction and retain trust.
Monitor Feedback:

Track similar complaints over time to identify systemic issues and address root causes."

------------------------------------------------------------------------------------------------------------------

-- Orders_dataset

-- Understand the contents & structure of the table

SELECT *
FROM olist_orders_dataset;

-- Check for Missing Values

DO $$
DECLARE
	col_name TEXT;
	total_rows INT;
	missing_values INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_orders_dataset'
	LOOP
		EXECUTE format(' SELECT COUNT(*) AS total_rows,
						  		SUM(CASE WHEN %I IS NULL THEN 1 ELSE 0 END) AS missing_values
						  FROM olist_orders_dataset;',
						  col_name
		)
		INTO total_rows, missing_values;
		RAISE NOTICE 'Column: %, Total Rows: %, Missing Values: %', col_name, total_rows, missing_values;
	END LOOP;
END $$;

SELECT order_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date
FROM olist_orders_dataset
WHERE order_approved_at IS NULL 
  AND order_delivered_carrier_date IS NOT NULL 
  AND order_delivered_customer_date IS NOT NULL; -- There are 14 orders that were not approved, at least not recorded 
  												  -- but they were delivered.
  

SELECT order_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date
FROM olist_orders_dataset
WHERE order_approved_at IS NULL 
  AND order_delivered_carrier_date IS NULL 
  AND order_delivered_customer_date IS NULL; -- Canceled orders. There are 146 canceled orders... These orders were never 
  											  -- approved and never delivered.



SELECT *
FROM olist_orders_dataset
WHERE order_status = 'canceled'; -- Orders marked as canceled

SELECT *
FROM olist_orders_dataset
WHERE order_approved_at IS NOT NULL AND
order_status = 'canceled'; -- Approved orders but canceled

SELECT *
FROM olist_orders_dataset
WHERE order_approved_at IS NOT NULL AND
order_delivered_customer_date IS NULL;

SELECT a.order_id, b.review_comment_title, b.review_comment_message
FROM olist_orders_dataset AS a
LEFT JOIN olist_order_reviews_dataset AS b
ON a.order_id = b.order_id
WHERE a.order_status = 'canceled'
  AND a.order_approved_at IS NOT NULL
  AND a.order_delivered_customer_date IS NOT NULL; 
  -- With this, we discovered orders that were marked as canceled and delivered at the same time 
  -- but NEVER got to the customer as revealed by the CUSTOMER REVIEW DATASET
-- The First 3 reviews were Negative, the first 2 was about customers not getting the order yet
-- it was marked as delivered, the 3rd review was about a disatisfied customer with quality of the
-- Product.
-- The 4th review was positive as the customer affirmed, he would buy again because they were on tim
-- and delivered the right product and quality

-- The 5th review was "Product was not delivered. Invoice was not sent. Complaint phone number doesn't work."
-- 6th review "The product was not shipped because I made a mistake when typing my house number."

-- From these info, we can see of 6 orders that were marked canceled, yet approved and delivered, only 2 were
-- delivered.
												

SELECT order_status, 
       COUNT(*) AS total_orders, 
       SUM(CASE WHEN order_approved_at IS NOT NULL THEN 1 ELSE 0 END) AS approved_orders,
       SUM(CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END) AS delivered_orders
FROM olist_orders_dataset
GROUP BY order_status;


SELECT a.order_id, 
       a.order_status, 
       a.order_purchase_timestamp, 
       a.order_approved_at, 
       a.order_delivered_customer_date, 
       b.review_comment_message,
       CASE 
           WHEN LOWER(b.review_comment_message) LIKE '%não%' OR 
                LOWER(b.review_comment_message) LIKE '%nao%' THEN 'Canceled & Disputed Delivery!'
           ELSE 'Canceled but Delivered'
       END AS delivery_status
FROM olist_orders_dataset AS a
LEFT JOIN olist_order_reviews_dataset AS b
ON a.order_id = b.order_id
WHERE a.order_status = 'canceled'
  AND a.order_approved_at IS NOT NULL
  AND a.order_delivered_customer_date IS NOT NULL; -- This returns all canceled orders that were also marked as approved and delivered
  													-- It flags orders that were canceled but also tagged delivered and importantly,
													-- It flags orders that were marked canceled but really was delivered


SELECT *
FROM (SELECT *,
       CASE 
           WHEN order_status = 'canceled' 
                AND order_delivered_customer_date IS NOT NULL 
                AND order_id IN (
                   SELECT a.order_id
                   FROM olist_orders_dataset AS a
                   LEFT JOIN olist_order_reviews_dataset AS b
                   ON a.order_id = b.order_id
                   WHERE LOWER(b.review_comment_message) LIKE '%não%'
                      OR LOWER(b.review_comment_message) LIKE '%nao%'
                ) THEN 'Canceled but Disputed Delivery'
           WHEN order_status = 'canceled' 
                AND order_delivered_customer_date IS NOT NULL THEN 'Canceled but Delivered'
           ELSE order_status
       END AS corrected_status
FROM olist_orders_dataset)
WHERE order_status = 'canceled' AND
order_delivered_customer_date IS NOT NULL;


-- Check for duplicates

DO $$
DECLARE
	col_name TEXT;
	duplicate_count INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'olist_orders_dataset'
	LOOP
		EXECUTE format(' SELECT (COUNT(*) - COUNT(DISTINCT %I)) AS duplicate_count
						 FROM olist_orders_dataset',
						 col_name
		)
		INTO duplicate_count;
		IF duplicate_count > 0 THEN
			RAISE NOTICE 'Column: %, Duplicate Count: %', col_name, duplicate_count;
		ELSE
			RAISE NOTICE 'Column: % has no duplicates', col_name;
		END IF;
	END LOOP;
END $$;

-- The above dynamic query revealed there are no duplications that mean data integrity issue.
-- customer_id has no duplicates
-- order_id has no duplicates
-- Other columns are expected to have duplicate due to the Business structure and nature of the table. for example
-- Delivery date: The same day will record many deliveries, making the entry seem like duplications when it actually just
-- means more than 1 order was delivered in the same day

-----------------------------------------------------------

-- Product Table

-- Understand the Content and structure of the table

SELECT *
FROM olist_products_dataset;

SELECT COUNT(product_id), COUNT(DISTINCT product_id)
FROM olist_products_dataset; -- Check for Unique identifier (Primary Key). This shows the table CAN NOT have duplicates

-------------------------------------------------------------

-- Sellers Table

-- Understand the Content and structure of the table

SELECT *
FROM olist_Sellers_dataset;

SELECT COUNT(Seller_id), COUNT(DISTINCT Seller_id)
FROM olist_Sellers_dataset; -- Check for Unique identifier (Primary Key). This shows the table CAN NOT have duplicates


--------------------------------------------------------------

-- Category Name Translation Table

-- Understand the Content and structure of the table

SELECT *
FROM product_category_name_translation;

SELECT COUNT(Seller_id), COUNT(DISTINCT Seller_id)
FROM olist_Sellers_dataset; -- Check for Unique identifier (Primary Key). This shows the table CAN NOT have duplicates

SELECT *
FROM product_category_name_translation
WHERE Product_category_name = null; --Checked for null or missing values in the first of 2 columns

SELECT *
FROM product_category_name_translation
WHERE Product_category_name_english = null; --Checked for null or missing values in the second of 2 columns

-- There is no null or missing values in this dataset

-- Check for Duplicates

DO $$
DECLARE
	col_name TEXT;
	duplicate_count INT;
BEGIN
	FOR col_name IN
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = 'product_category_name_translation'
	LOOP
		EXECUTE format(' SELECT (COUNT(*) - COUNT(DISTINCT %I)) AS duplicate_count
						 FROM product_category_name_translation',
						 col_name
		)
		INTO duplicate_count;
		IF duplicate_count > 0 THEN
			RAISE NOTICE 'Column: %, Duplicate Count: %', col_name, duplicate_count;
		ELSE
			RAISE NOTICE 'Column: % has no duplicates', col_name;
		END IF;
	END LOOP;
END $$;

-- The above Dynamic query confirms there is no duplicates in this table.
-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------


SELECT *
FROM olist_order_payments_dataset;


----------------------

WITH item_count_summary AS (
    SELECT 
        COUNT(order_id) AS order_count,
        item_count
    FROM (
        SELECT 
            order_id, 
            COUNT(order_item_id) AS item_count
        FROM olist_order_items_dataset
        GROUP BY order_id
    ) AS order_counts
    GROUP BY item_count
)
SELECT 
    item_count, 
    order_count, 
    ROUND(100.0 * order_count / SUM(order_count) OVER (), 2) AS percentage_of_orders
FROM item_count_summary
ORDER BY item_count ASC;


----------------------------------------------------------------------------------------------------------------------------------------

-- LET'S DELVE INTO THE ANALYSIS

-- REVENUE ANALYSIS


-- 	Overall Revenue
-- 		Total Revenue
SELECT TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS "Total Revenue"
FROM olist_order_payments_dataset; -- Total Revenue - $16M

-- AVERAGE ORDER VALUE
SELECT TO_CHAR(SUM(payment_value) / COUNT(order_id), '$FM999,999,999.00') AS "Average Order Value"
FROM olist_order_payments_dataset; -- AOV is $154.01

-- 	Canceled Orders
SELECT TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS "To Be Refunded"
FROM olist_order_payments_dataset AS p
JOIN olist_orders_dataset AS o
ON p.order_id = o.order_id
WHERE order_status = 'canceled'; -- Revenue lost to cancelation is $143k

-- INSIGHTS
-- Total Revenue is 16M
-- Average Order Value is $154.10
-- Lost revenue to Canceled orders is $143k

--------------------------------------------------------------------------------------------------------------------------------------------------

-- 		Monthly/Yearly Revenue
SELECT 
    DATE_PART('year', order_purchase_timestamp) AS "Year",
	TO_CHAR(order_purchase_timestamp, 'Mon') AS "Month Name",
    DATE_PART('month', order_purchase_timestamp) AS "Month No",
    TO_CHAR(SUM(payment_value), 'FM$999,999,999.00') AS "Total Monthly Revenue"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY "Year", "Month No", "Month Name"
ORDER BY "Year"; -- This query returns Monthly Revenue, but it also uncovered that Nov 2016 has no recorded sales in the dataset
						  -- mostly due to the absense of sales, 2016 was by far the worst year for the business in revenue.

SELECT *
FROM olist_orders_dataset
WHERE DATE_PART('year', order_purchase_timestamp) = 2016
  AND DATE_PART('month', order_purchase_timestamp) = 11; -- To further Investigate the above revelation, this query is used to confirm whether or not
  														  -- there are values for November, 2016.
															
														  -- INSIGHTS
														  -- The query reveals that there is no values for Nov, 2016.
														  -- This dataset covers transaction starting from SEPT, 2016 so missing values for Nov 2016
														  -- is an anomaly.

														  -- RECOMMENDATION
														  -- The missing month is an anomaly that should be investigated immediately.
														  -- Before settling for the above conclusion, it's more reasonable to look at this through the whole
														  -- revenue for 2016 which is $59k compared to millions in revenue in the subsequent years
														  -- With this, it is safe to say Nov doesn't have a missing value, it just doesn't have any values.

-- Handling the Missing Values																		 

WITH calendar AS (
    SELECT generate_series(DATE '2016-09-01', DATE '2018-10-17', '1 month') AS month_start
)
SELECT 
    EXTRACT(YEAR FROM c.month_start) AS "Year",
    TO_CHAR(c.month_start, 'Mon') AS "Month Name",
    EXTRACT(MONTH FROM c.month_start) AS "Month No",
    COALESCE(SUM(p.payment_value), 0) AS "Total Monthly Revenue",
	ROUND(100.0 * COALESCE(SUM(p.payment_value), 0) / SUM(COALESCE(SUM(p.payment_value), 0)) OVER(), 4) "% Revenue"
FROM calendar AS c
LEFT JOIN olist_orders_dataset AS o
    ON DATE_TRUNC('month', o.order_purchase_timestamp) = c.month_start
LEFT JOIN olist_order_payments_dataset AS p
    ON o.order_id = p.order_id
GROUP BY "Year", "Month No", "Month Name"
ORDER BY "% Revenue" DESC; -- Continuing with the analysis, the missing values for Nov, 2016 has been changed to zero to ensure the month is 
						  -- included. Another observation is the next month also has incredibly low sales at $19.62.
						  -- Nov 2017 saw the highest revenue with Sales at 1.194M(7.46%) ot total sales, it is followed closely by April and
							-- March 2018 with sales at 1.160M(7.25%) and 1.159M(7.24%).



-- 		Yearly Revenue
SELECT DATE_PART('year', o.order_purchase_timestamp) AS "Year",
	   TO_CHAR(SUM(p.payment_value), '$FM999,999,999.00') AS "Revenue",
	   ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER(), 2) AS "% Revenue"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY "Year"
ORDER BY "% Revenue" DESC; -- 2018 is the best year even with incomplete records (The record for 2018 ends in Oct) with total revenue at 
							-- $8.7M(54.34%) of total revenue
	   
-- INSIGHTS
-- 2018 saw the highest Sales revenue at 54.34%($8.74M) of total revenue
-- Nov 2017 saw the highest revenue with Sales at 1.194M(7.46%) ot total sales, it is followed closely by April and
-- March 2018 with sales at 1.160M(7.25%) and 1.159M(7.24%.
-- There is no values or entries for Nov 2016, this can be due to little to no transaction in 2016 as a whole, but it's worth looking into.
--------------------------------------------------------------------------------------------------------------------------------------------------

-- Top Performing Payment Type by Total Revenue Generated

SELECT 
    payment_type AS "Payment Type", 
    TO_CHAR(COUNT(order_id), 'FM999,999,999') AS "Total Orders", 
    TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS "Total Revenue",
    ROUND(100.0 * COUNT(order_id) / SUM(COUNT(order_id)) OVER (), 2) AS "Payment Count (%)",
    ROUND(100.0 * SUM(payment_value) / SUM(SUM(payment_value)) OVER (), 2) AS "Revenue (%)"
FROM olist_order_payments_dataset
WHERE payment_type <> 'not_defined'
GROUP BY payment_type
ORDER BY "Revenue (%)" DESC; -- Most customers paid using credit card with total transactions at a staggering 73.92%(76,795) and 
							  -- Revenue at 78.34%(12.5M)


-- Canceled Orders

SELECT 
    REPLACE(a.payment_type, '_', ' ') AS "Payment Type",
    TO_CHAR(COUNT(a.order_id), 'FM999,999,999') AS "Total Orders",
    SUM(CASE WHEN b.order_status = 'canceled' THEN 1 ELSE 0 END) AS "Canceled Orders", 
    SUM(CASE WHEN b.order_status = 'canceled' THEN payment_value ELSE 0 END) AS "Revenue",
    ROUND(100.0 * SUM(CASE WHEN b.order_status = 'canceled' THEN 1 ELSE 0 END) / COUNT(b.order_id), 2) AS "Cancelation (%)"
FROM olist_order_payments_dataset AS a
JOIN olist_orders_dataset AS b
ON a.order_id = b.order_id
GROUP BY a.payment_type
HAVING payment_type <> 'not_defined'
ORDER BY "Cancelation (%)" DESC; -- Credit card has most cancelation at 444 cancelations but Voucher has most cancelation rate at approximately
								  -- 2% of total payment by that means.


-- Freight's Contribution to Revenue
SELECT TO_CHAR((SELECT SUM(payment_value) FROM olist_order_payments_dataset), '$FM999,999,999.00') AS "Total Revenue",
       TO_CHAR(SUM(freight_value), '$FM999,999,999.00') AS "Total Freight Value",
       ROUND(100.0 * SUM(freight_value) / (SELECT SUM(payment_value) FROM olist_order_payments_dataset), 2) AS "Freight as % of Revenue"
FROM olist_order_items_dataset; -- This query efficiently returns the desired result by avoiding unecessary JOINS that may cause duplication 
								 -- and take more time to run.

								 -- INSIGHTS
								 -- Freight contributed 14% to total revenue at $2.252M of $16M total revenue.



		-- Top 5 States by Revenue Performance
SELECT a.customer_state, COUNT(DISTINCT b.customer_id) AS "Total Customers", TO_CHAR(SUM(b.payment_value), '$FM999,999,999.00') AS "Total Revenue",
	   ROUND(100 * SUM(b.payment_value)/SUM(SUM(b.payment_value)) OVER(), 2) AS "% Revenue",
	   ROUND(100 * COUNT(DISTINCT b.customer_id)/SUM(COUNT(DISTINCT b.customer_id)) OVER(), 2) AS "% Customer"
FROM olist_customers_dataset AS a
JOIN (SELECT b.customer_id, a.payment_value
	  FROM olist_order_payments_dataset AS a
	  JOIN olist_orders_dataset AS b
	  ON a.order_id = b.order_id
	  ) AS b
ON a.customer_id = b.customer_id
GROUP BY a.customer_state
ORDER BY "% Revenue" DESC
LIMIT 5; -- Sao Paolo raked in more revenue at approximately $6M contributing 37.47% of total revenue
		  -- This query went further to put the number of customers in each state into perspective to allow for better understanding  
		  -- as to the reason for the success or failure of each state.

		  -- INSIGHT
		  -- Sao Paolo raked in more revenue at approximately $6M contributing 37.47% of total revenue
		  -- At least for the top 10 states by revenue performance, states with higher number of customers did better
		  -- 42% of total customers are from Sao Paolo, that is what makes Sao Paolo by far the top state by Revenue generated.



		-- Top 5 Cities by Revenue Performance
SELECT a.customer_city AS "Customer City", TO_CHAR(SUM(b.payment_value), 'FM999,999,999.00') AS "Total Revenue",
	   ROUND(100 * SUM(b.payment_value) / SUM(SUM(b.payment_value)) OVER(), 2) AS "% Revenue"
FROM olist_customers_dataset AS a
JOIN (SELECT b.customer_id, a.payment_value
	  FROM olist_order_payments_dataset AS a
	  JOIN olist_orders_dataset AS b
	  ON a.order_id = b.order_id
	  ) AS b
ON a.customer_id = b.customer_id
GROUP BY a.customer_city
ORDER BY "% Revenue" DESC
LIMIT 5; -- 13.76%(2.2M) of total revenue came from Sao Polo followed by Rio De Janeiro at 7.26%(1.162M)


-- INSIGHTS
-- 1. Most customers paid using credit card with total transactions at a staggering 73.92%(76,795) and revenue at 78.34%(12.5M).
-- 2. Credit card has most cancelation at 444 cancelations but Voucher has most cancelation rate at approximately 2% of total
-- payment by that means.
-- 3. Freight contributed 14% to total revenue at $2.252M of $16M total revenue.
-- 4. Sao Paolo raked in more revenue at approximately $6M contributing 37.47% of total revenue
-- 5. At least for the top 5 states by revenue performance, states with higher number of customers did better
-- 6. 42% of total customers are from Sao Paolo, that is what makes Sao Paolo by far the top state by Revenue generated.
-- 7. 13.76%(2.2M) of total revenue came from Sao Polo followed by Rio De Janeiro at 7.26%(1.162M)
---------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------

-- SALES AND PRODUCT PERFORMANCE

-- Total Product Sold
SELECT TO_CHAR(COUNT(product_id), 'FM999,999,999') AS "Total Product Type Sold"
FROM olist_products_dataset; -- Total products sold is 32,951.

-- Total Units Sold
SELECT TO_CHAR(COUNT(order_item_id), 'FM999,999,999') AS "Total Units Sold"
FROM olist_order_items_dataset; -- Total units sold is 112,650

-- Top 5 Performing Product

WITH OrderPayments AS (SELECT order_id, SUM(payment_value) AS total_payment
    				   FROM olist_order_payments_dataset
    					GROUP BY order_id),
OrderItemProducts AS (SELECT order_id, product_id, COUNT(order_item_id) AS units_sold
    				  FROM olist_order_items_dataset 
    				  GROUP BY order_id, product_id)
SELECT
    oip.product_id AS "Product id",
    TO_CHAR(SUM(oip.units_sold), 'FM999,999,999') AS "Total Units Sold",
    TO_CHAR(SUM(op.total_payment), 'FM$999,999,999.00') AS "Total Revenue",
	ROUND(100 * SUM(oip.units_sold)/SUM(SUM(oip.units_sold)) OVER(), 2) AS "Units Sold %",
	ROUND(100 * SUM(op.total_payment)/SUM(SUM(op.total_payment)) OVER(), 2) AS "Revenue %"
FROM OrderItemProducts AS oip
JOIN OrderPayments AS op ON oip.order_id = op.order_id
GROUP BY oip.product_id
ORDER BY SUM(op.total_payment) DESC
LIMIT 5; -- This shows Total revenue per product and Total units sold
  		  -- This query reveals there are different types of products with prices higher than each other.
		  -- The top product is "bb50f2e236e5eea0100680137654686c" with only 195 units totaling $68k(0.40% of total Revenue) while the  
		  -- second top performing product sold 343 units yet made $63k

-- Top performing Product Category 

WITH OrderPayments AS (SELECT order_id, SUM(payment_value) AS total_payment
    				   FROM olist_order_payments_dataset
    				   GROUP BY order_id),
OrderItemCategories AS (SELECT oi.order_id, p.product_category_name, COUNT(oi.order_item_id) AS units_sold
    					FROM olist_order_items_dataset oi
    					JOIN olist_products_dataset p ON oi.product_id = p.product_id
    					GROUP BY oi.order_id, p.product_category_name)
SELECT
    oic.product_category_name AS "Product Category",
    TO_CHAR(SUM(oic.units_sold), 'FM999,999,999') AS "Total Units Sold",
    TO_CHAR(SUM(op.total_payment), '$FM999,999,999.00') AS Total_Revenue,
	ROUND(100.0 * SUM(oic.units_sold)/SUM(SUM(oic.units_sold)) OVER(), 2) AS "Units Sold %",
	ROUND(100.0 * SUM(op.total_payment)/SUM(SUM(op.total_payment)) OVER(), 2) AS "Total Revenue %"
FROM OrderItemCategories oic
LEFT JOIN OrderPayments op ON oic.order_id = op.order_id
GROUP BY oic.product_category_name
ORDER BY SUM(op.total_payment) DESC
LIMIT 5;

-- INSIGHTS
-- There are 74 product categories
-- Total unique products Sold is 32,951
-- Total Units of all products sold is 112,650
-- This analysis reveals there are different types of products with prices higher than each other.

-- The top product is "bb50f2e236e5eea0100680137654686c" with only 195 units totaling $68k while the second top 
-- performing product sold 343 units yet made $63k

-- Beleza saude is the top product category, raking in total revenue of $1.48M (9.02% of Total Revenue) 
-- It is followed closely by Relogios Presentes at $1.31M (8.16% of Total Revenue)
---------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------

-- CUSTOMER ANALYSIS

--	Total Customers
SELECT TO_CHAR(COUNT(DISTINCT customer_id), 'FM999,999,999') AS "Total Customers"
FROM olist_customers_dataset; -- This business has a total of 99,441 customers

-- Average Order Value by Customer
SELECT TO_CHAR(SUM(op.payment_value) / COUNT(c.customer_id), '$FM999,999,999.0') AS "AOV per Customer"
FROM olist_customers_dataset AS c
JOIN olist_orders_dataset AS o
ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset AS op
ON o.order_id = op.order_id; -- Average Order Value Per Customer is $154.1 


-- Repeat customers
SELECT customer_id, COUNT(order_id) AS total_orders
FROM olist_orders_dataset
GROUP BY customer_id
HAVING COUNT(order_id) > 1
ORDER BY total_orders DESC; 
-- Repeat purchase
-- This revealed this business has no customer retention
							 
-- Possible Reasons
-- 1. This might be due to the possibility that the dataset is a snapshot or represents one-off transactions, 
-- this might reflect reality
							 
-- 2. Olist might target new customers primarily or not record repeat purchases under the same customer_id.

-- 3. The customer_id field may be inconsistent or mismanaged (e.g., different customer_id values for the same individual).

-- INSIGHTS
-- 1. This reveals high customer acquisition but low retention.
-- 2. Possible need to focus on customer retention.
---------------------------------------------------------------------------------------------------------------------------------------------------------

-- Customer Distribution by State (Top 5)
SELECT customer_state, TO_CHAR(COUNT(DISTINCT customer_id), '999,999,999') AS "Customers",
	   ROUND(100.0 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER(), 2) AS "% Contribution"
FROM olist_customers_dataset
GROUP BY customer_state
ORDER BY "Customers" DESC
LIMIT 5; -- SP leads the way as top state with customer distribution at 41.98%(41,746) of total customers

-- Customer Distribution by City (Top 5)
SELECT customer_city, TO_CHAR(COUNT(DISTINCT customer_id), '999,999,999') AS "Customers",
	   ROUND(100.0 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER(), 2) AS "% Contribution"
FROM olist_customers_dataset
GROUP BY customer_city
ORDER BY "Customers" DESC
LIMIT 5; -- Sao Paulo leads the way as top city with customer distribution at 15.63%(15,540) of total customers
---------------------------------------------------------------------------------------------------------------------------------------------
			
-- 		Monthly Customer Aquisition
SELECT 
    DATE_PART('year', order_purchase_timestamp) AS year,
	TO_CHAR(order_purchase_timestamp, 'Mon') AS month_name,
    DATE_PART('month', order_purchase_timestamp) AS month_no,
    TO_CHAR(COUNT(customer_id), 'FM999,999,999') AS New_Customers,
	ROUND(100.0 * COUNT(customer_id) / SUM(COUNT(customer_id)) OVER(), 2) AS "New Customer %"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY year, month_no, month_name
ORDER BY "New Customer %" DESC; 
-- Nov 2017 saw new customers totaling 7,863 followed closely by Jan & March 2018 at 7,563 & 7,512 customers
								


-- 		Yearly Customer Aquisition
SELECT DATE_PART('year', o.order_purchase_timestamp) AS Year,
	   TO_CHAR(COUNT(o.customer_id), 'FM999,999,999') AS "New Customers",
	   ROUND(100.0 * COUNT(o.customer_id) / SUM(COUNT(o.customer_id)) OVER(), 2) AS "% Customer Aquisition"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY Year
ORDER BY "% Customer Aquisition" DESC; 
-- 2018 is the best year even with incomplete records (The record for 2018 ends in Oct) with total acquired 
									    -- at 53.92%(56,015) of total customers over 3 years

-- City Count by State
SELECT customer_state, COUNT(DISTINCT customer_city) AS "Cities",
	   ROUND(100.0 * COUNT(DISTINCT customer_city) / SUM(COUNT(DISTINCT customer_city)) OVER(), 2) AS "% Contribution"
FROM olist_customers_dataset
GROUP BY customer_state
ORDER BY "Cities" DESC; 

-- INSIGHTS
-- This business has a total of 99,441 customers
-- Average Order Value Per Customer is $154.1
-- This business has no customer retention		 
-- Possible Reasons
	-- 1. This might be due to the possibility that the dataset is a snapshot or represents one-off transactions, 
	-- this might reflect reality
							 
	-- 2. Olist might target new customers primarily or not record repeat purchases under the same customer_id.

	-- 3. The customer_id field may be inconsistent or mismanaged (e.g., different customer_id values for the same 
	-- individual).

-- MORE INSIGHTS
-- 1. This reveals high customer acquisition but low retention.
-- 2. Possible need to focus on customer retention.

-- SP leads the way as top state with customer distribution at 41.98%(41,746) of total customers
-- Sao Paulo leads the way as top city with customer distribution at 15.63%(15,540) of total customers

---------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------

-- OPERATIONAL PERFORMANCE ANALYSIS

-- Order Status Distribution
SELECT order_status, TO_CHAR(COUNT(order_status), 'FM999,999,999') AS Order_Status_Distribution,
	   ROUND(100.0 * COUNT(order_status) / SUM(COUNT(order_status)) OVER(), 2) AS "% Distribution"
FROM olist_orders_dataset
GROUP BY order_status
ORDER BY Order_Status_Distribution DESC; -- This shows a success rate of 97% in terms of delivered orders which is very 
-- good

---------------------------------------------------------------------------------------------------
-- Let's point our attention towards Canceled Orders
-- There are 625 canceled orders
-- 550 of the canceled Orders are indeed canceled and treated appropriately
-- 69 of the canceled orders are canceled but Carrier claimed to have delivered the order to the customer
-- The remaining 6 orders were marked as delivered at the same time as canceled
-- Of the 6 orders marked as delivered to customer at the same time as marked as canceled, only 2 were actually delivered 
-- as confirmed by customer review, 1 of the delivered order was a wrong product.
-- 1 of the 3 undelivered orders was due to customer's mistake as written by the customer in the review.
---------------------------------------------------------------------------------------------------

SELECT *
FROM olist_orders_dataset
WHERE order_status = 'canceled'
AND(order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NULL); -- This reveals total orders truly canceled and treated
																					   -- appropriately (550 orders)

SELECT *
FROM olist_orders_dataset
WHERE order_status = 'canceled'
AND (order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date IS NULL); -- This reveals total orders marked as canceled but 
																							   -- have disputes i.e, Carrier claimed to deliver, 
																							   -- Customer claimed not to have received the order
																							   -- (69)
																							  
SELECT *
FROM olist_orders_dataset
WHERE order_status = 'canceled'
AND (order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date IS NOT NULL); -- This reveals total orders marked as canceled but 
																							   -- but claimed to have been delivered to the customer 
																							   -- (6)
																							   -- 


SELECT 
    CASE 
        WHEN order_status = 'canceled' 
             AND order_delivered_carrier_date IS NOT NULL 
             AND order_delivered_customer_date IS NOT NULL THEN 'Canceled but Delivered'
        WHEN order_status = 'canceled' 
             AND order_delivered_carrier_date IS NOT NULL 
             AND order_delivered_customer_date IS NULL THEN 'Canceled but Carrier Only Delivered'
        WHEN order_status = 'canceled' THEN 'Other Canceled Cases'
        ELSE 'Non-Canceled Orders'
    END AS "Category",
    COUNT(*) AS "Order Count"
FROM olist_orders_dataset
WHERE order_status = 'canceled'
  AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL)
GROUP BY "Category"
ORDER BY "Order Count" DESC; -- Categorized Disputed orders into 2 headers. 1. Canceled but Carrier Claimed Delivery (69) and 
							  -- 2. Canceled but Claimed to have been delivered to the customer with the customer's confirmation 

SELECT 
    order_id,
    order_status,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    CASE 
        WHEN order_status = 'canceled' 
             AND order_delivered_carrier_date IS NOT NULL 
             AND order_delivered_customer_date IS NOT NULL THEN 'Canceled but Delivered'
        WHEN order_status = 'canceled' 
             AND order_delivered_carrier_date IS NOT NULL 
             AND order_delivered_customer_date IS NULL THEN 'Canceled but Carrier Only Delivered'
        WHEN order_status = 'canceled' THEN 'Other Canceled Cases'
        ELSE 'Non-Canceled Orders'
    END AS "Category"
FROM olist_orders_dataset
WHERE order_status = 'canceled'
  AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL); 
  -- For further analysis into claimed delivery to customer


SELECT *
FROM olist_order_reviews_dataset
WHERE order_id IN ('1950d777989f6a877539f53795b4c3c3', 'dabf2b0e35b423f94618bf965fcb7514', '770d331c84e5b214bd9dc70a10b829d0',
				   '8beb59392e21af5eb9547ae1a9938d06', '65d1e226dfaeb8cdc42f665422522d14', '2c45c33d2f9cb8ff8b1c86cc28c11c30'); 
-- picked out the order_id of the 6 orders to be further
-- investigated
-- When checked against customer reviews, It is revealed
-- that only 2 of the 6 canceled orders claimed to have
-- been delivered to the customer were actually delivered
-- of the 2 delivered order, 1 was a wrong delivery

-- One of the failed delivery was due to the customer's mistake

																							  
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
-- SENTIMENT ANALYSIS

-- Sentiment Analysis By State
SELECT  c.customer_state,
 		SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) AS "Negative Reviews",
		SUM(CASE WHEN r.review_score = 3 THEN 1 ELSE 0 END) AS "Neutral Reviews",
		SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) AS "Positive Reviews",
		COUNT(r.*) AS "Ratings Count",
		ROUND(100.0 * SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) / SUM(COUNT(r.*)) OVER (PARTITION BY c.customer_state), 2) AS "Negative Ratings Per State (%)",
		ROUND(100.0 * SUM(CASE WHEN r.review_score = 3 THEN 1 ELSE 0 END) / SUM(COUNT(r.*)) OVER (PARTITION BY c.customer_state), 2) AS "Neutral Ratings Per State (%)",
		ROUND(100.0 * SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) / SUM(COUNT(r.*)) OVER (PARTITION BY c.customer_state), 2) AS "Positive Ratings Per State (%)",
		ROUND(100.0 * COUNT(r.*) / SUM(COUNT(r.*)) OVER (), 2) AS "Ratings as a Percent of Total"
FROM olist_order_reviews_dataset AS r
JOIN olist_orders_dataset AS o
ON r.order_id = o.order_id
JOIN olist_customers_dataset AS c
ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY "Positive Reviews" DESC; 
-- SP leads the way in the 3 category of ratings holding 80%(33,126) of positive review of a total review for same state at 41,690
-- It is followed but not closely by RJ which has a lot of negative rating 20.7%(2,647) of total rating of 12,765 
-- compared to its total reviews
-- AL is the top state with negative reviews with negative review getting as high as approximately 24% of total review for the same state

-- Ratings is better considered on the bases of kind of rating as a percentage of total rating from the same state instead of just running 
-- this as a percentage of total which wouldn't really show accurate picture of the contribution of each state.

-- Sentiment Analysis By City
SELECT  c.customer_city AS "Customer City",
 		TO_CHAR(SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END), 'FM999,999,999') AS "Negative Reviews",
		TO_CHAR(SUM(CASE WHEN r.review_score = 3 THEN 1 ELSE 0 END), 'FM999,999,999') AS "Neutral Reviews",
		TO_CHAR(SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END), 'FM999,999,999') AS "Positive Reviews",
		TO_CHAR(COUNT(r.*), 'FM999,999,999') AS "Ratings Count",
		ROUND(100.0 * SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) / SUM(COUNT(r.*)) OVER (PARTITION BY c.customer_city), 2) AS "Negative Ratings Per State (%)",
		ROUND(100.0 * SUM(CASE WHEN r.review_score = 3 THEN 1 ELSE 0 END) / SUM(COUNT(r.*)) OVER (PARTITION BY c.customer_city), 2) AS "Neutral Ratings Per State (%)",
		ROUND(100.0 * SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) / SUM(COUNT(r.*)) OVER (PARTITION BY c.customer_city), 2) AS "Positive Ratings Per State (%)",
		ROUND(100.0 * COUNT(r.*) / SUM(COUNT(r.*)) OVER (), 2) AS "Ratings as a Percent of Total"
FROM olist_order_reviews_dataset AS r
JOIN olist_orders_dataset AS o
ON r.order_id = o.order_id
JOIN olist_customers_dataset AS c
ON o.customer_id = c.customer_id
GROUP BY c.customer_city
ORDER BY COUNT(r.*) DESC;
-- Same goes for Sao Paolo city. It by far dominates the cities by any kind of review since it housed more customers and sold more units
-- It is followed very closely and impresively by Rio de Janeiro which although has just 6,841 reviews managed to rake in 4,930 positive 
-- reviews.
---------------------------------------------------------------------------------------------------------------------------------------------


-- Product Sentiment Analysis

-- Product segmentation by review rating
SELECT REPLACE(p.product_category_name, '_', ' ') AS "Product Category name",
       ROUND(100.0 * SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) / COUNT(o.order_item_id), 2) AS "Positive Review %",
	   ROUND(100.0 * SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) / COUNT(o.order_item_id), 2) AS "Negative Review %",
       TO_CHAR(COUNT(o.order_item_id), 'FM999,999,999') AS "Total Units Sold",
       TO_CHAR(SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END), 'FM999,999,999') AS "Positive Reviews",
	   TO_CHAR(SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END), 'FM999,999,999') AS "Negative Reviews"
FROM olist_order_items_dataset AS o
JOIN olist_order_reviews_dataset AS r
	ON r.order_id = o.order_id
JOIN olist_products_dataset AS p
    ON o.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY COUNT(o.order_item_id) DESC;
-- Cama Mesa Banho sold most units although it came third in revenue generated. It also has 71%(7,916) of its review positive.
-- CDS DVDS MUSICALS has the highest positive review percentage at 92.86% with NO negative review but this product category and the others 
-- that rank close to it all sold very little units

-- Understandbly, Cama Mesa Banho has the most negative reviews at approximately 19%(2,112) of its total reviews.

-- Review ratings have been segmented into 3 groups 'Negative (1,2)', 'Neutral (3)'
-- 'Positive (4,5)'.

-- Cama Mesa Banho is the product with most negative reviews at a total of 2112 negative reviews
-- Cama Mesa Banho is also the top product by neutral reviews at 1109 reviews
-- Cama Mesa Banho is also the top product by positive reviews at 7916 reviews

-- As this query reveals, Cama Mesa Banho sold most unit, hence has many reactions from customer's falling into positive or negative 
-- rating category

-- This is not to say improvements should not be made to reduce the number of complaints.
---------------------------------------------------------------------------------------------------------------------------------------------

-- DELIVERY DAYS ANALYSIS

-- Average Delivery Time by State
SELECT b.customer_state AS "Customer State", a.order_status AS "Order Status",
       ROUND(AVG(order_delivered_customer_date::DATE - order_purchase_timestamp::DATE), 0) AS "Avg Delivery Days"
FROM olist_orders_dataset AS a
JOIN olist_customers_dataset AS b
ON a.customer_id = b.customer_id
WHERE order_status = 'delivered'
GROUP BY b.customer_state, a.order_status
ORDER BY "Avg Delivery Days" ASC
LIMIT 5; 
-- Most top state by revenue have average delivery days less than 2 weeks or a day above 2 weeks.
-- The fastest state in this regard is SP with an avg delivery days of approximately 9 and this explains why it accounts for lots of
-- revenue too.

-- Average Delivery Time by City
SELECT b.customer_city AS "Customer City", a.order_status AS "Order Status",
       ROUND(AVG(order_delivered_customer_date::DATE - order_purchase_timestamp::DATE), 0) AS "Avg Delivery Days"
FROM olist_orders_dataset AS a
JOIN olist_customers_dataset AS b
ON a.customer_id = b.customer_id
WHERE order_status = 'delivered'
GROUP BY b.customer_city, a.order_status
ORDER BY "Avg Delivery Days" ASC
LIMIT 5;


-- Delivery Time by Product Category
SELECT REPLACE(oip.product_category_name, '_', ' ') AS "Product Category Name", 
	   ROUND(AVG(o.order_delivered_customer_date::DATE - o.order_purchase_timestamp::DATE), 0) AS "Avg Delivery Days",
	   TO_CHAR(SUM(op.payment_value), '$FM999,999,999.0') AS "Category Revenue"
FROM (SELECT oi.order_id, p.product_category_name
		FROM olist_order_items_dataset AS oi
		JOIN olist_products_dataset AS p
		ON oi.product_id = p.product_id
		GROUP BY oi.order_id, p.product_category_name) AS oip
JOIN olist_orders_dataset AS o
	 ON oip.order_id = o.order_id
JOIN (SELECT order_id, SUM(payment_value) AS payment_value 
		FROM olist_order_payments_dataset 
		GROUP BY order_id )AS op
	 ON oip.order_id = op.order_id
GROUP BY oip.product_category_name
ORDER BY AVG(o.order_delivered_customer_date::DATE - o.order_purchase_timestamp::DATE) ASC
LIMIT 10; 
-- Top product categories keep their average delivery dates to below 2 weeks or just a day above.
-- There is a lot of product category that delivered in 4 days on average but still had low sales

----------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

-- TOP SELLERS ANALYSIS

SELECT *
FROM olist_sellers_dataset; -- Understand the Dataset

-- TOP 5 SELLERS
SELECT ois.Seller_id AS "Seller's ID", ois.seller_state AS "Seller's State", 
		TO_CHAR(SUM(op.payment_value), 'FM999,999,999') AS "Amount Sold",
		TO_CHAR(SUM(ois.units_sold), '$FM999,999,999') AS "Units Sold",
		ROUND(100.0 * SUM(op.payment_value) / SUM(SUM(op.payment_value)) OVER(), 2) AS "Revenue %",
		ROUND(100.0 * SUM(ois.units_sold) / SUM(SUM(ois.units_sold)) OVER(), 2)"Units Sold %"
FROM (SELECT oi.order_id, s.seller_id, s.seller_state, COUNT(oi.order_item_id) AS units_sold
		FROM olist_order_items_dataset AS oi
		JOIN olist_sellers_dataset AS s
		ON oi.seller_id = s.seller_id
		GROUP BY oi.order_id, s.seller_id, s.seller_state) AS ois
LEFT JOIN (SELECT order_id, SUM(payment_value) AS payment_value
		FROM olist_order_payments_dataset
		GROUP BY order_id) AS op
	ON ois.order_id = op.order_id
GROUP BY ois.seller_state, ois.seller_id
ORDER BY "Revenue %" DESC
LIMIT 5; 

		 -- INSIGHTS
		 -- "4869f7a5dfa277a7dca6462dcf3b52b2" is the top seller accounting for 1.56%($252k) of total revenue even as they are not the top
		 -- seller by transaction count.

		 -- 4 of the top 5 customers are from SP state.


-- TOP 5 SELLER'S STATE
SELECT ois.seller_state AS "Seller's State",
		TO_CHAR(ssi.Total_Sellers, 'FM999,999,999') AS "Sellers",
		TO_CHAR(SUM(ois.total_transactions), 'FM999,999,999') AS "Units Sold",
		TO_CHAR(SUM(op.payment_value), 'FM$999,999,999.00')AS "Amount Sold",		
		ROUND(100.0 * SUM(op.payment_value) / SUM(SUM(op.payment_value)) OVER(), 2) AS "Revenue %",
		ROUND(100.0 * SUM(ois.total_transactions) / SUM(SUM(ois.total_transactions)) OVER(), 2) AS "Units Sold %"
FROM (SELECT order_id, SUM(payment_value) AS payment_value 
		FROM olist_order_payments_dataset
		GROUP BY order_id) AS op
RIGHT JOIN (SELECT oi.order_id, s.seller_state, COUNT(oi.order_item_id) AS total_transactions
		FROM olist_order_items_dataset AS oi
		JOIN olist_sellers_dataset AS s
		ON oi.seller_id = s.seller_id
		GROUP BY oi.order_id, s.seller_state) AS ois
	ON op.order_id = ois.order_id
JOIN (SELECT seller_state, COUNT(seller_id) AS Total_Sellers 
		FROM olist_sellers_dataset 
		GROUP BY seller_state ) AS ssi
	ON ois.seller_state = ssi.seller_state
GROUP BY ois.seller_state, ssi.total_sellers
ORDER BY "Revenue %" DESC
LIMIT 5; 
-- All we have for out sellers as is for customers are their ids for confidentiality

-- INSIGHTS
-- SP is by far the top state by all seller metrics
-- SP tops total number of sellers in a state at 59.74%(1,849 sellers) of total sellers in this dataset, the next state
-- is PR at 11%

-- SP tops total total transaction count at a whooping 70.77% (70,188) of total transactions in this dataset
-- SP also top REVENUE metrics as expected by 64.72%($10.5M) of total revenue

-- NOTE: SP is also the top state among customers. This proximity between customers and sellers might be the biggest
-- Influence in SP's success as the top state where major sales are made.