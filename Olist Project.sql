SELECT *
FROM olist_customers_dataset;

SELECT *
FROM olist_order_items_dataset;

SELECT *
FROM olist_order_payments_dataset;

SELECT *
FROM olist_orders_dataset;

SELECT                   -- This can be used to check null values but takes much time since this needs to be used for each column in each table.
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_values
FROM olist_customers_dataset;

-- Check for null & duplicate values
-- For better efficiency, only check for null & duplicate values in tables without Unique Identifier(Primary key)

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
END $$;




SELECT *
FROM olist_customers_dataset;

SELECT COUNT(DISTINCT customer_id)
FROM olist_customers_dataset;

CREATE TABLE olist_geolocation_dataset(geolocation_zip_code_prefix DECIMAL(2,10),
									    geolocation_lat	DECIMAL(2,10),
										geolocation_lng	DECIMAL(2,10),
										geolocation_city TEXT,
										geolocation_state TEXT
);

DROP TABLE olist_geolocation_dataset;

CREATE TABLE olist_geolocation_dataset(geolocation_zip_code_prefix INT,
									    geolocation_lat	FLOAT,
										geolocation_lng	FLOAT,
										geolocation_city TEXT,
										geolocation_state TEXT
);

SELECT *
FROM olist_geolocation_dataset;

COPY olist_geolocation_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT *
FROM olist_geolocation_dataset;

CREATE TABLE olist_order_reviews_dataset(review_id TEXT,	
										  order_id TEXT,
										  review_score INT,
										  review_comment_title TEXT,
										  review_comment_message TEXT,
										  review_creation_date TIMESTAMP,
										  review_answer_timestamp TIMESTAMP
);

SELECT *
FROM olist_order_reviews_dataset;

DROP TABLE olist_order_reviews_dataset;

COPY olist_order_reviews_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE olist_products_dataset(product_id TEXT PRIMARY KEY,
									 product_category_name TEXT,
									 product_name_lenght INT,
									 product_description_lenght	INT,
									 product_photos_qty	INT,
									 product_weight_g INT,
									 product_length_cm INT,
									 product_height_cm	INT,
									 product_width_cm INT
);

COPY olist_products_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT *
FROM olist_products_dataset;

CREATE TABLE olist_sellers_dataset(seller_id TEXT PRIMARY KEY,
									seller_zip_code_prefix INT,
									seller_city TEXT,
									seller_state TEXT
);

COPY olist_sellers_dataset
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

SELECT *
FROM olist_sellers_dataset;

CREATE TABLE product_category_name_translation(product_category_name TEXT,
												product_category_name_english TEXT
);

COPY product_category_name_translation
FROM 'C:\Program Files\PostgreSQL\17\data\Data Copy\product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

SELECT *
FROM product_category_name_translation;

-- Check for null & duplicate values
-- For better efficiency, only check for null & duplicate values in tables without Unique Identifier(Primary key)

-- Olist_geolocation_dataset

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
END $$;



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

-- The above Dynamic query shows all the column in Order_payments table has duplicates
-- 1. Payment sequential: This is expected since the sequencing is restarted for each order e.g 1,2,3. so there is multiple 1s, 2s etc.
-- 2. Payment type: Since the same types are payment types are used, duplicates are the norms
-- 3. Payment Installments: Relates to how many times a single Order was payed for. 1,2 etc. times. for each order.
-- 4. Payment Value: Since an order can be payed for in installments, payment value is bound to occur more than once for the same order.
-- 5. Order ID: This is a foreign key from Orders table and duplication is allowed coupled with the fact that installmental payments 
-- 	  were made and the same order was payed for multiple times in many occations.

-- Observations on Duplicate Columns in olist_order_payments_dataset
-- payment_sequential:
-- Explanation: This column represents the sequence of payments for each order, starting from 1. As the sequence resets for every order_id, duplicates are expected (e.g., multiple rows with 1, 2, etc., across different orders).

-- payment_type:
-- Explanation: This column indicates the payment method (e.g., credit card, voucher). Since payment methods are reused across multiple orders, duplicates are normal.

-- payment_installments:
-- Explanation: This column shows the number of installments for which an order is paid. Repetition of common installment counts (e.g., 1, 2) is expected due to multiple orders opting for similar installment plans.

-- payment_value:
-- Explanation: This represents the payment amount in a single transaction. For orders paid in installments, the same payment_value may appear multiple times for a single order. Additionally, similar amounts might appear across different orders due to price standardization.

-- order_id:
-- Explanation: As a foreign key from the orders table, order_id duplication is valid. It occurs because a single order can involve multiple payment transactions due to installment payments or split payments across different methods.

-- Additional Notes
-- The presence of duplicates across these columns is expected behavior based on the dataset's design.
-- These duplicates reflect the multi-faceted nature of payment processing (e.g., installments, multiple payment methods) and do not indicate data issues.


SELECT *
FROM olist_order_payments_dataset;

SELECT order_id, COUNT(payment_value) AS "Count of Payments", SUM(payment_value) AS "Total Payments"
FROM olist_order_payments_dataset
GROUP BY order_id
ORDER BY "Count of Payments" DESC;

SELECT order_id, COUNT(payment_value) AS "Count of Payments", SUM(payment_value) AS "Total Payments"
FROM olist_order_payments_dataset
GROUP BY order_id
ORDER BY "Total Payments" DESC;

SELECT *
FROM olist_order_payments_dataset
WHERE order_id = 'fa65dad1b0e818e3ccc5cb0e39231352';

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
  AND a.order_delivered_customer_date IS NOT NULL; -- With this, we discovered orders that were marked as canceled and delivered at the same time 
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



SELECT *
FROM olist_orders_dataset;

SELECT *
FROM olist_order_reviews_dataset;

SELECT *
FROM olist_orders_dataset
ORDER BY order_approved_at DESC;

SELECT *
FROM olist_orders_dataset
WHERE order_approved_at IS NULL;

SELECT *
FROM olist_orders_dataset;

SELECT *
FROM olist_order_payments_dataset;

SELECT *
FROM olist_order_items_dataset;

SELECT 
    CASE 
        WHEN order_approved_at IS NOT NULL 
          AND order_delivered_customer_date IS NOT NULL THEN 'Delivered'
        WHEN order_approved_at IS NOT NULL THEN 'Approved but Not Delivered'
        ELSE 'No Approval or Delivery'
    END AS anomaly_type,
    COUNT(*) AS count
FROM olist_orders_dataset
WHERE order_status = 'canceled'
GROUP BY anomaly_type;


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

SELECT *
FROM Olist_geolocation_dataset;

-------------------------------------------------------------------------------------------------------------

SELECT *
FROM olist_order_items_dataset
WHERE order_item_id IN (
    SELECT order_item_id
    FROM olist_order_items_dataset
    GROUP BY order_item_id
    HAVING COUNT(*) > 1
);

SELECT order_item_id, COUNT(*)
FROM olist_order_items_dataset
GROUP BY order_item_id
HAVING COUNT(*) > 1
ORDER BY order_item_id ASC;


SELECT COUNT(order_id) AS "Total_Orders", COUNT(DISTINCT order_id) AS "Total Distinct Orders", COUNT(product_id) AS "Total_Products", COUNT(DISTINCT product_id) AS "Total Distinct Products"
FROM olist_order_items_dataset;

SELECT *
FROM olist_order_items_dataset;

SELECT a.order_item_id, b.product_category_name, b.product_id, c.order_id
FROM olist_order_items_dataset AS a
JOIN olist_products_dataset AS b ON a.product_id = b.product_id
JOIN olist_orders_dataset AS c ON a.order_id = c.order_id
ORDER BY a.order_item_id ASC;

-- This shows the number of items in most orders. this shows for example that most orders only have 1 item, followed by orders that have 2 items etc
SELECT order_item_id, COUNT(order_id) AS "Total Orders", COUNT(DISTINCT product_id)
FROM olist_order_items_dataset
GROUP BY order_item_id
HAVING COUNT(DISTINCT order_id) > 1 OR COUNT(DISTINCT product_id) > 1
ORDER BY "Total Orders" DESC;

-- Top 10 Orders by Items Ordered
-- This shows the top orders by the number of items ordered. The top order has 21 orders followed closely by 2 more orders with 20 orders each.
SELECT order_id, COUNT(order_item_id) AS "Total Items"
FROM olist_order_items_dataset
GROUP BY order_id
ORDER BY COUNT(order_item_id) DESC
LIMIT 10;

-- Top 10 orders by Total Order Value

SELECT a.order_id, b.Total_Order_Value
FROM (SELECT order_id, SUM(payment_value) AS Total_Order_Value
	  FROM olist_order_payments_dataset
	  GROUP BY order_id) AS b
JOIN olist_order_items_dataset AS a
ON a.order_id = b.order_id
GROUP BY a.order_id, b.Total_Order_Value
ORDER BY b.Total_Order_Value DESC
LIMIT 10;

-- Top 10 customer by Total Purchases
SELECT
    MAX(o.customer_id),
    op.order_id,
    SUM(op.payment_value) AS Total_Order_Value
FROM
    olist_orders_dataset AS o
JOIN
    olist_order_payments_dataset AS op ON o.order_id = op.order_id
GROUP BY
    op.order_id
ORDER BY
    Total_Order_Value DESC
LIMIT 10;


-- For the purpose of this analysis, this query returns the wrong result, a result that doesn't fulfil the purpose intended.
SELECT a.order_id, b.Total_Order_Value
FROM (SELECT order_id, SUM(payment_value) AS Total_Order_Value
	  FROM olist_order_payments_dataset
	  GROUP BY order_id) AS b
JOIN olist_order_items_dataset AS a
ON a.order_id = b.order_id
ORDER BY b.Total_Order_Value DESC;

-- This does a good job but runs slowly
SELECT a.customer_id, b.order_id, a.Total_Order_Value
FROM (SELECT b.customer_id, a.order_id, SUM(a.payment_value) AS Total_Order_Value
	  FROM olist_order_payments_dataset AS a
	  JOIN olist_orders_dataset AS b
	  ON a.order_id = b.order_id
	  GROUP BY a.order_id, b.customer_id
	  ) AS a
JOIN olist_order_items_dataset AS b
ON a.order_id = b.order_id
GROUP BY b.order_id, a.customer_id, a.Total_Order_Value
ORDER BY a.Total_Order_Value DESC;




SELECT *
FROM olist_customers_dataset;

SELECT *
FROM olist_order_payments_dataset;

SELECT *
FROM olist_order_payments_dataset
WHERE order_id = '03caa2c082116e1d31e67e9ae3700499';

SELECT *
FROM olist_order_items_dataset
WHERE order_id = '03caa2c082116e1d31e67e9ae3700499';

SELECT *
FROM olist_customers_dataset;

SELECT 
    order_id, 
    product_id, 
    COUNT(*) AS row_count,
	SUM(price) AS Price,
    SUM(freight_value) AS total_freight_value
FROM olist_order_items_dataset
WHERE order_id = '03caa2c082116e1d31e67e9ae3700499'
GROUP BY order_id, product_id;

SELECT 
    order_id, 
    product_id, 
    COUNT(*) AS "items_ordered",
	SUM(price) AS Price,
    SUM(freight_value) AS total_freight_value
FROM olist_order_items_dataset
GROUP BY order_id, product_id
ORDER BY Price DESC;


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



SELECT *
FROM olist_products_dataset;

----------------------------------------------------------------------------------------------------------------------------------------




-- REVENUE ANALYSIS


-- 	Overall Revenue
-- 		Total Revenue
SELECT TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS "Total Revenue"
FROM olist_order_payments_dataset; -- Total Revenue - 16M

-- AVERAGE ORDER VALUE
SELECT TO_CHAR(SUM(payment_value) / COUNT(order_id), '$FM999,999,999.00') AS average_order_value
FROM olist_order_payments_dataset;

-- 	Canceled Orders
SELECT TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS "To Be Refunded"
FROM olist_order_payments_dataset AS p
JOIN olist_orders_dataset AS o
ON p.order_id = o.order_id
WHERE order_status = 'canceled';

-- INSIGHTS
-- Total Revenue is 16M
-- Average Order Value is $154.10
-- Lost revenue to Canceled orders is $143k

--------------------------------------------------------------------------------------------------------------------------------------------------

-- 		Monthly/Yearly Revenue
SELECT 
    DATE_PART('year', order_purchase_timestamp) AS year,
	TO_CHAR(order_purchase_timestamp, 'Mon') AS month_name,
    DATE_PART('month', order_purchase_timestamp) AS month_no,
    SUM(payment_value) AS total_monthly_revenue
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY DATE_PART('year', order_purchase_timestamp), DATE_PART('month', order_purchase_timestamp), TO_CHAR(order_purchase_timestamp, 'Mon')
ORDER BY year, month_no; -- This query returns Monthly Revenue, but it also uncovered missing values for the whole month of NOV, 2016.


SELECT 
    DATE_PART('year', order_purchase_timestamp) AS year,
	TO_CHAR(order_purchase_timestamp, 'Mon') AS month_name,
    DATE_PART('month', order_purchase_timestamp) AS month_no,
    TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS total_monthly_revenue,
	ROUND(100.0 * SUM(payment_value) / SUM(SUM(payment_value)) OVER(), 2) "% Revenue"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY DATE_PART('year', order_purchase_timestamp), DATE_PART('month', order_purchase_timestamp), TO_CHAR(order_purchase_timestamp, 'Mon')
ORDER BY "% Revenue" DESC; -- Nov 2017 saw the highest revenue with Sales at 1.194M(7.46%) ot total sales, it is followed closely by April and
							-- March 2018 with sales at 1.160M(7.25%) and 1.159M(7.24%).
							 


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

-- Handling the Missing Values																		 

WITH calendar AS (
    SELECT generate_series(DATE '2016-09-01', DATE '2018-10-17', '1 month') AS month_start
)
SELECT 
    EXTRACT(YEAR FROM c.month_start) AS year,
    TO_CHAR(c.month_start, 'Mon') AS month_name,
    EXTRACT(MONTH FROM c.month_start) AS month_no,
    COALESCE(SUM(p.payment_value), 0) AS total_monthly_revenue
FROM calendar AS c
LEFT JOIN olist_orders_dataset AS o
    ON DATE_TRUNC('month', o.order_purchase_timestamp) = c.month_start
LEFT JOIN olist_order_payments_dataset AS p
    ON o.order_id = p.order_id
GROUP BY year, month_name, month_no
ORDER BY year, month_no; -- Continuing with the analysis, the missing values for Nov, 2016 has been changed to zero to ensure the month is included.
						  -- Another observation is the next month also has incredibly low sales at $19.62.



-- 		Yearly Revenue
SELECT DATE_PART('year', o.order_purchase_timestamp) AS Year,
	   TO_CHAR(SUM(p.payment_value), '$FM999,999,999.00') AS "Revenue",
	   ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER(), 2) AS "% Revenue"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY Year
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
    payment_type, 
    COUNT(order_id) AS "Total Orders", 
    TO_CHAR(SUM(payment_value), '$FM999,999,999.00') AS "Total Revenue",
    ROUND(100.0 * COUNT(order_id) / SUM(COUNT(order_id)) OVER (), 2) AS "Payment Count (%)",
    ROUND(100.0 * SUM(payment_value) / SUM(SUM(payment_value)) OVER (), 2) AS "Revenue (%)"
FROM olist_order_payments_dataset
GROUP BY payment_type
ORDER BY "Revenue (%)" DESC; -- Most customers paid using credit card with total transactions at a staggering 73.92%(76,795) and 
							  -- Revenue at 78.34%(12.5M)


-- Canceled Orders

SELECT 
    REPLACE(a.payment_type, '_', ' ') AS "Payment Type",
    TO_CHAR(COUNT(a.order_id), 'FM999,999,999') AS total_orders,
    SUM(CASE WHEN b.order_status = 'canceled' THEN 1 ELSE 0 END) AS "Canceled Orders", 
    TO_CHAR(SUM(CASE WHEN b.order_status = 'canceled' THEN payment_value ELSE 0 END), '$FM999,999,999.00') AS "Revenue",
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
FROM olist_order_items_dataset; -- This query efficiently returns the desired result by avoiding unecessary JOINS that may cause duplication and
								 -- take more time to run.

								 -- INSIGHTS
								 -- Freight contributed 14% to total revenue at $2.252M of $16M total revenue.



		-- Top 10 States by Revenue Performance

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
LIMIT 10; -- Sao Paolo raked in more revenue at approximately $6M contributing 37.47% of total revenue
		  -- This query went further to put the number of customers in each state into perspective to allow for better understanding as to the 
		  -- reason for the success or failure of each state.

		  -- INSIGHT
		  -- Sao Paolo raked in more revenue at approximately $6M contributing 37.47% of total revenue
		  -- At least for the top 10 states by revenue performance, states with higher number of customers did better
		  -- 42% of total customers are from Sao Paolo, that is what makes Sao Paolo by far the top state by Revenue generated.



		-- Top 10 Cities by Revenue Performance
SELECT a.customer_city, TO_CHAR(SUM(b.payment_value), 'FM999,999,999.00') AS "Total Revenue",
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
LIMIT 10; -- 13.76%(2.2M) of total revenue came from Sao Polo followed by Rio De Janeiro at 7.26%(1.162M)


-- INSIGHTS
-- Most customers paid using credit card with total transactions at a staggering 73.92%(76,795) and revenue at 78.34%(12.5M).
-- Credit card has most cancelation at 444 cancelations but Voucher has most cancelation rate at approximately 2% of total payment by that means.
-- Freight contributed 14% to total revenue at $2.252M of $16M total revenue.
-- Sao Paolo raked in more revenue at approximately $6M contributing 37.47% of total revenue
-- At least for the top 10 states by revenue performance, states with higher number of customers did better
-- 42% of total customers are from Sao Paolo, that is what makes Sao Paolo by far the top state by Revenue generated.
-- 13.76%(2.2M) of total revenue came from Sao Polo followed by Rio De Janeiro at 7.26%(1.162M)
---------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------

-- SALES AND PRODUCT PERFORMANCE

-- Total Product Sold
SELECT TO_CHAR(COUNT(product_id), 'FM999,999,999') AS "Total Products Sold"
FROM olist_products_dataset; -- Total products sold is 32,951.

SELECT TO_CHAR(COUNT(order_item_id), 'FM999,999,999') AS "Total Units Sold"
FROM olist_order_items_dataset; -- Total units sold is 112,650

-- Top Performing Product

SELECT 
    i.product_id AS "Product_id", 
    TO_CHAR(COUNT(i.order_item_id), 'FM999,999,999') AS "Total Units Sold",
	TO_CHAR(SUM(i.price), '$FM999,999,999.00')AS Price,
    TO_CHAR(SUM(i.freight_value), '$FM999,999,999.00') AS total_freight_value,
	TO_CHAR(ROUND(SUM(i.price) / COUNT(i.*), 2), '$FM999,999,999.00') AS "Average Order Value",
	TO_CHAR(SUM(p.payment_value), '$FM999,999,999.00') AS Total_Revenue,
	ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER(), 2) AS "% Revenue"
FROM olist_order_items_dataset AS i
JOIN olist_order_payments_dataset AS p
ON i.order_id = p.order_id
GROUP BY i.product_id
ORDER BY "% Revenue" DESC; -- This shows Total revenue per product and Total units sold
  		  -- This query reveals there are different types of products with prices higher than each other.
		  -- The top product is "5769ef0a239114ac3a854af00df129e4" with only 8 units totaling $109k while the second top 
		  -- performing product sold 250 units yet made $82k


-- Top performing Product Category 

SELECT b.product_category_name AS "Product Category Name", TO_CHAR(SUM(p.payment_value), 'FM999,999,999.00') AS "Total Revenue",
	   TO_CHAR(COUNT(b.order_item_id), 'FM999,999,999') AS "Units Sold",
	   ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER(), 2) AS "% Revenue Distribution"
FROM olist_order_payments_dataset AS p
JOIN (SELECT p.product_id, i.order_item_id, p.product_category_name, i.order_id
	 FROM olist_products_dataset AS p
	 JOIN olist_order_items_dataset AS i
	 ON p.product_id = i.product_id) AS b
ON p.order_id = b.order_id
GROUP BY b.product_category_name
ORDER BY "% Revenue Distribution" DESC
LIMIT 10;    				    -- Cama Mesa Banho is the top product category, raking in total revenue of $1.7M (8.43% of Total Revenue) 
							    -- It is followed closely by Belaza Saude at $1.65M (8.16% of Total Revenue)
SELECT *
FROM olist_order_items_dataset;

-- INSIGHTS
-- Total Unit Sold is 32,951
-- This analysis reveals there are different types of products with prices higher than each other.
-- The top product is "5769ef0a239114ac3a854af00df129e4" with only 8 units totaling $109k while the second top 
-- performing product sold 250 units yet made $82k.
-- Cama Mesa Banho is the top product category, raking in total revenue of $1.7M (8.43% of Total Revenue) 
-- It is followed closely by Belaza Saude at $1.65M (8.16% of Total Revenue)
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
SELECT customer_id, 
       COUNT(order_id) AS total_orders
FROM olist_orders_dataset
GROUP BY customer_id
HAVING COUNT(order_id) > 1
ORDER BY total_orders DESC; -- Repeat purchase
							 -- This revealed this business has no customer retention
							 
							 -- Possible Reasons
							 -- 1. This might be due to the possibility that the dataset is a snapshot or represents one-off transactions, 
							 -- this might reflect reality
							 
							 -- 2. Olist might target new customers primarily or not record repeat purchases under the same customer_id.

							 -- 3. The customer_id field may be inconsistent or mismanaged (e.g., different customer_id values for the same individual).

							 -- INSIGHTS
							 -- 1. This reveals high customer acquisition but low retention.
							 -- 2. Possible need to focus on customer retention.

-- Customer Distribution by State
SELECT customer_state, TO_CHAR(COUNT(DISTINCT customer_id), '999,999,999') AS "Customers",
	   ROUND(100.0 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER(), 2) AS "% Contribution"
FROM olist_customers_dataset
GROUP BY customer_state
ORDER BY "Customers" DESC; -- SP leads the way as top state with customer distribution at 41.98%(41,746) of total customers

-- Customer Distribution by City
SELECT customer_city, TO_CHAR(COUNT(DISTINCT customer_id), '999,999,999') AS "Customers",
	   ROUND(100.0 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER(), 2) AS "% Contribution"
FROM olist_customers_dataset
GROUP BY customer_city
ORDER BY "Customers" DESC; -- Sao Paulo leads the way as top city with customer distribution at 15.63%(15,540) of total customers





			
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
ORDER BY "New Customer %" DESC; -- Nov 2017 saw new customers totaling 7,863 followed closely by Jan & March 2018 at 7,563 & 7,512 customers
								


-- 		Yearly Customer Aquisition
SELECT DATE_PART('year', o.order_purchase_timestamp) AS Year,
	   TO_CHAR(COUNT(o.customer_id), 'FM999,999,999') AS "New Customers",
	   ROUND(100.0 * COUNT(o.customer_id) / SUM(COUNT(o.customer_id)) OVER(), 2) AS "% Customer Aquisition"
FROM olist_orders_dataset AS o
JOIN olist_order_payments_dataset AS p
ON o.order_id = p.order_id
GROUP BY Year
ORDER BY "% Customer Aquisition" DESC; -- 2018 is the best year even with incomplete records (The record for 2018 ends in Oct) with total acquired 
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

	-- 3. The customer_id field may be inconsistent or mismanaged (e.g., different customer_id values for the same individual).

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
ORDER BY Order_Status_Distribution DESC; -- This shows a success rate of 97% in terms of delivered orders which is very good

---------------------------------------------------------------------------------------------------
-- Let's point our attention towards Canceled Orders
-- There are 625 canceled orders
-- 550 of the canceled Orders are indeed canceled and treated appropriately
-- 69 of the canceled orders are canceled but Carrier claimed to have delivered the order to the customer
-- The remaining 6 orders were marked as delivered at the same time as canceled
-- Of the 6 orders marked as delivered to customer at the same time as marked as canceled, only 2 were actually delivered as confirmed by customer
-- review, 1 of the delivered order was a wrong product.
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
  AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL); -- For further analysis into claimed delivery to customer


SELECT *
FROM olist_order_reviews_dataset
WHERE order_id IN ('1950d777989f6a877539f53795b4c3c3', 'dabf2b0e35b423f94618bf965fcb7514', '770d331c84e5b214bd9dc70a10b829d0', '8beb59392e21af5eb9547ae1a9938d06',
				   '65d1e226dfaeb8cdc42f665422522d14', '2c45c33d2f9cb8ff8b1c86cc28c11c30'); -- picked out the order_id of the 6 orders to be further
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
ORDER BY "Ratings Count" DESC;

-- Sentiment Analysis By City
SELECT  c.customer_city,
 		SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) AS "Negative Reviews",
		SUM(CASE WHEN r.review_score = 3 THEN 1 ELSE 0 END) AS "Neutral Reviews",
		SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) AS "Positive Reviews",
		COUNT(r.*) AS "Ratings Count",
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
ORDER BY "Ratings Count" DESC;


-- Product Sentiment Analysis

-- Product segmentation by review rating

SELECT REPLACE(p.product_category_name, '_', ' ') AS "Product Category name",
       ROUND(100.0 * SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) / COUNT(o.order_item_id), 2) AS "Positive Review %",
	   ROUND(100.0 * SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) / COUNT(o.order_item_id), 2) AS "Negative Review %",
       COUNT(o.order_item_id) AS "total Units Sold",
       SUM(CASE WHEN r.review_score IN (4, 5) THEN 1 ELSE 0 END) AS "Positive Reviews",
	   SUM(CASE WHEN r.review_score IN (1, 2) THEN 1 ELSE 0 END) AS "Negative Reviews"
FROM olist_order_items_dataset AS o
JOIN olist_order_reviews_dataset AS r
	ON r.order_id = o.order_id
JOIN olist_products_dataset AS p
    ON o.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY "total Units Sold" DESC;


-- Review ratings have been segmented into 3 groups 'Negative (1,2)', 'Neutral (3)'
-- 'Positive (4,5)'.

-- Cama Mesa Banho is the product with most negative reviews at a total of 2112 negative reviews
-- Cama Mesa Banho is also the top product by neutral reviews at 1109 reviews
-- Cama Mesa Banho is also the top product by positive reviews at 7916 reviews

-- As this query reveals, Cama Mesa Banho sold most unit, hence has many reactions from customer's falling into positive or negative rating category
-- This is not to say improvements should not be made to reduce the number of complaints.



-------------------------------------------------------------------------------------------

-- DELIVERY DAYS ANALYSIS

-- Average Delivery Time by State
SELECT b.customer_state, a.order_status,
       AVG(order_delivered_customer_date::DATE - order_purchase_timestamp::DATE) AS avg_delivery_days
FROM olist_orders_dataset AS a
JOIN olist_customers_dataset AS b
ON a.customer_id = b.customer_id
WHERE order_status = 'delivered'
GROUP BY b.customer_state, a.order_status
ORDER BY avg_delivery_days ASC;


-- Average Delivery Time by City
SELECT b.customer_city, a.order_status,
       AVG(order_delivered_customer_date::DATE - order_purchase_timestamp::DATE) AS avg_delivery_days
FROM olist_orders_dataset AS a
JOIN olist_customers_dataset AS b
ON a.customer_id = b.customer_id
WHERE order_status = 'delivered'
GROUP BY b.customer_city, a.order_status
ORDER BY avg_delivery_days ASC;


-- Delivery Time by Product Category
SELECT p.product_category_name, AVG(o.order_delivered_customer_date::DATE - o.order_purchase_timestamp::DATE) AS "Avg Delivery Days",
	   SUM(op.payment_value) AS "Category Revenue"
FROM olist_products_dataset AS p
JOIN olist_order_items_dataset AS oi
	 ON p.product_id = oi.product_id
JOIN olist_orders_dataset AS o
	 ON oi.order_id = o.order_id
JOIN olist_order_payments_dataset AS op
	 ON oi.order_id = op.order_id
GROUP BY p.product_category_name
ORDER BY "Category Revenue" DESC; -- I really can't see the correlation between AVG DELIVERY DAYS and Revenue from each category.
								   -- For example, Cama Mesa Banho is the top product by revenue generated even as it has an avg delivery days of
								   -- 13 days, that's a day short of 2 weeks for delivery. That's incredibly long, yet this product category excels
								   -- in sales.




