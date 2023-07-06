**Python and SQL ELT Documentation**

As  a data engineer in ABC company, I am tasked with developing a ELT pipeline to aid in analytics.

**Table of Contents**



1. Prerequisites
2. Extract
3. Load
4. Transform
5. Export and Upload CSV files

    1. Prerequisites

* Before proceeding with the ELT process, ensure that the following prerequisites are met: 
* Python version 3 is installed.
* Required Python packages/libraries (e.g., pandas, boto3, psycopg2) 
* Access to the source database - amazon S3 bucket  and the target database Postgres SQL with correct credentials.

	2. Extract



* Create user id variable  `user_id = 'samundeg7748'`
* During this phase, Install the necessary Python libraries: boto3 for interacting with AWS S3, and psycopg2 for interacting with PostgreSQL
* Download the required files from the S3 bucket using the code below:

    	_bucket_name = 'd2b-internal-assessment-bucket'_


    _file_paths = [_


    _    'orders_data/orders.csv',_


    _    'orders_data/reviews.csv',_


    _    'orders_data/shipments_deliveries.csv'_


    _]_

* Create a staging schema using my id

    	`# Create staging schema if it doesn't exist`


    ```
    staging_schema = f'{user_id}_staging'
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {staging_schema};')
        conn.commit()
    ```



    3. Load:

* Load the raw data files into the staging schema tables using psycopg2

        ```
         # Load data into staging tables
            table_name = file_name.split('.')[0]
            with open(local_file_path, 'r') as file:
                with conn.cursor() as cur:
                    cur.copy_from(file, f'{staging_schema}.{table_name}', sep=',', null='')
                    conn.commit()
        ```



    4. Transform: \
	_# Perform transformations using SQL queries_


    _with conn.cursor() as cur:_


    _    # Transformation 1: Calculate total number of orders placed on public holidays every month_


    _    cur.execute(f'''_


    _        INSERT INTO {user_id}_analytics.agg_public_holiday (ingestion_date, tt_order_hol_jan, tt_order_hol_feb, tt_order_hol_mar, tt_order_hol_apr,_


    _        tt_order_hol_may, tt_order_hol_jun, tt_order_hol_jul, tt_order_hol_aug, tt_order_hol_sep, tt_order_hol_oct, tt_order_hol_nov, tt_order_hol_dec)_


    _        SELECT _


    _            '{ingestion_date}' AS ingestion_date,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 1) AS tt_order_hol_jan,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 2) AS tt_order_hol_feb,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 3) AS tt_order_hol_mar,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 4) AS tt_order_hol_apr,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 5) AS tt_order_hol_may,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 6) AS tt_order_hol_jun,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 7) AS tt_order_hol_jul,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 8) AS tt_order_hol_aug,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 9) AS tt_order_hol_sep,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 10) AS tt_order_hol_oct,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 11) AS tt_order_hol_nov,_


    _            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 12) AS tt_order_hol_dec_


    _        FROM_


    _            {staging_schema}.orders AS o_


    _        JOIN_


    _            {user_id}_common.dim_dates AS d ON o.order_date = d.calendar_dt_


    _        WHERE_


    _            d.working_day = false_


    _            AND d.day_of_the_week_num BETWEEN 1 AND 5_

* This code performs a data transformation and inserts aggregated information into a table named **agg_public_holiday **in staging schema within the PostgreSQL database. 
* An I**NSERT INTO... SELEC**T statement is how the query is set up. The results of the **SELECT **query are inserted into the designated table. Using the **DATE_PART('month', o.order_date) **function, the **SELECT** query counts the number of orders for each month.
* The **COUNT(*) FILTER** clauses in the **SELECT** query count the number of orders placed for each individual month (January through December). The counts, which indicate the total orders placed on public holidays for each month, are aliased as_ tt_order_hol_jan, tt_order_hol_feb_, etc.

# Transformation 2: Calculate total number of late shipments and undelivered shipments

_    cur.execute(f'''_

_        INSERT INTO {user_id}_analytics.agg_shipments (ingestion_date, tt_late_shipments, tt_undelivered_items)_

_        SELECT_

_            '{ingestion_date}' AS ingestion_date,_

_            COUNT(*) FILTER (WHERE s.shipment_date >= o.order_date + INTERVAL '6 days' AND s.delivery_date IS NULL) AS tt_late_shipments,_

_            COUNT(*) FILTER (WHERE o.order_date + INTERVAL '15 days' &lt;= current_date AND s.delivery_date IS NULL AND s.shipment_date IS NULL) AS tt_undelivered_items_

_        FROM_

_            {staging_schema}.orders AS o_

_        LEFT JOIN_

_            {staging_schema}.shipments_deliveries AS s ON o.order_id = s.shipment_id_

_    ''')_



* This code performs a data transformation and inserts aggregated information into a table named** agg_shipments**
* An I**NSERT INTO... SELECT** statement is how the query is set up. The results of the **SELECT** query are inserted into the designated table.
* Two **COUNT(*) FILTER **clauses in the **SELECT** statement count late shipments and undeliverable items according to predefined criteria:
1. **_s.shipment_date >= o.order_date + INTERVAL '6 days' AND s.delivery_date IS NULL; COUNT(*) FILTER As for "late shipments," _**This clause counts the number of shipments with a shipment_date that is NULL and a delivery_date that is greater than or equal to the order date plus 6 days (o.order_date + INTERVAL '6 days'). These shipments are regarded as tardy.
2. **_COUNT(*) FILTER (WHERE o.order_date + INTERVAL '15 days' &lt;= current_date AND s.delivery_date IS NULL AND s.shipment_date IS NULL) AS tt_undelivered_items: _**This clause counts the number of items that have an order_date plus 15 days (o.order_date + INTERVAL '15 days') that is less than or equal to the current date (current_date), and both the delivery_date and shipment_date are NULL. These are considered undelivered items.

# Transformation 3: Calculate best performing product

   _ cur.execute(f'''_

_        INSERT INTO {user_id}_analytics.best_performing_product (ingestion_date, product_name, most_ordered_day, is_public_holiday, tt_review_points,_

_        pct_one_star_review, pct_two_star_review, pct_three_star_review, pct_four_star_review, pct_five_star_review, pct_early_shipments, pct_late_shipments)_

_        SELECT_

_            '{ingestion_date}' AS ingestion_date,_

_            p.product_name,_

_            d.calendar_dt AS most_ordered_day,_

_            CASE WHEN d.working_day = false AND d.day_of_the_week_num BETWEEN 1 AND 5 THEN true ELSE false END AS is_public_holiday,_

_            r.review AS tt_review_points,_

_            COUNT(*) FILTER (WHERE r.review = 1) * 100.0 / COUNT(*) AS pct_one_star_review,_

_            COUNT(*) FILTER (WHERE r.review = 2) * 100.0 / COUNT(*) AS pct_two_star_review,_

_            COUNT(*) FILTER (WHERE r.review = 3) * 100.0 / COUNT(*) AS pct_three_star_review,_

_            COUNT(*) FILTER (WHERE r.review = 4) * 100.0 / COUNT(*) AS pct_four_star_review,_

_            COUNT(*) FILTER (WHERE r.review = 5) * 100.0 / COUNT(*) AS pct_five_star_review,_

_            COUNT(*) FILTER (WHERE s.shipment_date &lt; o.order_date + INTERVAL '6 days') * 100.0 / COUNT(*) AS pct_early_shipments,_

_            COUNT(*) FILTER (WHERE s.shipment_date >= o.order_date + INTERVAL '6 days') * 100.0 / COUNT(*) AS pct_late_shipments_

_        FROM_

_            {staging_schema}.orders AS o_

_        JOIN_

_            {staging_schema}.products AS p ON o.product_id = p.product_id_

_        JOIN_

_            {staging_schema}.reviews AS r ON o.product_id = r.product_id_

_        JOIN_

_            {user_id}_common.dim_dates AS d ON o.order_date = d.calendar_dt_

_        GROUP BY_

_            p.product_name, d.calendar_dt, d.working_day, d.day_of_the_week_num, r.review_

_        ORDER BY_

_            COUNT(*) DESC_

_        LIMIT 1_

_    ''')_

This code does the following; 

**'ingestion_date' AS ingestion_date**: This assigns the ingestion_date value to the constant string 'ingestion_date'. It represents the date when the data is ingested.

**p.product_name: **This selects the product_name column from the products table and includes it in the result set.

**d.calendar_dt AS most_ordered_day:** This selects the calendar_dt column from the dim_dates table and aliases it as most_ordered_day. It represents the day with the highest number of orders for a particular product.

**CASE WHEN d.working_day = false AND d.day_of_the_week_num BETWEEN 1 AND 5 THEN true ELSE false END AS is_public_holiday:** This constructs a Boolean column is_public_holiday based on the values of working_day and day_of_the_week_num columns. It indicates whether the day is a public holiday or not.

**r.review AS tt_review_points: **This selects the review column from the reviews table and aliases it as tt_review_points. It represents the total review points for a particular product.

**COUNT(*) FILTER (WHERE r.review = 1) * 100.0 / COUNT(*) AS pct_one_star_review: **This calculates the percentage of one-star reviews by counting the number of records where review is 1 and dividing it by the total count of records.

Similar calculations are done for **_pct_two_star_review, pct_three_star_review, pct_four_star_review, pct_five_star_review, pct_early_shipments, and pct_late_shipments._**

**Export and Upload CSV files**

# Export the tables to CSV files

_with conn.cursor() as cur:_

_    # Export agg_public_holiday_

_    cur.execute(f'''_

_        SELECT * FROM {user_id}_analytics.agg_public_holiday_

_    ''')_

_    rows = cur.fetchall()_

**with conn.cursor() as cur:**: This line establishes a connection to the PostgreSQL database and creates a cursor object named cur. The with statement ensures that the cursor is properly closed after the code block is executed.

**cur.execute(f'''...'''): **This line executes an SQL statement using the execute() method of the cursor object. The SQL statement is a SELECT query that retrieves all rows from the agg_public_holiday table.

**SELECT * FROM {user_id}_analytics.agg_public_holiday:** This part of the query selects all columns (*) from the agg_public_holiday table in the specified user-specific schema.

**rows = cur.fetchall(): **This line fetches all the rows returned by the SELECT query and assigns them to the rows variable. The fetchall() method retrieves all the remaining rows of the result set as a list of tuples. \
 \
** # Write to CSV file**

   _ csv_file_path = f'path/to/local/directory/agg_public_holiday.csv'  # Specify the local directory to save the file_

_    with open(csv_file_path, 'w', newline='') as file:_

_        writer = csv.writer(file)_

_        writer.writerow([desc[0] for desc in cur.description])  # Write header_

_        writer.writerows(rows)_

** # Upload CSV file to S3**

    s3.upload_file(csv_file_path, bucket_name, f'{s3_export_path}/agg_shipments.csv') \
 \
 **# Export best_performing_product**

    cur.execute(f'''

        SELECT * FROM {user_id}_analytics.best_performing_product

    ''')

    rows = cur.fetchall()

** # Write to CSV file**

   _ csv_file_path = f'path/to/local/directory/best_performing_product.csv'  # Specify the local directory to save the file_

_    with open(csv_file_path, 'w', newline='') as file:_

_        writer = csv.writer(file)_

_        writer.writerow([desc[0] for desc in cur.description])  # Write header_

_        writer.writerows(rows)_

** # Upload CSV file to S3**

    _s3.upload_file(csv_file_path, bucket_name, f'{s3_export_path}/{best_performing_product_filename}')_
