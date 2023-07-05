import os
import csv
import datetime
import psycopg2     # This package interacts with the Posgtres database
import boto3
from botocore import UNSIGNED
from botocore.client import Config

# Set up S3 client
s3 = boto3.client('s3')

# Set up Postgres connection
postgres_host = 'my_host'
postgres_port = 'my_port'
postgres_db = 'my_db'
postgres_user = 'wahome'
postgres_password = '123@678'

conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    database=postgres_db,
    user=postgres_user,
    password=postgres_password
)

# Set user_id
user_id = 'user1234'

# Set the current date
ingestion_date = datetime.date.today()

# Define S3 export path
s3_export_path = f'd2b-internal-assessment-bucket/analytics_export/{user_id}'

# Define file names
best_performing_product_filename = 'best_performing_product.csv'

# Create staging schema if it doesn't exist
staging_schema = f'{user_id}_staging'
with conn.cursor() as cur:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {staging_schema};')
    conn.commit()

# Download raw data files from S3 and load into staging tables
bucket_name = 'd2b-internal-assessment-bucket'
file_paths = [
    'orders_data/orders.csv',
    'orders_data/reviews.csv',
    'orders_data/shipments_deliveries.csv'
]

for file_path in file_paths:
    file_name = file_path.split('/')[-1]
    local_file_path = f'path/to/local/directory/{file_name}'  # Specify the local directory to save the files
    s3.download_file(bucket_name, file_path, local_file_path) # Download files from S3 bucket to local directory

    # Load data into staging tables
    table_name = file_name.split('.')[0]
    with open(local_file_path, 'r') as file:
        with conn.cursor() as cur:
            cur.copy_from(file, f'{staging_schema}.{table_name}', sep=',', null='')
            conn.commit()

# Perform transformations using SQL queries
with conn.cursor() as cur:
    # Transformation 1: Calculate total number of orders placed on public holidays every month
    cur.execute(f'''
        INSERT INTO {user_id}_analytics.agg_public_holiday (ingestion_date, tt_order_hol_jan, tt_order_hol_feb, tt_order_hol_mar, tt_order_hol_apr,
        tt_order_hol_may, tt_order_hol_jun, tt_order_hol_jul, tt_order_hol_aug, tt_order_hol_sep, tt_order_hol_oct, tt_order_hol_nov, tt_order_hol_dec)
        SELECT 
            '{ingestion_date}' AS ingestion_date,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 1) AS tt_order_hol_jan,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 2) AS tt_order_hol_feb,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 3) AS tt_order_hol_mar,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 4) AS tt_order_hol_apr,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 5) AS tt_order_hol_may,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 6) AS tt_order_hol_jun,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 7) AS tt_order_hol_jul,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 8) AS tt_order_hol_aug,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 9) AS tt_order_hol_sep,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 10) AS tt_order_hol_oct,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 11) AS tt_order_hol_nov,
            COUNT(*) FILTER (WHERE DATE_PART('month', o.order_date) = 12) AS tt_order_hol_dec
        FROM
            {staging_schema}.orders AS o
        JOIN
            {user_id}_common.dim_dates AS d ON o.order_date = d.calendar_dt
        WHERE
            d.working_day = false
            AND d.day_of_the_week_num BETWEEN 1 AND 5
    ''')

    # Transformation 2: Calculate total number of late shipments and undelivered shipments
    cur.execute(f'''
        INSERT INTO {user_id}_analytics.agg_shipments (ingestion_date, tt_late_shipments, tt_undelivered_items)
        SELECT
            '{ingestion_date}' AS ingestion_date,
            COUNT(*) FILTER (WHERE s.shipment_date >= o.order_date + INTERVAL '6 days' AND s.delivery_date IS NULL) AS tt_late_shipments,
            COUNT(*) FILTER (WHERE o.order_date + INTERVAL '15 days' <= current_date AND s.delivery_date IS NULL AND s.shipment_date IS NULL) AS tt_undelivered_items
        FROM
            {staging_schema}.orders AS o
        LEFT JOIN
            {staging_schema}.shipments_deliveries AS s ON o.order_id = s.shipment_id
    ''')

    # Transformation 3: Calculate best performing product
    cur.execute(f'''
        INSERT INTO {user_id}_analytics.best_performing_product (ingestion_date, product_name, most_ordered_day, is_public_holiday, tt_review_points,
        pct_one_star_review, pct_two_star_review, pct_three_star_review, pct_four_star_review, pct_five_star_review, pct_early_shipments, pct_late_shipments)
        SELECT
            '{ingestion_date}' AS ingestion_date,
            p.product_name,
            d.calendar_dt AS most_ordered_day,
            CASE WHEN d.working_day = false AND d.day_of_the_week_num BETWEEN 1 AND 5 THEN true ELSE false END AS is_public_holiday,
            r.review AS tt_review_points,
            COUNT(*) FILTER (WHERE r.review = 1) * 100.0 / COUNT(*) AS pct_one_star_review,
            COUNT(*) FILTER (WHERE r.review = 2) * 100.0 / COUNT(*) AS pct_two_star_review,
            COUNT(*) FILTER (WHERE r.review = 3) * 100.0 / COUNT(*) AS pct_three_star_review,
            COUNT(*) FILTER (WHERE r.review = 4) * 100.0 / COUNT(*) AS pct_four_star_review,
            COUNT(*) FILTER (WHERE r.review = 5) * 100.0 / COUNT(*) AS pct_five_star_review,
            COUNT(*) FILTER (WHERE s.shipment_date < o.order_date + INTERVAL '6 days') * 100.0 / COUNT(*) AS pct_early_shipments,
            COUNT(*) FILTER (WHERE s.shipment_date >= o.order_date + INTERVAL '6 days') * 100.0 / COUNT(*) AS pct_late_shipments
        FROM
            {staging_schema}.orders AS o
        JOIN
            {staging_schema}.products AS p ON o.product_id = p.product_id
        JOIN
            {staging_schema}.reviews AS r ON o.product_id = r.product_id
        JOIN
            {user_id}_common.dim_dates AS d ON o.order_date = d.calendar_dt
        GROUP BY
            p.product_name, d.calendar_dt, d.working_day, d.day_of_the_week_num, r.review
        ORDER BY
            COUNT(*) DESC
        LIMIT 1
    ''')

    conn.commit()

# Export the tables to CSV files
with conn.cursor() as cur:
    # Export agg_public_holiday
    cur.execute(f'''
        SELECT * FROM {user_id}_analytics.agg_public_holiday
    ''')
    rows = cur.fetchall()

    # Write to CSV file
    csv_file_path = f'path/to/local/directory/agg_public_holiday.csv'  # Specify the local directory to save the file
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([desc[0] for desc in cur.description])  # Write header
        writer.writerows(rows)

    # Upload CSV file to S3
    s3.upload_file(csv_file_path, bucket_name, f'{s3_export_path}/agg_public_holiday.csv')

    # Export agg_shipments
    cur.execute(f'''
        SELECT * FROM {user_id}_analytics.agg_shipments
    ''')
    rows = cur.fetchall()

    # Write to CSV file
    csv_file_path = f'path/to/local/directory/agg_shipments.csv'  # Specify the local directory to save the file
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([desc[0] for desc in cur.description])  # Write header
        writer.writerows(rows)

    # Upload CSV file to S3
    s3.upload_file(csv_file_path, bucket_name, f'{s3_export_path}/agg_shipments.csv')

    # Export best_performing_product
    cur.execute(f'''
        SELECT * FROM {user_id}_analytics.best_performing_product
    ''')
    rows = cur.fetchall()

    # Write to CSV file
    csv_file_path = f'path/to/local/directory/best_performing_product.csv'  # Specify the local directory to save the file
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([desc[0] for desc in cur.description])  # Write header
        writer.writerows(rows)

    # Upload CSV file to S3
    s3.upload_file(csv_file_path, bucket_name, f'{s3_export_path}/{best_performing_product_filename}')

conn.close()
