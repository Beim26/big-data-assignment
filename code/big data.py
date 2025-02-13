# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace
import duckdb
import os
import plotly.express as px
import pandas as pd

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("EcommerceETL").getOrCreate()

# Step 2: Read CSV into PySpark DataFrame
csv_path = "C:\\Users\\Postlab\\Downloads\\Telegram Desktop\\2019-Dec.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Step 3: Data Cleaning & Transformation
# Handle missing values (drop nulls)
df = df.dropna()

# Remove duplicates
df = df.dropDuplicates()

# Replace dots with spaces in specific columns
df = df.withColumn("event_type", regexp_replace(col("event_type"), r"\.", " "))
df = df.withColumn("category_code", regexp_replace(col("category_code"), r"\.", " "))
df = df.withColumn("brand", regexp_replace(col("brand"), r"\.", " "))
df = df.withColumn("user_session", regexp_replace(col("user_session"), r"\.", " "))

# Convert event_time to Timestamp
df = df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

# Step 4: Load Data into DuckDB
db_path = "C:\\Users\\Postlab\\Desktop\\po\\ecommerce.db"
conn = duckdb.connect(db_path)

# Create Table Schema (if not exists)
create_table_query = """
CREATE TABLE IF NOT EXISTS ecommerce_data (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id BIGINT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price FLOAT,
    user_id BIGINT,
    user_session TEXT
);
"""
conn.execute(create_table_query)

# Convert PySpark DataFrame to Pandas (DuckDB requires Pandas for inserts)
df_pandas = df.toPandas()

# Insert Data into DuckDB
conn.execute("INSERT INTO ecommerce_data SELECT * FROM df_pandas")

# Step 5: Export DuckDB Data to Parquet for Power BI/Tableau
parquet_path = "C:\\Users\\Postlab\\Desktop\\po\\ecommerce.parquet"
conn.execute(f"COPY ecommerce_data TO '{parquet_path}' (FORMAT 'parquet');")

# Close connections
conn.close()
spark.stop()

# Step 6: Load the Data into Power BI using Python Script

# Connect to DuckDB and query the table (Power BI's Python script integration)
conn = duckdb.connect('C:/Users/Postlab/Desktop/po/ecommerce.db')

# Query the table
query = "SELECT * FROM ecommerce_data"
df = conn.execute(query).fetchdf()

# Return the dataframe to Power BI
df

# Step 7: Create Visualizations with Plotly (Optional for Power BI)
# Sales by Brand
brand_sales = df.groupby('brand')['price'].sum().reset_index()
fig1 = px.bar(brand_sales, x='brand', y='price', title='Sales by Brand')
fig1.update_layout(xaxis_title='Brand', yaxis_title='Total Sales')
fig1.show()

# Scatter plot of Price vs Sales by Category
fig2 = px.scatter(df, x='price', y='category_id', title='Price vs Sales by Category', labels={'price': 'Price', 'category_id': 'Category'})
fig2.show()

# Pie chart of product category share
category_share = df.groupby('category_code')['price'].sum().reset_index()
fig3 = px.pie(category_share, names='category_code', values='price', title='Product Category Share')
fig3.update_traces(textinfo='percent+label', pull=[0.5, 0.5, 0.5, 0.5, 0.5])  
fig3.show()

# Customer Segmentation by Spending
customer_spending = df.groupby('user_id')['price'].sum().reset_index()
fig4 = px.histogram(customer_spending, x='price', nbins=50, title='Customer Segmentation by Spending', labels={'price': 'Total Spending'})
fig4.show()

# Sales Trend Over Time
df['date'] = df['event_time'].dt.date  # Extract date from timestamp
sales_trend = df.groupby('date')['price'].sum().reset_index()
fig5 = px.line(sales_trend, x='date', y='price', title='Sales Trend Over Time', labels={'price': 'Total Sales'})
fig5.update_layout(xaxis_title='Date', yaxis_title='Sales')
fig5.show()

print(f"ETL Pipeline Completed Successfully!")
