from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder.appName("FactDimensionLoad").getOrCreate()

# Load customer dimension from csv
# ----------------------------------
customer_df = spark.read.csv("/Users/ad/Downloads/customer_dim.csv",header=True,inferSchema=True)
customer_df.printSchema()
print("\n Customer Dimension Table.\n")
customer_df.show(truncate=False)


# Load product dimension from csv
# -------------------------------
product_df = spark.read.csv("/Users/ad/Downloads/product_dim.csv",header=True,inferSchema=True)
product_df.printSchema()
print("\n Product Dimension Table.\n")
product_df.show(truncate=False)

# Load raw orders data from dsv
# ------------------------------
raw_order_df = spark.read.csv("/Users/ad/Downloads/order_details.csv",header=True,inferSchema=True)
raw_order_df.printSchema()
raw_order_df.show(truncate=False)


# Load the date dimension 2025 - 2026
# -----------------------------------
def generate_date_dimension(start_date="2025-01-01", end_date="2026-12-31"):
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    date_data = []
    date_sk = 1

    current = start
    while current <= end:
        date_data.append((
            date_sk,
            current.strftime("%Y-%m-%d"),
            int(current.strftime("%Y")),
            int(current.strftime("%m")),
            current.strftime("%B"),
            int(current.strftime("%d")),
            current.strftime("%A"),
            current.weekday() + 1,  # Monday=1, Sunday=7
            1 if current.weekday() < 5 else 0,  # Is weekday
            1 if current.weekday() in [5, 6] else 0  # Is weekend
        ))
        current += datetime.timedelta(days=1)
        date_sk += 1

    return date_data


date_data = generate_date_dimension()
date_schema = StructType([
    StructField("date_sk", IntegerType(), False),
    StructField("full_date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_weekday", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True)
])

df_date = spark.createDataFrame(date_data, schema=date_schema)
print("\n Date Dimension Table.\n")
df_date.show(5, truncate=False)

fact_order_df = raw_order_df\
    .join(customer_df.select('customer_id', 'customer_sk'), on='customer_id', how='left')\
    .join(product_df.select('product_id', 'product_sk'), on='product_id', how='left')\
    .join(df_date, (raw_order_df.date_id == df_date.full_date), how='left')\
    .select(
    'order_id',
    'customer_sk',
    'product_sk',
    'date_sk',
    'quantity',
    'unit_price',
    'discount',
    'status'
)

fact_order_df.show(5, truncate=False)

# Product category wise sales.
#______________________________

cat_sales_df = fact_order_df\
    .filter(fact_order_df.status == 'COMPLETED')\
    .join(product_df, (fact_order_df.product_sk == product_df.product_sk), how='inner')\
    .groupby('category')\
    .agg(sum(col('unit_price')-col('discount')).alias('total_amount'))\
    .select(
    'category',
    'total_amount'
)
print("\n Category wise sales/revenue.\n")
cat_sales_df.show(5, truncate=False)

# Highest revenue generating customers.
# -------------------------------------

top_customers_df = fact_order_df.alias('f') \
    .filter(col('f.status') == 'COMPLETED') \
    .join(customer_df.alias('c'), col('f.customer_sk') == col('c.customer_sk'), 'inner') \
    .groupBy(col('c.customer_sk'), col('c.customer_name')) \
    .agg(
        sum(col('f.unit_price') - col('f.discount')).alias('total_amount'),
        avg(col('f.unit_price')).alias('avg_price'),
        max(col('f.unit_price')).alias('max_price'),
        min(col('f.unit_price')).alias('min_price')
    )
print("\n Top Customer Analysis.\n")
top_customers_df.show(5, truncate=False)
