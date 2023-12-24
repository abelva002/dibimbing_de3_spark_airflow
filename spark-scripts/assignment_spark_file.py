# Import Library
import pyspark
import os

from dotenv import load_dotenv
from pathlib import Path

# Load Path (Does not work)
# dotenv_path = Path('/resources/.env')
# load_dotenv(dotenv_path=dotenv_path)

# Get Postgres Information (Does not work)
# postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
# postgres_db = os.getenv('POSTGRES_DB')
# postgres_user = os.getenv('POSTGRES_USER')
# postgres_password = os.getenv('POSTGRES_PASSWORD')

# Create Spark Session
spark = pyspark.sql.SparkSession.builder \
        .appName('day15_assignment') \
        .config('spark.jars', '/jars/postgresql-42.2.27.jar') \
        .master('spark://dataeng-spark-master:7077') \
        .getOrCreate()

# Form JDBC Parameters
jdbc_url = f'jdbc:postgresql://dataeng-postgres/postgres_db'
jdbc_properties = {
    'user': 'user',
    'password': 'password',
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read Retail DataFrame
retail_df = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

# Analysis 1:
# Top spending countries
# Getting the top spending countries can help determine how active each countries are as well as see where the biggest purchases are coming from
# and countries that need improvements in terms of strategy, marketing, etc. to improve sales on the region. Top Countries are mostly
# populated by European Countries with some others like Australia and Japan occasionally.

retail_df.createOrReplaceTempView('retail')
spark.sql('''
    SELECT
        country,
        ROUND(SUM(quantity * unitprice), 2) AS total_spendings,
        RANK() OVER(ORDER BY SUM(quantity * unitprice) DESC) AS ranking
    FROM
        retail
    GROUP BY
        country
    ORDER BY
        total_spendings DESC
''').show()

# Analysis 2:
# Top 10 spending customers
# Knowing top spenders are important so you can keep those users in the ecosystem.
# Giving them top spenders exclusive benefits could lower the chance of them churning and give them more incentive to spend.

retail_df.createOrReplaceTempView('retail')
spark.sql('''
    SELECT
        customerid,
        ROUND(SUM(quantity * unitprice), 2) AS total_spendings,
        RANK() OVER(ORDER BY SUM(quantity * unitprice) DESC) AS ranking
    FROM
        retail
    WHERE
        customerid IS NOT NULL
    GROUP BY
        customerid
    ORDER BY
        total_spendings DESC
    LIMIT 10
''').show()

# Analysis 3:
# Top customers by frequency
# As with top spenders in monetary value, getting to know your most frequent spenders is also beneficial.
# Similarly you could give them special offers to encourage them to spend more often or offer membership program where for a set amount
# of cost each month/year, the could get some % of their bill. It will at least interest them if they spend often.

retail_df.createOrReplaceTempView('retail')
spark.sql('''
    SELECT
        customerid,
        COUNT(DISTINCT invoiceno) AS times_shopped,
        RANK() OVER(ORDER BY COUNT(DISTINCT invoiceno) DESC) AS ranking
    FROM
        retail
    WHERE
        customerid IS NOT NULL
    GROUP BY
        customerid
    ORDER BY
        times_shopped DESC
    LIMIT 10
''').show()

# Analysis 4:
# Most popular item per country
# Getting to know the most popular item in each country could be beneficial because you can see where the profits are coming from
# in each countries. This information could also be used to hold certain campaign like for example if users bought these popular items
# up to a certain amount, they could be eligible for discount of other items which could improve sales from another product as well.

retail_df.createOrReplaceTempView('retail')
spark.sql('''
    WITH ranked_items AS (
        SELECT
            country,
            description AS item_name,
            SUM(quantity) AS numbers_sold,
            RANK() OVER(PARTITION BY country ORDER BY SUM(quantity) DESC) AS ranking
        FROM
            retail
        WHERE
            customerid IS NOT NULL
        GROUP BY
            country,
            description
        ORDER BY
            country
    )

    SELECT
        country,
        item_name,
        numbers_sold,
        ranking
    FROM
        ranked_items
    WHERE
        ranking IN (1,2)
''').show()

# Analysis 5:
# Monthly popular items
# Getting to know the most profitable items per month can be beneficial as it allows you to see what sold the most in any given month.
# As with the Items per Country, similar campaign could be conducted to improve sales.

retail_df.createOrReplaceTempView('retail')
spark.sql('''
    WITH ranked_items AS (
        SELECT
            DATE_PART('month', invoicedate) AS month_number,
            description AS item_name,
            SUM(quantity) AS numbers_sold,
            RANK() OVER(PARTITION BY DATE_PART('month', invoicedate) ORDER BY SUM(quantity) DESC) AS ranking
        FROM
            retail
        WHERE
            customerid IS NOT NULL
        GROUP BY
            DATE_PART('month', invoicedate),
            description
        ORDER BY
            month_number
    )

    SELECT
        month_number,
        item_name,
        numbers_sold,
        ranking
    FROM
        ranked_items
    WHERE
        ranking IN (1,2)
''').show()