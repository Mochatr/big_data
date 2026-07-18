from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("BikeSharingSparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


# 1. Data Loading & Exploration
section("1.1 Load CSV into a DataFrame")
df = spark.read.csv("data/bike_sharing.csv", header=True, inferSchema=True)
print("Loaded data/bike_sharing.csv")

section("1.2 Schema")
df.printSchema()

section("1.3 First 5 rows")
df.show(5, truncate=False)

section("1.4 Number of rentals")
print("Total rentals:", df.count())

# 2. Temporary View
df.createOrReplaceTempView("bike_rentals_view")

# 3. Basic SQL Queries
section("3.1 Rentals longer than 30 minutes")
spark.sql(
    "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
).show(10, truncate=False)

section("3.2 Rentals starting at 'Station A'")
spark.sql(
    "SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'"
).show(10, truncate=False)

section("3.3 Total revenue")
spark.sql(
    "SELECT SUM(price) AS total_revenue FROM bike_rentals_view"
).show()

# 4. Aggregation Queries
section("4.1 Rentals per start station")
spark.sql(
    """
    SELECT start_station, COUNT(*) AS nb_rentals
    FROM bike_rentals_view
    GROUP BY start_station
    ORDER BY nb_rentals DESC
    """
).show(50, truncate=False)

section("4.2 Average rental duration per start station")
spark.sql(
    """
    SELECT start_station, ROUND(AVG(duration_minutes), 2) AS avg_duration
    FROM bike_rentals_view
    GROUP BY start_station
    ORDER BY avg_duration DESC
    """
).show(50, truncate=False)

section("4.3 Station with the highest number of rentals")
spark.sql(
    """
    SELECT start_station, COUNT(*) AS nb_rentals
    FROM bike_rentals_view
    GROUP BY start_station
    ORDER BY nb_rentals DESC
    LIMIT 1
    """
).show(truncate=False)

# 5. Time-Based Analysis
section("5.1 Extract hour from start_time")
spark.sql(
    "SELECT rental_id, start_time, HOUR(start_time) AS start_hour FROM bike_rentals_view"
).show(5, truncate=False)

section("5.2 Rentals per hour (peak hours)")
spark.sql(
    """
    SELECT HOUR(start_time) AS start_hour, COUNT(*) AS nb_rentals
    FROM bike_rentals_view
    GROUP BY start_hour
    ORDER BY start_hour
    """
).show(24)

section("5.3 Most popular start station in the morning (7-12)")
spark.sql(
    """
    SELECT start_station, COUNT(*) AS nb_rentals
    FROM bike_rentals_view
    WHERE HOUR(start_time) BETWEEN 7 AND 12
    GROUP BY start_station
    ORDER BY nb_rentals DESC
    LIMIT 1
    """
).show(truncate=False)

# 6. User Behavior Analysis
section("6.1 Average age of users")
spark.sql("SELECT ROUND(AVG(age), 2) AS avg_age FROM bike_rentals_view").show()

section("6.2 Users by gender")
spark.sql(
    """
    SELECT gender, COUNT(DISTINCT user_id) AS nb_users
    FROM bike_rentals_view
    GROUP BY gender
    """
).show()

section("6.3 Age group that rents the most")
spark.sql(
    """
    SELECT
        CASE
            WHEN age BETWEEN 18 AND 30 THEN '18-30'
            WHEN age BETWEEN 31 AND 40 THEN '31-40'
            WHEN age BETWEEN 41 AND 50 THEN '41-50'
            ELSE '51+'
        END AS age_group,
        COUNT(*) AS nb_rentals
    FROM bike_rentals_view
    GROUP BY age_group
    ORDER BY nb_rentals DESC
    """
).show()

spark.stop()
