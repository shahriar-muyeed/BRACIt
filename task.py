from pyspark.sql import SparkSession

# 1 : Starting a Spark Session
spark = SparkSession.builder.appName("WalmartStock").getOrCreate()

# Loading the Walmart CSV in Spark session
df = spark.read.csv("Walmart.csv", header=True, inferSchema=True)

# 2 : Showing the dataframe schema to confirm data types were inferred
df.printSchema()

# Filtering the df from 2012-2017 by converting date string to datetime
from pyspark.sql.functions import to_date , year

df = df.withColumn("Date", to_date(df["Date"], "dd-MM-yyyy"))
df_filtered = df.filter((year(df["Date"]) >= 2012) & (year(df["Date"]) <= 2017))


# 3 : What is the max and min of the Volume column? 
# Max and Min of Volume column. There are no Volume column in the csv but I think it is referring to the weekly sales column
print("Printing 3 : ")
from pyspark.sql.functions import max, min
try:
    max_Weekly_Sales = df_filtered.select(max("Weekly_Sales")).collect()[0][0]
    min_Weekly_Sales = df_filtered.select(min("Weekly_Sales")).collect()[0][0]

    print(f"Max Weekly_Sales: {max_Weekly_Sales}")
    print(f"Min Weekly_Sales: {min_Weekly_Sales}")
    print()
except Exception as e:
    print(f"Some error in 3 : {e}")


# 4:  How many days was the Close lower than 60 dollars? 
# Looking at the data it seems the Temperature coloumn can the data like 60
print("Printing 4 : ")
try:
    days_below_60 = df_filtered.filter(df_filtered["Temperature"] < 60).count()

    print(f"Number of days with Temperature lower than 60 degrees: {days_below_60}")

except Exception as e:
    print(f"Some error in 4 : {e}")
print()


# 5: What is the Pearson correlation between High and Volume?
#  Considering the "Weekly_sales" as Volume and "CPI" as High
print("Printing 5 : ")
from pyspark.sql.functions import corr

try:
    correlation = df_filtered.select(corr("Weekly_sales", "CPI")).collect()[0][0]

    print(f"Pearson Correlation between CPI and Weekly_sales: {correlation}")
    print()
except Exception as e:
    print(f"Some error in 5 : {e}")


# 6 : What day had the Peak High in Price?
# The only price value is in "Fuel_Price": 
print("Printing 6 : ")
from pyspark.sql.functions import corr

try:
    peak_high_day = df_filtered.orderBy(df_filtered["Fuel_Price"].desc()).head(1)[0]["Date"]

    print(f"Day with the peak high in price: {peak_high_day}")
    print()  
except Exception as e:
    print(f"Some error in 6 : {e}")

#7. What is the average Close for each Calendar Month?
# considering temp as Close
print("Printing 7 : ")
from pyspark.sql.functions import month, avg
try:

    #creating a df for Month
    df_with_month = df_filtered.withColumn("Month", month("Date"))

    monthly_avg_close = df_with_month.groupBy("Month").agg(avg("Temperature").alias("AvgTemperature")).orderBy("Month")

    monthly_avg_close.show()
    print()

except Exception as e:
    print(f"Some error in 7 : {e}")
