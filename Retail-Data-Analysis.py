# Databricks notebook source
# MAGIC %md
# MAGIC Retail Sales Project
# MAGIC Business Requirements:
# MAGIC we have superstore sample reltail data and our stakeholder wants to track the below Business metrics
# MAGIC 1. How many total number of customers we have?
# MAGIC 2. total number of orders we have
# MAGIC 3. Total number of sales as of now
# MAGIC 4. Total profit
# MAGIC 5. Top sales by country
# MAGIC 6. Most profitable region and country
# MAGIC 7. Top sales category products
# MAGIC 8. Top 10 sales sub category products
# MAGIC 9. Most ordered quantity product
# MAGIC 10. Top customer based on sales, city
# MAGIC Note: dashboard/Notebook should be refresh based on weekly and monthly basis

# COMMAND ----------

dbutils.widgets.dropdown("time_Period",'Weekly',['Weekly','Monthly'])

# COMMAND ----------

from datetime import date,timedelta,datetime
from pyspark.sql.functions import * 


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

time_period= dbutils.widgets.get("time_Period")
print(time_period)
today = date.today()

if time_period=='Weekly':
    start_date=today-timedelta(days=today.weekday(),weeks=1)-timedelta(days=1)
    end_date=start_date+timedelta(days=6)

else:
    first=today.replace(day=1)
    end_date=first-timedelta(days=1)
    start_date=first-timedelta(days=end_date.day)

print(start_date,end_date)

# COMMAND ----------

/FileStore/tables/SuperStore/superstore.csv

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/SuperStore/superstore.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

display(spark.sql(f"""select distinct Customer_id from sample where order_date between '{start_date}' and '{end_date}'"""))

# COMMAND ----------

select =df.select (col("Customer_id")).distinct().count()
print(f"count of customer: {select}")

# COMMAND ----------

display(spark.sql(f""" select count(distinct order_id) from sample where order_date between '{start_date}' and 'end_date' """))

# COMMAND ----------

display(spark.sql(f""" select sum(Sales), sum(Profit) from sample"""))

# COMMAND ----------

reasult=df.groupby("Country")\
    .agg(sum("Sales"))\
    .orderBy(col("Country").desc())
reasult.show()


# COMMAND ----------

display(spark.sql(f""" select sum(Sales),Country from sample group by Country"""))

# COMMAND ----------

reasult2=df.groupBy("Country","Region")\
            .agg(sum("sales").alias("Total_Sales"))\
            .orderBy(col("Total_Sales").desc())

reasult2.show()



# COMMAND ----------

display(spark.sql(f""" select sum(sales)total_sales,Country , Region from sample group by Country, Region order by total_sales desc"""))

# COMMAND ----------

reasult3=df.groupBy("Category")\
            .agg(sum("sales").alias("Total_Sales"))\
            .orderBy(col("Total_Sales").desc())

reasult3.show()



# COMMAND ----------

display(spark.sql(f""" select sum(sales)total_sales,Category from sample group by Category order by total_sales desc """))

# COMMAND ----------

reasult4=df.groupby(col("Sub_Category"))\
            .agg(sum(col("Sales")).alias("total_Sales"))\
             .orderBy(col("Total_Sales").desc())
reasult4.show()

# COMMAND ----------

display(spark.sql(f""" select sum(sales)total_sales,Sub_Category from sample group by Sub_Category order by total_sales desc limit 10 """))

# COMMAND ----------

r=spark.sql(f""" select sum(Quantity)total_quentity, Product_Name from sample group by Product_Name order by 1 desc""")
r.show()

# COMMAND ----------

display(spark.sql(f""" select sum(Sales)total_Sales, Customer_Name,City from sample group by city,Customer_Name order by  1 desc      """))
