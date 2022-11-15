#importing modules
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date,col,expr,unix_timestamp,year,col
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import collect_set,when,concat_ws
import sys
#from pyspark.sql.functions import col

print("modules imported")

#creating spark session
#Other values can be given as required for configuration , for eg no of cores , shuffle partitions etc
spark = SparkSession\
.builder\
.appName("analysis for customers and transactions")\
.config("spark.driver.memory","20g")\
.config("spark.executor.memory","6g")\
.config("spark.speculation","true")\
.config("spark.driver.memoryOverhead","3072")\
.config("spark.driver.maxResultSize","12g")\
.enableHiveSupport()\
.getOrCreate()

print("spark session created")
spark.sparkContext.setLogLevel("OFF")
spark.conf.set("spark.sql.legacy.timeParserPolicy","legacy")
file_location = "/FileStore/tables/customer.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_customers = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table_name = "customer_csv"

df_customers = df_customers.withColumnRenamed("loyal_customer","loyal_customer_original")


schema_transactions = StructType()\
.add("CustomerID",StringType(),True)\
.add("Product line",StringType(),True)\
.add("Unit price",DoubleType(),True)\
.add("Quantity",IntegerType(),True)\
.add("Tax 5%",DoubleType(),True)\
.add("Total",DoubleType(),True)\
.add("Date_creation",StringType(),True)\
.add("Time",StringType(),True)\
.add("Payment",StringType(),True)\
.add("cogs",DoubleType(),True)\
.add("gross margin percentage",DoubleType(),True)\
.add("gross income",DoubleType(),True)\
.add("Rating",DoubleType(),True)

# File location and type for transactions 
file_location = "/FileStore/tables/transactions.csv"
file_type = "csv"

# CSV options
infer_schema = "False"
first_row_is_header = "True"
delimiter = ","


# The applied options are for CSV files. For other file types, these will be ignored.
df_transactions = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .schema(schema_transactions) \
  .load(file_location)

#display(df_transactions) 	
temp_table_name = "transactions_csv"

df_transactions.printSchema()
#to drop any rows without customer ID , taking it like primary key
df_transactions.dropna(subset="CustomerID")

# getting the date , year , month and day of transation
df2 = df_transactions.withColumn("check_dt",date_format(to_date(df_transactions.Date_creation,'MM/dd/yyyy'),'yyyy-MM-dd')).withColumn("year_transact",year('check_dt')).withColumn("month_transact",month('check_dt')).withColumn("day_transact",date_format(col("check_dt"), "EEEE"))

# to do the windowing based on customer id , year , month . This is being done to get the sum of amounts for each customer in a month . We are getting the loyalty based on if any customer does transations of more than 1000 for each month. So Customer is loyal , if all the months ,he/she does transactions greater than 1000.
# if any month transaction is less than 1000 , he/she is not loyal.
row_numbers_win = Window.partitionBy(df2.CustomerID,df2.year_transact,df2.month_transact).orderBy(df2.month_transact)
df3 = df2.withColumn("sum_month",sum(df2.Total).over(row_numbers_win))

#this temp_loyal_customer flag is added for each month and each customer.
df4 = df3.withColumn("temp_loyal_customer",when(df3.sum_month > 1000,"True").otherwise("False"))

df5 = df4.groupBy("CustomerID").agg(collect_set("temp_loyal_customer").alias("distinct_loyal"))
#df6 = df5.withColumn("loyal_customer_list",when(col(df5.distinct_loyal).contains(","),"True").otherwise("False"))
df_loyal = df5.withColumn("loyal_customer",~concat_ws(",",col("distinct_loyal")).contains(",")).drop("distinct_loyal").distinct()

df_joined = df4.join(df_loyal,['CustomerID'],how="inner")

# for filtering out customers who are younger than 20 years
df_customers = df_customers.filter(col("age")>= 20)
# if length of postcode is greater than 4 , it is masked
df_customers = df_customers.withColumn("masked_postcode",when(length(df_customers.postcode)>5,lit("******")).otherwise(df_customers.postcode))

# udf this is used to create bucket column based on age
def categorizer(age):
  if age < 25:
    return '[20-24]'
  elif age < 30:
    return '[25-29]'
  elif age < 35:
    return '[30-34]'
  elif age < 40:
    return '[35-39]'
  else: 
    return '[40-]'

bucket_udf = udf(categorizer, StringType() )
df_customers_bucketed = df_customers.withColumn("bucket", bucket_udf("age"))

#to get the customers with no transactions , leftanti join is used on the same
df_joined_no_trans = df_joined.select("CustomerID").distinct()
df_customers_distinct = df_customers.select("person_id").distinct()
#df_customers_distinct.count()
#df_joined_no_trans.count()
join_condition_1 = [col('a.person_id') == col('b.CustomerID')]
df_customers_no_transaction = df_customers_distinct.alias("a").join(df_joined_no_trans.alias("b"),join_condition_1,how="leftanti")

#to join the 2 df and eliminate customers with no transactions , inner join eliminates customers with no transactions
join_condition_2 = [col('a.person_id') == col('b.CustomerID')]
df_joined = df_customers.alias("a").join(df_joined.alias("b"),join_condition_2,how="inner")
df_joined = df_joined.distinct()

# customers spend based on days for week from Sunday to Saturday
df_spend_based_on_days = df_joined.groupBy(df_joined.day_transact)\
.agg(sum(df_joined.Total).alias("days"))
df_spend_based_on_days.sortWithinPartitions("day_transact")

#for transactions happening on wednesday, it is marked as 99.00 if value of total is greater than 100
df_wednesday = df_joined.withColumn("new_amount",when((df_joined.day_transact=="Wednesday") & (df_joined.Total>=100) ,lit(99.00)).otherwise(df_joined.Total))

#modifying the value of time based on s round down by 15 min periods. Example: 
#19:21 - becomes 19:15, 17:36 - becomes 17:30, 10:12 - becomes 10:00, 12:50 -
#becomes 12:45 
df_time_modified = df_wednesday.withColumn('first_time_part',split(df_wednesday['Time'],':').getItem(0))\
.withColumn('second_time_part',split(df_wednesday['Time'],':').getItem(1))\
.withColumn('second_time_modified',expr("CASE WHEN second_time_part >='00' and second_time_part<='14' THEN '00' " +
           "WHEN second_time_part >='15' and second_time_part<='29' THEN '15'  WHEN second_time_part >='30' and second_time_part<='44' THEN '30'" +
           "ELSE '45' END"))\
.withColumn("new_time",expr("concat(first_time_part,':',second_time_modified)"))

df_time_modified.printSchema()
#dropping the temporary columns as required
df_time_final = df_time_modified.drop("check_dt","year_transact","month_transact","day_transact","sum_month","temp_loyal_customer","first_time_part","second_time_part","second_time_modified","Total")

df_time_final.withColumnRenamed("Product line","Product_line")

df_time_final.write.mode('Overwrite').option("header",True)\
        .option("delimiter",",")\
        .csv("transactions_final.csv")
		
spark.sql("drop table if exists customers_bucketed")
# below command can be used to write to metastore
#df_customers_bucketed.write\
#    .mode('overwrite')\
#    .bucketBy(5, 'bucket') \
#    .saveAsTable('customers_bucketed', format='parquet')

#file-path = "/FileStore/tables/customer_output.csv"

df_customers_bucketed.write.mode('Overwrite').option("header",True)\
        .option("delimiter",",")\
        .csv("customer_output1.csv")
	
