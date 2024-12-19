# Databricks notebook source
# Configuration for Azure Storage Account
storage_account_name = "telcopracticefaa"  # Replace with your storage account name
storage_account_key = "v1+Swk/36i3Ji2hpOFluC79ImPjbxZgDwXHkSYBQXAv4drITAZ9jhJuKJ0qXOgdCxq+pGGeGjgq++AStlVTUsQ=="  # Replace with your storage account key
container_name = "silver"  # Replace with your container name

# Set Spark configuration
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Allrounder, Batters,Bowlers, DataFrame**
# MAGIC
# MAGIC This section provides all transformations and KPIs. This portion contains data in the Gold Layer.
# MAGIC

# COMMAND ----------

# Corrected code to read Delta format
gold_allrounder_df = spark.read.format("delta").load("abfss://silver@telcopracticefaa.dfs.core.windows.net/allrounders_silver")
gold_allrounder_df.display()


# COMMAND ----------

# Corrected code to read Delta format
gold_batters_df = spark.read.format("delta").load("abfss://silver@telcopracticefaa.dfs.core.windows.net/batters_silver")
gold_batters_df.display()


# COMMAND ----------

# Corrected code to read Delta format
gold_bowlers_df = spark.read.format("delta").load("abfss://silver@telcopracticefaa.dfs.core.windows.net/bowlers_silver")
gold_bowlers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Allrounder, Batters,Bowlers, Delta table Creation**
# MAGIC
# MAGIC  This portion contains tables in the Gold Layer.
# MAGIC

# COMMAND ----------

# Create the schema (database) using PySpark
spark.sql("CREATE DATABASE IF NOT EXISTS pakistancricket")

# Write the Gold DataFrames as Delta tables in the pakistancricket schema
gold_bowlers_df.write.format("delta").mode("overwrite").saveAsTable("pakistancricket.bowlers")
gold_batters_df.write.format("delta").mode("overwrite").saveAsTable("pakistancricket.batters")
gold_allrounder_df.write.format("delta").mode("overwrite").saveAsTable("pakistancricket.allrounders")



# COMMAND ----------

# Reading the Delta tables for Batters, Bowlers, and Allrounders
final_batters_df = spark.read.table("pakistancricket.batters")
final_bowlers_df = spark.read.table("pakistancricket.bowlers")
final_allrounders_df = spark.read.table("pakistancricket.allrounders")


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Batters, KPI Creation**
# MAGIC
# MAGIC  This portion contains KPI in the Gold Layer, which are going to store in gen2.
# MAGIC

# COMMAND ----------

# Calculate Batting Average
batting_avg_df = final_batters_df.groupBy("player_name").agg(
    (sum("runs_scored") / count("innings_played")).alias("batting_avg")
)

# Calculate Strike Rate
strike_rate_df = gold_batters_df.groupBy("player_name").agg(
    (sum("runs_scored") / sum("balls_faced") * 100).alias("strike_rate")
)

# Calculate Boundary Percentage
boundary_percentage_df = gold_batters_df.groupBy("player_name").agg(
    (sum("boundaries") / sum("runs_scored") * 100).alias("boundary_percentage")
)

# Join all the KPI DataFrames for Batters
batters_kpi_df = batting_avg_df.join(strike_rate_df, "player_name")\
                               .join(boundary_percentage_df, "player_name")

# Write the Batting KPIs to Delta table
batters_kpi_df.write.format("delta").mode("overwrite").saveAsTable("pakistancricket.batters_kpi")
# Batting KPIs for batters
batting_avg_df = final_batters_df.groupBy("battingName").agg(
    (sum("total") / count("battingName")).alias("batting_avg")
)

strike_rate_df = final_batters_df.groupBy("battingName").agg(
    (sum("total") / sum("balls_faced") * 100).alias("strike_rate")
)

boundary_percentage_df = final_batters_df.groupBy("battingName").agg(
    (sum("boundaries") / sum("total") * 100).alias("boundary_percentage")
)

# Combine Batting KPIs into one DataFrame
batters_kpi_df = batting_avg_df.join(strike_rate_df, "battingName")\
                               .join(boundary_percentage_df, "battingName")

# Writing Batting KPIs to Delta Table
batters_kpi_df.write.format("delta").mode("overwrite").saveAsTable("pakistancricket.batters_kpi")
from pyspark.sql import functions as F
from datetime import datetime

# 1. Total Batters by Playing Role
total_batters_by_role = final_batters_df.groupBy("playing_role").count()
total_batters_by_role.show()

# 2. Percentage of Batters in Each Playing Role
total_batters_count = final_batters_df.count()
percentage_batters_by_role = total_batters_by_role.withColumn(
    "percentage", 
    (F.col("count") * 100) / total_batters_count
)
percentage_batters_by_role.show()

# 3. Top Playing Role by Player Count
top_playing_role = total_batters_by_role.orderBy("count", ascending=False).limit(1)
top_playing_role.show()

# 4. Distribution of Batting Roles
distribution_batting_roles = total_batters_by_role.orderBy("playing_role")
distribution_batting_roles.show()

# 5. Count of Batters in Specific Playing Roles
specific_playing_roles = final_batters_df.filter(
    final_batters_df.playing_role.isin('middle-order batter', 'opening batter', 'top-order batter', 'wicketkeeper batter')
).groupBy("playing_role").count()
specific_playing_roles.show()

# 6. Average Age by Playing Role
current_date = datetime.now().date()
final_batters_df = final_batters_df.withColumn(
    "age", 
    (F.datediff(F.lit(current_date), F.col("birth_date")) / 365).cast("int")
)
average_age_by_role = final_batters_df.groupBy("playing_role").agg(
    F.avg("age").alias("average_age")
)
average_age_by_role.show()



# COMMAND ----------

# MAGIC %md
# MAGIC **Total Batters by Playing Role**

# COMMAND ----------

#Total Batters by Playing Role
from pyspark.sql import functions as F
from datetime import datetime

# Grouping by playing role and counting players in each role
total_batters_by_role = final_batters_df.groupBy("playing_role").count()

# Show the result
total_batters_by_role.schema


# COMMAND ----------

# MAGIC %md
# MAGIC **Age of Players**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Ensure the birth_date column is in the correct Date format (if not already)
final_batters_df = final_batters_df.withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))

# Get the current date
current_date = F.current_date()

# Calculate age by subtracting birth year from current year and adjusting for the day and month
final_batters_df = final_batters_df.withColumn(
    "age", 
    F.floor((F.datediff(current_date, F.col("birth_date")) / 365.25))  # Dividing by 365.25 to account for leap years
)

# Show the DataFrame with the new age column
# Select specific columns and display them
final_batters_df.select("player_name", "age", "batting_style").show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Percentage of Batters in Each Playing Role**

# COMMAND ----------

# Total number of batters
total_batters_count = final_batters_df.count()

# Calculating the percentage of batters for each playing role
percentage_batters_by_role = total_batters_by_role.withColumn(
    "percentage_batters_by_role", 
    (F.col("count") * 100) / total_batters_count
)

# Show the result
percentage_batters_by_role.schema


# COMMAND ----------

# MAGIC %md
# MAGIC **Total Count and Percentage of Players by Gender**

# COMMAND ----------

# Group by gender and count the number of players for each gender
total_batters_by_gender = final_batters_df.groupBy("gender").count()

# Calculate percentage of each gender based on total batters
total_batters_count = final_batters_df.count()
percentage_batters_by_gender = total_batters_by_gender.withColumn(
    "percentage_batters_by_gender", 
    (F.col("count") * 100) / total_batters_count
)

# Show the result
percentage_batters_by_gender.schema


# COMMAND ----------

# MAGIC %md
# MAGIC **Batting style and count the number of players for each style**

# COMMAND ----------

# Group by batting style and count the number of players for each style
total_batters_by_batting_style = final_batters_df.groupBy("batting_style").count()

# Calculate percentage of each batting style based on total batters
percentage_batters_by_batting_style = total_batters_by_batting_style.withColumn(
    "percentage_batters_by_batting_style", 
    (F.col("count") * 100) / total_batters_count
)

# Show the result
percentage_batters_by_batting_style.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Average age for each batting style**

# COMMAND ----------

# Calculate average age for each batting style
average_age_by_batting_style = final_batters_df.groupBy("batting_style").agg(
    F.avg("age").alias("average_age")
)

# Show the result
average_age_by_batting_style.show()


# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import DateType

# Ensure the birth_date column is in the correct Date format (if not already)
final_batters_df = final_batters_df.withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))

# Get the current date
current_date = F.current_date()

# Calculate age by subtracting birth year from current year and adjusting for the day and month
final_batters_df = final_batters_df.withColumn(
    "age", 
    F.floor((F.datediff(current_date, F.col("birth_date")) / 365.25))  # Dividing by 365.25 to account for leap years
)

# Show the DataFrame with the new age column
final_batters_df.select("playerId", "player_name", "age", "batting_style").show()

# Total number of batters
total_batters_count = final_batters_df.count()

# KPI 1: Total Batters by Playing Role
total_batters_by_role = final_batters_df.groupBy("playerId", "playing_role").count()

# KPI 2: Percentage of Batters by Role
percentage_batters_by_role = total_batters_by_role.withColumn(
    "percentage_batters_by_role", 
    (F.col("count") * 100) / total_batters_count
)

# KPI 3: Percentage of Batters by Gender
total_batters_by_gender = final_batters_df.groupBy("playerId", "gender").count()
percentage_batters_by_gender = total_batters_by_gender.withColumn(
    "percentage_batters_by_gender", 
    (F.col("count") * 100) / total_batters_count
)

# KPI 4: Percentage of Batters by Batting Style
total_batters_by_batting_style = final_batters_df.groupBy("playerId", "batting_style").count()
percentage_batters_by_batting_style = total_batters_by_batting_style.withColumn(
    "percentage_batters_by_batting_style", 
    (F.col("count") * 100) / total_batters_count
)

# KPI 5: Average Age by Batting Style
average_age_by_batting_style = final_batters_df.groupBy("playerId", "batting_style").agg(
    F.avg("age").alias("average_age")
)

# Join all the KPI DataFrames on 'playerId'
final_kpis_df = total_batters_by_role \
    .join(percentage_batters_by_role, on="playerId", how="outer") \
    .join(percentage_batters_by_gender, on="playerId", how="outer") \
    .join(percentage_batters_by_batting_style, on="playerId", how="outer") \
    .join(average_age_by_batting_style, on="playerId", how="outer")

# Show the combined DataFrame with all KPIs
final_kpis_df.display()


# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import DateType

# Ensure the birth_date column is in the correct Date format (if not already)
final_batters_df = final_batters_df.withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))

# Calculate age by subtracting birth year from current year and adjusting for the day and month
current_date = F.current_date()
final_batters_df = final_batters_df.withColumn(
    "age", 
    F.floor((F.datediff(current_date, F.col("birth_date")) / 365.25))  # Dividing by 365.25 to account for leap years
)

# Total number of batters
total_batters_count = final_batters_df.count()

# KPI 1: Total Batters by Playing Role
total_batters_by_role = final_batters_df.groupBy("playing_role").count().withColumnRenamed("count", "total_batters_by_role")

# KPI 2: Percentage of Batters by Role
percentage_batters_by_role = total_batters_by_role.withColumn(
    "percentage_batters_by_role", 
    (F.col("total_batters_by_role") * 100) / total_batters_count
)

# KPI 3: Percentage of Batters by Gender
total_batters_by_gender = final_batters_df.groupBy("gender").count().withColumnRenamed("count", "total_batters_by_gender")
percentage_batters_by_gender = total_batters_by_gender.withColumn(
    "percentage_batters_by_gender", 
    (F.col("total_batters_by_gender") * 100) / total_batters_count
)

# KPI 4: Percentage of Batters by Batting Style
total_batters_by_batting_style = final_batters_df.groupBy("batting_style").count().withColumnRenamed("count", "total_by_batting_style")
percentage_batters_by_batting_style = total_batters_by_batting_style.withColumn(
    "percentage_batters_by_batting_style", 
    (F.col("total_by_batting_style") * 100) / total_batters_count
)

# KPI 5: Average Age by Batting Style
average_age_by_batting_style = final_batters_df.groupBy("batting_style").agg(
    F.avg("age").alias("average_age")
)

# Join all the KPI DataFrames based on relevant columns
final_kpis_df = final_batters_df \
    .join(percentage_batters_by_role, "playing_role", "left") \
    .join(percentage_batters_by_gender, "gender", "left") \
    .join(percentage_batters_by_batting_style, "batting_style", "left") \
    .join(average_age_by_batting_style, "batting_style", "left")

# Selecting only relevant columns for final output
final_kpis_df = final_kpis_df.select(
    "playerId", "player_name", "playing_role", "total_batters_by_role", "percentage_batters_by_role", 
    "gender", "total_batters_by_gender", "percentage_batters_by_gender",
    "batting_style", "total_by_batting_style", "percentage_batters_by_batting_style", "age", "average_age"
)

# Show the final combined DataFrame
final_kpis_df.display()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming `final_batters_df` is the dataframe you provided

# Step 1: Count Batters by Role
role_window = Window.partitionBy("playing_role")
final_batters_df_with_counts = final_batters_df.withColumn(
    "total_batters_by_role_count", F.count("playerId").over(role_window)
)

# Step 2: Calculate the percentage of Batters by Role
final_batters_df_with_percentages = final_batters_df_with_counts.withColumn(
    "percentage_batters_by_role_calculated",
    F.col("total_batters_by_role_count") / F.count("playerId").over(Window()) * 100
)

# Step 3: Count Batters by Gender
gender_window = Window.partitionBy("gender")
final_batters_df_with_gender_counts = final_batters_df_with_percentages.withColumn(
    "total_batters_by_gender_count", F.count("playerId").over(gender_window)
)

# Step 4: Calculate the percentage of Batters by Gender
final_batters_df_with_gender_percentages = final_batters_df_with_gender_counts.withColumn(
    "percentage_batters_by_gender_calculated",
    F.col("total_batters_by_gender_count") / F.count("playerId").over(Window()) * 100
)

# Step 5: Count Batters by Batting Style
batting_style_window = Window.partitionBy("batting_style")
final_batters_df_with_batting_style_counts = final_batters_df_with_gender_percentages.withColumn(
    "total_by_batting_style_count", F.count("playerId").over(batting_style_window)
)

# Step 6: Calculate the percentage of Batters by Batting Style
final_batters_df_with_batting_style_percentages = final_batters_df_with_batting_style_counts.withColumn(
    "percentage_batters_by_batting_style_calculated",
    F.col("total_by_batting_style_count") / F.count("playerId").over(Window()) * 100
)

# Step 7: Calculate Age Distribution (example: age breakdown in ranges)
age_buckets = [20, 30, 40, 50, 60]
final_batters_df_with_age_distribution = final_batters_df_with_batting_style_percentages.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Under 30")
    .when(F.col("age") < 40, "30-39")
    .when(F.col("age") < 50, "40-49")
    .when(F.col("age") < 60, "50-59")
    .otherwise("60+")
)

# Step 8: Calculate percentage of batters in each age group
age_group_window = Window.partitionBy("age_group")
final_batters_df_with_age_percentages = final_batters_df_with_age_distribution.withColumn(
    "percentage_batters_by_age_group",
    F.count("playerId").over(age_group_window) / F.count("playerId").over(Window()) * 100
)

# Final dataframe with KPIs
final_batters_df_with_kpis = final_batters_df_with_age_percentages.select(
    "playerId", 
    "player_name", 
    "playing_role", 
    "gender", 
    "batting_style", 
    "age", 
    "percentage_batters_by_role_calculated", 
    "percentage_batters_by_gender_calculated", 
    "percentage_batters_by_batting_style_calculated",
    "percentage_batters_by_age_group"
)

# Show final results
final_batters_df_with_kpis.show()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming `final_batters_df` is the dataframe you provided

# Step 1: Count Batters by Role
role_window = Window.partitionBy("playing_role")
final_batters_df_with_counts = final_batters_df.withColumn(
    "total_batters_by_role_count", F.count("playerId").over(role_window)
)

# Step 2: Calculate the percentage of Batters by Role
window_spec = Window.orderBy("playerId")  # Correcting the window definition to be a WindowSpec
final_batters_df_with_percentages = final_batters_df_with_counts.withColumn(
    "percentage_batters_by_role_calculated",
    F.col("total_batters_by_role_count") / F.count("playerId").over(window_spec) * 100
)

# Step 3: Count Batters by Gender
gender_window = Window.partitionBy("gender")
final_batters_df_with_gender_counts = final_batters_df_with_percentages.withColumn(
    "total_batters_by_gender_count", F.count("playerId").over(gender_window)
)

# Step 4: Calculate the percentage of Batters by Gender
final_batters_df_with_gender_percentages = final_batters_df_with_gender_counts.withColumn(
    "percentage_batters_by_gender_calculated",
    F.col("total_batters_by_gender_count") / F.count("playerId").over(window_spec) * 100
)

# Step 5: Count Batters by Batting Style
batting_style_window = Window.partitionBy("batting_style")
final_batters_df_with_batting_style_counts = final_batters_df_with_gender_percentages.withColumn(
    "total_by_batting_style_count", F.count("playerId").over(batting_style_window)
)

# Step 6: Calculate the percentage of Batters by Batting Style
final_batters_df_with_batting_style_percentages = final_batters_df_with_batting_style_counts.withColumn(
    "percentage_batters_by_batting_style_calculated",
    F.col("total_by_batting_style_count") / F.count("playerId").over(window_spec) * 100
)

# Step 7: Calculate Age Distribution (example: age breakdown in ranges)
age_buckets = [20, 30, 40, 50, 60]
final_batters_df_with_age_distribution = final_batters_df_with_batting_style_percentages.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Under 30")
    .when(F.col("age") < 40, "30-39")
    .when(F.col("age") < 50, "40-49")
    .when(F.col("age") < 60, "50-59")
    .otherwise("60+")
)

# Step 8: Calculate percentage of batters in each age group
age_group_window = Window.partitionBy("age_group")
final_batters_df_with_age_percentages = final_batters_df_with_age_distribution.withColumn(
    "percentage_batters_by_age_group",
    F.count("playerId").over(age_group_window) / F.count("playerId").over(window_spec) * 100
)

# Final dataframe with KPIs
final_batters_df_with_kpis = final_batters_df_with_age_percentages.select(
    "playerId", 
    "player_name", 
    "playing_role", 
    "gender", 
    "batting_style", 
    "age", 
    "percentage_batters_by_role_calculated", 
    "percentage_batters_by_gender_calculated", 
    "percentage_batters_by_batting_style_calculated",
    "percentage_batters_by_age_group"
)

# Show final results
final_batters_df_with_kpis.display()


# COMMAND ----------

total_batters = final_batters_df_with_age_group.count()
print("Total number of batters:", total_batters)  # Output: total_batters

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming 'final_batters_df' is your DataFrame

# Step 1: Add 'age_group' column
final_batters_df_with_age_group = final_batters_df.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Under 30")
    .when((F.col("age") >= 30) & (F.col("age") <= 35), "30-35")
    .otherwise("Over 35")
)

# Calculate total number of batters
total_batters = final_batters_df_with_age_group.count()

# Step 2: Calculate the count of players by role
role_window = Window.partitionBy("playing_role")
final_batters_df_with_role_count = final_batters_df_with_age_group.withColumn(
    "role_count", F.count("playerId").over(role_window)
)

# Step 3: Calculate the percentage of batters by role
final_batters_df_with_percentages_by_role = final_batters_df_with_role_count.withColumn(
    "percentage_batters_by_role",
    (F.col("role_count") / total_batters) * 100
)

# Step 4: Percentage by Gender
gender_window = Window.partitionBy("gender")
final_batters_df_with_gender_count = final_batters_df_with_percentages_by_role.withColumn(
    "gender_count", F.count("playerId").over(gender_window)
)
final_batters_df_with_percentages_by_gender = final_batters_df_with_gender_count.withColumn(
    "percentage_batters_by_gender",
    (F.col("gender_count") / total_batters) * 100
)

# Step 5: Percentage by Batting Style
batting_style_window = Window.partitionBy("batting_style")
final_batters_df_with_batting_style_count = final_batters_df_with_percentages_by_gender.withColumn(
    "batting_style_count", F.count("playerId").over(batting_style_window)
)
final_batters_df_with_percentages_by_batting_style = final_batters_df_with_batting_style_count.withColumn(
    "percentage_batters_by_batting_style",
    (F.col("batting_style_count") / total_batters) * 100
)

# Step 6: Percentage by Age Group (using the 'age_group' column created above)
age_group_window = Window.partitionBy("age_group")
final_batters_df_with_age_group_count = final_batters_df_with_percentages_by_batting_style.withColumn(
    "age_group_count", F.count("playerId").over(age_group_window)
)
final_batters_df_with_percentages_by_age_group = final_batters_df_with_age_group_count.withColumn(
    "percentage_batters_by_age_group",
    (F.col("age_group_count") / total_batters) * 100
)

# Show the final DataFrame
final_batters_df_with_percentages_by_age_group.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Bowlers, KPI Creation**
# MAGIC
# MAGIC  This portion contains KPI in the Gold Layer, which are going to store in gen2.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import current_date, year

# Assuming 'final_bowlers_df' is your DataFrame

# Step 1: Calculate age based on 'birth_date'
final_bowlers_df_with_age = final_bowlers_df.withColumn(
    "age", 
    (year(current_date()) - year(F.col("birth_date"))).cast("int")
)

# Step 2: Add 'age_group' column
final_bowlers_df_with_age_group = final_bowlers_df_with_age.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Under 30")
    .when((F.col("age") >= 30) & (F.col("age") <= 35), "30-35")
    .otherwise("Over 35")
)

# Calculate total number of bowlers
total_bowlers = final_bowlers_df_with_age_group.count()

# Step 3: Calculate the count of players by role
role_window = Window.partitionBy("playing_role")
final_bowlers_df_with_role_count = final_bowlers_df_with_age_group.withColumn(
    "role_count", F.count("playerId").over(role_window)
)

# Step 4: Calculate the percentage of bowlers by role
final_bowlers_df_with_percentages_by_role = final_bowlers_df_with_role_count.withColumn(
    "percentage_bowlers_by_role",
    (F.col("role_count") / total_bowlers) * 100
)

# Step 5: Percentage by Gender
gender_window = Window.partitionBy("gender")
final_bowlers_df_with_gender_count = final_bowlers_df_with_percentages_by_role.withColumn(
    "gender_count", F.count("playerId").over(gender_window)
)
final_bowlers_df_with_percentages_by_gender = final_bowlers_df_with_gender_count.withColumn(
    "percentage_bowlers_by_gender",
    (F.col("gender_count") / total_bowlers) * 100
)

# Step 6: Percentage by Bowling Style
bowling_style_window = Window.partitionBy("bowling_style")
final_bowlers_df_with_bowling_style_count = final_bowlers_df_with_percentages_by_gender.withColumn(
    "bowling_style_count", F.count("playerId").over(bowling_style_window)
)
final_bowlers_df_with_percentages_by_bowling_style = final_bowlers_df_with_bowling_style_count.withColumn(
    "percentage_bowlers_by_bowling_style",
    (F.col("bowling_style_count") / total_bowlers) * 100
)

# Step 7: Percentage by Age Group (using the 'age_group' column created above)
age_group_window = Window.partitionBy("age_group")
final_bowlers_df_with_age_group_count = final_bowlers_df_with_percentages_by_bowling_style.withColumn(
    "age_group_count", F.count("playerId").over(age_group_window)
)
final_bowlers_df_with_percentages_by_age_group = final_bowlers_df_with_age_group_count.withColumn(
    "percentage_bowlers_by_age_group",
    (F.col("age_group_count") / total_bowlers) * 100
)

# Show the final DataFrame
final_bowlers_df_with_percentages_by_age_group.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **KPI ALLROUNDER**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import current_date, year

# Assuming 'final_allrounders_df' is your DataFrame

# Step 1: Calculate age based on 'birth_date'
final_allrounders_df_with_age = final_allrounders_df.withColumn(
    "age", 
    (year(current_date()) - year(F.col("birth_date"))).cast("int")
)

# Step 2: Add 'age_group' column
final_allrounders_df_with_age_group = final_allrounders_df_with_age.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Under 30")
    .when((F.col("age") >= 30) & (F.col("age") <= 35), "30-35")
    .otherwise("Over 35")
)

# Calculate total number of allrounders
total_allrounders = final_allrounders_df_with_age_group.count()

# Step 3: Group and count by playing role, gender, batting style, bowling style, and age group
grouped_df = final_allrounders_df_with_age_group.groupBy(
    "playing_role", "gender", "batting_style", "bowling_style", "age_group"
).agg(
    F.count("playerId").alias("role_count")
)

# Step 4: Add total count and percentages for each category
grouped_df = grouped_df.withColumn(
    "percentage_allrounders_by_role", (F.col("role_count") / total_allrounders) * 100
).withColumn(
    "percentage_allrounders_by_gender", (F.col("role_count") / total_allrounders) * 100
).withColumn(
    "percentage_allrounders_by_batting_style", (F.col("role_count") / total_allrounders) * 100
).withColumn(
    "percentage_allrounders_by_bowling_style", (F.col("role_count") / total_allrounders) * 100
).withColumn(
    "percentage_allrounders_by_age_group", (F.col("role_count") / total_allrounders) * 100
)

# Step 5: Add Experience category based on age
final_allrounders_df_with_experience = final_allrounders_df_with_age_group.withColumn(
    "experience_category",
    F.when(F.col("age") < 25, "Novice")
    .when((F.col("age") >= 25) & (F.col("age") <= 30), "Experienced")
    .otherwise("Veteran")
)

# Join all aggregated data back to the original DataFrame
final_df = final_allrounders_df_with_experience.join(grouped_df, on=["playing_role", "gender", "batting_style", "bowling_style", "age_group"], how="left")

# Show the final DataFrame
final_df.display()


# COMMAND ----------

# MAGIC %md 
# MAGIC **writing it in gold layer**

# COMMAND ----------

gold_container_path="abfss://gold@telcopracticefaa.dfs.core.windows.net/"


# Saving 'final_batters_df_with_percentages_by_age_group' in Delta format
final_batters_df_with_percentages_by_age_group.write.format("delta").mode("overwrite").save(
    gold_container_path + "batters_with_percentages_by_age_group.delta")

# Saving 'final_bowlers_df_with_percentages_by_age_group' in Delta format
final_bowlers_df_with_percentages_by_age_group.write.format("delta").mode("overwrite").save(
    gold_container_path + "bowlers_with_percentages_by_age_group.delta")

# Saving 'final_df' (for allrounders) in Delta format
final_df.write.format("delta").mode("overwrite").save(
    gold_container_path + "allrounders_with_percentages_by_age_group.delta")
