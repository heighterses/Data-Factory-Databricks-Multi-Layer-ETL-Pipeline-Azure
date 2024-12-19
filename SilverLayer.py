# Databricks notebook source
# Configuration for Azure Storage Account
storage_account_name = "telcopracticefaa"  # Replace with your storage account name
storage_account_key = "v1+Swk/36i3Ji2hpOFluC79ImPjbxZgDwXHkSYBQXAv4drITAZ9jhJuKJ0qXOgdCxq+pGGeGjgq++AStlVTUsQ=="  # Replace with your storage account key
container_name = "bronze"  # Replace with your container name

# Set Spark configuration
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

silver_df = spark.read.parquet("abfss://bronze@telcopracticefaa.dfs.core.windows.net/players_bronze")
silver_df.display()


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def categorize_players(df: DataFrame) -> DataFrame:
    """
    Categorizes players into 'batter' and 'bowler' based on their playing roles.

    Parameters:
    df (DataFrame): Input DataFrame containing player information.

    Returns:
    DataFrame: DataFrame with an additional column indicating the category.
    """
    # Define keywords for classification
    batter_keywords = ["batter", "bat"]
    bowler_keywords = ["bowler"]
    allrounder_keyword=["allrounder"]

    # Categorize players
    df = df.withColumn(
        "category",
        F.when(F.col("playingRole").rlike('|'.join(batter_keywords)), "batter")
         .when(F.col("playingRole").rlike('|'.join(bowler_keywords)), "bowler")
         .when(F.col("playingRole").rlike('|'.join(allrounder_keyword)), "allrounder")
         .otherwise("allrounder")  # For any roles not classified
    )
    
    return df


# COMMAND ----------

# Step 2: Apply the categorization function
categorized_silver_df = categorize_players(silver_df)

# Step 3: Display the result
categorized_silver_df.display()

# COMMAND ----------

# Create separate DataFrames for each category
batters_df = categorized_silver_df.filter(categorized_silver_df.category == 'batter')
display(batters_df)
# bowlers_df = categorized_silver_df.filter(categorized_silver_df.category == 'bowler')
# allrounders_df = categorized_silver_df.filter(categorized_silver_df.category == 'allrounder')


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Renaming columns and casting data types
renamed_casted_df = (
    cleaned_batters_df
    .withColumnRenamed("battingName", "batters")#same
    .withColumnRenamed("battingStyle", "batting_style_abv")
    .withColumnRenamed("bowlingStyle", "bowling_style_abv")
    .withColumnRenamed("countryTeamId", "country_team_id")
    .withColumnRenamed("birthDate", "birth_day")
    .withColumnRenamed("birthMonth", "birth_month")
    .withColumnRenamed("birthYear", "birth_year")
    .withColumnRenamed("fieldingName", "fielding_name")
    .withColumnRenamed("longBattingStyle", "batting_style")
    .withColumnRenamed("longBowlingStyle", "bowling_style")
    .withColumnRenamed("longName", "player_name")#same
    .withColumnRenamed("playingRole", "playing_role")#same
    .withColumnRenamed("playingRoleType", "playing_role_type_abv")
    .withColumnRenamed("playerPlayingRole", "player_playing_order")#same
    .withColumnRenamed("category", "player_category")
    # Cast columns to appropriate data types
    .withColumn("country_team_id", F.col("country_team_id").cast(IntegerType()))
    .withColumn("birth_day", F.col("birth_day").cast(IntegerType()))
    .withColumn("birth_month", F.col("birth_month").cast(IntegerType()))
    .withColumn("birth_year", F.col("birth_year").cast(IntegerType()))
    .withColumn("playerId", F.col("playerId").cast(IntegerType()))
)

# Show schema and sample rows to verify changes
renamed_casted_df.printSchema()
renamed_casted_df.show(truncate=False)


# COMMAND ----------

 bowlers_df = categorized_silver_df.filter(categorized_silver_df.category == 'bowler')
 display(bowlers_df)

# COMMAND ----------

allrounders_df = categorized_silver_df.filter(categorized_silver_df.category == 'allrounder')
display(allrounders_df)

# COMMAND ----------

# Import necessary libraries
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session (This is typically done automatically in Databricks)
spark = SparkSession.builder \
    .appName("DataFrame Processing") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_dataframe(df, specific_columns, null_handling_dict=None, drop_duplicates=True):
    """
    Processes the given PySpark DataFrame by checking for duplicates and nulls,
    and keeping only specific columns.

    Parameters:
    - df: The DataFrame to process.
    - specific_columns: List of columns to retain.
    - null_handling_dict: Dictionary defining null handling for specific columns.
    - drop_duplicates: Whether to drop duplicate rows.

    Returns:
    - Cleaned DataFrame.
    """

    # Preprocess: Replace empty strings or strings with only spaces with null values
    for column in specific_columns:
        df = df.withColumn(column, F.when(F.trim(df[column]) == '', None).otherwise(df[column]))

    # Check for duplicates and drop them if specified
    if drop_duplicates:
        initial_count = df.count()
        df = df.dropDuplicates()
        duplicates_removed = initial_count - df.count()
        logging.info(f'Dropped {duplicates_removed} duplicate rows.')

    # Check for null values and handle according to the specified strategy
    if null_handling_dict is None:
        null_handling_dict = {col: 'fill' for col in specific_columns}  # Default to 'fill' for all columns

    for column, strategy in null_handling_dict.items():
        if column in specific_columns:  # Ensure the column is in the specific columns
            null_count = df.filter(df[column].isNull()).count()
            if strategy == 'drop':
                df = df.na.drop(subset=[column])
                logging.info(f'Dropped {null_count} rows with nulls in column: {column}.')
            elif strategy == 'fill':
                df = df.na.fill({column: 'N/A'})  # Fill nulls with 'N/A' or any other placeholder
                logging.info(f'Filled {null_count} nulls in column: {column}.')

    # Keep only specific columns
    df = df.select(specific_columns)

    return df


# COMMAND ----------

# Example specific columns to keep in batters df
specific_columns = ['battingName', 'battingStyle', 'bowlingStyle', 'countryTeamId', 
                    'birthDate', 'birthMonth', 'birthYear', 'fieldingName', 'gender', 
                    'playerId', 'longBattingStyle', 'longBowlingStyle', 'longName', 
                    'objectId', 'playingRole', 'playingRoleType', 'playerPlayingRole', 
                    'category']

# Define null handling strategies
null_handling_strategies = {
    'fieldingName': 'fill',  # Fill nulls for fieldingName
    'birthDate': 'drop',     # Drop rows with null birthDate
    # Add more columns with desired strategies as needed
}

# Process the batters DataFrame
cleaned_batters_df = process_dataframe(batters_df, specific_columns, null_handling_dict=null_handling_strategies)

# Display the cleaned DataFrame
cleaned_batters_df.display()

# COMMAND ----------

# Example specific columns to keep in bowlers df
specific_columns = ['battingName', 'battingStyle', 'bowlingStyle', 'countryTeamId', 
                    'birthDate', 'birthMonth', 'birthYear', 'fieldingName', 'gender', 
                    'playerId', 'longBattingStyle', 'longBowlingStyle', 'longName', 
                    'objectId', 'playingRole', 'playingRoleType', 'playerPlayingRole', 
                    'category']

# Define null handling strategies
null_handling_strategies = {
    'fieldingName': 'fill',  # Fill nulls for fieldingName
    'birthDate': 'drop',     # Drop rows with null birthDate
    # Add more columns with desired strategies as needed
}

# Process the batters DataFrame
cleaned_bowlers_df = process_dataframe(bowlers_df, specific_columns, null_handling_dict=null_handling_strategies)

# Display the cleaned DataFrame
cleaned_bowlers_df.display()

# COMMAND ----------

# Example specific columns to keep
specific_columns = ['battingName', 'battingStyle', 'bowlingStyle', 'countryTeamId', 
                    'birthDate', 'birthMonth', 'birthYear', 'fieldingName', 'gender', 
                    'playerId', 'longBattingStyle', 'longBowlingStyle', 'longName', 
                    'objectId', 'playingRole', 'playingRoleType', 'playerPlayingRole', 
                    'category']

# Define null handling strategies
null_handling_strategies = {
    'fieldingName': 'fill',  # Fill nulls for fieldingName
    'birthDate': 'drop',     # Drop rows with null birthDate
    # Add more columns with desired strategies as needed
}

# Process the batters DataFrame
cleaned_allrounders_df = process_dataframe(allrounders_df, specific_columns, null_handling_dict=null_handling_strategies)

# Display the cleaned DataFrame
cleaned_allrounders_df.display()

# COMMAND ----------

# MAGIC %md Renaming columns and datatype casting

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Renaming columns, casting data types, and dropping duplicates
batters_casted_df = (
    cleaned_batters_df
    # Rename columns
    .withColumnRenamed("battingStyle", "batting_style_abv")
    .withColumnRenamed("bowlingStyle", "bowling_style_abv")
    .withColumnRenamed("countryTeamId", "country_team_id")
    .withColumnRenamed("birthDate", "birth_day")
    .withColumnRenamed("birthMonth", "birth_month")
    .withColumnRenamed("birthYear", "birth_year")
    .withColumnRenamed("fieldingName", "fielding_name")
    .withColumnRenamed("longBattingStyle", "batting_style")
    .withColumnRenamed("longBowlingStyle", "bowling_style")
    .withColumnRenamed("longName", "player_name")
    .withColumnRenamed("playingRole", "playing_role")
    .withColumnRenamed("category", "player_category")
    # Cast columns to appropriate data types
    .withColumn("country_team_id", F.col("country_team_id").cast(IntegerType()))
    .withColumn("birth_day", F.col("birth_day").cast(IntegerType()))
    .withColumn("birth_month", F.col("birth_month").cast(IntegerType()))
    .withColumn("birth_year", F.col("birth_year").cast(IntegerType()))
    .withColumn("playerId", F.col("playerId").cast(IntegerType()))
    # Drop duplicate columns
    .drop("battingName", "playingRoleType", "playerPlayingRole")
    
)
# Show schema and sample rows to verify changes
batters_casted_df.printSchema()
batters_casted_df.show(truncate=False)
batters_casted_df.display()


# COMMAND ----------

from pyspark.sql import functions as F

batters_final = batters_casted_df.withColumn(
    "birth_date",
    F.concat(
        F.col("birth_year").cast("string"), 
        F.lit("-"), 
        F.lpad(F.col("birth_month").cast("string"), 2, "0"), 
        F.lit("-"), 
        F.lpad(F.col("birth_day").cast("string"), 2, "0")
    )
)

# Step 2: Convert the string to date
batters_final = batters_final.withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))

# Show the result
batters_final.display()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Renaming columns, casting data types, and dropping duplicates
bowlers_casted_df = (
    cleaned_bowlers_df
    # Rename columns
    .withColumnRenamed("battingStyle", "batting_style_abv")
    .withColumnRenamed("bowlingStyle", "bowling_style_abv")
    .withColumnRenamed("countryTeamId", "country_team_id")
    .withColumnRenamed("birthDate", "birth_day")
    .withColumnRenamed("birthMonth", "birth_month")
    .withColumnRenamed("birthYear", "birth_year")
    .withColumnRenamed("fieldingName", "fielding_name")
    .withColumnRenamed("longBattingStyle", "batting_style")
    .withColumnRenamed("longBowlingStyle", "bowling_style")
    .withColumnRenamed("longName", "player_name")
    .withColumnRenamed("playingRole", "playing_role")
    .withColumnRenamed("category", "player_category")
    # Cast columns to appropriate data types
    .withColumn("country_team_id", F.col("country_team_id").cast(IntegerType()))
    .withColumn("birth_day", F.col("birth_day").cast(IntegerType()))
    .withColumn("birth_month", F.col("birth_month").cast(IntegerType()))
    .withColumn("birth_year", F.col("birth_year").cast(IntegerType()))
    .withColumn("playerId", F.col("playerId").cast(IntegerType()))
    # Drop duplicate columns
    .drop("battingName", "playingRoleType", "playerPlayingRole")
    
)
# Show schema and sample rows to verify changes
bowlers_casted_df.printSchema()
bowlers_casted_df.show(truncate=False)
bowlers_casted_df.display()


# COMMAND ----------

from pyspark.sql import functions as F

bowlers_final = bowlers_casted_df.withColumn(
    "birth_date",
    F.concat(
        F.col("birth_year").cast("string"), 
        F.lit("-"), 
        F.lpad(F.col("birth_month").cast("string"), 2, "0"), 
        F.lit("-"), 
        F.lpad(F.col("birth_day").cast("string"), 2, "0")
    )
)

# Step 2: Convert the string to date
bowlers_final = bowlers_final.withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))

# Show the result
bowlers_final.display()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Renaming columns, casting data types, and dropping duplicates
allrounders_casted_df  = (
    cleaned_allrounders_df
    # Rename columns
    .withColumnRenamed("battingStyle", "batting_style_abv")
    .withColumnRenamed("bowlingStyle", "bowling_style_abv")
    .withColumnRenamed("countryTeamId", "country_team_id")
    .withColumnRenamed("birthDate", "birth_day")
    .withColumnRenamed("birthMonth", "birth_month")
    .withColumnRenamed("birthYear", "birth_year")
    .withColumnRenamed("fieldingName", "fielding_name")
    .withColumnRenamed("longBattingStyle", "batting_style")
    .withColumnRenamed("longBowlingStyle", "bowling_style")
    .withColumnRenamed("longName", "player_name")
    .withColumnRenamed("playingRole", "playing_role")
    .withColumnRenamed("category", "player_category")
    # Cast columns to appropriate data types
    .withColumn("country_team_id", F.col("country_team_id").cast(IntegerType()))
    .withColumn("birth_day", F.col("birth_day").cast(IntegerType()))
    .withColumn("birth_month", F.col("birth_month").cast(IntegerType()))
    .withColumn("birth_year", F.col("birth_year").cast(IntegerType()))
    .withColumn("playerId", F.col("playerId").cast(IntegerType()))
    # Drop duplicate columns
    .drop("battingName", "playingRoleType", "playerPlayingRole")
    
)
# Show schema and sample rows to verify changes
allrounders_casted_df.printSchema()
allrounders_casted_df.show(truncate=False)
allrounders_casted_df.display()


# COMMAND ----------

from pyspark.sql import functions as F

allrounders_final = allrounders_casted_df.withColumn(
    "birth_date",
    F.concat(
        F.col("birth_year").cast("string"), 
        F.lit("-"), 
        F.lpad(F.col("birth_month").cast("string"), 2, "0"), 
        F.lit("-"), 
        F.lpad(F.col("birth_day").cast("string"), 2, "0")
    )
)

# Step 2: Convert the string to date
allrounders_final = bowlers_final.withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))

# Show the result
allrounders_final.display()

# COMMAND ----------

# Assuming you have your DataFrames for batters, bowlers, and all-rounders
batters_final.write.format("delta").mode("overwrite").save("abfss://silver@telcopracticefaa.dfs.core.windows.net/batters_silver")
bowlers_final.write.format("delta").mode("overwrite").save("abfss://silver@telcopracticefaa.dfs.core.windows.net/bowlers_silver")
allrounders_final.write.format("delta").mode("overwrite").save("abfss://silver@telcopracticefaa.dfs.core.windows.net/allrounders_silver")
