# Databricks notebook source
# spark.conf.set( 
# "fs.azure.account.key.telcopracticefaa.dfs.core.windows.net", "v1+Swk/36i3Ji2hpOFluC79ImPjbxZgDwXHkSYBQXAv4drITAZ9jhJuKJ0qXOgdCxq+pGGeGjgq++AStlVTUsQ==")

# COMMAND ----------

# Configuration for Azure Storage Account
storage_account_name = "telcopracticefaa"  # Replace with your storage account name
storage_account_key = "v1+Swk/36i3Ji2hpOFluC79ImPjbxZgDwXHkSYBQXAv4drITAZ9jhJuKJ0qXOgdCxq+pGGeGjgq++AStlVTUsQ=="  # Replace with your storage account key
container_name = "olddata"  # Replace with your container name

# Set Spark configuration
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

df = spark.read.json("abfss://olddata@telcopracticefaa.dfs.core.windows.net/player_INTERNATIONAL.json", multiLine=True)
df.printSchema()
# df.display()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Define the schema
schema = StructType([
    StructField("total", LongType(), True),  # Root-level field
    StructField("results", ArrayType(
        StructType([
            StructField("battingName", StringType(), True),
            StructField("battingStyles", ArrayType(StringType()), True),
            StructField("bowlingStyles", ArrayType(StringType()), True),
            StructField("countryTeamId", LongType(), True),
            StructField("dateOfBirth", StructType([
                StructField("date", LongType(), True),
                StructField("month", LongType(), True),
                StructField("year", LongType(), True)
            ]), True),
            StructField("dateOfDeath", StringType(), True),
            StructField("fieldingName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("id", LongType(), True),
            StructField("image", StructType([
                StructField("caption", StringType(), True),
                StructField("credit", StringType(), True),
                StructField("height", LongType(), True),
                StructField("id", LongType(), True),
                StructField("longCaption", StringType(), True),
                StructField("objectId", LongType(), True),
                StructField("peerUrls", StringType(), True),
                StructField("photographer", StringType(), True),
                StructField("slug", StringType(), True),
                StructField("url", StringType(), True),
                StructField("width", LongType(), True)
            ]), True),
            StructField("indexName", StringType(), True),
            StructField("longBattingStyles", ArrayType(StringType()), True),
            StructField("longBowlingStyles", ArrayType(StringType()), True),
            StructField("longName", StringType(), True),
            StructField("mobileName", StringType(), True),
            StructField("name", StringType(), True),
            StructField("objectId", LongType(), True),
            StructField("playingRole", StringType(), True),
            StructField("playingRoleType", StringType(), True),
            StructField("playingRoles", ArrayType(StringType()), True),
            StructField("slug", StringType(), True)
        ])
    ), True)
])


# COMMAND ----------

from pyspark.sql.functions import col, explode

# First, explode the 'results' array to flatten the player data
flattened_df = (df
    .select(
        col("total"),
        explode(col("results")).alias("result")
    )
    .select(
        col("total"),
        col("result.battingName"),
        explode(col("result.battingStyles")).alias("battingStyle"),  # Explode battingStyles
        explode(col("result.bowlingStyles")).alias("bowlingStyle"),  # Explode bowlingStyles
        col("result.countryTeamId"),
        col("result.dateOfBirth.date").alias("birthDate"),
        col("result.dateOfBirth.month").alias("birthMonth"),
        col("result.dateOfBirth.year").alias("birthYear"),
        col("result.dateOfDeath"),
        col("result.fieldingName"),
        col("result.gender"),
        col("result.id").alias("playerId"),
        col("result.image.caption").alias("imageCaption"),
        col("result.image.credit").alias("imageCredit"),
        col("result.image.height").alias("imageHeight"),
        col("result.image.id").alias("imageId"),
        col("result.image.longCaption").alias("imageLongCaption"),
        col("result.image.objectId").alias("imageObjectId"),
        col("result.image.peerUrls").alias("imagePeerUrls"),
        col("result.image.photographer").alias("imagePhotographer"),
        col("result.image.slug").alias("imageSlug"),
        col("result.image.url").alias("imageUrl"),
        col("result.image.width").alias("imageWidth"),
        col("result.indexName"),
        explode(col("result.longBattingStyles")).alias("longBattingStyle"),  # Explode longBattingStyles
        explode(col("result.longBowlingStyles")).alias("longBowlingStyle"),  # Explode longBowlingStyles
        col("result.longName"),
        col("result.mobileName"),
        col("result.name"),
        col("result.objectId"),
        col("result.playingRole"),
        col("result.playingRoleType"),
        explode(col("result.playingRoles")).alias("playerPlayingRole")  # Explode playingRoles
    )
)

# Show the flattened schema and data
flattened_df.printSchema()
flattened_df.show()
flattened_df.display()



# COMMAND ----------

# Write the DataFrame to the bronze layer in your Data Lake
bronze_path = "abfss://bronze@telcopracticefaa.dfs.core.windows.net/players_bronze"
flattened_df.write.mode("overwrite").parquet(bronze_path)

# COMMAND ----------

