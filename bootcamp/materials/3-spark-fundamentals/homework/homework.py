"""
This module provides a Spark-based data processing pipeline for analyzing game match data.

It includes functions to create a Spark session, load data from CSV files, perform various analyses,
and write results to Iceberg tables. The analyses include calculating player kill averages, playlist
popularity, map popularity, and identifying maps with the most Killing Spree medals.

Functions:
    create_spark_session() -> SparkSession: Configures and returns a Spark session.
    load_data(spark: SparkSession, base_path: str) -> Tuple[DataFrame, ...]: Loads CSV files into DataFrames.
    perform_analysis(spark: SparkSession, ...) -> Tuple[DataFrame, ...]: Performs data analysis on match data.
    main() -> None: Executes the data processing pipeline.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, col, broadcast, avg, count, sum
from typing import Tuple
import os

BASE_PATH = "/home/iceberg/data"

def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession.

    This function sets up a SparkSession with specified memory configurations for
    the driver and executor. It also disables the automatic broadcast join threshold.

    Returns:
        SparkSession: A configured SparkSession object.
    """
    from pyspark import SparkConf
    conf = SparkConf()
    conf.setAll([
        ("spark.driver.memory", "8g"),
        ("spark.executor.memory", "8g")
    ])
    
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("Spark-Iceberg-Homework") \
        .getOrCreate()
    
    # Disable automatic broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    return spark


def load_data(spark: SparkSession, base_path: str = BASE_PATH) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Load CSV data files into Spark DataFrames.

    This function attempts to read multiple CSV files from a specified base path
    and returns them as Spark DataFrames. It includes error handling for file
    reading operations.

    Args:
        spark (SparkSession): The SparkSession to use for reading data.
        base_path (str): The base directory path where the CSV files are located.

    Returns:
        tuple: A tuple containing Spark DataFrames for match_details, matches,
        medal_matches_players, medals, and maps.
    """
    # Load CSV files with error handling and schema inference
    def safe_read_csv(filename):
        try:
            return spark.read.csv(f"{base_path}/{filename}", header=True, inferSchema=True)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            return None

    match_details = safe_read_csv("match_details.csv")
    matches = safe_read_csv("matches.csv")
    medal_matches_players = safe_read_csv("medals_matches_players.csv")
    medals = safe_read_csv("medals.csv")
    maps = safe_read_csv("maps.csv")
    
    return match_details, matches, medal_matches_players, medals, maps


def perform_analysis(spark: SparkSession, match_details: DataFrame, matches: DataFrame, medal_matches_players: DataFrame, medals: DataFrame, maps: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Perform data analysis on game match data.

    This function conducts several analyses on game match data, including
    calculating player kill averages, playlist popularity, map popularity,
    and identifying maps with the most Killing Spree medals. It utilizes
    broadcast joins for efficiency.

    Args:
        spark (SparkSession): The SparkSession to use for analysis.
        match_details (DataFrame): DataFrame containing match details.
        matches (DataFrame): DataFrame containing match information.
        medal_matches_players (DataFrame): DataFrame linking medals to matches and players.
        medals (DataFrame): DataFrame containing medal information.
        maps (DataFrame): DataFrame containing map information.

    Returns:
        tuple: A tuple containing DataFrames for player_kills_avg, playlist_popularity,
        map_popularity, and killing_spree_map.
    """
    # Broadcast small tables
    medals_broadcast = broadcast(medals)
    maps_broadcast = broadcast(maps)

    joined_df = match_details.join(matches, "match_id", "inner") \
        .join(medal_matches_players, ["match_id", "player_gamertag"], "inner") \
        .repartition(16, "match_id")

    # 1. Which player averages the most kills per game?
    player_kills_avg = joined_df.groupBy("player_gamertag") \
        .agg(
            avg("player_total_kills").alias("avg_kills_per_game"),
            count("match_id").alias("total_matches")
        ).orderBy(col("avg_kills_per_game").desc())

    # 2. Which playlist gets played the most?
    playlist_popularity = joined_df.groupBy("playlist_id") \
        .agg(count("match_id").alias("match_count")) \
        .orderBy(col("match_count").desc())

    # 3. Which map gets played the most?
    map_popularity = joined_df.join(maps_broadcast, matches.mapid == maps_broadcast.mapid) \
        .groupBy(maps_broadcast.name) \
        .agg(count("match_id").alias("match_count")) \
        .orderBy(col("match_count").desc())

    # 4. Which map do players get the most Killing Spree medals on?
    killing_spree_map = joined_df.join(medals_broadcast, 
        joined_df.medal_id == medals_broadcast.medal_id) \
        .filter(medals_broadcast.classification == "KillingSpree") \
        .join(maps_broadcast, joined_df.mapid == maps_broadcast.mapid) \
        .groupBy(maps_broadcast.name) \
        .agg(
            sum("count").alias("killing_spree_count")
        ) \
        .orderBy(col("killing_spree_count").desc())

    return player_kills_avg, playlist_popularity, map_popularity, killing_spree_map


def main() -> None:
    """
    Main function to execute the Spark data processing pipeline.

    This function creates a SparkSession, creates a database if it doesn't exist,
    loads data, performs analysis, and writes results to Iceberg tables.
    """
    spark = create_spark_session()
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")

    # Load data
    match_details, matches, medal_matches_players, medals, maps = load_data(spark)

    # Perform analysis
    player_kills_avg, playlist_popularity, map_popularity, killing_spree_map = perform_analysis(
        spark, match_details, matches, medal_matches_players, medals, maps
    )

    # Experiment with sortWithinPartitions
    # High cardinality (almost 1:1 ratio); NOT recommended for sort
    # player_kills_avg.sortWithinPartitions("player_gamertag")
    # playlist_id is low cardinality
    playlist_popularity.sortWithinPartitions("playlist_id")
    # map name is low cardinality
    map_popularity.sortWithinPartitions("name")
    # same
    killing_spree_map.sortWithinPartitions("name")

    # Write to Iceberg
    playlist_popularity.writeTo("bootcamp.playlist_popularity").create()

    map_popularity.writeTo("bootcamp.map_popularity") \
    .partitionedBy("name") \
    .create()

    player_kills_avg.writeTo("bootcamp.player_kills_avg") \
    .partitionedBy("player_gamertag") \
    .create()

    killing_spree_map.writeTo("bootcamp.killing_spree_map") \
    .partitionedBy("name") \
    .create()


if __name__ == "__main__":
    main()
