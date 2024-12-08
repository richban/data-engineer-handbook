import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    ArrayType,
    DecimalType
)
from pyspark.sql import Row
from pyspark.sql.functions import col
from ..jobs.actors_job import do_actor_transformation
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple

ActorFilm = namedtuple("ActorFilm", "Actor ActorId Film Year votes Rating FilmID")
Actor = namedtuple("Actor", "actorid actor films quality_class is_active current_year")


def test_actor_scd_transformation(spark):
    # Sample data for actor_films
    actor_films_data = [
        ActorFilm(
            "Fred Astaire",
            "nm0000001",
            "The Towering Inferno",
            1974,
            39888,
            7.0,
            "tt0072308",
        ),
        ActorFilm(
            "Fred Astaire",
            "nm0000001",
            "The Amazing Dobermans",
            1976,
            369,
            5.3,
            "tt0074130",
        ),
        ActorFilm(
            "Fred Astaire", "nm0000001", "The Purple Taxi", 1977, 533, 6.6, "tt0076851"
        ),
    ]
    actor_films_df = spark.createDataFrame(actor_films_data)

    actors_data = [
        Actor(
            "nm0000001",
            "Fred Astaire",
            [
                Row(
                    film="The Towering Inferno",
                    votes=39888,
                    rating=7.0,
                    filmid="tt0072308",
                ),
                Row(
                    film="The Amazing Dobermans",
                    votes=369,
                    rating=5.3,
                    filmid="tt0074130",
                ),
            ],
            "bad",
            True,
            1976,
        ),
    ]

    # Define film_struct as a StructType
    film_struct = StructType(
        [
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("filmid", StringType(), True),
        ]
    )

    # Define schema for actors
    actors_schema = StructType(
        [
            StructField("actorid", StringType(), True),
            StructField("actor", StringType(), True),
            StructField("films", ArrayType(film_struct), True),
            StructField("quality_class", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("current_year", IntegerType(), True),
        ]
    )
    actors_df = spark.createDataFrame(actors_data, schema=actors_schema)

    # Expected output data
    expected_data = [
        Actor(
            "nm0000001",
            "Fred Astaire",
            [
                Row(
                    film="The Towering Inferno",
                    votes=39888,
                    rating=7.0,
                    filmid="tt0072308",
                ),
                Row(
                    film="The Amazing Dobermans",
                    votes=369,
                    rating=5.3,
                    filmid="tt0074130",
                ),
                Row(film="The Purple Taxi", votes=533, rating=6.6, filmid="tt0076851"),
            ],
            "average",
            True,
            1977,
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)
    # Update the expected DataFrame to cast the rating field
    expected_df = expected_df.withColumn("films", 
        col("films").cast(ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", DecimalType(3, 1), True),
            StructField("filmid", StringType(), True)
        ])))
    )

    # print(expected_df.select("films").show(truncate=False))

    # Perform the transformation
    actual_df = do_actor_transformation(spark, actors_df, actor_films_df)
    actual_df = actual_df.withColumn("films", 
        col("films").cast(ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", DecimalType(3, 1), True),
            StructField("filmid", StringType(), True)
        ])))
    )

    # print(actual_df.select("films").show(truncate=False))

    # Assert DataFrame equality
    assert_df_equality(actual_df, expected_df)
