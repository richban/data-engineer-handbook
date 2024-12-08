from pyspark.sql import SparkSession

# Define the SQL query for processing actors data
query = '''
WITH last_year AS (
    SELECT
        actorid,
        actor,
        films,
        quality_class,
        is_active,
        current_year
    FROM actors
    WHERE current_year = 1976  -- Replace with the previous year being processed
),
this_year AS (
    SELECT
        actorid,
        actor,
        ARRAY_AGG(STRUCT(film, votes, rating, filmid)) AS films,
        CASE 
            WHEN AVG(COALESCE(rating, 0)) > 8 THEN 'star'
            WHEN AVG(COALESCE(rating, 0)) > 7 THEN 'good'
            WHEN AVG(COALESCE(rating, 0)) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        CASE 
            WHEN SIZE(ARRAY_AGG(STRUCT(film, votes, rating, filmid))) > 0 THEN TRUE
            ELSE FALSE
        END AS is_active,
        year AS current_year
    FROM actor_films
    WHERE year = 1977  -- Replace with the current year being processed
    GROUP BY actorid, actor, year
)
SELECT
    COALESCE(ly.actorid, ty.actorid) AS actorid,
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.films, ARRAY()) || COALESCE(ty.films, ARRAY()) AS films,
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    COALESCE(ty.is_active, ly.is_active) AS is_active,
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actorid = ty.actorid
'''

def do_actor_transformation(spark, actors_df, actor_films_df):
    # Register DataFrames as temporary views
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")
    # Execute the SQL query
    result_df = spark.sql(query)
    return result_df

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()

    # Example DataFrames for actors and actor_films
    actors_df = spark.table("actors")
    actor_films_df = spark.table("actor_films")
    output_df = do_actor_scd_transformation(spark, actors_df, actor_films_df)
    output_df.write.mode("overwrite").insertInto("actors")

    # Show the result
    output_df.show()

if __name__ == "__main__":
    main()
