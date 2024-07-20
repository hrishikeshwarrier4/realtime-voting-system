import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

from pyspark.sql.functions import sum as _sum

if __name__ == '__main__':
    spark = (SparkSession.builder.appName('RealTimeVotingSystem')
             .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0')
             .config('spark.jars','/Users/hrishikeshwarrier/Desktop/Hrishikesh/realtime-voting-system/realtime-voting-system/postgresql-42.7.3.jar')
             .config('spark.sql.adaptive.enable','false')
             .getOrCreate())


    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("polling_station", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("candidate_date_of_birth", StringType(), True),
        StructField("candidate_gender", StringType(), True),
        StructField("education", StringType(), True),
        StructField("experience", StringType(), True),
        StructField("campaign_funds", IntegerType(), True),
        StructField("voter_date_of_birth", StringType(), True),
        StructField("voter_gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])


    votes_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers','localhost:9092')
                .option('subscribe','votes_topic')
                .option('startingOffsets','earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'),vote_schema).alias('data'))
                .select('data.*')
                )

    # # Write the data to console (for debugging)
    # query = votes_df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()
    #
    # query.awaitTermination()



    # Data preprocessing
    votes_df = votes_df.withColumn('voting_time', col('voting_time').cast(TimestampType())) \
                .withColumn('vote', col('vote').cast(IntegerType()))


    enriched_votes_df = votes_df.withWatermark('voting_time', '1 minute')

    # Aggregate votes per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party_affiliation",
                                                    "photo_url").agg(_sum("vote").alias("total_votes"))
    turnout_by_location = enriched_votes_df.groupBy("address.state").count().alias("total_votes")

    # Write aggregated data to Kafka topics ('aggregated_votes_per_candidate', 'aggregated_turnout_by_location')
    votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "/Users/hrishikeshwarrier/Desktop/Hrishikesh/realtime-voting-system/realtime-voting-system/checkpoints/checkpoint1") \
        .outputMode("update") \
        .start()

    turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_turnout_by_location") \
        .option("checkpointLocation", "/Users/hrishikeshwarrier/Desktop/Hrishikesh/realtime-voting-system/realtime-voting-system/checkpoints/checkpoint2") \
        .outputMode("update") \
        .start()

    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()