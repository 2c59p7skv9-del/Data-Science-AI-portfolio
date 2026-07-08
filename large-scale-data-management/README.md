# Large Scale Data Management

This course focuses on the design and implementation of systems for processing and managing large-scale data.

## Overview

Covers distributed data processing frameworks and techniques for handling big data efficiently.

## Topics Covered

- Hadoop ecosystem
- MapReduce programming model
- Distributed file systems (HDFS)
- Spark and large-scale data processing
- Hive and SQL on big data
- Cassandra

## Projects
1st Project: 
- **Hadoop MapReduce Programming**: Implemented distributed data processing logic using the MapReduce model, separating the computation into mapper, combiner, and reducer stages.
- **HDFS-Based Data Processing**: Used Hadoop Distributed File System to store input datasets and retrieve the generated output files.
- **Dockerized Hadoop Environment**: Set up and executed the Hadoop cluster locally using Docker containers instead of a virtual machine.
- **Java MapReduce Implementation**: Developed the application in Java, including custom mapper, reducer, combiner, and driver logic.
- **Custom Writable Data Type**: Created a custom Hadoop Writable object to transfer multiple values through MapReduce pipeline, including song name, maximum danceability, total danceability sum, and count.
- **CSV Parsing and Data Cleaning**: Parsed CSV records while correctly handling commas inside quoted fields, ignored the header row, and extracted only the necessary columns.
- **Aggregation by Composite Key**: Grouped records by (country, month) in order to compute statistics for each country-month pair.
- **Combiner Optimization**: Used a combiner to perform local aggregation before the reducer stage, reducing unnecesary intermediate data transfer.
- **Statistical Aggregation**: Computed the maximum dancebility song and the average danceability score for each coutry-month group.
  
2nd Project:
- **Real-Time Data Processing Pipeline**: Designed and implemented an end-to-end streaming pipeline for simualated Spotify listening events.
- **Event Streaming with Apache Kafka**: Used Python Kafka producer to generate and publish simulated listening events to a Kafka topic.
- **Stream Processing with Spark Structured Streaming**: Consumed Kafka messages using PySpark and processed the incoming data in micro-batches.
- **Stream-to-Static Data Enrichment**: Joined real-time listening events with a static Spotify songs dataset in order to enrich each event with song metadata such as artists, danceability, and tempo.
- **Distributed Database Storage with Apache Cassandra**: Stored the processed streaming data in Cassandra, using schema designed for efficient user-based and time-based queries.
- **Query-Oriented Cassandra Data Modeling**: Designed the Cassandra table around the expected access patterns, using (person_name, listen_hour) as the partition key and listen_time, song_name as clustering columns.
- **Spark Caching Optimization**: Cached the static Spotify songs dataset in Spark to avoid repeated reads and improve performance during the stream-to-static join.
- **Micro-Batch Processing**: Processed streaming records in small batches using Spark Structured Streaming before writing them to Cassandra.
- **CQL Quering and Validation**: Used Cassandra Query Lnaguage to validate data ingestion and run analytical queries, such as average danceability per user and hour.
- **Containerized Local Execution**: Ran the full pipeline locally using Docker containers for Kafka, Spark, and Cassandra services.


## Tools & Technologies

- Java, Python, Apache Hadoop, Hadoop MapReduce, HDFS, Apache Kafka, Apache Spark Structured Streaming, PySpark, Apache Cassandra, CQL, Docker, Maven
