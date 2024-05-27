# Data enrichment template

This basic template contains:

A real time data processing pipeline with these services:

 - Lookup data ingestion - a one-time job that ingests CSV data into a Redis database.
 - CSV data source - A continuously looping data source containing mock sensor data.
 - Data enrichment - Performs a real-time lookup on Redis data for each processed message and appends the extra data to the message.
 - Console logger - This destination service simply logs the data being recieved to the console. Adapt it to suit your needs.

