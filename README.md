# py-iceberg-etl
A simple setup to demo batch and streaming workloads using PyIceberg

This repo is geared towards GCP workloads

## Iceberg Catalog
We are using a Docker image locally for postgres as a JDBC catalog, but this could be easily substituted with Cloud SQL on GCP, an Iceberg REST catalog etc. - anything thats supported by Iceberg

## Streaming
This is a Apache Beam/Cloud Dataflow pipeline ingesting data from a pub-sub topic and writing data to GCS in the Iceberg spec

## Batch
A simple python notebook