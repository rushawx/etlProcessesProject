# ETL Project

This project sets up an ETL (Extract, Transform, Load) pipeline using Apache Airflow, MongoDB, and PostgreSQL. The pipeline generates data in MongoDB, replicates it to PostgreSQL, and creates data marts in PostgreSQL.

## Project Structure

## Setup

### Prerequisites

- Docker
- Docker Compose

### Environment Variables

Copy the `.env.example` file to `.env` and adjust the values as needed.

```sh
cp .env.example .env
```

## Start Services
To start the services, run the following command:

## Initialize Database
To initialize the PostgreSQL database, run the following command:

## Airflow DAGs

### Mongo Data Generator
The mongo_data_generator.py DAG generates fake user session data and inserts it into MongoDB.

### Mongo to Postgres Data Replicator
The mongo_to_postgres_data_replicator.py DAG replicates the data from MongoDB to PostgreSQL.

### Postgres Data Mart Creator
The postgres_data_mart_creator.py DAG creates data marts in PostgreSQL.

## Connections
The connections to MongoDB and PostgreSQL are defined in the airflow/data/connections.yaml file.
