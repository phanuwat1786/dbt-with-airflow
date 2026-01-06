default:
    just --list

create-docker-network:
    - docker network create -d bridge pipeline-network

up-airflow: create-docker-network
    - docker compose -f airflow/docker-compose.yml up -d --build

down-airflow:
    - docker compose -f airflow/docker-compose.yml down

clean-airflow: down-airflow
    - docker network remove pipeline-network
    - docker volume rm -f airflow_postgres-db-airflow

up-pg: create-docker-network
    - docker compose -f db/postgres/docker-compose.yml up -d

down-pg: 
    - docker compose -f db/postgres/docker-compose.yml down