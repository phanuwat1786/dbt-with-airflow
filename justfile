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

up-minio: create-docker-network
    - docker compose -f MinIO/docker-compose.yml up -d

down-minio:
    - docker compose -f MinIO/docker-compose.yml down

up-socket-proxy: create-docker-network
    - docker compose -f docker_socket_proxy/docker-compose.yml up -d

down-socket-proxy:
    - docker compose -f docker_socket_proxy/docker-compose.yml down

up-all: up-socket-proxy up-airflow up-pg up-minio 

down-all: down-socket-proxy down-airflow down-pg down-minio