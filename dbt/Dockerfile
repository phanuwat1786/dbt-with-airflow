FROM ghcr.io/dbt-labs/dbt-postgres:1.9.0

COPY . /usr/app/dbt
ENV DBT_PROFILES_DIR=/usr/app/dbt

ENTRYPOINT ["dbt"]