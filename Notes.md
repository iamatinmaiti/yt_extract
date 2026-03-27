# Run these in local to start airflow server

  export AIRFLOW_HOME="/Users/atinmaiti/Documents/Github/yt_extract"
  export AIRFLOW__CORE__DAGS_FOLDER="/Users/atinmaiti/Documents/Github/yt_extract/dags"
  airflow standalone

# Add these to main docker file

    ENV AIRFLOW_HOME=/home/narutouzumaki/CursorProjects/yt_extract
    RUN airflow standalone
    change the user id and password from simpleauth[simple_auth_manager_passwords.json.generated](airflow/simple_auth_manager_passwords.json.generated)
    and update jwt_issuer = airflow to login
    RUN superset run -p 8088 --with-threads --reload --debugger --debug

# Add this to connect with duckdb

    duckdb:////app/data/yt_trending.duckdb