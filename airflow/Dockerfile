FROM apache/airflow:3.1.7-python3.12
USER airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

EXPOSE 8080
CMD ["airflow", "standalone"]
