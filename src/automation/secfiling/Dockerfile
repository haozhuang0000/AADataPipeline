#Dockerfile
FROM apache/airflow:2.9.3-python3.10
USER root
COPY requirements.txt ./requirements.txt
USER airflow
# RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --upgrade pip
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r ./requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./requirements.txt