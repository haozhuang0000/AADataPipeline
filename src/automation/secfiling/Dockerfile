#Dockerfile
FROM apache/airflow:2.9.3-python3.10
# RUN apt-get update && apt-get install -y wget unzip gnupg apt-transport-https
# RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg
# RUN echo "deb [signed-by=/usr/share/keyrings/google-linux-signing-key.gpg arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
# RUN echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
# RUN apt-get install -y google-chrome-stable


# Install additional dependencies
USER root

# RUN apt-get update --fix-missing && \
#     apt-get install -y wget libx11-xcb1 libgdk-pixbuf2.0-0 libnss3 && \
#     wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
#     apt-get install -y ./google-chrome-stable_current_amd64.deb && \
#     rm google-chrome-stable_current_amd64.deb && \
#     apt-get clean

COPY requirements.txt ./requirements.txt
USER airflow
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --upgrade pip
# Set up additional Python dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r ./requirements.txt