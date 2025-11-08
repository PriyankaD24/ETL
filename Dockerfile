FROM apache/airflow:2.7.3

USER root

# Install OpenJDK 11
RUN apt-get update && apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch to airflow user before installing Python packages
USER airflow

# Install Python packages
RUN pip install --no-cache-dir kaggle minio pyspark

