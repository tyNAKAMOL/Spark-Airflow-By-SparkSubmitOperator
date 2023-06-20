FROM apache/airflow:2.5.3-python3.9

USER root
RUN apt-get update && apt-get install -y curl wget


# SET JAVA ENV VARIABLES
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:/usr/local/bin:$JAVA_HOME

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Download PostgreSQL JDBC driver
RUN apt-get update && apt-get install -y curl
RUN mkdir -p /usr/share/java/
RUN curl -L -o /usr/share/java/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

USER airflow

# Install PYTHON requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt





