# Use an official Apache Spark image as the base image
FROM bitnami/spark:latest

# Set the working directory
WORKDIR /app

# Copy the PostgreSQL JDBC driver JAR file into the image
COPY JDBC_DRIVER/postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

# Set the command to run your Spark application
COPY . /app/

# Install any additional dependencies if needed
RUN pip install requests

# Command to run the PySpark script
CMD ["spark-submit", "--jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar", "/app/main.py"]
