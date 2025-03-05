# GitHub Most Popular Repositories

## Project Overview

This project leverages Apache Spark to extract and analyze data from JSON files containing information about the most starred repositories on GitHub based on various search terms. The processed data is then loaded into a PostgreSQL database for further use.

The workflow begins with the creation of a Spark session and loading the data. Subsequent steps include data transformations such as standardization and handling missing values, followed by necessary aggregations and analysis. Finally, the project connects to a PostgreSQL database and loads the results into predefined destination tables.

### Project Structure

- The **`docker-compose.yml`** file sets up the following services:  
  - A **PostgreSQL database** for storing the final data.  
  - **PgAdmin** for database management and table creation.  
  - A **Spark Jupyter Notebook** for data exploration (located in the `notebooks_folder`).  
 
- The `Dockerfile` containerizes the final Spark script.

- **`JDBC_DRIVER` Folder**:  
  Contains the `.jar` file required for Spark to connect to PostgreSQL.

- **`DDL Scripts` Folder**:  
  Includes the SQL script for creating the destination database tables.

---

## How to Run the Project

1. **Download the Repository**:  
   Clone or download the repository to your local machine.

2. **Set Up the Infrastructure**:  
   Navigate to the `docker_files` directory and run the following command to build and start the Docker containers:  
   ```bash
   cd docker_files
   docker-compose up -d
   ```

3. **Access PgAdmin**:  
   Open PgAdmin in your browser at:  
   ```
   http://localhost:5050/
   ```  
   Log in using your credentials, connect to the PostgreSQL database, and execute the `tables_creation` DDL script to create the required tables.

4. **Run the Jupyter Notebook (Optional)**:  
   If you wish to explore the data interactively, access the Jupyter Notebook at:  
   ```
   http://localhost:8085/
   ```

5. **Run the Spark Script**:  
   To execute the Spark script, build and run the Docker container using the following commands:  
   ```bash
   docker build -t spark-app .
   docker run --network=spark_github-repos-net spark-app
   ```