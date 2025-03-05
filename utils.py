import requests
from pyspark.sql.functions import isnull, col, when
from pyspark.sql import Row
from config import spark, github_token

def fill_missing_values(df, col_name, github_col_name):
    full_names_df = df.filter(isnull(col(col_name))).select("full_name")
    repo_data = {}
    headers = {"Authorization": f"token {github_token}"}
    
    for row in full_names_df.collect():
        full_name = row["full_name"]
        repo_url = f"https://api.github.com/repos/{full_name}"
        response = requests.get(repo_url, headers = headers)
        if response.status_code == 200:
            if col_name == "type" or col_name == "username":
                repo_data[full_name] = response.json()['owner'][github_col_name]
            else:
                repo_data[full_name] = response.json()[github_col_name]
        else:
            repo_data[full_name] = None 
        
    repo_df = spark.createDataFrame([Row(full_name=k, col_filled=v) for k, v in repo_data.items()])

    res = df.join(repo_df, on="full_name", how="left").withColumn(
        col_name,
        when(col(col_name).isNull(), col("col_filled"))
        .otherwise(col(col_name))
    ).drop("col_filled")
    
    return res 

def write_dataframe(df, db, table_name, user_name, password, ):
    df.write.format("jdbc") \
    .option("url", f"jdbc:postgresql://postgres:5432/{db}") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", table_name) \
    .option("user", user_name) \
    .option("password", password) \
    .mode("overwrite") \
    .save()