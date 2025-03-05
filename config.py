from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("SimpleApp") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField("created", TimestampType()),
    StructField("description", StringType()),
    StructField("forks", IntegerType()),
    StructField("full_name", StringType()),
    StructField("id", IntegerType()),
    StructField("language", StringType()),
    StructField("open_issues", IntegerType()),
    StructField("repo_name", StringType()),
    StructField("stars", IntegerType()),
    StructField("subscribers", IntegerType()),
    StructField("topics", ArrayType(StringType())),
    StructField("type", StringType()),
    StructField("username", StringType())
])

db = "github_repo"
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
github_token = os.getenv("GITHUB_TOKEN")