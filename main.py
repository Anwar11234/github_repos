from pyspark.sql.functions import input_file_name, regexp_replace, substring_index, col, lower
from config import spark, schema, db, username, password
from utils import fill_missing_values, write_dataframe

df = spark.read.json('data/*.json', schema = schema)

df_file_names = df.withColumn("search_term", input_file_name())

df_search_terms = df_file_names.withColumn("search_term",regexp_replace(substring_index("search_term", '/', -1), ".json", ""))

df_filled_full_name = df_search_terms.na.fill("afritzler/docker-spark-arm", subset = ["full_name"])

df_filled_created = fill_missing_values(df_filled_full_name, "created","created_at")
df_filled_stars = fill_missing_values(df_filled_created, "stars","stargazers_count")
df_filled_issues = fill_missing_values(df_filled_stars, "open_issues","open_issues_count")
df_filled_subscribers = fill_missing_values(df_filled_issues, "subscribers","subscribers_count")
df_filled_type = fill_missing_values(df_filled_subscribers,"type","type")
df_filled_forks = fill_missing_values(df_filled_type, "forks","forks_count")
df_filled = fill_missing_values(df_filled_forks, "username","login")

df_filled_languages = df_filled.na.fill("No Language Detected", ["language"])

df_lower = df_filled_languages.withColumn('language', lower(col('language')))

df_langs = df_lower.groupby('language').count()
df_orgs = df_filled.filter("type = 'Organization'").groupby('username').sum('stars').withColumnRenamed("sum(stars)", "total_stars")

df_relevance_scores = df_filled.selectExpr("search_term", "repo_name", "1.5 * forks + 1.32 * subscribers + 1.04 * stars as relevance_score")

write_dataframe(df_langs, db, "programmin_lang",username, password)
write_dataframe(df_orgs, db, "organizations_stars",username, password)
write_dataframe(df_relevance_scores, db, "search_terms_relevance",username, password)