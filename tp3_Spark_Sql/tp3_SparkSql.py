import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pathlib import Path



config = SparkConf().setAppName("TP3")
sc = SparkContext.getOrCreate(conf=config)
sqlContext = SQLContext(sc)

fichier = sc.textFile(Path(__file__).with_name("ngram_TP3.csv").as_uri())
tableau = fichier.map(lambda ligne: ligne.strip().replace("\t", ",").split(","))
tableau = tableau.filter(lambda l: len(l) == 5 and l[1].isdigit())
tableau = tableau.map(lambda l: (l[0], int(l[1]), int(l[2]), int(l[3]), int(l[4])))

schema = StructType([
    StructField("ngram", StringType()),
    StructField("Year", IntegerType()),
    StructField("Count", IntegerType()),
    StructField("Pages", IntegerType()),
    StructField("Books", IntegerType())
])

ngram = sqlContext.createDataFrame(tableau, schema)
ngram.createOrReplaceTempView("ngram")

print('q1:le count > 5')
r1_sql = sqlContext.sql("SELECT * FROM ngram WHERE Count > 5")
r1_sql.show()

r1_api = ngram.filter(col("Count") > 5)
r1_api.show()

print('q2:le nombre total dans chaque anne')
r2_sql = sqlContext.sql("SELECT Year, SUM(Count) AS total FROM ngram GROUP BY Year")
r2_sql.show()

r2_api = ngram.groupBy("Year").sum("Count")
r2_api.show()


print('q3:le plus grand count dans chaque annes')
r3_sql = sqlContext.sql("""
    SELECT n.ngram, n.Year, n.Count
    FROM ngram n
    JOIN (SELECT Year, MAX(Count) AS maxc FROM ngram GROUP BY Year) m
    ON n.Year = m.Year AND n.Count = m.maxc
""")
r3_sql.show()

maxpa = ngram.groupBy("Year").max("Count") \
    .withColumnRenamed("Year", "annee") \
    .withColumnRenamed("max(Count)", "maxc")
r3_api = ngram.join(maxpa, (ngram.Year == maxpa.annee) & (ngram.Count == maxpa.maxc)) \
    .select("ngram", "Year", "Count")
r3_api.show()

print('q4:apparait dans 20 and different')
r4_sql = sqlContext.sql("""
    SELECT ngram FROM ngram
    GROUP BY ngram
    HAVING COUNT(DISTINCT Year) = 20
""")
r4_sql.show()

r4_api = ngram.select("ngram", "Year").distinct() \
    .groupBy("ngram").count() \
    .filter(col("count") == 20) \
    .select("ngram")
r4_api.show()

print('q5:! dans la premiere parite et 9 dans le 2eme ')
r5_sql = sqlContext.sql("""
    SELECT DISTINCT ngram FROM ngram
    WHERE split(ngram, ' ')[0] LIKE '%!%'
    AND split(ngram, ' ')[1] LIKE '%9%'
""")
r5_sql.show()

parties = split(col("ngram"), " ")
r5_api = ngram.filter(parties.getItem(0).contains("!") & parties.getItem(1).contains("9")) \
    .select("ngram").distinct()
r5_api.show()

print('q6:bi-gramme present dans tous les annes')
nb_annees = sqlContext.sql("SELECT COUNT(DISTINCT Year) AS n FROM ngram").first()["n"]

r6_sql = sqlContext.sql(f"""
    SELECT ngram FROM ngram
    GROUP BY ngram
    HAVING COUNT(DISTINCT Year) = {nb_annees}
""")
r6_sql.show()

r6_api = ngram.select("ngram", "Year").distinct() \
    .groupBy("ngram").count() \
    .filter(col("count") == nb_annees) \
    .select("ngram")
r6_api.show()

print('q7:total de pages et de livres par bi-gramme et par anne trie ALPH')
r7_sql = sqlContext.sql("""
    SELECT ngram, Year, SUM(Pages) AS total_pages, SUM(Books) AS total_books
    FROM ngram
    GROUP BY ngram, Year
    ORDER BY ngram, Year
""")
r7_sql.show()

r7_api = ngram.groupBy("ngram", "Year").sum("Pages", "Books").sort("ngram", "Year")
r7_api.show()

print('q8:nombre de gramm diffrent par anne Dec')
r8_sql = sqlContext.sql("""
    SELECT Year, COUNT(DISTINCT ngram) AS nb_distincts
    FROM ngram
    GROUP BY Year
    ORDER BY Year DESC
""")
r8_sql.show()

r8_api = ngram.select("Year", "ngram").distinct() \
    .groupBy("Year").count() \
    .sort(desc("Year"))
r8_api.show()