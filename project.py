import sys
import pyspark
import string
from csv import reader

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col
import pyspark.sql.functions as F

if __name__ == "__main__":

    directory = "/user/hm74/NYCOpenData"

    sc = SparkContext()
    fileNames = sc.textFile(directory+"/datasets.tsv").map(lambda x: x.split('\t')[0]).collect()
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    #test code
    # filePath = directory + "/" + fileNames[0] +".tsv.gz"
    # fileRDD = sc.textFile(filePath).map(lambda x: x.split('\t'))
    # header = fileRDD.first()
    # fileRDD = fileRDD.filter(lambda x: x!= header)
    # fileDF = fileRDD.toDF(header)
    # disNumDF = fileDF.agg(*(F.countDistinct(col(c)).alias(c) for c in fileDF.columns))

    for name in fileNames:
        filePath = directory + "/" + name +".tsv.gz"
        fileRDD = sc.textFile(filePath).map(lambda x: x.split('\t'))
        header = fileRDD.first()
        #rdd doesnt has correct names of keys
        fileRDD = fileRDD.filter(lambda x: x!= header)
        #add header
        fileDF = fileRDD.toDF(header)
        #fileDF.coalesce(1).write.format('json').save('test.json')
        #1 non empty cell
        noEmptyDF = fileDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in fileDF.columns]).show()
        #2 empty cell
        emptyDF = fileDF.select([count(when(not isnan(c) and not col(c).isNull(), c)).alias(c) for c in fileDF.columns]).show()
        #3 distinct cell
        disNumDF = fileDF.agg(*(F.countDistinct(col(c)).alias(c) for c in fileDF.columns))
        #4 most frequent top 5
        
        
    