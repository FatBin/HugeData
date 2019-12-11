#import pandas
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from itertools import chain, combinations
#import pyspark.sql.functions as F

def key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )

directory = "/user/hm74/NYCOpenData"
sc = SparkContext()
fileNames = sc.textFile(directory+"/datasets.tsv").map(lambda x: x.split('\t')[0]).collect()
spark = SparkSession \
.builder \
.appName("Python Spark SQL Project") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

# test_code for one file
name = fileNames[0]
filePath = directory + "/" + name +".tsv.gz"
fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
# fileDF = pandas.read_csv(filePath, compression='gzip')

#key_options = key_options(list(fileDF))
#for candidate in key_options:
for candidate in key_options(list(fileDF)):
    deduped = fileDF.drop_duplicates(candidate)
    if len(deduped.index) == len(fileDF.index):
        print(','.join(candidate))