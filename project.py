import sys
import pyspark
import string
import json

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
    # name = fileNames[5]
    # filePath = directory + "/" + name +".tsv.gz"
    # fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
    # outputDicts = {}
    # noEmptyDF = fileDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in fileDF.columns])
    # outputDicts["columns"] = []
    # for c in fileDF.columns:
    #     pdict = {
    #         "column_name": c
    #     }
    #     nonEmptyCell = noEmptyDF.select(c).first()[c]
    #     pdict["number_non_empty_cells"] = int(nonEmptyCell)
    #     outputDicts["columns"].append(pdict)
    # outString = str(outputDicts)

    finalOut = []
    cnt = 0
    fNum = len(fileNames)
    for name in fileNames:
        outputDicts = {}
        cnt += 1
        print('*'*20)
        print('{}/{}'.format(cnt, fNum))
        outputDicts["dataset_name"] = name
        filePath = directory + "/" + name +".tsv.gz"
        fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
        outputDicts = {}
        outputDicts["columns"] = []
        #1 non empty cell
        print('*'*20)
        noEmptyDF = fileDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in fileDF.columns])
        print('finished 1')
        #2 empty cell
        print('*'*20)
        emptyDF = fileDF.select([count(when(~isnan(c) & ~col(c).isNull(), c)).alias(c) for c in fileDF.columns])
        print('finished 2')
        #3 distinct cell
        print('*'*20)
        disNumDF = fileDF.select([F.countDistinct(col(c)).alias(c) for c in fileDF.columns])
        print('finished 3')
        #4 most frequent top 5

        ## add to output json
        outputDicts["columns"] = []
        colCnt = 0
        colNum = len(fileDF.columns)
        for c in fileDF.columns:
            colCnt += 1
            print('{}/{} col: {}/{}'.format(cnt, fNum, colCnt, colNum))
            pdict = {
                "column_name": c
            }
            #1
            print('#1')
            nonEmptyCells = noEmptyDF.select(c).first()[c]
            pdict["number_non_empty_cells"] = int(nonEmptyCells)
            print('#1')
            #2
            print('#2')
            emptyCells = emptyDF.select(c).first()[c]
            pdict["number_empty_cells"] = int(emptyCells)
            print('#2')
            #3
            print('#3')
            disNum = disNumDF.select(c).first()[c]
            pdict["number_empty_cells"] = int(disNum)
            print('#3')
            
            outputDicts["columns"].append(pdict)
        outString = str(outputDicts)
        finalOut.append(outString)
    outRDD = sc.parallelize(finalOut)
    outRDD.saveAsTextFile("test.out")

       
        
    # out put format
    # {
    #     "dataset_name": "",
    #     "columns":[
    #         {
    #             "column_name": "Category",
    #             "number_non_empty_cells": 20,
    #                 ....
    #         },
    #         {
    #             "column_name": "Cohort Year",
    #             "number_non_empty_cells": 20,
    #                 ....
    #         }
    #     ]
    #     "semantic_types": [
    #         {
    #         "semantic_type": label of the semantic type, e.g.: "Business
    #         name" (type: string)
    #         "count": the number of values in the column that belong to
    #         that semantic type (type: integer)
    #         },
    #     ]
    # }

    