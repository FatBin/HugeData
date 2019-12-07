import sys
import pyspark
import string
import json
import re
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col
import pyspark.sql.functions as F
import numpy as np
from numpy import long
from pyspark.sql.functions import udf
import datetime
from dateutil.parser import parse
import difflib

import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

@udf("string")
def str_type(string):
    if string is None:
        return None
    try:
        long(string)
        if '.' not in str(string):
            return 'INTEGER (LONG)'
    except:
        string
    try:
        float(string)
        return 'REAL'
    except:
        string
    try:
        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%Y%m%d", "%m%d%Y"]:
            try:
                datetime.datetime.strptime(string, fmt).date()
                return 'DATE/TIME'
            except:
                continue
    except:
        string
    return 'TEXT'



@udf("int")
def transfer_to_int(data):
    try:
        result = long(data)
        return result
    except:
        return None


@udf("double")
def transfer_to_double(data):
    try:
        result = float(data)
        return result
    except:
        return None

@udf("string")
def uniform_date_format(data):
    try:
        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%Y%m%d", "%m%d%Y"]:
            try:
                datetime.datetime.strptime(data, fmt).date()
                return 'DATE/TIME'
            except:
                continue
    except:
        return None
    return None

@udf("int")
def count_text_length(data):
    try:
        result = long(len(data))
        return result
    except:
        return None

emptyWordList = ["", "no data", "n/a", "null"]

def checkEmpty(x):
    if not x[0]:
        return ('number_empty_cells',x[1])
    mat = str(x[0])
    if mat in emptyWordList:
        return ('number_empty_cells',x[1])
    return ('number_non_empty_cells',x[1])

exceptList = []

def outErrorList(i):
    erlst = []
    print('processing file index {} excepted'.format(i))
    if os.path.isfile("./errorList.txt"):
        with open('./errorList.txt', 'r', encoding='UTF-8') as f:
            erStr = f.readlines()
            for line in erStr:
                line = line.replace("\n","")
                erlst.append(line)
        if str(i) not in erlst:
            with open('./errorList.txt', 'a', encoding='UTF-8') as f:
                line = str(i)+"\n"
                f.write(line)
    else:
        with open("./errorList.txt", 'w') as f:
            for i in exceptList:
                line = str(i)+"\n"
                f.write(line)

if __name__ == "__main__":

    directory = "/user/hm74/NYCOpenData"
    outDir = "./task1out"

    sc = SparkContext()
    fileNames = sc.textFile(directory+"/datasets.tsv").map(lambda x: x.split('\t')[0]).collect()
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    fNum = len(fileNames)
    for i in range(580, len(fileNames)):
        try:
            name = fileNames[i]
            outputDicts = {}
            print('*'*50)
            print('current step {}/{}'.format(i+1, fNum))
            outputDicts["dataset_name"] = name
            filePath = directory + "/" + name +".tsv.gz"
            fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath).cache()
            print('creating dataframe for ' + name)
            # #1 non empty cell
            # noEmptyDF = fileDF.select([count(when((~col("`"+c+"`").isin(emptyWordsList)) & (~col("`"+c+"`").isNull(), c))).alias(c) for c in fileDF.columns])
            # #2 empty cell     
            # emptyDF = fileDF.select([count(when((col("`"+c+"`").isin(emptyWordsList)) | (col("`"+c+"`").isNull(), c))).alias(c) for c in fileDF.columns])
            #5
            print('#4 prepare data type info')
            new_fileDF = fileDF.select([str_type("`"+c+"`").alias(c + 'Type') for c in fileDF.columns] + ["`"+c+"`" for c in fileDF.columns]).cache()
            Integer_Real_Date_info = new_fileDF.select([F.count(transfer_to_int(F.when(new_fileDF["`"+c+'Type`'] == 'INTEGER (LONG)', new_fileDF["`"+c+"`"]))).alias('int_count_' + c) for c in fileDF.columns] \
                        + [F.max(transfer_to_int(F.when(new_fileDF["`"+c+'Type`'] == 'INTEGER (LONG)', new_fileDF["`"+c+"`"]))).alias('int_max_' + c) for c in fileDF.columns] \
                        + [F.min(transfer_to_int(F.when(new_fileDF["`"+c+'Type`'] == 'INTEGER (LONG)', new_fileDF["`"+c+"`"]))).alias('int_min_' + c) for c in fileDF.columns] \
                        + [F.mean(transfer_to_int(F.when(new_fileDF["`"+c+'Type`'] == 'INTEGER (LONG)', new_fileDF["`"+c+"`"]))).alias('int_mean_' + c) for c in fileDF.columns] \
                        + [F.stddev(transfer_to_int(F.when(new_fileDF["`"+c+'Type`'] == 'INTEGER (LONG)', new_fileDF["`"+c+"`"]))).alias('int_stddev_' + c) for c in fileDF.columns]\
                    + [F.count(transfer_to_double(F.when(new_fileDF["`"+c+'Type`'] == 'REAL', new_fileDF["`"+c+"`"]))).alias('double_count_' + c) for c in fileDF.columns]\
                        + [F.max(transfer_to_double(F.when(new_fileDF["`"+c+'Type`'] == 'REAL', new_fileDF["`"+c+"`"]))).alias('double_max_' + c) for c in fileDF.columns]\
                        + [F.min(transfer_to_double(F.when(new_fileDF["`"+c+'Type`'] == 'REAL', new_fileDF["`"+c+"`"]))).alias('double_min_' + c) for c in fileDF.columns]\
                        + [F.mean(transfer_to_double(F.when(new_fileDF["`"+c+'Type`'] == 'REAL', new_fileDF["`"+c+"`"]))).alias('double_mean_' + c) for c in fileDF.columns]\
                        + [F.stddev(transfer_to_double(F.when(new_fileDF["`"+c+'Type`'] == 'REAL', new_fileDF["`"+c+"`"]))).alias('double_stddev_' + c) for c in fileDF.columns]\
                    + [F.count(uniform_date_format(F.when(new_fileDF["`"+c+'Type`'] == 'DATE/TIME', new_fileDF["`"+c+"`"]))).alias('date_count_' + c) for c in fileDF.columns]\
                        + [F.max(uniform_date_format(F.when(new_fileDF["`"+c+'Type`'] == 'DATE/TIME', new_fileDF["`"+c+"`"]))).alias('date_max_' + c) for c in fileDF.columns]\
                        + [F.min(uniform_date_format(F.when(new_fileDF["`"+c+'Type`'] == 'DATE/TIME', new_fileDF["`"+c+"`"]))).alias('date_min_' + c) for c in fileDF.columns]).first()
            # ## add to output json
            outputDicts["columns"] = []
            colCnt = 0
            colNum = len(fileDF.columns)
            print('finished creating dataframe')
            for c in fileDF.columns:
                colCnt += 1
                print('current step {}/{} col: {}/{}'.format(i+1, fNum, colCnt, colNum))
                pdict = {
                    "column_name": c
                }
                #3
                print('#1 number_distinct_values')
                disRDD = fileDF.select("`"+c+"`").rdd
                rddCol = disRDD.map(lambda x: (x[c], 1))
                disRDD = rddCol.reduceByKey(lambda x,y:(x+y))
                disCol = disRDD.collect()
                disCell = len(disCol)
                pdict["number_distinct_values"] = int(disCell)
                print('#1 finished')
                #1
                print('#2 number_non_empty_cells & number_non_empty_cells')
                NEmptyRDD = disRDD.map(lambda x: checkEmpty(x)).reduceByKey(lambda x,y:(x+y))
                NEList = NEmptyRDD.collect()
                for ne in NEList:
                    pdict[ne[0]] = str(ne[1])
                if 'number_empty_cells' not in pdict:
                    pdict['number_empty_cells'] = 0
                if 'number_non_empty_cells' not in pdict:
                    pdict['number_non_empty_cells'] = 0
                print('#2 finished')   
                # pdict["number_non_empty_cells"] = int(nonEmptyCells)
                # print('#1 finished')
                # print('#2 number_empty_cells')
                # emptyCells = emptyDF.select("`"+c+"`").first()[c]
                # pdict["number_empty_cells"] = int(emptyCells)
                # print('#2 finished')
                #4
                print('#3 frequent_values')
                topRDD = disRDD.sortBy(lambda x: -x[1]).take(5)
                topList = []
                for index in range(len(topRDD)):
                    topList.append(str(topRDD[index][0]))
                pdict["frequent_values"] = topList
                print('#3 finished')
                #5
                print('#4 data types')
                data_types_List = []
                if Integer_Real_Date_info['int_count_' + c] != 0:
                    int_data_type = {}
                    int_data_type['type'] = 'INTEGER (LONG)'
                    int_data_type['count'] = str(Integer_Real_Date_info['int_count_'+ c])
                    int_data_type['max_value'] = str(Integer_Real_Date_info['int_max_'+ c])
                    int_data_type['mean_value'] = str(Integer_Real_Date_info['int_mean_'+ c])
                    int_data_type['stddev_value'] = str(Integer_Real_Date_info['int_stddev_' + c])
                    data_types_List.append(int_data_type)
                if Integer_Real_Date_info['double_count_' + c] != 0:
                    double_data_type = {}
                    double_data_type['type'] = 'REAL'
                    double_data_type['count'] = str(Integer_Real_Date_info['double_count_'+ c])
                    double_data_type['max_value'] = str(Integer_Real_Date_info['double_max_'+ c])
                    double_data_type['mean_value'] = str(Integer_Real_Date_info['double_mean_'+ c])
                    double_data_type['stddev_value'] = str(Integer_Real_Date_info['double_stddev_' + c])
                    data_types_List.append(double_data_type)
                if Integer_Real_Date_info['date_count_' + c] != 0:
                    date_data_type = {}
                    date_data_type['type'] = 'DATE/TIME'
                    date_data_type['count'] = str(Integer_Real_Date_info['date_count_'+ c])
                    date_data_type['max_value'] = str(Integer_Real_Date_info['date_max_' + c])
                    date_data_type['min_value'] = str(Integer_Real_Date_info['date_min_' + c])
                    data_types_List.append(date_data_type)
                if  new_fileDF.filter(new_fileDF['`'+c + 'Type`'] == 'TEXT').count() != 0:
                    shortest_values = new_fileDF.sort(count_text_length('`'+c+'`').asc()).select("`"+c+"`").limit(5).collect()
                    shortest_values = [shortest_values[i][0] for i in range(len(shortest_values))]
                    longest_values = new_fileDF.sort(count_text_length('`'+c+'`').desc()).select("`"+c+"`").limit(5).collect()
                    longest_values = [longest_values[i][0] for i in range(len(longest_values))]
                    average_length = new_fileDF.select(F.mean(count_text_length('`'+c+'`'))).first()[0]
                    text_data_type = {}
                    text_data_type['type'] = 'TEXT'
                    text_data_type['shortest_values'] = str(shortest_values)
                    text_data_type['longest_values'] = str(longest_values)
                    text_data_type['average_length'] = str(average_length)
                    data_types_List.append(text_data_type)
                pdict['data_types'] = data_types_List
                print('#4 finished')    
                #add to out dicts
                outputDicts["columns"].append(pdict)
            with open(outDir+"/"+name+"_generic.json", 'w') as fw:
                json.dump(outputDicts,fw)
            print('Finished output file: {}, the index is: {}'.format(name, i))
        except Exception as e:
            exceptList.append(i)
            outErrorList(i)