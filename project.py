import sys
import pyspark
import string
import json
import re

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

def editDis(str1, str2):
    len_str1 = len(str1) + 1
    len_str2 = len(str2) + 1
    #create matrix
    matrix = [0 for n in range(len_str1 * len_str2)]
    #init x axis
    for i in range(len_str1):
        matrix[i] = i
    #init y axis
    for j in range(0, len(matrix), len_str1):
        if j % len_str1 == 0:
            matrix[j] = j // len_str1
        
    for i in range(1, len_str1):
        for j in range(1, len_str2):
            if str1[i-1] == str2[j-1]:
                cost = 0
            else:
                cost = 1
            matrix[j*len_str1+i] = min(matrix[(j-1)*len_str1+i]+1,
                                        matrix[j*len_str1+(i-1)]+1,
                                        matrix[(j-1)*len_str1+(i-1)] + cost)      
    return matrix[-1]

def cosSim(str1, str2):
    edit_distance = editDis(str1, str2)
    edit_distance_similarity=1 - edit_distance / max(len(str1), len(str2))
    return edit_distance_similarity

#zip code
zipPat = r'^[0-9]{5}(?:-[0-9]{4})?$'
#phone number 
phonePatList = [r'^[2-9]\d{2}-\d{3}-\d{4}$', r'((\(\d{3}\) ?)|(\d{3}-))?\d{3}-\d{4}'\
    r'^(1?(-?\d{3})-?)?(\d{3})(-?\d{4})$']
#website
webSitePat = r'^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$'
#brouugh
boroughList = ['brooklyn', 'manhattan', 'queens', 'the bronx', 'staten island']
#car make
carMakeList = ['acura', 'alfa romeo', 'aston martin', 'audi', 'bentley', 'bmw', 'bugatti', 'buick', 'cadillac', 'chevrolet', \
    'chrysler', 'citroen', 'dodge', 'ferrari', 'fiat', 'ford', 'geely', 'general motors', 'gmc', 'honda', 'hyundai', \
        'infiniti', 'jaguar', 'jeep', 'kia', 'koenigsegg', 'lamborghini', 'land rover', 'lexus', 'masrati', \
            'mazda', 'mclaren', 'mercedes benz', 'mercedes-benz', 'mini', 'mitsubishi', 'nissan', 'pagani', 'peugeot', 'porsche', \
                'ram', 'renault', 'rolls royce', 'saab', 'subaru', 'suzuki', 'tata motors', 'tesla', \
                    'toyota', 'volkswagen', 'volvo']
#color 
colorList = ['white', 'yellow', 'blue', 'red', 'green', 'black', 'brown', 'azure', 'ivory', 'teal', \
    'silver', 'purple', 'navy blue', 'pea green', 'gray', 'orange', 'maroon', 'charcoal', 'aquamarine', 'coral', 'aquamarine', 'coral', \
        'fuchsia', 'wheat', 'lime', 'crimson', 'khaki', 'hot pink', 'megenta', 'olden', 'plum', 'olive', 'cyan']
#business name
businessNamePat = r"^((?![\^!@#$*~ <>?]).)((?![\^!@#$*~<>?]).){0,73}((?![\^!@#$*~ <>?]).)$"
#person name
personNamePat = r"^[a-zA-Z]+(([',. -][a-zA-Z ])?[a-zA-Z]*)*$" 
# What if only have first name or last name? The A-Z is not required since all are lower()
#vehicle type
vehicleTypeList = ['ambulance', 'boat', 'trailer', 'motorcycle', 'bus', 'taxi', 'van']
#parks/playgrounds
ppPat = r"([a-zA-Z0-9]{1,10} ){1,5}(park|playground)$"
#street name
streetPat = r"([a-zA-Z0-9]{1,10} ){1,5}(avenue|ave|court|ct|street|st|drive|dr|lane|ln|road|rd|blvd|plaza|parkway|pkwy)$"
#type of location
typeLocationList = ['abandoned building', 'airport terminal', 'airport', 'bank', 'church', 'clothing', 'boutique']
#lat/lon coordinates
latLonCoordPat = r"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$"
#address
addressPat = r"^\d+?[A-Za-z]*\s\w*\s?\w+?\s\w{2}\w*\s*\w*$"
#neighborhood

#school level
schoollevel_list = ['elementary', 'k-8', 'k-3', 'k-2', 'high school', 'middle', 'high school transfer', 'YABC']

def semanticMap(x):
    mat = str(x[0])
    lowerMat = mat.lower()
    #type of location
    if lowerMat in typeLocationList:
        return ('Type of location', x[1])
    #vehicle type
    if lowerMat in vehicleTypeList:
        return ('Vehicle Type', x[1])
    #color 
    if lowerMat in colorList:
        return ('Color', x[1])
    # for color in colorList:
    #     if cosSim(color, lowerMat ) >= 0.8:
    #         return ('Color', x[1])

    #borough
    if  lowerMat in boroughList:
        return ('Borough', x[1])
    # for borough in boroughList:
    #     if cosSim(borough, lowerMat) >= 0.8:
    #         return ('Borough', x[1])

    #car make
    if lowerMat in carMakeList:
        return ('Car make', x[1])
    # for carMake in carMakeList:
    #     # python effiecent lib for sim
    #     if difflib.SequenceMatcher(None, carMake, lowerMat).ratio() >= 0.8:
    #         return ('Car make', x[1])

    #school level
    if lowerMat in schoollevel_list:
        return ('School Level', x[1])

    #parks/playgrounds
    if re.match(ppPat, mat):
        return ('Parks/Playgrounds', x[1])
    #zip code
    if re.match(zipPat, mat):
        return ('Zip code', x[1])
    #phone number 
    for pat in phonePatList:
        if re.match(pat, mat):
            return ('Phone Number', x[1])    
    #website
    if re.match(webSitePat, lowerMat):
        return ('Websites', x[1])
    #street name
    if re.match(streetPat, lowerMat):
        return ('Street name', x[1])
    #lat/lon coordinates
    if re.match(latLonCoordPat, lowerMat):
        return ('Address', x[1])
    #address
    if re.match(addressPat, lowerMat):
        return ('Address', x[1])
    #business name
    # if re.match(businessNamePat, mat):
    #     return ('Business name', x[1])
    #person name
    if re.match(personNamePat, mat):
        return ('Person name', x[1])
    
    return ('Other', x[1])

# test code here
def test(sc):
    name = fileNames[1]
    filePath = directory + "/" + name +".tsv.gz"
    fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
    outputDicts = {}
    fileDF.agg(*(F.countDistinct(col(c)).alias(c) for c in fileDF.columns))
    # noEmptyDF = fileDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in fileDF.columns])
    # outputDicts["columns"] = []
    # for c in fileDF.columns:
    #     pdict = {
    #         "column_name": c
    #     }
    #     nonEmptyCell = noEmptyDF.select(c).first()[c]
    #     pdict["number_non_empty_cells"] = int(nonEmptyCell)
    #     outputDicts["columns"].append(pdict)
    outString = str(outputDicts)
    print(outString)

# profile tasks here
# remeber removing output files from hfs
def profile(sc):
    fNum = len(fileNames)
    cnt = 0
    for name in fileNames:
        datasetList = []
        outputDicts = {}
        cnt += 1
        print('{}/{}'.format(cnt, fNum))
        outputDicts["dataset_name"] = name
        filePath = directory + "/" + name +".tsv.gz"
        fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
        
        print('creating dataframe for ' + name)
        #1 non empty cell
        noEmptyDF = fileDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in fileDF.columns])
        #2 empty cell
        print('*'*20)
        emptyDF = fileDF.select([count(when(~isnan(c) & ~col(c).isNull(), c)).alias(c) for c in fileDF.columns])
        #3 distinct cell
        
        #4 most frequent top 5

        #5
        # print('#5')
        # new_fileDF = fileDF.select([str_type(col).alias(col + 'Type') for col in fileDF.columns] + fileDF.columns)
        # Integer_Real_Date_info = new_fileDF.select([F.count(transfer_to_int(F.when(new_fileDF[c+'Type'] == 'INTEGER (LONG)', new_fileDF[c]))).alias('int_count_' + c) for c in fileDF.columns] \
        #               + [F.max(transfer_to_int(F.when(new_fileDF[c+'Type'] == 'INTEGER (LONG)', new_fileDF[c]))).alias('int_max_' + c) for c in fileDF.columns] \
        #               + [F.min(transfer_to_int(F.when(new_fileDF[c+'Type'] == 'INTEGER (LONG)', new_fileDF[c]))).alias('int_min_' + c) for c in fileDF.columns] \
        #               + [F.mean(transfer_to_int(F.when(new_fileDF[c+'Type'] == 'INTEGER (LONG)', new_fileDF[c]))).alias('int_mean_' + c) for c in fileDF.columns] \
        #               + [F.stddev(transfer_to_int(F.when(new_fileDF[c+'Type'] == 'INTEGER (LONG)', new_fileDF[c]))).alias('int_stddev_' + c) for c in fileDF.columns]\
        #           + [F.count(transfer_to_double(F.when(new_fileDF[c+'Type'] == 'REAL', new_fileDF[c]))).alias('double_count_' + c) for c in fileDF.columns]\
        #               + [F.max(transfer_to_double(F.when(new_fileDF[c+'Type'] == 'REAL', new_fileDF[c]))).alias('double_max_' + c) for c in fileDF.columns]\
        #               + [F.min(transfer_to_double(F.when(new_fileDF[c+'Type'] == 'REAL', new_fileDF[c]))).alias('double_min_' + c) for c in fileDF.columns]\
        #               + [F.mean(transfer_to_double(F.when(new_fileDF[c+'Type'] == 'REAL', new_fileDF[c]))).alias('double_mean_' + c) for c in fileDF.columns]\
        #               + [F.stddev(transfer_to_double(F.when(new_fileDF[c+'Type'] == 'REAL', new_fileDF[c]))).alias('double_stddev_' + c) for c in fileDF.columns]\
        #           + [F.count(uniform_date_format(F.when(new_fileDF[c+'Type'] == 'DATE/TIME', new_fileDF[c]))).alias('date_count_' + c) for c in fileDF.columns]\
        #             + [F.max(uniform_date_format(F.when(new_fileDF[c+'Type'] == 'DATE/TIME', new_fileDF[c]))).alias('date_max_' + c) for c in fileDF.columns]\
        #             + [F.min(uniform_date_format(F.when(new_fileDF[c+'Type'] == 'DATE/TIME', new_fileDF[c]))).alias('date_min_' + c) for c in fileDF.columns]).first()
        # ## add to output json
        outputDicts["columns"] = []
        colCnt = 0
        colNum = len(fileDF.columns)
        print('finished creating')
        for c in fileDF.columns:
            colCnt += 1
            print('{}/{} col: {}/{}'.format(cnt, fNum, colCnt, colNum))
            pdict = {
                "column_name": c
            }
            #1
            print('#1 number_non_empty_cells')
            nonEmptyCells = noEmptyDF.select(c).first()[c]
            pdict["number_non_empty_cells"] = int(nonEmptyCells)
            print('#1 finished')
            #2
            print('#2 number_empty_cells')
            emptyCells = emptyDF.select(c).first()[c]
            pdict["number_empty_cells"] = int(emptyCells)
            print('#2 finished')
            #3
            print('#3 number_distinct_values')
            disRDD = fileDF.select(c).rdd
            rddCol = disRDD.map(lambda x: (x[c], 1))
            disRDD = rddCol.reduceByKey(lambda x,y:(x+y))
            disCol = disRDD.collect()
            emptyCells = len(disCol)
            pdict["number_distinct_values"] = emptyCells
            print('#3 finished')
            #4
            print('#4 frequent_values')
            topRDD = disRDD.sortBy(lambda x: -x[1]).take(5)
            topList = []
            for index in range(5):
                topList.append(topRDD[index][0])
            pdict["frequent_values"] = topList
            print('#4 finished')
            # Semantic
            print('Semantic')
            pdict['semantic_types'] = []
            sRDD = disRDD.map(lambda x: semanticMap(x))
            SemRDD = sRDD.reduceByKey(lambda x,y:(x+y))
            SemList = SemRDD.collect()
            #print(SemRDD.collect())
            for sem in SemList:
                pdict['semantic_types'].append({
                    'semantic_type': sem[0],
                    'count': sem[1]
                })
            print(pdict['semantic_types'])
            #add to out dicts
            outputDicts["columns"].append(pdict)
        outString = str(json.dumps(outputDicts,indent=1))
        datasetList.append(outString)
        #print(datasetList)
        outRDD = sc.parallelize(datasetList)
        outRDD.saveAsTextFile(name + ".jsonOut")
            # #task 1.5
            # print('#5 data types')
            # data_types_List = []
            # if Integer_Real_Date_info['int_count_' + c] != 0:
            #     int_data_type = {}
            #     int_data_type['type'] = 'INTEGER (LONG)'
            #     int_data_type['count'] = Integer_Real_Date_info['int_count_'+ c]
            #     int_data_type['max_value'] = Integer_Real_Date_info['int_max_'+ c]
            #     int_data_type['mean_value'] = Integer_Real_Date_info['int_mean_'+ c]
            #     int_data_type['stddev_value'] = Integer_Real_Date_info['int_stddev_' + c]
            #     data_types_List.append(int_data_type)
            # if Integer_Real_Date_info['double_count_' + c] != 0:
            #     double_data_type = {}
            #     double_data_type['type'] = 'REAL'
            #     double_data_type['count'] = Integer_Real_Date_info['double_count_'+ c]
            #     double_data_type['max_value'] = Integer_Real_Date_info['double_max_'+ c]
            #     double_data_type['mean_value'] = Integer_Real_Date_info['double_mean_'+ c]
            #     double_data_type['stddev_value'] = Integer_Real_Date_info['double_stddev_' + c]
            #     data_types_List.append(double_data_type)
            # if Integer_Real_Date_info['date_count_' + c] != 0:
            #     date_data_type = {}
            #     date_data_type['type'] = 'DATE/TIME'
            #     date_data_type['count'] = Integer_Real_Date_info['date_count_'+ c]
            #     date_data_type['max_value'] = Integer_Real_Date_info['date_max_' + c]
            #     date_data_type['min_value'] = Integer_Real_Date_info['date_min_' + c]
            #     data_types_List.append(date_data_type)
            # if  new_fileDF.filter(new_fileDF[c + 'Type'] == 'TEXT').count() != 0:
            #     shortest_values = new_fileDF.sort(count_text_length(c).asc()).select(c).limit(5).collect()
            #     shortest_values = [shortest_values[i][0] for i in range(0,5)]
            #     longest_values = new_fileDF.sort(count_text_length(c).desc()).select(c).limit(5).collect()
            #     longest_values = [longest_values[i][0] for i in range(0,5)]
            #     average_length = new_fileDF.select(F.mean(count_text_length(c))).first()[0]
            #     text_data_type = {}
            #     text_data_type['type'] = 'TEXT'
            #     text_data_type['shortest_values'] = shortest_values
            #     text_data_type['longest_values'] = longest_values
            #     text_data_type['average_length'] = average_length
            #     data_types_List.append(text_data_type)
            # pdict['data_types'] = data_types_List
            # print('#5 finished')
        

if __name__ == "__main__":

    directory = "/user/hm74/NYCOpenData"

    sc = SparkContext()
    fileNames = sc.textFile(directory+"/datasets.tsv").map(lambda x: x.split('\t')[0]).collect()
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    profile(sc)
    #test(sc)
    
       
        
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

    