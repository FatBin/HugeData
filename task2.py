import sys
import pyspark
import string
import json
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

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
schoollevel_list = ['k-1', 'k-2', 'k-3', 'k-4','k-5','k-6','k-7','k-8','k-9','k-10','k-11','k-12'\
    'elementary', 'elementary school', 'primary', 'primary school', 'high school', 'middle', 'middle school', 'high school transfer', 'yabc', \
        'senior high school', 'college']

def semanticMap(x):
    mat = str(x[0])
    lowerMat = mat.lower()
    #type of location
    if lowerMat in typeLocationList:
        return ('location_type', x[1])
    #vehicle type
    if lowerMat in vehicleTypeList:
        return ('vehicle_type', x[1])
    #color 
    # if lowerMat in colorList:
    #     return ('Color', x[1])
    for color in colorList:
        if cosSim(color, lowerMat ) >= 0.8:
            return ('color', x[1])

    #borough
    # if  lowerMat in boroughList:
    #     return ('Borough', x[1])
    for borough in boroughList:
        if cosSim(borough, lowerMat) >= 0.8:
            return ('borough', x[1])

    #car make
    # if lowerMat in carMakeList:
    #     return ('Car make', x[1])
    for carMake in carMakeList:
        if cosSim(carMake, lowerMat) >= 0.8:
            return ('car_make', x[1])
        # python effiecent lib for sim
        # if difflib.SequenceMatcher(None, carMake, lowerMat).ratio() >= 0.8:
        #     return ('car_make', x[1])

    #school level
    if lowerMat in schoollevel_list:
        return ('school_level', x[1])

    #parks/playgrounds
    if re.match(ppPat, mat):
        return ('park_playground', x[1])
    #zip code
    if re.match(zipPat, mat):
        return ('zip_code', x[1])
    #phone number 
    for pat in phonePatList:
        if re.match(pat, mat):
            return ('phone_number', x[1])    
    #website
    if re.match(webSitePat, lowerMat):
        return ('websites', x[1])
    #street name
    if re.match(streetPat, lowerMat):
        return ('street_name', x[1])
    #lat/lon coordinates
    if re.match(latLonCoordPat, lowerMat):
        return ('lat_lon_cord', x[1])
    #address
    if re.match(addressPat, lowerMat):
        return ('address', x[1])
    #business name
    # if re.match(businessNamePat, mat):
    #     return ('Business name', x[1])
    #person name
    if re.match(personNamePat, mat):
        return ('person_name', x[1])
    
    return ('other', x[1])

if __name__ == "__main__":
    directory = "/user/hm74/NYCOpenData"
    outDir = "./task2out"

    sc = SparkContext()
    fileLst = []
    with open('./cluster1.txt', 'r') as f:
        contentStr = f.read()
        fileLst = contentStr.replace('[',"").replace(']',"").replace("'","").replace("\n","").split(', ')
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    fNum = len(fileLst)
    cnt = 0
    for i in range(0, len(fileLst)):
        fileInfo = fileLst[i]
        cnt += 1
        fStr = fileInfo.split(".")
        fileName = fStr[0]
        colName = fStr[1]
        print('Processing file: {} with column: {}, current step: {}/{}'.format( \
            fileName, colName, cnt, fNum))
        outputDicts = {}
        outputDicts['column_name'] = colName
        outputDicts['semantic_types'] = []
        filePath = directory + "/" + fileName +".tsv.gz"
        fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(filePath)
        columns = fileDF.columns
        if colName not in columns:
            colName = colName.replace("_", " ") 
        print('Finished selecting column from dataframe: {}'.format(fileName))
        disRDD = fileDF.select(colName).rdd
        rddCol = disRDD.map(lambda x: (x[colName], 1))
        disRDD = rddCol.reduceByKey(lambda x,y:(x+y))
        sRDD = disRDD.map(lambda x: semanticMap(x))
        SemRDD = sRDD.reduceByKey(lambda x,y:(x+y))
        SemList = SemRDD.collect()
        for sem in SemList:
            outputDicts['semantic_types'].append({
                'semantic_type': "unknow",
                'label': sem[0],
                'count': sem[1]
            })
        with open(outDir+"/"+fileName+"_semantic.json", 'w') as fw:
            json.dump(outputDicts,fw)
        print('Finished output file: {}, the index is: {}'.format(fileName, cnt-1))