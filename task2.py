import sys
import pyspark
import string
import json
import re
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

rowsNum = 0

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
    edit_distance_similarity=1 - float(edit_distance) / max(len(str1), len(str2))
    return edit_distance_similarity
#zip code
zipPat = r'^[0-9]{5}(?:-[0-9]{4})?$'
#phone number 
phonePatList = [r'^[2-9]\d{2}-\d{3}-\d{4}$', r'((\(\d{3}\) ?)|(\d{3}-))?\d{3}-\d{4}'\
    r'^(1?(-?\d{3})-?)?(\d{3})(-?\d{4})$']
#website
webSitePat = r'^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$'
#brouugh
boroughList = ['k', 'm', 'q', 'r', 'x', 'new york', 'brooklyn', 'manhattan', 'queens', 'bronx', 'the bronx', 'staten island', 'clifto', \
    'baldwin', 'astoria','mt.kisco', 'charlotte', 'bklyn', 'dobbs ferry', 'staten island', 'elmhurst', 'maspeth', 'nyc']
#car make
carMakeList = ['peter', 'inter', 'chevr', 'nissa', 'workh', 'acura', 'alfa romeo', 'aston martin', 'audi', 'bentley', \
               'bmw', 'bugatti', 'buick', 'cadillac', 'chevrolet',  'datsun', 'olds', 'chev', 'chrysl', 'mercur'\
    'chrysler', 'citroen', 'dodge', 'ferrari', 'fiat', 'ford', 'geely', 'general motors', 'gmc', 'honda', 'hyundai', \
        'infiniti', 'jaguar', 'jeep', 'kia', 'koenigsegg', 'lamborghini', 'land rover', 'lexus', 'masrati', \
            'mazda', 'mclaren', 'mercedes benz', 'mercedes-benz', 'mini', 'mitsubishi', 'nissan', 'pagani', 'peugeot', 'porsche', \
                'ram', 'renault', 'rolls royce', 'saab', 'subaru', 'suzuki', 'tata motors', 'tesla', 'chevy', 'chev',\
                    'toyota', 'volkswagen', 'volvo', 'cadi', 'unk', 'pont', 'hobbs', 'pontia', 'linc', 'plym', 'lincol',\
              'v.w', 'vw', 'gm', 'volks']
#color 
colorList = ['gry', 'gr', 'blk',  'orang', 'yellow', 'blue', 'red', 'green', 'black', 'brown', 'azure', 'ivory', 'teal', \
    'silver', 'purple', 'navy blue', 'navy', 'pea green', 'gray', 'orange', 'maroon', 'charcoal', 'aquamarine', 'coral', 'aquamarine', 'coral', \
        'fuchsia', 'wheat', 'lime', 'crimson', 'khaki', 'hot pink', 'megenta', 'olden', 'plum', 'olive', 'cyan', 'tan', 'biege',\
            'bl', 'bk', 'gy', 'wht', 'wh', 'gy', 'ltg', 'white', 'rd', 'silve', 'silvr', 'tn', 'gray', 'yw', 'dkg', 'grn', 'brn']
#business name,
businessList = ['market', 'pizza', 'restaurant', 'kitchen', 'shop', 'cafe', 'sushi', 'panda', 'noodle',\
               'bar', 'deli', 'hotel', 'service', 'pub', 'transportation', 'svce', 'cars', 'line']
#person name
personNamePat = r"^[a-z ,.'-]+$" 
# What if only have first name or last name? The A-Z is not required since all are lower()
#vehicle type
vehicleTypeList = ['station wagon/sport utility vehicle', 'ambulance', 'boat', 'trailer', 'motorcycle', 'bus', 'taxi', 'van', 'sedan', 'truck', \
                   'box truck', 'passenger vehicle', 'sport utility / station wagon', 'beverage truck', 'garbage or refuse', 'pick-up truck',\
                  'motorcycle', 'pk', 'tractor truck diesel', 'flat bed', 'bike']
#parks/playgrounds
ppPat = r"([a-zA-Z0-9]{1,10} ){1,5}(park|playground)$"
#street name
streetPat = r"([a-zA-Z0-9]{1,10} ){1,5}(place|avenue|ave|ave\.|court|ct|street|st|drive|dr|lane|ln|road|rd|blvd|plaza|parkway|pkwy)$"
#type of location
typeLocationList = ['abandoned building', 'airport terminal', 'airport', 'bank', 'church', 'clothing', 'boutique']
#lat/lon coordinates
latLonCoordPat = r"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$"
latLonCoordPat2 = r"^(([1-9]\d?)|(1[0-7]\d))(\.\d{3,7})|180|0(\.\d{1,6})?"
#address
addressPat = r"^\d+?[A-Za-z]*\s\w*\s?\w+?\s\w{2}\w*\s*\w*"
#neighborhood

#school level
schoollevel_list = ['k-1', 'k-2', 'k-3', 'k-4','k-5','k-6','k-7','k-8','k-9','k-10','k-11','k-12',\
    'elementary', 'elementary school', 'primary', 'primary school', 'high school', 'middle', 'middle school', 'high school transfer', 'yabc', \
        'senior high school', 'college']
#area_of_study
area_of_study_list = ['business', 'health professions', 'law & government', 'science & math',\
                      'architecture','visual art & design', 'engineering', 'film/video', 'hospitality, travel, & tourism',\
                      'environmental science', 'communications', 'teaching', 'jrotc', 'zoned', 'animal science']
#subject_in_school
subject_in_school_list = ['english', 'math', 'science', 'social studies']

#building_classification
buildingClassificationPat = r"[a-zA-Z][0-9]{1,2}-(walk-up|elevator|condops)"

#neighborhood
neighborhoodList = ['rosebank', 'new springville', 'grant city', 'sunnyside', 'grymes hill', 'belmont', 'fordham ', 'fordham heights', \
                    'fordham manor', 'jerome park', 'kingsbridge ', 'kingsbridge heights', 'van cortlandt village', 'marble hill', \
                    'norwood', 'riverdale ', 'central riverdale', 'fieldston', 'hudson hill', 'north riverdale', 'spuyten duyvil', \
                    'university heights', 'woodlawn', 'bathgate', 'claremont', 'concourse', 'east tremont', 'highbridge', 'hunts point', \
                    'longwood ', 'foxhurst', 'woodstock', 'melrose', 'morris heights', 'morrisania ', 'crotona park east', 'mott haven ', \
                    'port morris', 'the hub', 'tremont ', 'fairmount', 'mount eden', 'mount hope', 'west farms', 'allerton ', 'bronxwood', \
                    'laconia', 'baychester', 'bronxdale', 'city island', 'co-op city', 'eastchester', 'edenwald', 'pelham gardens', \
                    'pelham parkway', 'wakefield ', 'washingtonville', 'williamsbridge ', 'olinville', 'bronx river', 'bruckner', \
                    'clason point', 'country club', 'harding park', 'morris park ', 'indian village', 'parkchester', 'park versailles', \
                    'van nest', 'westchester heights', 'pelham bay', 'soundview', 'schuylerville', 'throggs neck', 'edgewater park', \
                    'unionport ', 'castle hill', 'westchester square', 'the pelham islands', 'the blauzes', 'chimney sweeps islands', \
                    'city island', 'hart island', 'high island', 'hunter island', 'rat island', 'twin island', 'north brother island', \
                    'south brother island', 'rikers island']

#city names dict, which will be loaded in main function
cityDict = {}
#city agency list
agencyDict = {}

def semanticMap(x):
    mat = str(x[0])
    lowerMat = mat.lower()
    #neighborhood
    if lowerMat in neighborhoodList:
        return ('neighborhood', x[1])
    #city
    if lowerMat in cityDict:
        return ('city', x[1])
    #city agency
    if lowerMat in agencyDict:
        return ('city_agency', x[1])
    #type of location
    if lowerMat in typeLocationList:
        return ('location_type', x[1])
    #vehicle type
    if lowerMat in vehicleTypeList:
        return ('vehicle_type', x[1])
    #school level
    if lowerMat in schoollevel_list:
        return ('school_level', x[1])
    #business name
    for business in businessList:
        if lowerMat.find(business) >= 0:
            return ('business_name', x[1])
    #zip code
    if re.match(zipPat, mat):
        return ('zip_code', x[1])
    #buildingClassification
    if re.match(buildingClassificationPat, lowerMat):
        return ('building_classification', x[1])
    #website
    if re.match(webSitePat, lowerMat):
        return ('website', x[1])
    latlonMat = lowerMat.replace(")","").replace("(","").replace(" ","")
    #lat/lon coordinates
    if re.match(latLonCoordPat, latlonMat):
        return ('lat_lon_cord', x[1])
    llMat = lowerMat.replace("+","").replace("-","")
    if re.match(latLonCoordPat2, llMat):
        return ('lat_lon_cord', x[1])
    #parks/playgrounds
    if re.match(ppPat, mat):
        return ('park_playground', x[1])  
    #address
    if re.match(addressPat, lowerMat):
        return ('address', x[1])  
    #street name
    if re.match(streetPat, lowerMat):
        return ('street_name', x[1])
    #phone number 
    for pat in phonePatList:
        if re.match(pat, mat):
            return ('phone_number', x[1])    
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
    
    if re.match(personNamePat, mat):
        return ('person_name', x[1])
    return ('other', x[1])

emptyWordList = ["", "-", "no data", "n/a", "null", "na", "unspecified"]
perList = []
exceptList = []

def checkEmpty(x):
    if not x[0]:
        return ('number_empty_cells',x[1])
    mat = str(x[0])
    if mat in emptyWordList:
        return ('number_empty_cells',x[1])
    return ('number_non_empty_cells',x[1])

def outErrorList(i):
    erlst = []
    print('processing file index {} excepted'.format(i))
    if os.path.isfile("./errorList2.txt"):
        with open('./errorList2.txt', 'r', encoding='UTF-8') as f:
            erStr = f.readlines()
            for line in erStr:
                line = line.replace("\n","")
                erlst.append(line)
        if str(i) not in erlst:
            with open('./errorList2.txt', 'a', encoding='UTF-8') as f:
                line = str(i)+"\n"
                f.write(line)
    else:
        with open("./errorList2.txt", 'w') as f:
            for i in exceptList:
                line = str(i)+"\n"
                f.write(line)

def outPerList(i, p):
    plst = []
    if os.path.isfile("./percision.txt"):
        with open("./percision.txt", 'r', encoding='UTF-8') as f:
            pStr = f.readlines()
            for line in pStr:
                line = line = line.split(" ")[0]
                plst.append(line)
        with open("./percision.txt", 'a', encoding='UTF-8') as f:
            if str(i) not in plst:
                line = str(i) + " " + str(p)+"\n"
                f.write(line)
    else:
        with open("./percision.txt", 'w', encoding='UTF-8') as f:
            for j in range(len(perList)):
                line = str(perList[j][0]) + " " + str(perList[j][1])+"\n"
                f.write(line)
    print('finised output percision')

if __name__ == "__main__":
    directory = "/user/hm74/NYCOpenData"
    outDir = "./task2out"
    labelList = []
    sc = SparkContext()
    fileLst = []
    perList = []
    ### label list
    with open('./labellist.txt', 'r', encoding='UTF-8') as f:
        labels = f.readlines()
        for label in labels:
            labelList.append(label.split(" ")[1].split(",").replace("\n",""))
    ### cluster
    with open('./cluster1.txt', 'r', encoding='UTF-8') as f:
        contentStr = f.read()
        fileLst = contentStr.replace('[',"").replace(']',"").replace("'","").replace("\n","").split(', ')
    ### city names list
    with open('./citylist.txt', 'r', encoding='UTF-8') as f:
        cityNames = f.readlines()
        for cityName in cityNames:
            cityDict[cityName.replace("\n","")] = 1
    print("Loaded {} city names".format(len(cityDict.keys())))
    ### city agencies list
    cityAgencyDir = "./cityagencylist.txt"
    with open(cityAgencyDir, 'r', encoding='UTF-8') as f:
        agencys = f.readlines()
        for agency in agencys:
            if agency.find("(") >= 0:
                agencyL = agency.split("(")
                for a in agencyL:
                    agencyDict[a.strip().replace(")","").lower()] = 1
    print("Loaded {} city agency names(Abbreviations and full names)".format(len(agencyDict.keys())))
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    fNum = len(fileLst)
    for i in range(0, 90):
        try:
            fileInfo = fileLst[i]
            fStr = fileInfo.split(".")
            fileName = fStr[0]
            colName = fStr[1]    
            print('*'*50)
            print('Processing file: {} with column: {}, current step: {}/{}'.format( \
                fileName, colName, i+1, fNum))
            if fileName == 'jz4z-kudi':
                exceptList.append(i)
                outErrorList(i)
                continue
            outputDicts = {}
            outputDicts['column_name'] = colName
            outputDicts['semantic_types'] = []
            filePath = directory + "/" + fileName +".tsv.gz"
            fileDF = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t', encoding = 'UTF-8', multiLine = True).load(filePath)
            columns = fileDF.columns
            if colName not in columns:
                if colName.find('CORE_SUBJECT') >= 0:
                    colName = 'CORE SUBJECT'
                else:
                    colName = colName.replace("_", " ") 
                print('Renamed selected column name')
            if colName not in columns:
                if colName == 'CORE SUBJECT':
                    for c in columns:
                        if c.find(colName) >= 0:
                            colName = c
                            print('Renamed selected column name')
                            break
                else:
                    for c in columns:
                        if cosSim(colName, c) >=0.8:
                            colName = c
                            print('Renamed selected column name')
                            break
            disRDD = fileDF.select(colName).rdd.filter( \
                lambda x: (x[colName] is not None and str(x[colName]).lower() not in emptyWordList)).cache()
            print('Finished selecting column from dataframe: {}'.format(fileName))
            rddCol = disRDD.map(lambda x: (x[colName], 1))
            disRDD = rddCol.reduceByKey(lambda x,y:(x+y))
            rowsNum = len(disRDD.collect())
            sRDD = disRDD.map(lambda x: semanticMap(x))
            SemRDD = sRDD.reduceByKey(lambda x,y:(x+y))
            SemList = SemRDD.collect()
            correctCnt = 0
            for sem in SemList:
                outputDicts['semantic_types'].append({
                    'semantic_type': labelList[i],
                    'label': sem[0],
                    'count': sem[1]
                })
                if sem[0] in labelList[i]:
                    correctCnt += sem[0]
            with open(outDir+"/"+fileName+"_semantic.json", 'w', encoding='UTF-8') as fw:
                json.dump(outputDicts,fw)
            print('Finished output file: {}, the index is: {}'.format(fileName, i))
            
            NEmptyRDD = disRDD.map(lambda x: checkEmpty(x)).reduceByKey(lambda x,y:(x+y))
            NEList = NEmptyRDD.collect()
            neCnt = 0
            neDct = {}
            for ne in NEList:
                neDct[ne[0]] = int(ne[1])
            if 'number_non_empty_cells' not in neDct:
                perList.append('all cells empty')
            else:
                neCnt = int(neDct['number_non_empty_cells'])
                per = float(correctCnt)/neCnt
                perList.append((i, per))
                outPerList(i, per)
        except Exception as e:
            exceptList.append(i)
            outErrorList(i)
