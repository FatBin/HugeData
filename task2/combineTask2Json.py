import sys
import string
import json
import os

### cluster
fileLst = []
with open('./cluster1.txt', 'r') as f:
    contentStr = f.read()
    fileLst = contentStr.replace('[',"").replace(']',"").replace("'","").replace("\n","").split(', ')
with open('./task2-manual-labels.json', 'w') as manF:
    actTDct = {}
    actTDct['actual_types'] = ['address', 'area_of_study', 'borough', 'building_classification', 'business_name', 'car_make', 'city', 'city_agency', 'college_name', 'color', 'lat_lon_cord', 'location_type', 'neighborhood', 'other', 'park_playground', 'person_name', 'phone_number', 'school_level', 'school_name', 'street_name', 'subject_in_school', 'vehicle_type', 'website', 'zip_code']
    manF.write(str(actTDct) + "\n")
    with open('./labellist.txt') as f:
        labels = f.readlines()
        for i in range(len(fileLst)):
            datasetName = fileLst[i][0]
            colName = fileLst[i][1]
            label = labels[i]
            label = label.replace("\n","")
            llst = label.split(" ")[1].split(",")
            pdct = {}
            pdct['dataset_name'] = datasetName
            pdct['column_name'] = colName
            pdct['manual_labels'] = {}
            pdct['manual_labels']['semantic_type'] = llst
            manF.write(str(pdct)+"\n")
            
path = r"./task2out/"

list_dirs = os.walk(path) 
cnt = 0
for root, dirs, files in list_dirs: 
    fNum = len(files)
    #Combine json files
    with open('./task2.json', 'w') as cjsf:
        for f in files: 
            cnt += 1
            sys.stdout.write("\rcurrent step: {}/{}".format(cnt,fNum))
            sys.stdout.flush()
            fp = os.path.join(root, f)
            with open(fp,'r') as jsf:
                jstr = jsf.read()
                cjsf.write(jstr+'\n')