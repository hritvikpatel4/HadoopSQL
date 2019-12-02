#!/bin/bash

import os
import json
import uuid
import query_handler
import MR_utils

while(True):
    
    query = input("shell> ")
    
    query_list = query.split()

    if(query == ""):
        continue
    
    elif(query_list[0] == "exit"):
        break

    # Creating the database
    elif(query_list[0] == "create" and query_list[1] == "database"):
        # Check if database already exists
        check = MR_utils.isDbExists(f"/hive_test/{query_list[2]}")

        if not check:
            # Create directory on HDFS
            cmd = f"hadoop fs -mkdir -p /hive_test/{query_list[2]}/input"
            os.system(cmd)
        else:
            print(f"{query_list[2]} already exists")
        
    # Loading the database with specifying the schema
    elif(query_list[0] == "load" and query_list[2] == "as"):

        # Path to the database
        path = f"/hive_test/{query_list[1]}"
        table = query_list[1].split('/')[1]
        db = query_list[1].split('/')[0]
        check = MR_utils.isFileExists(path)

        if path:
            schemaDict = {}
            schemaFile = open(f"schema_{table}.json", "w+")
            schemaStr = ""

            for i in range(3, len(query_list)):
                schemaStr += query_list[i]
            schemaList = schemaStr.split(",")
            
            intList = []

            for i in range(len(schemaList)):
                name, datatype = schemaList[i].split(":")
                schemaDict[name] = [i, datatype]
                if datatype.strip() == "int":
                    intList.append(i)

            check_mapper = "check_mapper_" + str(uuid.uuid4().hex) + ".py"
            cmapper = open(check_mapper, "w+")
            MR_utils.write_check_mapper(intList, cmapper, len(schemaDict))

            check_reducer = "check_reducer_" + str(uuid.uuid4().hex) + ".py"
            creducer = open(check_reducer, "w+")
            MR_utils.write_check_reducer(creducer)

            outputdir = "output" + str(uuid.uuid4().hex)

            runcmd = f"hadoop jar /home/hduser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -mapper ./{check_mapper} -reducer ./{check_reducer} -input /datasets/{table}/ -output /hive_test/{outputdir}"
            os.system(runcmd)

            test = "test_" + str(uuid.uuid4().hex) + ".txt"
            check = f"hadoop fs -cat /hive_test/{outputdir}/part-00000 > {test}"
            os.system(check)

            t = open(test, "r")
            if(t.read().strip() == "0"):
                schemaFile.close()
                print("Schema does not match the dataset")

            else:
                # Jsonify the schema dictionary and write it to the file
                jsonDict = json.dumps(schemaDict)
                schemaFile.write(jsonDict)
                schemaFile.close()

                # Put the schema onto HDFS
                cmd = f"hadoop fs -put ./schema_{table}.json /hive_test/{db}/"
                os.system(cmd)
                os.system(f"hadoop fs -cp /datasets/{table}/{table} /hive_test/{db}/input/")

            # Remove the temporary files and output files
            os.system(f"rm -f ./schema_{table}.json")

            rm = f"rm -f {test} {check_mapper} {check_reducer}"
            os.system(rm)

            rmoutput = f"hadoop fs -rm -r /hive_test/{outputdir}"
            os.system(rmoutput)

        else:
            print("File does not exist")

    # Select queries with different variations
    elif(query_list[0] == "select"):
        if((query_list.count("select") == 1) and (query_list.count("from") == 1)):
            query_handler.run(query)
        else:
            print("Command unrecognizable")

    else:
        print("Command unrecognizable")
