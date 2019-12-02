import os
import re
import json
import uuid
import MR_utils

def run(query):

    endCols = re.search("from", query).start()
    projectCols = query[6:endCols].strip()
    projectForPrint = projectCols
    
    code = 0

    if "where" in query:
        start_where = re.search("where", query).start()
        table = query[endCols + 4: start_where - 1].strip()
    else:
        table = query[endCols + 4:].strip()
    

    schemaFile = f"/hive_test/{table.split('/')[0]}/schema_{table.split('/')[1]}.json"
    check = MR_utils.isFileExists(schemaFile)

    ufolder = "schema_" + str(uuid.uuid4().hex)
    os.system(f"mkdir ./{ufolder}")
    cmd = f"hadoop fs -get {schemaFile} ./{ufolder}"
    os.system(cmd)

    with open(f"./{ufolder}/schema_{table.split('/')[1]}.json", "r") as f:
        schema = json.load(f)

    
    if(("max(" in projectCols) or ("min(" in projectCols) or ("count(" in projectCols)):
        code = 1
        aggCols = projectCols.split(',')
        projectCols = ""
        codeList = []
        for i in aggCols:
            i = i.strip()
            if("max(" in i):
                codeList.append(1)
                if i[4:].strip(")") == '*':
                    print("* is not valid for max")
                    return
                else:
                    projectCols += i[4:].strip(")") + ","
            elif("min(" in i):
                codeList.append(2)
                if i[4:].strip(")") == '*':
                    print("* is not valid for min")
                    return
                else:
                    projectCols += i[4:].strip(")") + ","
            else:
                codeList.append(3)
                if i[6:].strip(")") == '*':
                    projectCols += str(list(schema.keys())[0]) + ","
                else:
                    projectCols += i[6:].strip(")") + ","

        projectCols = projectCols.strip(",")
    
    if check:
        
        colIndexes = []
        colList = projectCols.split(',')

        for col in colList:
            if col.strip() == '*':
                for i, j in schema.items():
                    colIndexes.append(schema[i][0])
            elif col.strip() not in schema:
                print("Invalid column name")
                return
            else:
                colIndexes.append(schema[col.strip()][0])

        m_filename = "mapper_" + str(uuid.uuid4().hex) + ".py"
        mapper = open(m_filename, "w")
        if(len(re.findall("where", query)) == 1):
            # Have to parse query to get condition

            start_cond = re.search("where", query).start() + 5
            condition = query[start_cond:]
            for valid_col, col_data in schema.items():
                if valid_col in condition:
                    if col_data[1] == "int":
                        condition = condition.replace(valid_col, f"int(rowValues[{col_data[0]}])")
                    else:
                        condition = condition.replace(valid_col, f"str(rowValues[{col_data[0]}])")
            
            condition = condition.replace("<=", "<*")
            condition = condition.replace(">=", ">*")
            condition = condition.replace("!=", "!*")
            condition = condition.replace("=", "==")
            condition = condition.replace("*", "=")

            MR_utils.write_map_select(colIndexes, condition.strip(), mapper)

        elif(len(re.findall("where", query)) == 0):
            MR_utils.write_map_project(colIndexes, mapper)

        else:
            print("Command unrecognizable")
            return

        r_filename = "reducer_" + str(uuid.uuid4().hex) + ".py"
        reducer = open(r_filename, "w")

        if(code == 0):
            MR_utils.write_red_identity(reducer)
        else:
            MR_utils.write_red_aggregate(codeList, reducer)

        outputdir = "output" + str(uuid.uuid4().hex)

        if projectForPrint.strip() == '*':
            for i in schema.keys():
                if i == list(schema.keys())[len(list(schema.keys())) - 1]:
                    print(i)
                else:
                    print(i + ",", end = "")
        else:
            print(projectForPrint)

        runcmd = f"hadoop jar /home/hduser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -mapper {m_filename} -reducer {r_filename} -input /hive_test/{table.split('/')[0]}/input -output /hive_test/{table.split('/')[0]}/{outputdir}"

        os.system(runcmd)

        displaycmd = f"hadoop fs -cat /hive_test/{table.split('/')[0]}/{outputdir}/part-00000"

        os.system(displaycmd)

        rm_rm = f"rm -f {m_filename} {r_filename}"
        rm1 = f"hadoop fs -rm -r /hive_test/{table.split('/')[0]}/{outputdir}"
        os.system(rm_rm)
        os.system(rm1)
    
    else:
        print("Table does not exist")

    rm_schema = f"rm -rf ./{ufolder}"
    os.system(rm_schema)
