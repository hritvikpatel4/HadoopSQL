import os
import subprocess
import re

def isDbExists(path):
    
    cmd = f"hadoop fs -test -d {path};echo $?"
    check = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE).communicate()

    if '1' not in str(check):
        return 1
    else:
        return 0


def isFileExists(path):

    cmd = f"hadoop fs -test -f {path};echo $?"
    check = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE).communicate()

    if '1' not in str(check):
        return 1
    else:
        return 0

def write_map_select(indexList, condition, mapper):

    mapper.write("#!/usr/bin/python3\n")
    mapper.write("import sys\n")
    mapper.write("infile = sys.stdin\n")
    mapper.write("try:\n")
    mapper.write("\tfor line in infile:\n")
    mapper.write("\t\tline = line.strip()\n")
    mapper.write("\t\trowValues = line.split(',')\n")
    mapper.write(f"\t\tif({condition}):\n")
    mapper.write("\t\t\tprint(")
    for index in range(len(indexList) - 1):        
        mapper.write(f"rowValues[{indexList[index]}], ',' ,")
    mapper.write(f"rowValues[{indexList[len(indexList) - 1]}])\n")
    mapper.write("except NameError:\n")
    mapper.write("\tprint('Invalid where clause')")
    mapper.close()

def write_map_project(indexList, mapper):

    mapper.write("#!/usr/bin/python3\n")
    mapper.write("import sys\n")
    mapper.write("infile = sys.stdin\n")
    mapper.write("for line in infile:\n")
    mapper.write("\tline = line.strip()\n")
    mapper.write("\trowValues = line.split(',')\n")
    mapper.write("\tprint(")
    for index in range(len(indexList) - 1):        
        mapper.write(f"rowValues[{indexList[index]}], ',' ,")
    mapper.write(f"rowValues[{indexList[len(indexList) - 1]}])")
    mapper.close()

def write_check_mapper(intList, mapper, length):

    mapper.write("#!/usr/bin/python3\n")
    mapper.write("import sys\n")
    mapper.write("infile = sys.stdin\n")
    mapper.write("flag = 1\n")
    mapper.write("for line in infile:\n")
    mapper.write("\tline = line.strip()\n")
    mapper.write("\trowValues = line.split(',')\n")
    mapper.write(f"\tif len(rowValues) != {length}:\n")
    mapper.write("\t\tflag = 0\n")
    mapper.write("\t\tprint(flag)\n")
    mapper.write("\t\tbreak \n")
    mapper.write(f"\tfor i in {intList}:\n")
    mapper.write("\t\ttry:\n")
    mapper.write("\t\t\tint(rowValues[i])\n")
    mapper.write("\t\texcept ValueError:\n")
    mapper.write("\t\t\tflag = 0\n\t\t\tbreak\n")
    mapper.close()

def write_check_reducer(reducer):

    reducer.write("#!/usr/bin/python3\n")
    reducer.write("import sys\n")
    reducer.write("for line in sys.stdin:\n")
    reducer.write("\tprint(line.strip())")
    reducer.close()

def write_red_identity(reducer):

    reducer.write("#!/usr/bin/python3\n")
    reducer.write("import sys\n")
    reducer.write("var = None\n")
    reducer.write("count = 0\n")
    reducer.write("for line in sys.stdin:\n")
    reducer.write("\tprint(line)\n")    
    reducer.close()
    
def write_red_max(col, reducer):
    
    reducer.write("var = None\n")
    reducer.write("for line in infile:\n")
    reducer.write(f"\tline = line.split(',')[{col}]\n")
    reducer.write("\tline = line.strip()\n")
    reducer.write("\tif var == None:\n")
    reducer.write("\t\tvar = line\n")
    reducer.write("\telse:\n")
    reducer.write("\t\tif line > var:\n")
    reducer.write("\t\t\tvar = line\n")
    reducer.write("l.append(var)\n")

def write_red_min(col, reducer):
    
    reducer.write("var = None\n")
    reducer.write("for line in infile:\n")
    reducer.write(f"\tline = line.split(',')[{col}]\n")
    reducer.write("\tline = line.strip()\n")
    reducer.write("\tif var == None:\n")
    reducer.write("\t\tvar = line\n")
    reducer.write("\telse:\n")
    reducer.write("\t\tif line < var:\n")
    reducer.write("\t\t\tvar = line\n")
    reducer.write("l.append(var)\n")

def write_red_count(col, reducer):

    reducer.write("count = 0\n")
    reducer.write("for line in infile:\n")
    reducer.write("\tline = line.strip()\n")
    reducer.write("\tcount = count + 1\n")
    reducer.write("l.append(count)\n")

def write_red_aggregate(codeList, reducer):

    reducer.write("#!/usr/bin/python3\n")
    reducer.write("import sys\n")
    reducer.write("l=[]\n")
    reducer.write("infile = sys.stdin.readlines()\n")

    i = 0
    while(i < len(codeList)):
        if codeList[i] == 1:
            write_red_max(i, reducer)
        
        elif codeList[i] == 2:
            write_red_min(i, reducer)
        
        else:
            write_red_count(i, reducer)
        i = i + 1

    reducer.write("print(*l, sep = ',')\n")
    reducer.close()
