import random
import pickle
import bz2
import os
import sys, json
import csv
import numpy as np
import pandas as pd
import _pickle as cPickle
import re
# Some Useful functions copied from SANTOS

# This function saves dictionaries as pickle files in the storage.
def saveDictionaryAsPickleFile(dictionary, dictionaryPath):
    if dictionaryPath.rsplit(".")[-1] == "pickle":
        filePointer=open(dictionaryPath, 'wb')
        pickle.dump(dictionary,filePointer, protocol=pickle.HIGHEST_PROTOCOL)
        filePointer.close()
    else: #pbz2 format
        with bz2.BZ2File(dictionaryPath, "w") as f: 
            cPickle.dump(dictionary, f)

            
# load the pickle file as a dictionary
def loadDictionaryFromPickleFile(dictionaryPath):
    print("Loading dictionary at:", dictionaryPath)
    if dictionaryPath.rsplit(".")[-1] == "pickle":
        filePointer=open(dictionaryPath, 'rb')
        dictionary = pickle.load(filePointer)
        filePointer.close()
    else: #pbz2 format
        dictionary = bz2.BZ2File(dictionaryPath, "rb")
        dictionary = cPickle.load(dictionary)
    # print("The total number of keys in the dictionary are:", len(dictionary))
    return dictionary


# load csv file as a dictionary. Further preprocessing may be required after loading
def loadDictionaryFromCsvFile(filePath):
    if(os.path.isfile(filePath)):
        with open(filePath) as csv_file:
            reader = csv.reader(csv_file)
            dictionary = dict(reader)
        return dictionary
    else:
        print("Sorry! the file is not found. Please try again later. Location checked:", filePath)
        sys.exit()
        return 0
 
# Load the JSON file
def load_join_json_gt_to_dict(file_path):
    try:
        data = None
        with open(file_path, 'r') as file:
            data = json.load(file)
        gt_dict = {}
        for query, joinable_list in data.items():
            query_name = ".".join(query.split(".")[:-1])
            query_col = query.split(".")[-1]
            key = (query_name, query_col)
            value = []
            for join_details in joinable_list:
                dl_name = ".".join(join_details.split(".")[:-1])
                dl_col = join_details.split(".")[-1]
                value.append((dl_name, dl_col))
            gt_dict[key] = value #[query_col, value]
        return gt_dict
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}
  
def get_column_name(filename, column_id):
    if filename+"." in column_id :
        return column_id.split(filename+".")[-1]
    return column_id  

# Load the JSONL file
def load_join_jsonl_to_dict(file_path):
    try:
        with open(file_path, 'r') as file:
            data = [json.loads(line) for line in file]
        gt_dict = {}
        for item in data:
            query_name = item['source']['filename']
            query_col = get_column_name(item['source']['filename'], item['source']['col'])
            key = (query_name, query_col)
            joinable_list = item['joinable_list']
            value = []
            for join_details in joinable_list:
                dl_name = join_details['filename']
                dl_col = get_column_name(join_details['filename'], join_details['col'])
                value.append((dl_name, dl_col))
            gt_dict[key] = value #[query_col, value]
        return gt_dict
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}
    
#removes punctuations and whitespaces from list items
def preprocessListValues(valueList):
    valueList = list(set(valueList))
    valueList = [x.lower() for x in valueList if checkIfNullString(x) !=0] #converts to lowercase
    valueList = [re.sub(r'[^\w]', ' ', string) for string in valueList] # Removes punctuations
    valueList = [x.replace('nbsp','') for x in valueList ] #remove html whitespace
    valueList = [" ".join(x.split()) for x in valueList] # Adds 1 space between every word.
    return valueList

def normalize_string(x):
    return " ".join(re.sub(r'[^\w]', ' ', str(x).lower()).replace('nbsp','').split()) 
    
    
def normalize( collection_values):
    if isinstance(collection_values,list):
        return [normalize_string(x) for x in set(collection_values) if checkIfNullString(x) !=0]
    elif isinstance(collection_values,dict):
        return {normalize_string(x):v for x, v in collection_values.items() if checkIfNullString(x) !=0}

#checks different types of nulls in string
def checkIfNullString(string):
    nullList = ['nan','-','unknown','other (unknown)','null','na', "", " "]
    if str(string).lower() not in nullList:
        return 1
    else:
        return 0


def load_dataframe(file_path, file_format=".csv"):
    if file_format == '.parquet':
        df = pd.read_parquet(file_path, engine='pyarrow')
    elif file_format == '.csv':
        try:
            df = pd.read_csv(file_path, on_bad_lines="skip", encoding='unicode_escape')
            if len(df.columns) == 1:
                try:
                    df = pd.read_csv(file_path, sep=';')
                except Exception as e:
                    try:
                        df = pd.read_csv(file_path, sep='\t')
                    except Exception as e:
                        df = pd.read_csv(file_path, on_bad_lines="skip", encoding='unicode_escape')
        except:
            raise Exception("cannot read file path", file_path)

    elif file_format == '.df':
        df = pd.read_pickle(file_path)
    return df

def convert_to_dict_of_list(jsonl_file):
    dict = {}
    with open(jsonl_file, "r") as f:
        data = f.readlines()
        for line in data:
            json_line = json.loads(line)
            key = json_line['source']['col'] if json_line['source']['filename'] in json_line['source']['col'] else f"{json_line['source']['filename']}.{json_line['source']['col']}"  
            dict[key] = [ i['col'] if i['filename'] in i['col'] else f"{i['filename']}.{i['col']}"  for i in json_line['joinable_list'] ]
    return dict

def convert_to_dict_with_scores(jsonl_file):
    dict = {}
    with open(jsonl_file, "r") as f:
        data = f.readlines()
        for line in data:
            json_line = json.loads(line)
            dict[json_line['source']['col']] = {i['col']: i['score'] for i in json_line['joinable_list'] }
    return dict
            
def get_groundtruth_with_scores(gt_file):
    gt_with_scores = {}
    if gt_file.endswith('.json'):
        # Return uniform score for json files 
        ground_truth = json.load(open(gt_file,'r'))
        for k, v in ground_truth.items():
            gt_with_scores[k] = {i: 1 for i in v}
    elif gt_file.endswith('.jsonl'):
        with open(gt_file, "r") as f:
            data = f.readlines()
            for line in data:
                json_line = json.loads(line)
                if 'score' in json_line['joinable_list'][0]:
                    gt_with_scores[json_line['source']['col']] = { i['col']: i['score'] for i in json_line['joinable_list'] }
                else:
                    # Use uniform score if not available
                    key = json_line['source']['col'] if json_line['source']['filename'] in json_line['source']['col'] else f"{json_line['source']['filename']}.{json_line['source']['col']}"  
                    if json_line['joinable_list'][0]['filename'] in json_line['joinable_list'][0]['col']:
                        gt_with_scores[key] = { i['col']: 1  for i in json_line['joinable_list'] }
                    else:
                        gt_with_scores[key] = { f"{i['filename']}.{i['col']}": 1 for i in json_line['joinable_list'] }
    return gt_with_scores

def convert_to_dict_with_uniform_scores(jsonl_file):
    dict = {}
    with open(jsonl_file, "r") as f:
        data = f.readlines()
        for line in data:
            json_line = json.loads(line)
            dict[json_line['source']['col']] = {i['col']: 1 for i in json_line['joinable_list'] }
    return dict
              
def convert_gt_to_dict_with_uniform_scores(gt):
    dict = {}
    for k, v in gt.items():
        dict[k] = {i: 1 for i in v}
    return dict

