import pandas as pd
import glob, os
import json, time
from myutils import utilities as utl
from myutils.logging_util import logger
# from dask.dataframe import from_pandas
from tqdm import tqdm
from . import INVERTED_INDEX



def create_index(config):
    datalake_dir=config['datalake_dir']
    file_format=config['file_format']
    datalake_tables = glob.glob(f"{datalake_dir}/*{file_format}")
    inverted_index = {}
    start_time = time.time()
    for table in tqdm(datalake_tables, file=logger):
        dataframe = utl.load_dataframe(table, file_format=file_format) 
        if dataframe.size == 0:
            continue
        table_name, _ = os.path.splitext(os.path.basename(table))
        for col_id, col_name in enumerate(dataframe.columns):
            current_values = utl.preprocessListValues(dataframe[col_name].map(str))
            for cell_value in current_values:
                key = cell_value
                if key not in inverted_index:
                    inverted_index[key] = {(table_name, col_name)}
                else:
                    inverted_index[key].add((table_name, col_name))
    end_time = time.time()
    logger.log(f"Time taken to preprocess {len(datalake_tables)} in seconds: { end_time - start_time}")
    logger.log("Dumping index")
    inverted_index_filepath=logger.get_snapshot_dir()+INVERTED_INDEX
    utl.saveDictionaryAsPickleFile(inverted_index, inverted_index_filepath)
    logger.log(f"Index saved to {inverted_index_filepath}")
    return logger.get_snapshot_dir()