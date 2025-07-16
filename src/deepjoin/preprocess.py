import pandas as pd
import glob, os
import json, time
from myutils import utilities as utl
from myutils.logging_util import logger
from . import COLUMN_EMBEDDING, EMBEDDING_INDEX, DOCUMENT_IDS, COLUMN_TEXT
from tqdm import tqdm
import numpy as np
from sentence_transformers import SentenceTransformer 
import shutil
import numpy as np
import hnswlib 

def create_index(config, limit_sample_values=1000000, batch_size=15 ):
    start_time = time.time()
    datalake_dir=config['datalake_dir']
    file_format=config['file_format']
    datalake_tables = glob.glob(f"{datalake_dir}/*{file_format}")
    column_texts = []
    column_ids = []
    start_time = time.time()
    for table in tqdm(datalake_tables, file=logger):
        dataframe = utl.load_dataframe(table, file_format=file_format) 
        if dataframe.size == 0:
            continue
        table_name, _ = os.path.splitext(os.path.basename(table))
        for col_id, col_name in enumerate(dataframe.columns):
            
            current_values = dataframe[col_name].map(str)
            max_len = len(max(current_values, key = len))
            min_len = len(min(current_values, key = len))
            mean_len = round(np.mean(list(map(len, current_values))),2)
            column_text = f"{table_name}. " # Table Name
            column_text += f"{col_name} contains { len(dataframe)} values ({max_len},{min_len},{mean_len}): " # Column name and stats
            column_text += ", ".join(current_values[:limit_sample_values])
            column_texts.append(column_text)
            column_ids.append((table_name, col_name))
    utl.saveDictionaryAsPickleFile(column_ids, logger.get_snapshot_dir()+ DOCUMENT_IDS)
    utl.saveDictionaryAsPickleFile(column_texts, logger.get_snapshot_dir()+ COLUMN_TEXT)
    logger.log("Bulk Processing")
    assert config['model'] is not None
    try:
        model  = SentenceTransformer(config['model'])
        logger.log("Starting ST encoding")
        column_embeddings = model.encode(column_texts, batch_size=batch_size, show_progress_bar=True)
        logger.log("Dumping data")
        utl.saveDictionaryAsPickleFile(column_embeddings, logger.get_snapshot_dir()+ COLUMN_EMBEDDING)
    except Exception as e:
        logger.log("Failure in bulk processing ")
        logger.log(e)
    logger.log(f"Indexing Columns")
    p = hnswlib.Index(space='cosine', dim=column_embeddings.shape[1])
    p.init_index(max_elements=len(column_embeddings), ef_construction = 200, M = 16)
    column_embeddings = np.array(column_embeddings, dtype= np.float32)
    assert column_embeddings.shape[0] == len(column_ids)
    p.add_items(column_embeddings, list(range(len(column_ids))))
    index_path=logger.get_snapshot_dir()+EMBEDDING_INDEX
    utl.saveDictionaryAsPickleFile(p , index_path)
    logger.log(f"IndexDumped Index at {index_path}")
    return logger.get_snapshot_dir()