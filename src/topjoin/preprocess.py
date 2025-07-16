import pandas as pd
import glob, os
import json, time
from myutils import utilities as utl
from myutils.logging_util import logger
from . import METADATA_INDEX, FREQ_VALUE_INDEX, MINHASH_INDEX, DOCUMENTS_DICT, DOCUMENT_IDS, DATA_TYPE
from tqdm import tqdm
import numpy as np
from myutils.indexers import AvailableIndexers
from myutils.indexers import _hash_func
import itertools
from dateutil.parser import parse
from datasketch import MinHashLSHForest, MinHash
from sentence_transformers import SentenceTransformer 
import shutil



def get_type(df, col):
    time_types = ['datetime64', 'datetime', 'date', 'timedelta64', 'timedelta', 'time', 'period']
    other_types = ['bytes', 'mixed-integer', 'complex', 'categorical', 'boolean', 'mixed', 'unknown-array']
    # col_types = {}
    # for col in df.columns:
    tp = pd.api.types.infer_dtype(df[col])
    if tp == 'string':
        try:
            dt = [parse(v) if v else None for v in df[col]]
            tp = DATA_TYPE.DATE
        except:
            tp = DATA_TYPE.STRING
    if tp == 'integer':
        tp = DATA_TYPE.INTEGER
    elif tp == 'floating' or tp == 'decimal' or tp == 'mixed-integer-float':
        tp = DATA_TYPE.FLOAT
    elif tp in time_types:
        tp = DATA_TYPE.DATE
    else:
        tp = DATA_TYPE.STRING
        # col_types[col] = tp
    #print(col_types)
    return tp

def create_index(config, limit_freq_values=100000, limit_sample_values=1000000, num_perm=100, batch_size=35):
    start_time = time.time()
    datalake_dir=config['datalake_dir']
    file_format=config['file_format']
    if 'limit_freq_values' not in config:
        config['limit_freq_values']= limit_freq_values
    if 'limit_sample_values' not in config:
        config['limit_sample_values']= limit_sample_values
    if 'num_perm' not in config:
        config['num_perm']= num_perm
    if 'batch_size' not in config:
        config['batch_size']= batch_size
    datalake_tables = glob.glob(f"{datalake_dir}/*{file_format}")
    start_time = time.time()
    column_ids = []
    utf_col_values = []
    column_documents = {}
    column_paragraph = []
    freq_values_string = []
    fresh_index = True
    if 'index_path' in config and config['index_path'] is not None:
        logger.log("Skipping document processing")   
        column_documents = utl.loadDictionaryFromPickleFile(config['index_path']+  DOCUMENTS_DICT)
        shutil.copyfile(config['index_path']+ MINHASH_INDEX, logger.get_snapshot_dir()+  MINHASH_INDEX)
        shutil.copyfile(config['index_path']+ DOCUMENT_IDS, logger.get_snapshot_dir()+  DOCUMENT_IDS)
        shutil.copyfile(config['index_path']+ DOCUMENTS_DICT, logger.get_snapshot_dir()+  DOCUMENTS_DICT)
        column_ids = utl.loadDictionaryFromPickleFile(config['index_path']+ DOCUMENT_IDS)
        fresh_index = False
        column_paragraph = [ column_documents[i]['column_metadata'] for i in column_ids ]
        for i in column_ids:
            col_data = column_documents[i]
            if col_data['type'] == DATA_TYPE.STRING and len(col_data['freq_values']) > 0 :
                freq_values_string.append(','.join(col_data['col_values']))
            else:
                freq_values_string.append('None')
    else:
        logger.log("Begin document processing")
        for table in tqdm(datalake_tables, file=logger):
            dataframe = utl.load_dataframe(table, file_format=file_format) 
            table_name, _ = os.path.splitext(os.path.basename(table))
            table_metadata = {}
            column_metadata = []
            table_description = ""
            
            if config['metadata_dir'] is not None:
                table_metadata = json.load(open(config['metadata_dir']+table_name+config['metadata_suffix']))
                column_metadata = table_metadata.pop('column_headers', [])
                if "file_name" in table_metadata:
                    table_description += f'{table_metadata.pop("file_name","")}:\n' 
                if "dataset_name" in table_metadata:
                    table_description += f'{table_name} is part of dataset {table_metadata.pop("dataset_name","")}.\n' 
                if "dataset_description" in table_metadata:
                    table_description += f'{table_metadata.pop("dataset_description","")}.\n' 
                if "table_description" in table_metadata:
                    table_description += f'Table description: \n {table_metadata.pop("table_description","")}. \n' 
                if "organization_id" in table_metadata:
                    table_description += f'Organization ID: \n {table_metadata.pop("organization_id","")}. \n' 
                if "organization_name" in table_metadata:
                    table_description += f'Organization Name: \n {table_metadata.pop("organization_name","")}. \n' 
                if "tags" in table_metadata:
                    table_description += f'Tags: \n {",".join(table_metadata.pop("tags"))} \n'
                if len(table_metadata) > 0 :
                    table_description += "Additional metadata: \n" + json.dumps(table_metadata) +"\n"
            table_description += f'{table_name} table contains following columns: \n {",".join(dataframe.columns)} \n'
            for col_idx, col_name in enumerate(dataframe.columns):
                column_ids.append((table_name, col_name))
                col_type = get_type(dataframe, col_name)
                column_description = f"{col_name} is a column from a table called {table_name}."
                if len(column_metadata) > 0 and isinstance(column_metadata[col_idx], dict) and 'desc' in column_metadata[col_idx].keys() and len(column_metadata[col_idx]['desc']) > 0 :
                    # logger.log(f"------{column_metadata[col_idx]}--")
                    # logger.log(column_metadata[col_idx])
                    column_description += f"Column Description: \n {column_metadata[col_idx]['desc']} \n"
                column_description += table_description
                column_values = dataframe[col_name].map(utl.normalize_string)
                freq_values = column_values.value_counts(sort=True)
                col_data = {'column_metadata': column_description,
                            'type': col_type.value, 
                            # 'num_nan': dataframe[col_name].isna().sum(), Not used
                            'col_values': list(freq_values.keys())[:config['limit_sample_values']],
                            'freq_values': dict(itertools.islice(freq_values.items(), config['limit_freq_values'])), 
                            'table': table_name, 
                            'count_distinct': len(column_values),
                            'num_rows': len(dataframe), 
                            'column_name': col_name}
                column_paragraph.append(column_description)
                utf_col_values.append([s.encode('utf-8') for s in set(column_values)])
                if col_data['type'] == DATA_TYPE.STRING and len(col_data['freq_values']) > 0 :
                    # sliced_freq = [str(x) for x in col_data['freq_values']][:config['limit_sample_values']]
                    freq_values_string.append(','.join(col_data['col_values']))
                else:
                    freq_values_string.append('None')
                column_documents[(table_name, col_name)] = col_data 
        
    logger.log("Bulk Processing")
    if fresh_index:
        minhashes = MinHash.bulk(utf_col_values, hashfunc=_hash_func, num_perm=config['num_perm'])
    try:
        model  = SentenceTransformer(config['model'])
        
        metadata_embeddings, column_freq_vals_embed =[], []
        for i in tqdm(range(0, len(column_paragraph), config['batch_size'])):
            metadata_embeddings.append(model.encode(column_paragraph[i:i+config['batch_size']]))
            column_freq_vals_embed.append(model.encode(freq_values_string[i:i+config['batch_size']]))
    
        metadata_embeddings = np.concatenate(metadata_embeddings, axis=0)
        column_freq_vals_embed = np.concatenate(column_freq_vals_embed, axis=0)
        
        
    except Exception as e:
        logger.log("Embedding Failed with following error: ")
        logger.log(e)
    logger.log("Updating documents")
    for idx , key in enumerate(column_documents.keys()):
        if fresh_index:
            column_documents[key]['minhashes'] = minhashes[idx].digest().tolist()
        column_documents[key]['paragraph_embedding'] = metadata_embeddings[idx].tolist()
        column_documents[key]['freq_value_embedding'] = column_freq_vals_embed[idx].tolist()
    if fresh_index:
        logger.log("Dumping document dictionary")
        documents_dictionary = logger.get_snapshot_dir()+  DOCUMENTS_DICT
        utl.saveDictionaryAsPickleFile(column_documents, documents_dictionary)
        utl.saveDictionaryAsPickleFile(column_ids, logger.get_snapshot_dir()+ DOCUMENT_IDS)
    
        logger.log(f"Indexing Minhash in {config['minhash_indexer']}")
        minhash_index = AvailableIndexers[config['minhash_indexer']](features=minhashes)
        
    logger.log(f"Indexing Metadata Paragraph in {config['embedding_indexer']}")
    metadata_index = AvailableIndexers[config['embedding_indexer']](features=metadata_embeddings)
    
    logger.log(f"Indexing Freq Value in {config['embedding_indexer']}")
    freq_value_index = AvailableIndexers[config['embedding_indexer']](features=column_freq_vals_embed) 
    end_time = time.time()
    logger.log(f"Time taken to preprocess {len(datalake_tables)} in seconds: { end_time - start_time}")
    
    logger.log("Dumping indexes")
    if fresh_index:  
        minhash_index_filepath = logger.get_snapshot_dir()+  MINHASH_INDEX
        minhash_index.dump(minhash_index_filepath)
        logger.log(f"Minhash Index saved to {minhash_index_filepath}")   
    
    metadata_index_filepath=logger.get_snapshot_dir()+ METADATA_INDEX
    metadata_index.dump(metadata_index_filepath)
    logger.log(f"Metadata Index saved to {metadata_index_filepath}")
    
    freq_value_index_filepath=logger.get_snapshot_dir()+ FREQ_VALUE_INDEX
    freq_value_index.dump(freq_value_index_filepath)
    logger.log(f"Freq Value Index saved to {freq_value_index_filepath}")
    return logger.get_snapshot_dir()