#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 7 16:08:47 2023
@author: aamodkhatiwada
"""
'''
Preprocessing step.
For each data lake table, compute their fast text and glove embeddings and store them for query time use.
We only use fasttext in our pipeline as it is broader but we compute glove as well for empirical evaluation.
'''

import pandas as pd
import os, glob, pickle
from datasketch import MinHashLSHEnsemble, MinHash
import _pickle as cPickle
import bz2, sys
from tqdm import tqdm

# from fasttext import load_model
from sklearn.feature_extraction.text import TfidfVectorizer
from urllib.request import urlopen
import gzip, shutil
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from myutils.logging_util import logger
from myutils import utilities as utl
import hnswlib 
import time 
from . import EMBEDDING_INDEX



# collect the columns within the whole data lake for lsh ensemble.
def create_index(config):
    if config['warpgate_encoder'] == 'fasttext':
        import myutils.fasttext_embeddings as ft
        model = ft.get_embedding_model(config['fasttext_path'])
    elif config['warpgate_encoder'] == 'webtable':
        import myutils.webtable_embedding as wt
        model = wt.get_embedding_model('ddrg/web_table_embeddings_combo64')
    elif config['warpgate_encoder'] == 'webtable_data':
        import myutils.webtable_embedding as wt
        model = wt.get_embedding_model('ddrg/web_table_embeddings_combo64')
    else:
        from warpgate.glove_embeddings import GloveTransformer
        model = GloveTransformer()

    datalake_dir=config['datalake_dir']
    file_format=config['file_format']
    # fasttext_model = ft.get_embedding_model() #todo : convert fastext to class like glove.
    datalake_tables = glob.glob(f"{datalake_dir}/*{file_format}")
    embedding_list = []
    keys = []
    start_time = time.time()
    for table in tqdm(datalake_tables, desc="Computing Embeddings", file=logger):
        try:
            df = utl.load_dataframe(table, file_format=file_format) 
            table_name, _ = os.path.splitext(os.path.basename(table))
            for idx, column in enumerate(df.columns):
                column_data = list(df[column].map(str))
                if config['warpgate_encoder'] == 'fasttext':
                    this_embedding = ft.get_fasttext_embeddings(model, column_data)
                elif config['warpgate_encoder'] == 'webtable':
                    this_embedding = wt.get_embeddings(model, column_data[:100000])
                elif config['warpgate_encoder'] == 'webtable_data':
                    this_embedding = wt.get_data_embeddings(model, column_data[:100000])
                else:
                    this_embedding = model.transform(column_data)
                if len(this_embedding) == 0:
                    continue
                embedding_list.append(this_embedding)
                keys.append((table_name, column))
        except Exception as e:
            print("Error in table:", table_name)
            print(e)
            continue
    logger.log(f"Time taken to compute {len(embedding_list)} embeddings in seconds: { time.time() - start_time}")
    start_time = time.time()
    dim = 300
    if config['warpgate_encoder'] == 'webtable' or config['warpgate_encoder'] == 'webtable_data' :
        dim = 64
    p = hnswlib.Index(space='cosine', dim=dim)
    p.init_index(max_elements=len(embedding_list), ef_construction = 200, M = 16)
    embeddings = np.array(embedding_list, dtype= np.float32)
    assert embeddings.shape[0] == len(keys)
    p.add_items(embeddings, list(range(len(embedding_list))))
    logger.log(f"Time taken to create HNSW Index: {time.time() - start_time}")
    logger.log("Dumping index")
    index_path=logger.get_snapshot_dir()+EMBEDDING_INDEX
    utl.saveDictionaryAsPickleFile((p,keys), index_path)
    logger.log(f"Index saved to {index_path}")
    return logger.get_snapshot_dir()