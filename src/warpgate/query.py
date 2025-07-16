import hnswlib
import pandas as pd
import json
import time
import os, glob, pickle
import _pickle as cPickle
import bz2, sys
from tqdm import tqdm

from myutils import utilities as utl
from myutils.logging_util import logger
import faulthandler
from . import EMBEDDING_INDEX


faulthandler.enable()

def search(config):
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

    query_path= config['datalake_dir']
    index_filepath = config['index_path']+EMBEDDING_INDEX
    groundtruth_filepath = config['groundtruth_filepath']
    file_format = config['file_format']
    top_k = config['top_k'] 
    p, keys = pickle.load(open(index_filepath,'rb'))
    ground_truth = None
    if groundtruth_filepath.endswith('.json'):
        ground_truth = utl.load_join_json_gt_to_dict(groundtruth_filepath)
    else:
        ground_truth = utl.load_join_jsonl_to_dict(groundtruth_filepath)
    start_time = time.time()
    results_filepath=logger.get_snapshot_dir()+"/search_results.jsonl"
    logger.log(f"Saving results to: {results_filepath}")
    start_time = time.time()
    with open(results_filepath, 'w') as outfile:
        for query in tqdm(ground_truth, file=logger):
            dataframe = utl.load_dataframe(f"{query_path}/{query[0]}{file_format}", file_format)
            if query[1].isdigit():
                query_column_id = int(query[1]) 
            else:
                query_column_id = list(dataframe.columns).index(query[1])
            query_values = list(set(dataframe.iloc[:, query_column_id].map(str)))[:100000]
            if config['warpgate_encoder'] == 'fasttext':
                query_embedding = ft.get_fasttext_embeddings(model, query_values).reshape(1,-1)
            elif config['warpgate_encoder'] == 'webtable':
                query_embedding = wt.get_embeddings(model, query_values).reshape(1,-1)
            elif config['warpgate_encoder'] == 'webtable_data':
                query_embedding = wt.get_data_embeddings(model, query_values).reshape(1,-1)
            else:
                query_embedding = model.transform(query_values).reshape(1,-1)

            if query_embedding.size == 0:
                logger.log(f"Skipping query {query}")
            else:
                labels, distances = p.knn_query(query_embedding, k = min(p.max_elements, top_k))
                joinable_list = []
                for l, d in zip(labels[0],distances[0]):
                    k = keys[l]
                    r = {"filename": k[0],
                        "col": f"{k[0]}.{k[1]}",
                        "score": float(d)}
                    joinable_list.append(r)
                result =   { "source": { "filename": query[0], "col": f"{query[0]}.{query[1]}"},
                            "joinable_list": joinable_list }
                json.dump(result, outfile)
                outfile.write('\n')
    end_time = time.time()
    logger.log(f"Time taken to query {len(ground_truth)} tables in seconds: {end_time - start_time}")
    return logger.get_snapshot_dir()