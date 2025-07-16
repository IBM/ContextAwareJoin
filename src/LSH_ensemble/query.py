import pandas as pd
import glob, os
import json
from myutils import utilities as utl
import time
from myutils.logging_util import logger
from tqdm import tqdm
from myutils.indexers import _hash_func
from . import DOCUMENTS_DICT, DOCUMENT_IDS
from myutils.indexers import DataSketch_LSHEnsemble_Indexer
from topjoin.rankers import containment_ranking
# from datasketch import MinHashLSHEnsemble, MinHash


def search(config):
    document_dict = utl.loadDictionaryFromPickleFile(config['index_path']+  DOCUMENTS_DICT)
    document_ids = utl.loadDictionaryFromPickleFile(config['index_path']+ DOCUMENT_IDS)
    groundtruth_filepath = config['groundtruth_filepath']
    enteries = [(doc['minhashes'], doc['count_distinct']) for i, doc in document_dict.items()]
    ensemble_index = DataSketch_LSHEnsemble_Indexer(minhash_size=enteries)
    
    ground_truth = None
    if groundtruth_filepath.endswith('.json'):
        ground_truth = utl.load_join_json_gt_to_dict(groundtruth_filepath)
    else:
        ground_truth = utl.load_join_jsonl_to_dict(groundtruth_filepath)
    results_filepath=logger.get_snapshot_dir()+"/search_results.jsonl"
    logger.log(f"Saving results to: {results_filepath}")
    start_time = time.time()
    with open(results_filepath, 'w') as outfile:
        for query in tqdm(ground_truth, file=logger):
            query_column = document_dict[(query[0], query[1])]
            D, items  =  ensemble_index.query((query_column['minhashes'], query_column['count_distinct']))
            item_docs = [document_dict[document_ids[i]] for i in list(set(items))] 
            score_items = containment_ranking(query_column, item_docs)
            ranked_items = sorted(score_items, key=lambda item: item[1], reverse=True)[:config['top_k']]
            joinable_list=[]
            for item, score in ranked_items:
                r = {"filename": item_docs[item]['table'],
                    "col": f"{item_docs[item]['table']}.{item_docs[item]['column_name']}",
                    "score": score}
                joinable_list.append(r)
            result =   { "source": { "filename": query[0], "col": f"{query[0]}.{query[1]}"},
                        "joinable_list": joinable_list }
            json.dump(result, outfile)
            outfile.write('\n')
    end_time = time.time()
    logger.log(f"Time taken to query {len(ground_truth)} tables in seconds: {end_time - start_time}")
    return logger.get_snapshot_dir()
