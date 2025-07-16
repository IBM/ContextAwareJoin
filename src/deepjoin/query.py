import hnswlib
import json
import time
from tqdm import tqdm
from myutils import utilities as utl
from myutils.logging_util import logger
import faulthandler
from . import EMBEDDING_INDEX
from . import COLUMN_EMBEDDING, EMBEDDING_INDEX, DOCUMENT_IDS
import pickle 
faulthandler.enable()

def search(config):
    query_path= config['datalake_dir']
    index_filepath = config['index_path']+EMBEDDING_INDEX
    groundtruth_filepath = config['groundtruth_filepath']
    file_format = config['file_format']
    top_k = config['top_k'] 
    p = utl.loadDictionaryFromPickleFile(index_filepath)
    document_ids = utl.loadDictionaryFromPickleFile(config['index_path'] + 
                                                            DOCUMENT_IDS )
    col_embeddings = utl.loadDictionaryFromPickleFile(config['index_path'] + 
                                                            COLUMN_EMBEDDING )
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
            query_column = col_embeddings[document_ids.index((query[0], query[1]))]
            labels, distances = p.knn_query(query_column, k = min(p.max_elements, top_k))
            joinable_list = []
            for l, d in zip(labels[0],distances[0]):
                k = document_ids[l]
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
