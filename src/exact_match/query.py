import pandas as pd
import glob, os
import json
from myutils import utilities as utl
import time
from myutils.logging_util import logger
from tqdm import tqdm
from . import INVERTED_INDEX

def search(config):
    query_path = config['datalake_dir']
    inverted_index_filepath = config['index_path']+INVERTED_INDEX
    groundtruth_filepath = config['groundtruth_filepath']
    file_format=config['file_format'] 
    top_k=config['top_k']
    ground_truth = None
    if groundtruth_filepath.endswith('.json'):
        ground_truth = utl.load_join_json_gt_to_dict(groundtruth_filepath)
    else:
        ground_truth = utl.load_join_jsonl_to_dict(groundtruth_filepath)
    inverted_index = utl.loadDictionaryFromPickleFile(inverted_index_filepath)
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
            query_values = utl.preprocessListValues(dataframe.iloc[:, query_column_id].map(str))
            total_query_values = len(query_values)
            overlapping_dl_cols = {}
            for cell_value in query_values:
                if cell_value in inverted_index:
                    dl_cols = inverted_index[cell_value]
                    for dl_col in dl_cols:
                        if dl_col in overlapping_dl_cols:
                            overlapping_dl_cols[dl_col] += (1 / total_query_values)
                        else:
                            overlapping_dl_cols[dl_col] = (1 / total_query_values)
            ranked_list = sorted(overlapping_dl_cols.items(), key=lambda item: item[1], reverse=True)[:top_k]
            joinable_list = []
            for r_tuple, score in ranked_list:
                r = {"filename": r_tuple[0],
                    "col": f"{r_tuple[0]}.{r_tuple[1]}",
                    "score": score}
                joinable_list.append(r)

            result =   { "source": { "filename": query[0], "col": f"{query[0]}.{query[1]}"},
                        "joinable_list": joinable_list }
            json.dump(result, outfile)
            outfile.write('\n')
    end_time = time.time()
    logger.log(f"Time taken to query {len(ground_truth)} tables in seconds: {end_time - start_time}")
    return logger.get_snapshot_dir()
