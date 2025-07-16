
import json, os
import time
from tqdm import tqdm 
import traceback
import gc
import warnings
from myutils import utilities as utl
warnings.filterwarnings("ignore")
from myutils.logging_util import logger
from .query_helper import Joinable_QueryHelper

def search(config):
    ground_truth = None 
    if config['groundtruth_filepath'].endswith('.json'):
        ground_truth = utl.load_join_json_gt_to_dict(config['groundtruth_filepath'])
    else:
        ground_truth = utl.load_join_jsonl_to_dict(config['groundtruth_filepath'])

    results_file=logger.get_snapshot_dir()+"/search_results.jsonl"
    logger.log(f"Saving results to: {results_file}")

    query_helper = Joinable_QueryHelper(config)
    
    total_time = 0
    start_time = time.time()
    with open(results_file, 'w') as outfile:
       for index, query in enumerate(tqdm(ground_truth, file=logger)):
            query_table = query[0]
            query_col = query[1] 
            
            try:
                response = query_helper.query_joinability(query_table, query_col)
                joinable_list = []
                for column_id, score in zip(response[0],response[1]):
                    r = {"filename": ".".join(column_id.split(".")[:-1]),
                        "col": column_id,
                        "score": score}
                    joinable_list.append(r)

                result =   { "source": { "filename":query_table, "col": f"{query_table}.{query_col}" },
                            "joinable_list": joinable_list }
                json.dump(result, outfile)
                outfile.write('\n')
            except Exception as e:
                print(f"query {index} with {json.dumps(query)} failed with error: {e}" )
                traceback.print_exc()
            if index % 100 == 0:
                collected = gc.collect()
    end_time = time.time()
    logger.log(f"Time taken to query {len(ground_truth)} tables in seconds: {end_time - start_time}")
    return logger.get_snapshot_dir()
