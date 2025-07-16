import math
from operator import itemgetter
import numpy as np
import json
from myutils.logging_util import logger
from myutils import utilities as utl

def _top_k_vals(results_dict, k, is_desc):
    sorted_ret_vals = sorted(results_dict.items(), key=itemgetter(1), reverse=is_desc)
    return [item[0] for item in sorted_ret_vals][:k]


def _precision_at_k(result_list, relevant_list, k):
    return len(set(result_list[:k+1]).intersection(set(relevant_list))) / (k+1) # Handling 0th element

def _average_precision(result_list, relevant_list, k):
    k = min(k, len(result_list))
    return np.sum([ _precision_at_k(result_list, relevant_list, i) for i in range(k) if result_list[i] in relevant_list ])/len(relevant_list)
      


def compute_precision_recall_at_k(groundtruth, resultFile, k, self_remove=True):
    '''
    ground_truth_dict: {'query_col': ['result1_col', 'result2_col']}
    results_dict: {'query_col': []'result1_col', 'result2_col']}
    '''
    true_positive = 0
    false_positive = 0
    false_negative = 0
    rec = 0
    if self_remove:
        for key, value in resultFile.items():
            if key in value:
                del resultFile[key][key] 
    for table in resultFile:
        if table not in groundtruth.keys():
            continue
        groundtruth_set = set(groundtruth[table])
        result_set = resultFile[table][:k]
        find_intersection = set(result_set).intersection(groundtruth_set)
        tp = len(find_intersection)
        fp = k - tp
        fn = len(groundtruth_set) - tp
        if len(groundtruth_set)>=k: 
            true_positive += tp
            false_positive += fp
            false_negative += fn
        rec += tp / (tp+fn)
    precision = None
    if (true_positive + false_positive) > 0:
        precision = true_positive / (true_positive + false_positive)
    recall = rec/len(resultFile)
    return precision, recall
    

def _mrr(retrieved_values, relevant_values):
    for i,v in enumerate(retrieved_values):
        if v in relevant_values:
            return 1/(i+1)
    return 0

def _ndcg(retrieved_values, relevant_values, k:int, lower_better:bool):
    def _dcg_sum(retrieved_subset):
        return sum([relevant_values.get(v, 0)/math.log(i+2, 2) for i,v in enumerate(retrieved_subset)])
    ideal_retrieved = sorted(relevant_values.keys(), key=lambda x: -relevant_values[x], reverse=lower_better)
    return _dcg_sum(retrieved_values[:k])/_dcg_sum(ideal_retrieved[:k])

def compute_mrr(ground_truth_dict, results_dict, k, self_remove=True):
    '''
    ground_truth_dict: {'query_col': {'result1_col': score, 'result2_col':score}}
    results_dict: {'query_col': {'result1_col': score, 'result2_col':score}}
    '''

    mrrs = []
    if self_remove:
        for key, value in results_dict.items():
            if key in value:
                del results_dict[key][key] 
                
    mrrs = []
    for index, key in enumerate(ground_truth_dict.keys()):
        
        if key not in results_dict:
            # logger.log(f"key not found in results {key}")
            continue
        # Sorting not needed
        # ret_values = _top_k_vals(results_dict[key], is_desc, k)
        gt_vals = list(ground_truth_dict[key].keys())
        mrr_val = _mrr(list(results_dict[key].keys())[:k], gt_vals)
        if mrr_val is None:
            logger.log(f"None MRR for index: { index}")
        else:
            logger.log(f"Reciprocal Rank for {key} is {mrr_val}")
        mrrs.append(mrr_val)
    return np.mean(mrrs)


def compute_mrr_from_list(ground_truth_list, results_list, k, self_remove=True):
    '''
    ground_truth_list: {'query_col': ['result1_col', 'result2_col']}
    results_list: {'query_col': ['result1_col':, 'result2_col']}
    '''

    mrrs = []
    if self_remove:
        for key, value in results_list.items():
            if key in value:
                results_list[key].remove(key) 
    for index, key in enumerate(ground_truth_list):
        
        if key not in results_list:
            # logger.log(f"key not found in results {key}")
            continue
        mrr_val = _mrr(results_list[key][:k], ground_truth_list[key])
        # if mrr_val is None:
        #     print("None MRR for index: ", index)
        # else:
            # print(f"Reciprocal Rank for {key} is {mrr_val}")
        mrrs.append(mrr_val)
    return np.mean(mrrs)

def compute_map_from_list(ground_truth_list, results_list, k, self_remove=True):
    '''
    ground_truth_list: {'query_col': ['result1_col', 'result2_col']}
    results_list: {'query_col': ['result1_col':, 'result2_col']}
    '''

    aps = []
    if self_remove:
        for key, value in results_list.items():
            if key in value:
                results_list[key].remove(key) 
    for index, key in enumerate(ground_truth_list):
        if key not in results_list:
            # logger.log(f"key not found in results {key}")
            continue
        aps.append(_average_precision(results_list[key][:k], ground_truth_list[key], k))
    return np.mean(aps)



def compute_ndcg(ground_truth_dict, results_dict, k=20, lower_better=False):
    '''
    ground_truth_dict: {'query_col': ['result1_col','result2_col']}}
             Ground Truth needs to be sorted before hand.
    results_dict: {'query_col': {'result1_col': score, 'result2_col':score}}
    lower_better : Should score of ground truth be sorted such that lower values are better
             Use True for lower better (ranking/distance) and False for higher better (similarity score) )
             
    '''
    ndcg_vals = []
    for index, key in enumerate(ground_truth_dict.keys()):  
        if key not in results_dict:
            # print(f"key not found in results {key}")
            continue
        # ret_values = list(results_dict[key].keys())  
        ret_values = results_dict[key]  
        gt_vals = ground_truth_dict[key]
        ndcg_val = _ndcg(ret_values, gt_vals, k, lower_better)
        ndcg_vals.append(ndcg_val)
    return np.mean(ndcg_vals)


def evaluate_results_file(config, result_file, start_k=None, max_k=None, step_k=None ):

    gt_file = config['groundtruth_filepath']
    gt_as_list = None
    if gt_file.endswith('.json'):
        gt_as_list = json.load(open(gt_file,'r'))
    elif gt_file.endswith('.jsonl'):
        gt_as_list = utl.convert_to_dict_of_list(gt_file)
    elif gt_file.endswith('.pickle'):
        raise NotImplementedError
    else:
        raise NotImplementedError
    result = utl.convert_to_dict_of_list(result_file)
    gt_with_score = utl.get_groundtruth_with_scores(gt_file)
    missing_queries =  set(gt_as_list.keys())-set(result.keys())
    logger.log(f"Result file is missing {len(missing_queries)} queries out of {len(gt_as_list)}")
    if start_k is None:
        k = config['top_k']
        evaluate_for_k(config, result_file, k, gt_as_list, result, gt_with_score)
    else:
        for k in range(start_k, max_k+1):
            evaluate_for_k(config, result_file, k, gt_as_list, result, gt_with_score, len(missing_queries), len(gt_as_list))
    logger.log(f"Search results for each query is dumped in {result_file}")
    logger.log(f"Evaluation results dumped in {logger.get_snapshot_dir()}/results.csv")
            

def evaluate_for_k(config, result_file, k, gt_as_list, result, gt_with_score, missing, total_queries):
    MRR = compute_mrr_from_list(gt_as_list, result, k)
    MAP = compute_map_from_list(gt_as_list, result, k)
    NDCG = compute_ndcg(gt_with_score, result, k=k, lower_better=config['lower_better'])
    Precision, Recall  = compute_precision_recall_at_k(gt_as_list, result, k)
    logger.record_tabular("method", config["method"])
    logger.record_tabular("benchmark", config["benchmark"])
    logger.record_tabular("groundtruth_filepath", config["groundtruth_filepath"])
    logger.record_tabular("search_result_file", result_file)
    logger.record_tabular("queries_count", total_queries)
    logger.record_tabular("missing_queries", missing)
    logger.record_tabular("K", k)
    logger.record_tabular("MRR", MRR)
    logger.record_tabular("NDCG", NDCG)
    logger.record_tabular("MAP", MAP)
    logger.record_tabular("Precision", Precision)
    logger.record_tabular("Recall", Recall)
    logger.dump_tabular()