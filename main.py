from myutils.logging_util import setup_logger
import argparse
import os
import sys
from myutils.evaluation import evaluate_results_file
from myutils.indexers import AvailableIndexers

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', type=str,
                        help='Method', choices=['exact_match', 'warpgate', 'topjoin', 'deepjoin','LSH_ensemble'])
    parser.add_argument('--benchmark', type=str,
                        help='Name of the benchmark')
    parser.add_argument('--groundtruth-filepath', type=str, required=True,
                        help='Jsonl groundtruth file' )
    parser.add_argument('--datalake-dir', type=str,
                        help='Path to directory with the data lake' )
    parser.add_argument('--file-format', type=str,  choices=['.csv', '.df', '.parquet'],
                        help='File Format' )
    parser.add_argument('--metadata-dir', type=str, default=None,
                        help='Directory containing metadata json for each table')
    parser.add_argument('--metadata-suffix', type=str, default=None,
                        help='Suffix used for each metadata file eg ".CSV.json" or ".json" or ".meta". ')
    parser.add_argument('--model', type=str, required='topjoin' in sys.argv or 'deepjoin' in sys.argv, 
                        help='Path (or ID) To Sentence Transformer Model to be used for Embedding')
    parser.add_argument('--embedding-indexer', choices=[i.name for i in list(AvailableIndexers)], default="NN")
    parser.add_argument('--minhash-indexer', choices=[i.name for i in list(AvailableIndexers)], default="LSH_FOREST")
    parser.add_argument('--top-k', type=int, default=100,
                        help='K')
    parser.add_argument('--candidate-k', type=int, default=100,
                        help='Candidate K')
    parser.add_argument('--start-k', type=int, default=1,
                        help='Maximum K')
    parser.add_argument('--max-k', type=int, default=10,
                        help='Maximum K')
    parser.add_argument('--topjoin-config', type=str, required='topjoin' in sys.argv,
                        help='Path To TopJoin Config (Required when method is `topjoin`)')
    parser.add_argument('--lower-better', default=False, action='store_true', help="Set to True if ground truth scores are ranking or distance (lower is better) and False for similarity scores (higher better). Default: False")
    parser.add_argument('--warpgate-encoder', default="fasttext", choices=['webtable', 'fasttext'],  help="Encoder for Warpgate only" )
    parser.add_argument('--fasttext-path', default="./cc.en.300.bin", help="Path to fasttext embedding file; Required when method is `warpgate`)")

    args = parser.parse_args()

    base_log_dir="./results/"

    # INDEX 
    
    
    config = args.__dict__
    setup_logger(f"{config['method']}_{args.benchmark}", variant=config, exp_id=os.getpid(), base_log_dir=base_log_dir)
    
    if config['method'] == 'exact_match':
        from exact_match import create_index
    elif config['method'] == 'warpgate':
        from warpgate import create_index    
    elif config['method'] == 'topjoin':
        from topjoin import create_index    
    elif config['method'] == 'deepjoin':
        from deepjoin import create_index      
    elif config['method'] == 'LSH_ensemble':
        from LSH_ensemble import create_index
    config['index_path'] = create_index(config)
    
    
    # SEARCH 

    if config['method'] == 'exact_match':
        from exact_match import search
    elif config['method'] == 'topjoin':
        from topjoin import search
        import json
        topjoin_config = json.load(open(args.topjoin_config, "r"))
        config.update(topjoin_config)
    elif config['method'] == 'warpgate':
        from warpgate import search
    elif config['method'] == 'deepjoin':
        from deepjoin import search 
    elif config['method'] == 'LSH_ensemble':
        from LSH_ensemble import search
    config['search_folder'] =  search(config)
    
    # Evaluation
    
    results_filepath = config['search_folder']+f"/search_results.jsonl"


    evaluate_results_file(config, results_filepath, 
                            start_k=config["start_k"], max_k=config["max_k"])



# %%
