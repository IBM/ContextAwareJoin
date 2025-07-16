# ContextAwareJoin

<p align="center">
    <a href="https://arxiv.org/abs/2507.11505">📄 Paper</a> • <a href="./GettingStarted.md">🔥 Getting Started</a>
</p>

<p align="center">
    <a href="#%EF%B8%8F-datasets">🗃️ Datasets</a> • 
    <a href="#-license">✋ License</a> •
    <a href="#-citation">📜 Citation</a>
</p>



This repo contains the code for Evaluating Joinable Column Discovery Approaches for Context-Aware Search!

## 🔥 Getting Started

1. Clone the repo

  ```
  git clone https://github.com/IBM/ContextAwareJoin.git
  ```

2. Create a new python env and activate

  ```
  python3.11 -m venv pyenv
  source pyenv/bin/activate
  ```

3. Install the repo

  ```
  pip install -e .
  ```

4. To run index, search and evaluation, use `main.py`.

```bash
$ python main.py --help
usage: main.py [-h] [--method {exact_match,warpgate,topjoin,deepjoin,LSH_ensemble}] [--benchmark BENCHMARK] --groundtruth-filepath GROUNDTRUTH_FILEPATH [--datalake-dir DATALAKE_DIR] [--file-format {.csv,.df,.parquet}]
               [--metadata-dir METADATA_DIR] [--metadata-suffix METADATA_SUFFIX] [--model MODEL] [--embedding-indexer {NN,NN_HAMMING,LSH_FOREST}] [--minhash-indexer {NN,NN_HAMMING,LSH_FOREST}] [--top-k TOP_K]
               [--candidate-k CANDIDATE_K] [--start-k START_K] [--max-k MAX_K] [--topjoin-config TOPJOIN_CONFIG] [--lower-better] [--warpgate-encoder {webtable,fasttext}] [--fasttext-path FASTTEXT_PATH]

options:
  -h, --help            show this help message and exit
  --method {exact_match,warpgate,topjoin,deepjoin,LSH_ensemble}
                        Method
  --benchmark BENCHMARK
                        Name of the benchmark
  --groundtruth-filepath GROUNDTRUTH_FILEPATH
                        Jsonl groundtruth file
  --datalake-dir DATALAKE_DIR
                        Path to directory with the data lake
  --file-format {.csv,.df,.parquet}
                        File Format
  --metadata-dir METADATA_DIR
                        Directory containing metadata json for each table
  --metadata-suffix METADATA_SUFFIX
                        Suffix used for each metadata file eg ".CSV.json" or ".json" or ".meta".
  --model MODEL         Path (or ID) To Sentence Transformer Model to be used for Embedding
  --embedding-indexer {NN,NN_HAMMING,LSH_FOREST}
  --minhash-indexer {NN,NN_HAMMING,LSH_FOREST}
  --top-k TOP_K         K
  --candidate-k CANDIDATE_K
                        Candidate K
  --start-k START_K     Maximum K
  --max-k MAX_K         Maximum K
  --topjoin-config TOPJOIN_CONFIG
                        Path To TopJoin Config (Required when method is `topjoin`)
  --lower-better        Set to True if ground truth scores are ranking or distance (lower is better) and False for similarity scores (higher better). Default: False
  --warpgate-encoder {webtable,fasttext}
                        Encoder for Warpgate only
  --fasttext-path FASTTEXT_PATH
                        Path to fasttext embedding file; Required when method is `warpgate`)
```

Example Usage:

```
BENCHMARK=go_sales
DATALAKE_DIR=./datasets/gosales/datalake
FILE_FORMAT=.df
GT_FILE=./datasets/gosales/gt.jsonl
python main.py --benchmark ${BENCHMARK} --datalake-dir ${DATALAKE_DIR} --file-format ${FILE_FORMAT}  --groundtruth-filepath ${GT_FILE}  --method LSH_ensemble
```



## 🗃️ Datasets

All the datasets used in the paper are publicly available, except the CIO dataset. Read more about the datasets [here](./datasets/README.md)

## ✋ License

> [!IMPORTANT]
> 
> This code is released with CC BY-NC-ND 4.0 License. In addition to that, please pay attention to the public disclosure below.

You are free to copy, modify and distribute this code only for the purpose of comparing this code to other code for scientific experimental purposes, where that distribution is not for a fee, nor does it accompany anything for which a fee is charged.

All content in these repositories including code has been provided by IBM under the associated restrictive-use software license and IBM is under no obligation to provide enhancements, updates, or support. IBM developers produced this code as a computer science project (not as an IBM product), and IBM makes no assertions as to the level of quality nor security, and will not be maintaining this code going forward.


## 📜 Citation

```
@inproceedings{kokel2025topjoin,
  title={TOPJoin: A Context-Aware Multi-Criteria Approach for
Joinable Column Search.},
  author={Harsha Kokel and Aamod Khatiwada and Tejaswini Pedapati and Haritha
Ananthakrishnan and Oktie Hassanzadeh and Horst Samulowitz and Kavitha
Srinivas.},
 booktitle    = {{VLDB} Workshops},
  year={2025}
}
```
