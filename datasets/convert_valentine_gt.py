import json
import os.path
import sys

with open(sys.argv[1]) as f:
    mapping = json.load(f)['matches']

    results = {}
    for m in mapping:
        results[m['source_table'] + '.' + m['source_column']] = [m['target_table'] + '.' + m['target_column']]

    out = os.path.join(os.path.dirname(sys.argv[1]), 'gt.json')
    with open(out, 'w') as of:
        json.dump(results, of)