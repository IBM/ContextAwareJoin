from sklearn.metrics.pairwise import euclidean_distances
from .rankers import AvailableRankers
from . import METADATA_INDEX, FREQ_VALUE_INDEX, MINHASH_INDEX,DOCUMENT_IDS, DOCUMENTS_DICT, DATA_TYPE
import pickle
from collections import defaultdict
import numpy as np 
from datasketch import MinHash
from functools import partial
from sentence_transformers import SentenceTransformer 
from myutils import utilities as utl
from myutils.indexers import AvailableIndexers

class Joinable_QueryHelper(object):

    
    def __init__(self, config):
        self.config=config
        self.model = SentenceTransformer(config['model'])
        # self.es_utils = ElasticSearchUtils(url=config['es_url'], join_index=config['es_index'])
        self.individual_ranking_methods = [AvailableRankers[ranker] for ranker in self.config['include_ranking']]
        if 'DISJOINT_SEMANTICS' in self.config['include_ranking']:
            # partially initialize DISJOINT_SEMANTICS with model
            idx = self.config['include_ranking'].index('DISJOINT_SEMANTICS')
            self.individual_ranking_methods[idx] = partial(self.individual_ranking_methods[idx], encoder=self.model)
        if 'REVERSE_JOIN_SIZE' in   self.config['include_ranking']:
            # partially initialize JOIN_SIZE with 'include_reverse'
            idx = self.config['include_ranking'].index('JOIN_SIZE')
            self.individual_ranking_methods[idx] = partial(self.individual_ranking_methods[idx], include_reverse=True)
            idx = self.config['include_ranking'].index('REVERSE_JOIN_SIZE')
            self.individual_ranking_methods.pop(idx)

        self.rankers_numeric_cols = []
        self.weights_numeric_cols = []
        self.order_numeric_cols = []
        for idx, include in enumerate(self.config['support_numeric']):
            if include:
                # TO ADJUST FOR JOIN_SIZE & REVERSE_JOIN_SIZE
                if idx < len(self.individual_ranking_methods):
                    self.rankers_numeric_cols.append(self.individual_ranking_methods[idx])
                self.weights_numeric_cols.append(self.config['ranking_weights'][idx])
                self.order_numeric_cols.append(self.config['ranking_order'][idx])
        
        self.minhash_index = AvailableIndexers[config['minhash_indexer']](index_path=config['index_path'] + 
                                                            MINHASH_INDEX) 
                
        self.metadata_index = AvailableIndexers[config['embedding_indexer']](index_path=config['index_path'] + 
                                                            METADATA_INDEX) 
        self.freq_value_index = AvailableIndexers[config['embedding_indexer']](index_path=config['index_path'] + 
                                                            FREQ_VALUE_INDEX) 
        self.document_dict = utl.loadDictionaryFromPickleFile(config['index_path'] + 
                                                            DOCUMENTS_DICT )
        self.document_ids = utl.loadDictionaryFromPickleFile(config['index_path'] + 
                                                            DOCUMENT_IDS )
        
    def calc_topsis(self, evaluation_matrix, weights, ranks=True):
        assert len(weights) == evaluation_matrix.shape[1]
        normalized_weight = np.array(weights/np.sum(weights),dtype=float)
        # Step 2: Normalize the matrix
        col_sums = np.square(evaluation_matrix).sum(axis=0)
        col_sums[col_sums == 0] =1 
        decision_matrix = evaluation_matrix / np.sqrt(col_sums[np.newaxis,:])
        # Step 3: Reweight the normalize matrix
        decision_matrix = decision_matrix*normalized_weight[np.newaxis,:]
        # Step 4: Compute worst & best alternative
        if ranks:
            worst_alternative = decision_matrix.min(axis=0)
            best_alternative = decision_matrix.max(axis=0)
        else:
            worst_alternative = decision_matrix.max(axis=0)
            best_alternative = decision_matrix.min(axis=0)
    
        # Step 5: Compute distance from best & worst alternative
        distances = euclidean_distances(decision_matrix, [best_alternative])
        # Step 6: Compute similarity to best alternative 
        # similarty = distances[:,0]/distances.sum(axis=1)
        # ranking = [i+1 for i in similarty.argsort()]
        # Return ranking for each item & distance from best solution
        return distances[:,0] 

    def generate_matrix(self, individual_rankings, order):
        # For elements that do not appear in a particular ranking but appear in other ranking
        
        # Adding [0] to list to handle cases where individual ranking list is empty
        # adding 0.1 to handle cases where max score from individual ranking is 0
        # multiplying by 2 to ensure the default score is higher than the actual max score
        max_dist = {x:max([item[1] for item in individual_rankings[x]]+[0])+0.1*2 for x in range(len(individual_rankings))}

        items_dic = defaultdict(lambda: max_dist.copy())
        for ranker in range(len(individual_rankings)):
            for item, ranking in individual_rankings[ranker]:
                items_dic[item][ranker] = ranking
        number_of_items= len(items_dic.keys())   
        
        matrix  = np.zeros((number_of_items, len(individual_rankings)))
        item_ids = []
        
        for i, item in enumerate(items_dic.keys()):
            item_ids.append(item)
            for ranker in range(len(individual_rankings)):
                if order[ranker]:
                    matrix[i, ranker] = items_dic[item][ranker]
                else:
                    # When ranking order is reversed (lower is better)
                    matrix[i, ranker] = items_dic[item][ranker]*-1
                    
        return item_ids, matrix

    def get_topsis_ranking(self, individual_rankings, weights=None, order=None):
        if weights is None:
            weights = [1 for i in range(0, len(individual_rankings))]
        # Compute evaluation Matrix
        item_ids, evaluation_matrix = self.generate_matrix(individual_rankings, order) # Eval_matrix size = # of times X # of rankings
        dist = self.calc_topsis(evaluation_matrix, weights)
        bests_ids = dist.argsort() 
        return [item_ids[i] for i in bests_ids], [dist[i] for i in bests_ids]

    def query_joinability(self, query_table, query_col):
        
        query_column = self.document_dict[(query_table, query_col)]

        D, candidates  =  self.minhash_index.query(query_column['minhashes'],k=self.config['candidate_k'])
        D, I = self.metadata_index.query(query_column['paragraph_embedding'],k=self.config['candidate_k'])
        candidates += list(I)
        if query_column['type'] == DATA_TYPE.STRING.value:
            D, I  = self.freq_value_index.query(query_column['freq_value_embedding'],k=self.config['candidate_k'])
            candidates += list(I)
        if len(candidates) == 0:
            return []
        processed_candidates = [ self.document_dict[self.document_ids[i]] for i in list(set(candidates))] 

        individual_rankings = [] 
        ranking_methods, ranking_weights, ranking_order = None, None, None
        if query_column['type'] == DATA_TYPE.STRING.value:
            ranking_methods, ranking_weights, ranking_order = self.individual_ranking_methods, self.config['ranking_weights'], self.config['ranking_order']
        else:
            ranking_methods, ranking_weights, ranking_order  = self.rankers_numeric_cols, self.weights_numeric_cols, self.order_numeric_cols
        for m in ranking_methods:
            if isinstance(m, partial) and 'include_reverse' in m.keywords:
                individual_rankings += m(query_column, processed_candidates)
            else:
                individual_rankings.append(m(query_column, processed_candidates))
        
        items, scores = self.get_topsis_ranking(individual_rankings, ranking_weights, ranking_order )
        ranked_items = [ f"{processed_candidates[i]['table']}.{processed_candidates[i]['column_name']}" for i in items[:self.config['top_k']]]

        return ranked_items, scores[:self.config['top_k']]
