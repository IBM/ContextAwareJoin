from enum import Enum
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import hamming
import json
import numpy as np
import myutils.fasttext_embeddings as ft

LARGE_NUMBER = 10e8

def hamming_distance_ranking(query, processed_candidates, **kwargs):
        hamming_distance_ranking = []
        for idx, item in enumerate(processed_candidates):
            # x = processed_candidates[idx]
            hamming_distance_ranking.append((idx, hamming(item['minhashes'], query['minhashes'])))
        return hamming_distance_ranking
    
def uniqueness_ranking(query, processed_candidates, **kwargs):
        result = []
        for idx, item in enumerate(processed_candidates):
                if item['num_rows'] > 0:
                        result.append((idx, item['count_distinct'] / item['num_rows']))
                else:
                        # print('num rows not available', item['column_name'])
                        result.append((idx,0))
        return result


def values_semantics_ranking(query, processed_candidates, **kwargs):
    similarity = cosine_similarity([query['freq_value_embedding']], [item['freq_value_embedding'] for item in processed_candidates] )[0]
    return list(enumerate(similarity))

def disjoint_semantics_FT_ranking(query, processed_candidates, **kwargs):
        all_disjoint_similarity = []
        query_values = query['col_values']
        idx_list = []
        query_disjoint_embedding = []
        candidate_disjoint_embedding = []
        for idx, item in enumerate(processed_candidates):
            candidate_values = item['col_values']
            query_disjoint_list = list(set(query_values) - set(candidate_values))
            candidate_disjoint_list = set(candidate_values) - set(query_values)
            if len(query_disjoint_list) == 0 or len(candidate_disjoint_list) == 0:
                if len(query_values) != 0 and len(candidate_values) != 0 :
                    all_disjoint_similarity.append((idx, 1.0))
                else:
                    all_disjoint_similarity.append((idx, 0.0))
            else:
                query_embedding = ft.get_fasttext_embeddings(kwargs["model"], query_disjoint_list)
                candidate_embedding = ft.get_fasttext_embeddings(kwargs["model"],candidate_disjoint_list)
                if query_embedding.shape[0] == 0 or candidate_embedding.shape[0] ==0 :
                    all_disjoint_similarity.append((idx, 1.0))
                    print("continueing")
                    continue
                idx_list.append(idx)
                query_disjoint_embedding.append(query_embedding)
                candidate_disjoint_embedding.append(candidate_embedding)
        if len(query_disjoint_embedding+candidate_disjoint_embedding) > 0:
            # embedding = .encode(query_disjoint+candidate_disjoint)
            similarity_scores = np.diagonal(cosine_similarity(query_disjoint_embedding, candidate_disjoint_embedding))
            all_disjoint_similarity += list(zip(idx_list, similarity_scores))
        return all_disjoint_similarity
    
     
def disjoint_semantics_ranking(query, processed_candidates, **kwargs):
        all_disjoint_similarity = []
        query_values = query['col_values']
        idx_list = []
        query_disjoint = []
        candidate_disjoint = []
        for idx, item in enumerate(processed_candidates):
            candidate_values = item['col_values']
            query_disjoint_list = list(set(query_values) - set(candidate_values))
            candidate_disjoint_list = set(candidate_values) - set(query_values)
            if len(query_disjoint_list) == 0 or len(candidate_disjoint_list) == 0:
                if len(query_values) != 0 and len(candidate_values) != 0 :
                    all_disjoint_similarity.append((idx, 1.0))
                else:
                    all_disjoint_similarity.append((idx, 0.0))
            else:
                idx_list.append(idx)
                query_disjoint.append(','.join(query_disjoint_list))
                candidate_disjoint.append(','.join(candidate_disjoint_list))
        if len(query_disjoint+candidate_disjoint) > 0:
            embedding = kwargs["encoder"].encode(query_disjoint+candidate_disjoint)
            similarity_scores = np.diagonal(cosine_similarity(embedding[:len(query_disjoint)], embedding[len(query_disjoint):]))
            all_disjoint_similarity += list(zip(idx_list, similarity_scores))
        return all_disjoint_similarity

def containment_ranking(query,  processed_candidates, **kwargs):
    query_values = set(query['col_values'])
    containment_ranking = []
    for idx, item in enumerate(processed_candidates):
        cand_values = set(item['col_values'])
        score = float(len(query_values.intersection(cand_values))) / float(len(cand_values))
        containment_ranking.append((idx, score))
    return containment_ranking

def overlap_size_ranking(query,  processed_candidates, **kwargs):
    query_values = set(query['col_values'])
    query_value_size = float(len(query_values))
    overlap_ranking = []
    for idx, item in enumerate(processed_candidates):
        cand_values = set(item['col_values'])
        score = float(len(query_values.intersection(cand_values)))/query_value_size
        overlap_ranking.append((idx, score))
    return overlap_ranking
  
def metadata_semantics_ranking(query,  processed_candidates, **kwargs):
    similarity = cosine_similarity([query['paragraph_embedding']], [item['paragraph_embedding'] for item in processed_candidates] )[0]
    return list(enumerate(similarity))

# compute join expansion ranking
def join_size_ranking(query, processed_candidates, **kwargs):
    query_join_expansion = []
    reverse_join_expansion = []
    query_values = query['freq_values']
    if len(query_values) > 0 :

        for idx, item in enumerate(processed_candidates):
            candidate_values = item['freq_values']
            common_values = set(query_values.keys()).intersection(candidate_values.keys())
            expansion = 0   # Estimating the joined table size
            for q in common_values:
                if type(candidate_values[q]) is not int or type(query_values[q]) is not int:
                    continue
                expansion += candidate_values[q] * query_values[q]
            #TODO: Handle x['num_rows'] == 0
            if expansion == 0 or (len(common_values)**2) == 0 or query['num_rows'] == 0:
                query_join_expansion.append((idx,LARGE_NUMBER))    
            else:
                query_join_expansion.append((idx, expansion / ((len(common_values)**2)*query['num_rows'])))
            if 'include_reverse' in kwargs and kwargs['include_reverse']:
                if expansion == 0 or (len(common_values)**2) == 0 or item['num_rows'] == 0:
                    reverse_join_expansion.append((idx, LARGE_NUMBER))
                else:    
                    reverse_join_expansion.append((idx, expansion / ((len(common_values)**2) * item['num_rows'])))
    if 'include_reverse' in kwargs and kwargs['include_reverse']:
        return query_join_expansion, reverse_join_expansion
    else:
        return query_join_expansion
    
    
class AvailableRankers(Enum):
    HAMMING = (hamming_distance_ranking,)
    UNIQUENESS = (uniqueness_ranking,)
    VALUE_SEMANTICS = (values_semantics_ranking,)
    DISJOINT_SEMANTICS = (disjoint_semantics_ranking,)
    DISJOINT_SEMANTICS_FT = (disjoint_semantics_FT_ranking,)

    METADATA_SEMANTICS = (metadata_semantics_ranking, )
    CONTAINMENT = (containment_ranking,)
    JOIN_SIZE = (join_size_ranking,)
    REVERSE_JOIN_SIZE = (join_size_ranking,)
    OVERLAP_SIZE = (overlap_size_ranking,)

    
    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)
        
    def __str__(self):
        return self.name

    @staticmethod
    def from_string(s):
        try:
            return AvailableRankers[s]
        except KeyError:
            raise ValueError()
