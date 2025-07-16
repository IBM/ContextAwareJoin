from abc import  abstractmethod
from sklearn.neighbors import NearestNeighbors
from enum import Enum
from scipy.spatial import distance
import numpy as np
from scipy.sparse import csr_matrix, vstack, hstack
import pickle
from datasketch import MinHashLSHForest, MinHash, MinHashLSHEnsemble
from myutils import utilities as utl

import xxhash

def _hash_func(d):
    return xxhash.xxh32(d).intdigest()

class TableIndexer:
    
    def __init__(self, index_path=None, features=None, **kwargs):
        self.features = features
    
    @abstractmethod 
    def dump(self, path):
        raise NotImplementedError
    
    @abstractmethod
    def column_search(self, query_column, k=10):
        raise NotImplementedError

    @abstractmethod
    def query(self, query_feature, k=10):
        raise NotImplementedError
    
    def __repr__(self):
        return f"{self.__class__.__name__}( \n feature={str(self.feature_extractor)},\n index_size={self.features_shape} \n )"


class NearestNeighbour_Indexer(TableIndexer):
       
    def __init__(self, index_path=None, features=None,  **kwargs):
        super(NearestNeighbour_Indexer, self).__init__(features=features, **kwargs)
        if index_path is None:
            self.index = NearestNeighbors(n_jobs=-1).fit(self.features)  
            self.index_size = len(self.features)
        else:
            # self.index = pickle.load(open(index_path,"rb"))
            self.index = utl.loadDictionaryFromPickleFile(index_path)
            self.index_size = self.index.n_samples_fit_

    def query(self, query_feature, k=10):
        D, I = self.index.kneighbors(np.asarray(query_feature).reshape(1,-1), n_neighbors=min(self.index_size, k))
        return D[0], I[0]
    
    def dump(self, path):
        utl.saveDictionaryAsPickleFile(self.index, path)
        # pickle.dump(self.index, open(path,"wb"))

class NN_Hamming_Indexer(NearestNeighbour_Indexer):
       
    def __init__(self, index_path,  features, **kwargs):
        super(NearestNeighbour_Indexer, self).__init__(features=features, **kwargs)
        if index_path is None:
            self.index = NearestNeighbors(n_jobs=-1, metric=distance.hamming).fit(self.features)  
            self.index_size = len(self.features)
        else:
            self.index = pickle.load(open(index_path,"rb"))
            self.index_size = self.index.n_samples_fit_

class DataSketch_LSHFOREST_Indexer(TableIndexer):
    
    def __init__(self, index_path=None, features=None, num_perm=100, hashfunc=_hash_func, **kwargs):
        super(DataSketch_LSHFOREST_Indexer, self).__init__(features=features, **kwargs)
        if index_path is None:
            self.num_perm = num_perm
            self.hashfunc = hashfunc
            self.index = MinHashLSHForest(num_perm=self.num_perm)
            for i, m in enumerate(self.features):
                self.index.add(i, m)
            self.index.index()
        else:
            self.index, self.num_perm, self.hashfunc = utl.loadDictionaryFromPickleFile(index_path)
    
    def column_search(self, query_column, top_n=10):
        if top_n > self.index_size:
            top_n = self.index_size
        query_id = self.get_id(query_column)
        results = self.index.query(self.features[query_id],k=top_n)
        return [(k, i) for i, k in enumerate(results) if k != query_column]

    def query(self, query_feature, k):
        query = MinHash(hashfunc=self.hashfunc, num_perm=len(query_feature), hashvalues=query_feature)
        I = self.index.query(query, k=k)
        return [1]*len(I), I
    
    def dump(self, path):
        utl.saveDictionaryAsPickleFile((self.index,self.num_perm, self.hashfunc), path)
        
 
class DataSketch_LSHEnsemble_Indexer(TableIndexer):
    
    def __init__(self, index_path=None, minhash_size=None, num_perm=100, hashfunc=_hash_func, threshold=0.2, num_part=60, **kwargs):
        super(DataSketch_LSHEnsemble_Indexer, self).__init__(features=minhash_size, **kwargs)
        if index_path is None:
            self.num_perm = num_perm
            self.hashfunc = hashfunc
            self.index = MinHashLSHEnsemble(threshold=threshold, num_perm=self.num_perm, num_part=num_part)
            enteries = []
            for i, (m,s) in enumerate(self.features):
                minhash = MinHash(hashfunc=self.hashfunc, num_perm=self.num_perm, hashvalues=m)
                enteries.append((i, minhash, s))
            self.index.index(enteries)
        else:
            self.index, self.num_perm, self.hashfunc = utl.loadDictionaryFromPickleFile(index_path)
    

    def query(self, query, k=100):
        query_minhash = MinHash(hashfunc=self.hashfunc, num_perm=self.num_perm, hashvalues=query[0])
        I = list(self.index.query(query_minhash, query[1]))
        return [1]*len(I), I
    
    def dump(self, path):
        utl.saveDictionaryAsPickleFile((self.index,self.num_perm, self.hashfunc), path)
        
 
 
class AvailableIndexers( Enum):
    NN = (NearestNeighbour_Indexer,)
    NN_HAMMING = (NN_Hamming_Indexer,)
    LSH_FOREST = (DataSketch_LSHFOREST_Indexer,)

    
    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)
        
    def __str__(self):
        return self.name

    @staticmethod
    def from_string(s):
        try:
            return AvailableIndexers[s]
        except KeyError:
            raise ValueError()