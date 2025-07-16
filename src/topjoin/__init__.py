METADATA_INDEX = "/metadata_index.bin"
FREQ_VALUE_INDEX = "/freq_value_index.bin"
MINHASH_INDEX = "/minhash_index.pickle.bz2"
DOCUMENTS_DICT = "/document_dict.pickle.bz2"
DOCUMENT_IDS = "/doc_ids.pickle"

from enum import Enum
class DATA_TYPE(str, Enum):
    STRING = 'string'
    FLOAT = 'float'
    INTEGER = 'integer'
    DATE = 'date'



from .preprocess import create_index
from .query import search