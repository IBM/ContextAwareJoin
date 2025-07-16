COLUMN_EMBEDDING = "/column_embeddings.pickle"
EMBEDDING_INDEX = "/deepjoin_index.bin"
DOCUMENT_IDS = "/doc_ids.pickle"
COLUMN_TEXT = "/column_text.pickle"

from .preprocess import create_index
from .query import search