# From https://github.com/guenthermi/table-embeddings
from table_embeddings import TableEmbeddingModel
from .fasttext_embeddings import select_tokens
import numpy as np

def get_embedding_model(model="ddrg/web_table_embeddings_combo64"):
    # Get embeddings for representative tokens using all column values
    return TableEmbeddingModel.load_model(model)

def get_embeddings(model, value_list):
    # Get embeddings for representative tokens using all column values
    representative_token = select_tokens(value_list)
    representative_token_embeddings = [model.get_header_vector(token) for token in representative_token]
    # Calculate the average embedding
    if len(representative_token_embeddings) == 0:
            return np.empty(0)
    combined_embedding = np.mean(representative_token_embeddings, axis=0)
    return combined_embedding


def get_data_embeddings(model, value_list):
    # Get embeddings for representative tokens using all column values
    representative_token = select_tokens(value_list)
    representative_token_embeddings = [model.get_data_vector(token) for token in representative_token]
    # Calculate the average embedding
    if len(representative_token_embeddings) == 0:
            return np.empty(0)
    combined_embedding = np.mean(representative_token_embeddings, axis=0)
    return combined_embedding