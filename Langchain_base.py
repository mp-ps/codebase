# GIT Equivalent - DataIngestion_Draft1_Langchain.py
# https://pscode.lioncloud.net/gen-ai-experiements/gen-ai-data-ingestion/-/blob/data-ingestion-dev/DataIngestion_Draft1_Langchain.py?ref_type=heads

from google.cloud import storage
import pickle
import json

#Download from GCS to tmp
def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)
    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

# Langchain pdf2txt
from langchain.document_loaders import PyPDFLoader
def pdf2txt_langchain(filepath):
    loader = PyPDFLoader(filepath)
    pages = loader.load_and_split()
    text = pages[0].page_content
    print(pages[0].page_content)
    return text

# Embeddings and formatting for vector DB
from vertexai.language_models import TextEmbeddingModel
def text_embedding(text) -> list:
    """Text embedding with a Large Language Model."""
    model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")
    embeddings = model.get_embeddings([text])

    for embedding in embeddings:
        vector = embedding.values
        print(f"Length of Embedding Vector: {len(vector)}")
        print(vector)
    return vector

# Chunking
def create_vector_database(text,chunk_size):
    """Creates a vector database from a text file using Vertex AI text embeddings."""
    # df = pd.DataFrame(columns=["text", "embedding"])
    # Chunk text and generate
    chunks_list = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    print(len(chunks_list))
    embeddings_list = []
    for i in range(0,len(chunks_list)):
        embedding = text_embedding(chunks_list[i])
        embeddings_list.append(embedding)
    # result_list = [{"id": idx, "text": text, "embedding": embedding} for idx, (text, embedding) in
    #                enumerate(zip(chunks_list, embeddings_list))]
    #print(embeddings_list)
    result_list = [{"embedding": embedding} for embedding in
                   embeddings_list]
    print(result_list)
    return result_list

# Store as pickle file
def save_to_pickle(data, file_path):
    with open(file_path, 'wb') as file:
        pickle.dump(data, file)

def upload_to_gcs(bucket_name, local_file_path, gcs_file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_file_path)

if __name__ == "__main__":
    bucket_name = "gen-ai-data-source"
    source_blob_name = "sample.pdf"
    destination_file_name = "tmp/sample.pdf"
    download_blob(bucket_name, source_blob_name, destination_file_name)

    text = pdf2txt_langchain(destination_file_name)
    text = text.replace("\n", "")
    vectordb = create_vector_database(text, 500)

    print(vectordb)
    print("##########################")
    # Add "id" to each dictionary with an incrementing value
    final_vector_db = [{"id": str(i), "embedding": item["embedding"]} for i, item in enumerate(vectordb, start=1)]

    print(final_vector_db)
    output_file_path = "tmp/embeddings.json"
    with open(output_file_path, 'w') as json_file:
        for item in final_vector_db:
            json.dump(item, json_file)
            json_file.write('\n')

'''
#Reading saved Pickle as json dict for testing. 

import pickle
import json
pickle_file_path = "tmp/sample.pkl"
#Read pickle file as Dict - Testing
def read_pickle_and_convert_to_json(pickle_file_path):
    with open(pickle_file_path, 'rb') as file:
        # Load data from pickle file
        data_from_pickle = pickle.load(file)

        # Convert data to JSON
        json_data = json.dumps(data_from_pickle, indent=2)

    return json_data

json_data = read_pickle_and_convert_to_json(pickle_file_path)
print(json_data)
'''

