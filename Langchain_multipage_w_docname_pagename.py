# 1 DOC Multiple Page
# Git equivalent - DataIngestion_Langchain_multipage_w_docname_pagename
# https://pscode.lioncloud.net/gen-ai-experiements/gen-ai-data-ingestion/-/blob/data-ingestion-dev/DataIngestion_Langchain_multipage_w_docname_pagename.py?ref_type=heads

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
    return pages

# Embeddings and formatting for vector DB
from vertexai.language_models import TextEmbeddingModel
def text_embedding(text) -> list:
    """Text embedding with a Large Language Model."""
    model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")
    embeddings = model.get_embeddings([text])

    for embedding in embeddings:
        vector = embedding.values
        #print(f"Length of Embedding Vector: {len(vector)}")
        #print(vector)
    return vector

# Chunking
def create_vector_database(text,chunk_size,source_blob_name,page_number):
    """Creates a vector database from a text file using Vertex AI text embeddings."""
    # Chunk text and generate
    chunks_list = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    #print(len(chunks_list))
    embeddings_list = []
    for i in range(0,len(chunks_list)):
        embedding = text_embedding(chunks_list[i])
        embeddings_list.append(embedding)
    result_list = [{"id": idx, "embedding": embedding, "metadata": {"text": text,"source_blob_name":source_blob_name,"page_number":page_number}} for idx, (text, embedding) in
                   enumerate(zip(chunks_list, embeddings_list))]
    #print(result_list)
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
    destination_file_name = f"tmp/{source_blob_name}"
    download_blob(bucket_name, source_blob_name, destination_file_name)

    pages = pdf2txt_langchain(destination_file_name)
    # print(pages)
    # print(len(pages))
    vectordb_allpages = []
    for page in pages:
        #print(page)
        text = page.page_content
        text = text.replace("\n","")
        #print(text)
        vectordb = create_vector_database(text, 500,source_blob_name,int(page.metadata["page"])+1)
        vectordb_allpages.extend(vectordb)
    #print(vectordb_allpages)

    # Add "id" to each dictionary with an incrementing value
    final_vector_db = [{"id": str(i), "embedding": item["embedding"],"metadata":item["metadata"]} for i, item in enumerate(vectordb_allpages, start=1)]

    print(final_vector_db)

    output_file_path = "tmp/samplefileembeddings.json"
    with open(output_file_path, 'w') as json_file:
        for item in final_vector_db:
            json.dump(item, json_file)
            json_file.write('\n')
