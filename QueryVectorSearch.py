from vertexai.language_models import TextEmbeddingModel
from google.cloud import aiplatform_v1
import json
import re
from google.cloud import storage

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

def text_embedding(text) -> list:
    """Text embedding with a Large Language Model."""
    model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")
    embeddings = model.get_embeddings([text])

    for embedding in embeddings:
        vector = embedding.values
    return vector

def vector_search(API_ENDPOINT,INDEX_ENDPOINT,DEPLOYED_INDEX_ID,feature_vector):
    # Configure Vector Search client
    client_options = {
      "api_endpoint": API_ENDPOINT
    }
    vector_search_client = aiplatform_v1.MatchServiceClient(
      client_options=client_options,
    )

    # Build FindNeighborsRequest object
    datapoint = aiplatform_v1.IndexDatapoint(
      feature_vector= feature_vector
    )
    query = aiplatform_v1.FindNeighborsRequest.Query(
      datapoint=datapoint,
      # The number of nearest neighbors to be retrieved
      neighbor_count=5
    )
    request = aiplatform_v1.FindNeighborsRequest(
      index_endpoint=INDEX_ENDPOINT,
      deployed_index_id=DEPLOYED_INDEX_ID,
      # Request can have multiple queries
      queries=[query],
      return_full_datapoint=False,
    )

    # Execute the request
    response = vector_search_client.find_neighbors(request)

    # Handle the response
    # print(response)

    return response

if __name__ == "__main__":
    # Set variables for the current deployed index.
    bucket_name = "gen-ai-data-source"
    folder_name = "VectorDB"

    API_ENDPOINT = "191176625.us-central1-402279900712.vdb.vertexai.goog"
    INDEX_ENDPOINT = "projects/402279900712/locations/us-central1/indexEndpoints/6089926625414086656"
    DEPLOYED_INDEX_ID = "finalvectordb_9docs_1704435727690"

    ## INPUT QUERIES:
    query = "Company name is lloyds."
    feature_vector = text_embedding(query)

    nearest_vectors = vector_search(API_ENDPOINT,INDEX_ENDPOINT,DEPLOYED_INDEX_ID,feature_vector)

    nearest_vectors = str(nearest_vectors)
    # print(nearest_vectors)
    # print(type(nearest_vectors))

    # Use regular expressions to extract datapoint_id and distance
    matches = re.findall(r'datapoint_id: "(.*?)".*?distance: (.*?)\n', nearest_vectors, re.DOTALL)

    # Create a list of dictionaries
    result_list = [{"datapoint_id": match[0], "distance": float(match[1])} for match in matches]

    # Print the resulting list of dictionaries
    print(result_list)

    # Extract only datapoint_ids using list comprehension
    datapoint_ids = [item['datapoint_id'] for item in result_list]

    # Print the resulting list of datapoint_ids
    print(datapoint_ids)

    ### Extract from metadata file
    ids_to_extract = datapoint_ids

    source_blob_name = folder_name + "/FinalVectorDB.json"
    destination_file_name = "tmp/FinalVectorDB.json"
    #Download vectordb from GCS in tmp
    download_blob(bucket_name, source_blob_name, destination_file_name)

    # Read the JSON file
    with open(destination_file_name, 'r') as json_file:
        data = [json.loads(line) for line in json_file]

    # Filter records based on the list of ids
    filtered_records = [record for record in data if record.get('id') in ids_to_extract]

    record_list = []
    # Print the filtered records
    for record in filtered_records:
        record_list.append(record)
    print(record_list)
