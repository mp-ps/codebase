""""# List only filenames, not folders from GCP bucket"""
from google.cloud import storage

def list_filenames(bucket_name, prefix=''):
    """List only filenames (not folders) from a GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    # Filter out directories (blobs with '/' in the name)
    filenames = [blob.name for blob in blobs if '/' not in blob.name]

    return filenames

if __name__ == "__main__":
    # Replace with your actual GCS bucket name and optional prefix
    gcs_bucket_name = 'your-gcs-bucket'
    prefix = 'your-prefix/'  # Optional prefix

    filenames = list_filenames(gcs_bucket_name, prefix)

    for filename in filenames:
        print(filename)

"""# Move file within a bucket after processing it. """

from google.cloud import storage

def move_file_within_bucket(bucket_name, source_blob_name, destination_folder, destination_blob_name):
    """Move a file within the same GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Get the source blob
    source_blob = bucket.blob(source_blob_name)

    # Create the destination blob with the new name and folder
    destination_blob = bucket.blob(f"{destination_folder}/{destination_blob_name}")

    # Copy the content from the source blob to the destination blob
    destination_blob.upload_from_string(source_blob.download_as_text())

    # Delete the source blob after successful copy
    source_blob.delete()

    print(f"File moved from '{source_blob_name}' to '{destination_folder}/{destination_blob_name}'")

# Example usage
bucket_name = "your_bucket_name"
source_blob_name = "source_folder/source_file.txt"
destination_folder = "destination_folder"
destination_blob_name = "destination_file.txt"

move_file_within_bucket(bucket_name, source_blob_name, destination_folder, destination_blob_name)


""""# Read json, add data, save again, move older to archive"""

import json
import datetime
from google.cloud import storage

def read_json_from_gcs(bucket_name, file_name):
    """Reads JSON from GCS file."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the file content
    content = blob.download_as_text()

    # Parse the JSON content
    data = json.loads(content)
    return data

def write_json_to_gcs(bucket_name, file_name, data):
    """Writes JSON to GCS file."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Convert data to JSON string
    json_content = json.dumps(data, indent=2)

    # Upload the updated content
    blob.upload_from_string(json_content, content_type='application/json')

def move_file_to_archive(bucket_name, old_file_name, new_file_name):
    """Moves old file to archive folder."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Copy the old file to the archive folder with a new name
    archive_blob = bucket.blob(f'archive/{new_file_name}')
    bucket.copy_blob(bucket.get_blob(old_file_name), archive_blob)

    # Delete the old file
    bucket.delete_blob(old_file_name)

def main():
    # Replace these with your actual GCS details
    gcs_bucket_name = 'your-gcs-bucket'
    input_file_name = 'your-input-file.json'

    # Read existing JSON from GCS
    existing_data = read_json_from_gcs(gcs_bucket_name, input_file_name)

    # Modify the JSON data (add new objects, etc.)
    # For example, adding a new object with a timestamp
    timestamp = datetime.datetime.now().isoformat()
    new_object = {"id": "5", "embedding": [1.0, 2.0, 3.0], "metadata": {"text": "New Object", "timestamp": timestamp}}
    existing_data.append(new_object)

    # Write the updated JSON back to GCS with a timestamped name
    timestamped_file_name = f'updated_{timestamp}_{input_file_name}'
    write_json_to_gcs(gcs_bucket_name, timestamped_file_name, existing_data)

    # Move the old file to the archive folder
    move_file_to_archive(gcs_bucket_name, input_file_name, timestamped_file_name)

if __name__ == "__main__":
    main()


"""# Sentence Chunking"""

# Chunk
def chunk_data(input_data, max_chunk_size):
    chunks = []
    current_chunk = []

    for sentence in input_data.split('. '):  # Assuming sentences end with a period (adjust as needed)
        if current_chunk and len(' '.join(current_chunk)) + len(sentence) <= max_chunk_size:
            current_chunk.append(sentence)
        else:
            if current_chunk:
                chunks.append(' '.join(current_chunk))
            current_chunk = [sentence]

    if current_chunk:
        chunks.append(' '.join(current_chunk))

    return chunks

# Example usage:
input_text = """tin experiences for our customers."""
max_chunk_size = 2 # Set your desired maximum character count

result_chunks = chunk_data(input_text, max_chunk_size)
print(result_chunks)
