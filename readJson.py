# Read Json

import json

def read_json_file(file_path):
    data = []

    with open(file_path, 'r') as file:
        for line in file:
            # Parse each line as a JSON object
            try:
                json_object = json.loads(line)
                data.append(json_object)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {line.strip()}")
                print(f"Error details: {e}")

    return data

# Example usage
file_path = 'path/to/your/json_file.json'
json_data = read_json_file(file_path)

# Now 'json_data' contains a list of JSON objects, where each line in the file is a separate object
for obj in json_data:
    print(obj)
