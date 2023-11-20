from flask import Flask, request, send_file, abort
from werkzeug.utils import secure_filename
import os
import uuid
import json

app = Flask(__name__)

# Get the current folder path
current_folder = os.path.dirname(os.path.abspath(__file__))

# Create a 'root' folder to store uploaded files if it doesn't exist
data_folder = os.path.join(current_folder, 'root')
os.makedirs(data_folder, exist_ok=True)


def init_data_node_status():
    #Initializes the DataNode status by reading or creating a unique UUID in a configuration file.
    config_path = os.path.join(current_folder, 'config.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            uuid_value = config.get('uuid')
    else:
        uuid_value = str(uuid.uuid4())
        with open(config_path, 'w') as config_file:
            json.dump({'uuid': uuid_value}, config_file)

    return {
        "is_active": True,
        "uuid": uuid_value
    }


data_node_status = init_data_node_status()

# Storage format: {'data_id': {'chunk_id': file_path}}
# storage = {}


@app.route("/is_active", methods=['GET'])
def is_active():
    if data_node_status["is_active"]:
        return {"uuid": data_node_status["uuid"]}
    else:
        return {"status": "DataNode is not active"}, 503


@app.route("/write_file/", methods=['POST'])
def write_file():
    #Endpoint to receive and store files on the DataNode.
    data_id = request.form['data_id']
    chunk_id = request.form['chunk_id']
    file = request.files['file']
    data_directory = os.path.join('root', secure_filename(data_id))
    os.makedirs(data_directory, exist_ok=True)
    file_location = os.path.join(data_directory, secure_filename(chunk_id))
    file.save(file_location)

    # if data_id not in storage:
    # storage[data_id] = {}
    # storage[data_id][chunk_id] = file_location

    return {"data_id": data_id, "chunk_id": chunk_id, "file_location": file_location}


@app.route("/read_file/<data_id>/<chunk_id>", methods=['GET'])
def read_file(data_id, chunk_id):
    #Endpoint to read and retrieve a specific file chunk.
    if not data_node_status["is_active"]:
        return {"status": "DataNode is not active"}, 503

    data_directory = os.path.join(data_folder, secure_filename(data_id))
    file_location = os.path.join(data_directory, secure_filename(chunk_id))

    print(f"Reading file - data_id: {data_id}, chunk_id: {chunk_id}, file_location: {file_location}")

    if os.path.exists(file_location):
        return send_file(file_location)
    else:
        print("File not found.")
        return abort(404, "File chunk not found")


@app.route("/delete_chunks/<data_id>", methods=['POST'])
def delete_chunks(data_id):
    #Endpoint to delete all chunks associated with a particular data_id
    data_directory = os.path.join(data_folder, secure_filename(data_id))

    if os.path.exists(data_directory):
        for file_name in os.listdir(data_directory):
            file_location = os.path.join(data_directory, file_name)
            os.remove(file_location)

        os.rmdir(data_directory)
        return {"message": f"Chunks for data_id {data_id} deleted successfully"}
    else:
        return abort(404, f"Chunks directory for data_id {data_id} not found")


if __name__ == "__main__":
    # Run the Flask app on port 5002 in debug mode
    app.run(port=5002, debug=True)
