# importing all the libraries and modules required for the project
from flask import Flask, request, jsonify, send_file, abort
from collections import defaultdict
import os
import io
import uuid
import threading
import requests
from werkzeug.utils import secure_filename
import time
import pymongo
import datetime
from pymongo import MongoClient


app = Flask(__name__)
os.makedirs('data', exist_ok=True)

# Connect to MongoDB
print("Connecting to MongoDB...")
client = MongoClient('mongodb://localhost:27017/')
db = client['namenode_db']
print(client)

# Collection for files metadata
files_collection = db['files']

# Collection for chunks locations
chunks_collection = db['chunks']

# Collection for replication chunk locations
replication_collection = db['replication_chunks']

# Collection for directories
directories_collection = db['directories']

# Collection for active datanodes
active_datanodes_collection = db['active_datanodes']

# Collection for failedNode_handled_status
failedNode_handled_collection = db['failedNode_handled']

# A dictionary to keep track of DataNode health status
active_datanodes = {}
# A dictionary to keep track of chunks and their locations on DataNodes
chunks_location = {}
# Lock for thread-safe operations on the chunks_location dictionary
chunks_location_lock = threading.Lock()

failed_node = []

# Keeps track of all paths which have to be deleted
deletepaths = []

deletefolder_path = []

# List of DataNode addresses - replace these with your actual DataNode addresses
datanode_addresses = [
    "http://localhost:5005",
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5006"
]

# Replication factor
replication_factor = 3

root_directory = "/"


@app.route('/upload_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    number_of_chunks = int(request.form['number_of_chunks'])

    # Get directory path from the request
    # In file systems, "/" refers to the root directory
    directory_path = request.form.get('directory_path', '/')

    # If the directory path doesn't start with '/', add the root directory
    if not directory_path.startswith('/'):
        directory_path = '/' + directory_path

    # Check if the provided directory path exists
    if not directory_exists(directory_path) and directory_path != "/":
        return jsonify({'error': 'Invalid directory path'}), 400

    data_id = str(uuid.uuid4())

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    split_file(request.files['file'].stream, number_of_chunks, data_id)

    # Save file metadata in MongoDB -
    file_data = {
        'id': data_id,
        'name': file.filename,
        'number_of_chunks': number_of_chunks,
        'replication_factor': replication_factor,
        'directory_path': directory_path,
        'upload_time': datetime.datetime.now()
    }
    files_collection.insert_one(file_data)

    # Save chunk locations in MongoDB
    with chunks_location_lock:
        for chunk_id, locations in chunks_location[data_id].items():
            for location in locations:
                chunk_data = {
                    'file_id': data_id,
                    'chunk_id': chunk_id,
                    'datanode_address': location
                }
                chunks_collection.insert_one(chunk_data)

    # Save directory information in MongoDB
    directories_collection.update_one(
        {'path': directory_path},
        {'$addToSet': {'content': {'file_name': file.filename}}}
    )

    return jsonify({"message": "File uploaded and split successfully", "data_id": data_id}), 200


def directory_exists(directory_path):
    # Check if the provided directory path exists
    return directories_collection.find_one({'path': directory_path}) is not None


@app.route('/get_info', methods=['GET'])
def get_info():
    # Retrieve metadata from MongoDB for files, directories, and chunks
    files_metadata = list(files_collection.find({}, {'_id': 0}))
    directories_metadata = list(directories_collection.find({}, {'_id': 0}))
    chunks_metadata = list(chunks_collection.find({}, {'_id': 0}))

    # Organize chunks by file name
    file_chunks = defaultdict(list)
    for chunk in chunks_metadata:
        file_chunks[chunk['file_id']].append(chunk)

    # Combine the retrieved metadata per file
    files_with_chunks = {}
    for file_info in files_metadata:
        file_id = file_info['id']
        if file_id in file_chunks:
            # Retrieve chunks for the file_id directly
            chunks_for_file = file_chunks[file_id]
        else:
            chunks_for_file = []  # If file_id not found, assign an empty list

        files_with_chunks[file_id] = {
            'file_info': file_info,
            'chunks': chunks_for_file
        }

    directories_with_files = {}

    for directory_info in directories_metadata:
        directory_name = directory_info['path']
        file_names = [file_obj['file_name']
                      for file_obj in directory_info['content']]

        # Retrieve files_with_chunks information based on file_names
        files_in_directory = {}
        for file_name in file_names:
            for file_id, file_data in files_with_chunks.items():
                if file_data['file_info']['name'] == file_name:
                    files_in_directory[file_id] = file_data
                    break  # Stop searching once matched

        directories_with_files[directory_name] = {
            'directory_info': directory_info,
            'files_with_chunks': files_in_directory
        }

    # Return the combined metadata per directory with files and chunks in a JSON response
    return jsonify({
        'directories_with_files_and_chunks': directories_with_files
    })


def fetch_chunk_from_datanode(datanode_address, data_id, chunk_id):
    # arguments datanode_address(str), data_id(str), chunk_id(int)

    # Construct the endpoint to fetch the chunk from the DataNode
    datanode_download_endpoint = f"{
        datanode_address}/read_file/{data_id}/{chunk_id}"

    try:
        response = requests.get(datanode_download_endpoint)
        if response.status_code == 200:
            # Return the content of the chunk if successfully fetched
            return response.content
        else:
            # Display an error message if fetching the chunk fails
            print(f"Failed to fetch chunk {chunk_id} from {datanode_address}")
            return None
    except requests.exceptions.RequestException:
        # Handle connection errors to the DataNode
        print(f"Failed to connect to {datanode_address}")
        return None


@app.route('/get_file', methods=['POST'])
def get_file():

    # Retrieve file_name and directory_path from the request
    file_name = request.form.get('file_name')
    directory_path = request.form.get('directory_path', '/')

    # Validate and retrieve file_id based on file_name and directory_path
    file_metadata = files_collection.find_one(
        {'name': file_name, 'directory_path': directory_path}, {'_id': 0, 'id': 1})

    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    data_id = file_metadata['id']

    # Retrieve chunks' metadata from MongoDB
    chunks_metadata = list(chunks_collection.find(
        {'file_id': data_id}, {'_id': 0}))

    if not chunks_metadata:
        return jsonify({'error': 'Invalid data_id or no chunks found'}), 400

    file_data = b""

    # Iterate through chunks and fetch their content from DataNodes
    for chunk_info in sorted(chunks_metadata, key=lambda x: x['chunk_id']):
        datanode_address = chunk_info['datanode_address']
        chunk_id = chunk_info['chunk_id']

        file_chunk = fetch_chunk_from_datanode(
            datanode_address, data_id, chunk_id)

        if file_chunk is not None:
            file_data += file_chunk
        else:
            # Handle cases where the initial fetch fails
            replication_entry = replication_collection.find_one(
                {'data_id': data_id, 'chunk_id': chunk_id})
            if replication_entry and replication_entry['status'] == 'success':
                # If replication was successful, attempt to fetch the chunk again'
                datanode_address = replication_entry['datanode_address']
                data_id = replication_entry['data_id']
                chunk_id = replication_entry['chunk_id']

                file_chunk_retry = fetch_chunk_from_datanode(
                    datanode_address, data_id, chunk_id)
                if file_chunk_retry is not None:
                    file_data += file_chunk_retry
                else:
                    print(f"Retry failed for chunk {chunk_id}")
            else:
                print(f"Replication failed for chunk {chunk_id}")

        # datanode_download_endpoint = f"{datanode_address}/read_file/{data_id}/{chunk_id}"

    if not file_data:
        # Return an error response if failed to fetch the file data
        return jsonify({'error': 'Failed to fetch file data'}), 500

    # Save the stitched file temporarily
    file_path = f"data/{data_id}_stitched.bin"
    with open(file_path, 'wb') as file:
        file.write(file_data)

    # Return the file as an attachment for download
    return send_file(file_path, as_attachment=True)


# Function to handle replication for a single chunk
def replicate_chunk(data_id, chunk_id, chunk_data, active_nodes):
    # arguments data_id(str), chunk_id(str), chunk_data(bytes), active_nodes(list)

    # Find the index of the original node where the chunk resides
    original_node_index = active_nodes.index(
        chunks_location[data_id][chunk_id][0])

    # Clear existing replication details for this chunk in MongoDB
    replication_collection.delete_many(
        {'data_id': data_id, 'chunk_id': chunk_id})

    # Loop through the replication attempts based on the replication factor
    for attempt in range(1, replication_factor):
        # Calculate the index for the next replica
        node_index = (original_node_index + attempt) % len(active_nodes)
        datanode_address = active_nodes[node_index]
        datanode_upload_endpoint = f"{datanode_address}/write_file/"

        # Prepare the chunk data to be sent to the DataNode
        chunk_file = {'file': (f'chunk_{chunk_id}.bin', chunk_data)}

        # Send the chunk to the DataNode for replication
        response = requests.post(datanode_upload_endpoint, files=chunk_file, data={
                                 'data_id': data_id, 'chunk_id': chunk_id})

        # Prepare replication entry to be stored in the database
        replication_entry = {
            'data_id': data_id,
            'chunk_id': chunk_id,
            'datanode_address': datanode_address,
            'status': 'success' if response.status_code == 200 else 'failure'
        }

        # Store replication details in MongoDB
        replication_collection.insert_one(replication_entry)

        # Update chunks location if replication is successful
        if response.status_code == 200:
            with chunks_location_lock:
                chunks_location[data_id][chunk_id].append(datanode_address)
        else:
            print(f"Failed to replicate chunk {
                  chunk_id} to {datanode_address}")


def start_replication(data_id, file_stream, number_of_chunks, chunk_size, extra_bytes, active_nodes):
    # arguments data_id(str), file_stream(file-like object), number_of_chunks(int), chunk_size(int), extra_bytes(int), active_nodes(list)

    for i in range(number_of_chunks):
        chunk_id = i + 1

        # Move the stream pointer to the start of the chunk
        file_stream.seek(i * chunk_size)

        # Read the chunk data from the stream
        chunk_data = file_stream.read(
            chunk_size + (1 if i < extra_bytes else 0))

        # Start a new thread for replication of the current chunk
        replication_thread = threading.Thread(
            target=replicate_chunk,
            args=(data_id, chunk_id, chunk_data, active_nodes)
        )
        replication_thread.start()


# Function to split the file and distribute chunks
def split_file(file_stream, number_of_chunks, data_id):
    # arguments file_stream(file like object), number_of_chunks(int), data_id(str)

    # Calculate file size and determine chunk size
    file_stream.seek(0, os.SEEK_END)
    file_size = file_stream.tell()
    file_stream.seek(0)
    chunk_size = file_size // number_of_chunks
    extra_bytes = file_size % number_of_chunks

    # Get addresses of active DataNodes
    active_nodes = list(
        filter(lambda node: active_datanodes[node], active_datanodes.keys()))

    # Split the file into chunks and distribute them among DataNodes using Round-Robin
    for i in range(number_of_chunks):
        chunk_data = file_stream.read(
            chunk_size + (1 if i < extra_bytes else 0))
        node_index = i % len(active_nodes)
        datanode_address = active_nodes[node_index]
        datanode_upload_endpoint = f"{datanode_address}/write_file/"

        files = {'file': (f'chunk_{i+1}.bin', io.BytesIO(chunk_data))}
        response = requests.post(datanode_upload_endpoint, files=files, data={
                                 'data_id': data_id, 'chunk_id': i+1})

        if response.status_code == 200:
            # Update chunks_location with DataNode information
            with chunks_location_lock:
                chunks_location.setdefault(data_id, {}).setdefault(
                    i+1, []).append(datanode_address)
        else:
            print(f"Failed to upload chunk {i+1} to {datanode_address}")

    # Once all chunks have been sent, start the replication process
    file_stream.seek(0)
    start_replication(data_id, file_stream, number_of_chunks,
                      chunk_size, extra_bytes, active_nodes)


def check_datanodes_health():
    while True:
        # Iterate through each DataNode address
        for address in datanode_addresses:
            try:
                # Attempt to fetch the DataNode's health status
                response = requests.get(f"{address}/is_active")

                # Check if the DataNode is active based on the response
                is_active = response.status_code == 200 and response.json().get(
                    'status') != "DataNode is not active"

                # Update the status of the DataNode in the active_datanodes dictionary
                active_datanodes[address] = is_active

                # Update the status of the DataNode in the database collection
                active_datanodes_collection.update_one(
                    {'address': address},
                    {'$set': {'status': 'Active' if is_active else 'Inactive'}},
                    upsert=True
                )

            except requests.exceptions.RequestException:

                # If there's an exception, mark the DataNode as inactive
                active_datanodes[address] = False

                # Update the status of the DataNode in the database as 'Inactive'
                active_datanodes_collection.update_one(
                    {'address': address},
                    {'$set': {'status': 'Inactive'}},
                    upsert=True
                )
        # Wait for 1 second before checking the health of DataNodes again
        time.sleep(1)


@app.route('/create_directory', methods=['POST'])
def create_directory():

    # Extract the directory path from the POST request
    directory_path = request.form.get('directory_path')

    # Ensure the directory path is valid (starts with '/')
    if not directory_path or not directory_path.startswith("/"):
        return jsonify({'error': 'Invalid directory path'}), 400

    # Check if the directory already exists
    existing_directory = directories_collection.find_one(
        {'path': directory_path})

    if existing_directory:
        # If the directory already exists, return an error response
        return jsonify({'error': f"Directory '{directory_path}' already exists"}), 400

    else:
        # Create the directory in the database
        directory_data = {
            'path': directory_path,
            'content': []  # Initialize an empty list for storing folder content
        }
        directories_collection.insert_one(directory_data)

        # Extract the folder name from the directory_path
        folder_name = directory_path.rsplit('/', 1)[-1]
        print(folder_name)

        # Update the content field of the parent directory
        parent_path = '/'.join(directory_path.split('/')[:-1])
        if parent_path:
            # Add the newly created folder to the content list of the parent directory
            directories_collection.update_one(
                {'path': parent_path},
                {'$addToSet': {'content': {'folder_name': folder_name}}}
            )
        # Return a success message upon successful directory creation
        return jsonify({"message": f"Directory '{directory_path}' created successfully"}), 200


@app.route('/get_directory', methods=['GET'])
def get_directory():
    # Retrieve directory information from MongoDB
    directories_metadata = list(directories_collection.find({}, {'_id': 0}))
    return jsonify({'directories': directories_metadata})


@app.route('/list_directory', methods=['GET'])
def list_directory():
    # Get directory path from the request
    # In file systems, "/" refers to the root directory
    directory_path = request.form.get('directory_path', '/')

    # If the directory path doesn't start with '/', add the root directory
    if not directory_path.startswith('/'):
        directory_path = '/' + directory_path

    # Check if the provided directory path exists
    directory = directories_collection.find_one({'path': directory_path})

    if not directory:
        return jsonify({'error': 'Invalid directory path'}), 400

    # Fetch the content of the directory
    content = directory.get('content')

    # Extract file names and folder names from the content
    files = [item['file_name'] for item in content if 'file_name' in item]
    folders = [item['folder_name']
               for item in content if 'folder_name' in item]

    return jsonify({'files': files, 'folders': folders})


# Endpoint to display the status of DataNodes on request
@app.route('/datanode_status', methods=['GET'])
def datanode_status():
    status_data = {}  # Dictionary to hold datanode status data

    # Loop through each datanode and retrieve its status
    for datanode_addresses, is_active in active_datanodes.items():
        # Determine if the datanode is active or inactive
        status_data[datanode_addresses] = 'Active' if is_active else 'Inactive'

        failedNode_handled = failedNode_handled_collection.find(
            {'address': datanode_addresses}, {'_id': 0, 'address': 1})
        failedNode_handled = [doc['address'] for doc in failedNode_handled]

        # Check if the current datanode is active and was previously marked as failed and handled
        if is_active and datanode_addresses in failedNode_handled:
            failedNode_handled_collection.delete_many(
                {'address': datanode_addresses})

    return jsonify(status_data)


@app.route('/re_replicate', methods=['POST'])
def re_replicate():

    file_name = request.form.get('file_name')
    directory_path = request.form.get('directory_path', '/')

    # Validate and retrieve file_id based on file_name and directory_path
    file_metadata = files_collection.find_one(
        {'name': file_name, 'directory_path': directory_path}, {'_id': 0, 'id': 1})

    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    data_id = file_metadata['id']
    failed_nodes = active_datanodes_collection.find({'status': 'Inactive'})
    failed_nodes = [doc['address'] for doc in failed_nodes]

    if len(failed_nodes) != 0:
        for address in failed_nodes:
            failedNode_handled = failedNode_handled_collection.find(
                {'address': address}, {'_id': 0, 'address': 1})
            failedNode_handled = [doc['address'] for doc in failedNode_handled]

            if len(failedNode_handled) == 0 or address not in failedNode_handled:
                failedNode_handled_collection.insert_one({'address': address})

                # DataNode is not active, initiate re-replication for chunks stored on the failed DataNode
                failed_datanode_chunks_org = list(chunks_collection.find(
                    {'datanode_address': address, 'file_id': data_id}, {'_id': 0, 'chunk_id': 1}))
                failed_datanode_chunks_rep = list(replication_collection.find(
                    {'datanode_address': address, 'data_id': data_id, 'status': 'success'}, {'_id': 0, 'chunk_id': 1}))
                failed_datanode_chunks = failed_datanode_chunks_org + failed_datanode_chunks_rep
                active_dn = active_datanodes_collection.find(
                    {'status': 'Active'})
                active_dn = [doc['address'] for doc in active_dn]

                for chunk_info in failed_datanode_chunks:
                    chunk_id = chunk_info['chunk_id']

                    # Trigger re-replication for the chunk
                    for index in range(len(active_dn)):
                        chunk_present = chunks_collection.find_one(
                            {'file_id': data_id, 'chunk_id': chunk_id, 'datanode_address': active_dn[index]})
                        if chunk_present is None:

                            # Select the next available active DataNode
                            node_index = (index+1) % len(active_dn)
                            datanode_address_fetch = active_dn[node_index]
                            datanode_upload_endpoint = f"{
                                active_dn[index]}/write_file/"

                            # Fetch the chunk data from another DataNode
                            chunk_data = fetch_chunk_from_datanode(
                                datanode_address_fetch, data_id, chunk_id)

                            # If the chunk data is successfully fetched, replicate it to the selected DataNode
                            if chunk_data is not None:

                                chunk_file = {
                                    'file': (f'chunk_{chunk_id}.bin', chunk_data)}
                                response = requests.post(datanode_upload_endpoint, files=chunk_file, data={
                                    'data_id': data_id, 'chunk_id': chunk_id})

                                replication_entry = {
                                    'data_id': data_id,
                                    'chunk_id': chunk_id,
                                    'datanode_address': active_dn[index],
                                    'status': 'success' if response.status_code == 200 else 'failure'
                                }

                                # Store replication details in MongoDB
                                replication_collection.insert_one(
                                    replication_entry)

                                if response.status_code == 200:
                                    print(f"Successful in replicating chunk {chunk_id} to {
                                          active_dn[index]} from {datanode_address_fetch}")
                                    break
                                else:
                                    print(f"Failed to fetch chunk {
                                          chunk_id} from {datanode_address_fetch}")

            else:
                print(f"Failure Handling done for DataNode {address}")
                break

    else:
        return jsonify({"message": "All DataNodes Active, No Re-Replication Required"}), 200

    return jsonify({"message": "Re-replication successful"}), 200


@app.route('/delete_file', methods=['POST'])
def delete_file():

    file_name = request.form.get('file_name')
    directory_path = request.form.get('directory_path', '/')

    # Validate and retrieve file_id based on file_name and directory_path
    file_metadata = files_collection.find_one(
        {'name': file_name, 'directory_path': directory_path}, {'_id': 0, 'id': 1})

    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    data_id = file_metadata['id']

    # Retrieve chunks' metadata from MongoDB
    chunks_metadata = list(chunks_collection.find(
        {'file_id': data_id}, {'_id': 0}))

    # Check if data_id is provided
    if not data_id:
        return jsonify({'error': 'Invalid data_id'}), 400

    # Fetch file metadata from MongoDB
    file_metadata = files_collection.find_one({'id': data_id}, {'_id': 0})

    # Check if the file exists
    if not file_metadata:
        return jsonify({'error': 'File not found'}), 404

    # Delete file metadata from MongoDB
    files_collection.delete_one({'id': data_id})

    # Delete chunk locations from MongoDB
    chunks_collection.delete_many({'file_id': data_id})

    # Delete replication chunks from MongoDB
    replication_collection.delete_many({'data_id': data_id})

    # Remove file from directories collection
    directory_path = file_metadata['directory_path']
    directories_collection.update_one(
        {'path': directory_path},
        {'$pull': {'content': {'file_name': file_metadata['name']}}}
    )

    # Delete chunks from DataNodes
    for datanode_address in datanode_addresses:

        datanode_delete_endpoint = f"{
            datanode_address}/delete_chunks/{data_id}"

        try:
            response = requests.post(datanode_delete_endpoint)

            if response.status_code != 200:
                print(f"Failed to delete from {datanode_address}")
        except requests.exceptions.RequestException:
            print(f"Failed to connect to {datanode_address}")

    return jsonify({"message": f"File '{file_name}' deleted successfully"}), 200


@app.route('/delete_folder', methods=['POST'])
def delete_folder():
    folder_name = request.form.get('folder_name')
    directory_path = request.form.get('directory_path', '/')

    # Validate input parameters
    if not folder_name or not directory_path:
        return jsonify({'error': 'Invalid input parameters'}), 400

    # Ensure that both paths start with '/'
    if not directory_path.startswith('/'):
        directory_path = '/' + directory_path

    # Check if the folder exists at the specified path
    folder_path = directory_path + '/' + folder_name
    folder_metadata = directories_collection.find_one(
        {'path': folder_path}, {'_id': 0})

    if not folder_metadata:
        return jsonify({'error': 'Folder not found'}), 404

    else:
        directories_collection.update_one(
            {'path': directory_path},
            {'$pull': {'content': {'folder_name': folder_name}}}
        )
        deletefolder_path.append(folder_path)

    # Recursively delete the folder and its contents
    delete_folder_recursive(folder_path)

    for path in deletefolder_path:
        directories_collection.delete_one({'path': path})

    return jsonify({"message": f"Folder '{folder_name}' deleted successfully"}), 200


def delete_folder_recursive(folder_path):
    # Retrieve folder information from MongoDB
    folder_metadata = directories_collection.find_one(
        {'path': folder_path}, {'_id': 0, 'content': 1})

    if folder_metadata and 'content' in folder_metadata:
        # Delete contents of the folder (files and subfolders)
        for item in folder_metadata['content']:
            if 'file_name' in item:
                # Delete file from MongoDB and DataNodes
                file_name = item['file_name']
                delete_file_from_datanodes(file_name, folder_path)

            elif 'folder_name' in item:
                # Recursively delete subfolders
                subfolder_name = item['folder_name']
                subfolder_path = folder_path + '/' + subfolder_name
                directories_collection.update_one(
                    {'path': folder_path},
                    {'$pull': {'content': {'folder_name': subfolder_name}}}
                )
                deletefolder_path.append(subfolder_path)
                delete_folder_recursive(subfolder_path)

    else:
        return


def delete_file_from_datanodes(file_name, folder_path):
    # arguments are of type str
    # Retrieve file metadata from MongoDB
    file_metadata = files_collection.find_one(
        {'name': file_name, 'directory_path': folder_path}, {'_id': 0, 'id': 1})

    if file_metadata:
        data_id = file_metadata['id']

        # Delete file metadata from MongoDB
        files_collection.delete_one({'id': data_id})

        # Delete chunk locations from MongoDB
        chunks_collection.delete_many({'file_id': data_id})

        # Delete replication chunks from MongoDB
        replication_collection.delete_many({'data_id': data_id})

        # Remove file from directories collection
        directory_path = folder_path

        directories_collection.update_one(
            {'path': directory_path},
            {'$pull': {'content': {'file_name': file_name}}}
        )

        # Delete chunks from DataNodes
        for datanode_address in datanode_addresses:

            # Create the endpoint for deleting chunks associated with the file
            datanode_delete_endpoint = f"{
                datanode_address}/delete_chunks/{data_id}"

            try:
                # Send a POST request to the DataNode's delete_chunks endpoint
                response = requests.post(datanode_delete_endpoint)

                if response.status_code != 200:
                    print(f"Failed to delete from {datanode_address}")
            except requests.exceptions.RequestException:
                print(f"Failed to connect to {datanode_address}")
    else:
        # File not found
        print(f"File not found: {file_name}")


@app.route('/copy_file', methods=['POST'])
def copy_file():
    original_path = request.form.get('original_path')
    destination_path = request.form.get('destination_path')
    file_name = request.form.get('file_name')

    # Validate input parameters
    if not original_path or not destination_path or not file_name:
        return jsonify({'error': 'Invalid input parameters'}), 400

    # Ensure that both paths start with '/'
    if not original_path.startswith('/') or not destination_path.startswith('/'):
        return jsonify({'error': 'Paths must start with \'/\''}), 400

    # Check if the file or folder exists at the original path
    file_metadata = files_collection.find_one(
        {'name': file_name, 'directory_path': original_path}, {'_id': 0, 'id': 1})

    if not file_metadata:
        return jsonify({'error': 'File not found at the original path'}), 404

    data_id = file_metadata['id']

    # Update the directory_path in the files_collection
    number_of_chunks = files_collection.find_one(
        {'id': data_id}, {'_id': 0, 'number_of_chunks': 1})
    file_data = {
        'id': data_id,
        'name': file_name,
        'number_of_chunks': number_of_chunks['number_of_chunks'],
        'replication_factor': replication_factor,
        'directory_path': destination_path,
        'upload_time': datetime.datetime.now()
    }
    files_collection.insert_one(file_data)

    # Update the directory information in MongoDB
    directories_collection.update_one(
        {'path': destination_path},
        {'$addToSet': {'content': {'file_name': file_name}}}
    )

    return jsonify({"message": f"File '{file_name}' copied successfully from {original_path} to {destination_path}"}), 200


if __name__ == '__main__':
    health_thread = threading.Thread(target=check_datanodes_health)
    # This ensures the thread exits when the main process does
    health_thread.daemon = True
    health_thread.start()

    app.run(port=5003, debug=True)
