import json
from prompt_toolkit import prompt
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
import requests
import os


commands = ["upload_file", "get_file", "get_info","exit","create_directory","move_file","move_folder","re_replicate","delete_file","get_directory","list_directory","datanode_status"]
command_completer = WordCompleter(commands)

# NameNode API endpoint
namenode_url = "http://localhost:5003"

def handle_upload(file_path, chunks, directory_path):
    with open(file_path, 'rb') as file:
            # Continue with the file upload
            files = {'file': (os.path.basename(file.name), file)}
            response = requests.post(
                f"{namenode_url}/upload_file",
                files=files,
                data={'number_of_chunks': chunks, 'directory_path': directory_path})
    # response = requests.post(f"{namenode_url}/upload_file", files=files, data={'number_of_chunks': chunks})
    indented_json = json.dumps(response.json(), indent = 2)
    print(indented_json)

def handle_get_file(file_name, directory_path):
    response = requests.post(
        f"{namenode_url}/get_file",
        data={'file_name': file_name, 'directory_path': directory_path}
    )
    print(response.text)

def handle_get_info():
    response = requests.get(f"{namenode_url}/get_info")
    indented_json = json.dumps(response.json(), indent = 2)
    print(indented_json)

def handle_get_directory():
    response = requests.get(f"{namenode_url}/get_directory")
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def handle_datanode_status():
    response = requests.get(f"{namenode_url}/datanode_status")
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def handle_create_directory(directory_path):
    response = requests.post(f"{namenode_url}/create_directory", data={'directory_path':directory_path})
    print(response.text)

def handle_move_file(original_path, destination_path, file_name):
    response = requests.post(f"{namenode_url}/move_file", data={"original_path":original_path,"destination_path":destination_path,"file_name":file_name})
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def handle_move_folder(original_path, destination_path, folder_name):
    response = requests.post(f"{namenode_url}/move_folder", data={'original_path':original_path,'destination_path':destination_path, 'folder_name':folder_name})
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def handle_re_replicate(file_name, directory_path):
    response = requests.post(f"{namenode_url}/re_replicate", data={'file_name':file_name, "directory_path":directory_path})
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def handle_delete_file(file_name, directory_path):
    response = requests.post(f"{namenode_url}/delete_file", data={'file_name':file_name, 'directory_path':directory_path})
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def handle_list_directory(directory_path):
    response = requests.get(f"{namenode_url}/list_directory",data={'directory_path':directory_path})
    indented_json = json.dumps(response.json(), indent=2)
    print(indented_json)

def main():
    history = InMemoryHistory()

    while True:
        user_input = prompt(">>> ",
                            history=history,
                            auto_suggest=AutoSuggestFromHistory(),
                            completer=command_completer)

        if user_input.lower() == 'exit':
            break
        elif user_input.lower() == 'upload_file':
            file_path = input("Enter the path to the file to upload: ")
            chunks = input("Enter number of chunks: ")
            directory_path = input("Enter the directory path: ")
            handle_upload(file_path, chunks, directory_path)
        elif user_input.lower() == 'get_info':
            handle_get_info()
        elif user_input.lower() == 'get_file':
            file_name = input("Enter the file name to download: ")
            directory_path = input("Enter the directory path (optional, press Enter to use '/'): ")
            handle_get_file(file_name, directory_path)
        elif user_input.lower() == "create_directory":
            directory_path = input("Enter directory path: ")
            handle_create_directory(directory_path)
        elif user_input.lower() == "move_file":
            original_path = input("Enter the original path: ")
            destination_path = input("Enter the destination path: ")
            file_name = input("Enter the name of file: ")
            handle_move_file(original_path, destination_path, file_name)
        elif user_input.lower() == "move_folder":
            original_path = input("Enter the original path: ")
            destination_path = input("Enter the destination path: ")
            folder_name = input("Enter the name of folder: ")
            handle_move_file(original_path, destination_path, folder_name)
        elif user_input.lower() == "re_replicate":
            file_name = input("Enter name of file: ")
            directory_path = input("Enter name of directory_path: ")
            handle_re_replicate(file_name, directory_path)
        elif user_input.lower() == "delete_file":
            file_name = input("Enter name of file: ")
            directory_path = input("Enter directory path: ")
            handle_delete_file(file_name, directory_path)
        elif user_input.lower() == "get_directory":
            handle_get_directory()
        elif user_input.lower() == "list_directory":
            directory_path = input("Enter directory path: ")
            handle_list_directory(directory_path)
        elif user_input.lower() == "datanode_status":
            handle_datanode_status()
        else:
            print(f"You entered: {user_input}")

# ["upload_file", "get_file", "get_info","exit","create_directory","move_file","move_folder","re_replicate","delete_file","get_directory","list_directory","datanode_status"]

if __name__ == "__main__":
    main()
