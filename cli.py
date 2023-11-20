import json
from prompt_toolkit import prompt
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
import requests


commands = ["upload_file", "get_file", "get_info","exit"]
command_completer = WordCompleter(commands)

# NameNode API endpoint
namenode_url = "http://localhost:5003"

def handle_upload(file_path, chunks):
    files = {'file': open(file_path, 'rb')} # binary form for reading
    response = requests.post(f"{namenode_url}/upload_file", files=files, data={'number_of_chunks': chunks})
    indented_json = json.dumps(response.json(), indent = 2)
    print(indented_json)

def handle_get_file(file_id):
    response = requests.post(f"{namenode_url}/get_file", data={'file_id': file_id})
    if response.status_code == 200:
        with open(f"downloaded_file_{file_id}.bin", 'wb') as file:
            file.write(response.content)
        print(f"File retrieved successfully as: downloaded_file_{file_id}.bin")
        with open(f"downloaded_file_{file_id}.bin","rb") as file:
            file_content = file.read()
            print(f"File content: \n{file_content.decode('utf-8')}")
    else:
        print(f"Failed to retrieve file. Error: {response.text}")

def handle_get_info():
    response = requests.get(f"{namenode_url}/get_info")
    indented_json = json.dumps(response.json(), indent = 2)
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
            handle_upload(file_path, chunks)
        elif user_input.lower() == 'get_info':
            handle_get_info()
        elif user_input.lower() == 'get_file':
            file_id = input("Enter the file ID for the file you want to download: ")
            handle_get_file(file_id)
        else:
            print(f"You entered: {user_input}")

if __name__ == "__main__":
    main()
