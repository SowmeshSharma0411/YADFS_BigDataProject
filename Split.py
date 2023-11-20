import os
class Split:
    def split_file(file_path, number_of_chunks, output_folder):
        """
        Splits a file into a specified number of chunks and stores them in an output folder.

        :param file_path: The path of the file to split
        :param number_of_chunks: The number of chunks to split the file into
        :param output_folder: The folder where the chunks will be stored
        """
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        file_size = os.path.getsize(file_path)
        chunk_size = file_size // number_of_chunks
        extra_bytes = file_size % number_of_chunks

        with open(file_path, 'rb') as file:
            for i in range(number_of_chunks):
                chunk_file_path = os.path.join(output_folder, f'chunk_{i+1}.bin')
                with open(chunk_file_path, 'wb') as chunk_file:
                    chunk_data = file.read(chunk_size + (1 if i < extra_bytes else 0))
                    chunk_file.write(chunk_data)

    def join_file(chunks_folder, output_file_path):
        chunks = [os.path.join(chunks_folder, f) for f in os.listdir(chunks_folder) if f.startswith('chunk_')]
        chunks.sort()

        # Join the chunks into a single file
        with open(output_file_path, 'wb') as output_file:
            for chunk_file_path in chunks:
                with open(chunk_file_path, 'rb') as chunk_file:
                    output_file.write(chunk_file.read())













# We will create a dummy file to demonstrate splitting and rejoining.
dummy_file_path = 'data/dummy_file.txt'
output_folder = 'data/chunks'
reassembled_file_path = 'data/reassembled_dummy_file.txt'

# Create a dummy file with some text
with open(dummy_file_path, 'w') as f:
    for i in range(10000):

        f.write(f"This is a test file. {i} ")  # Write the string 100 times to make the file large enough to split.

# Now we'll call the function to split the file into 5 chunks.
split_file(dummy_file_path, 5, output_folder)

# After that, we'll call the function to rejoin the file.
join_file(output_folder, reassembled_file_path)

# We can check if the contents of the original and reassembled files are the same to confirm the success of the process.
with open(dummy_file_path, 'r') as original, open(reassembled_file_path, 'r') as reassembled:
    original_content = original.read()
    reassembled_content = reassembled.read()

# Return the paths of the split and reassembled files along with a check for content integrity.
print((original_content == reassembled_content, dummy_file_path, output_folder, reassembled_file_path))
print(original_content==reassembled_content)