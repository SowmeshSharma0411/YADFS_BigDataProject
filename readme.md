# YaDFS: Yet Another Distributed File System

YaDFS (Yet another Distributed File System) is inspired by the Google File System (GFS), a technology developed by Google to handle vast amounts of data with efficiency and reliability. GFS was designed to support Google’s intensive data processing needs, such as web crawling and indexing the web, by distributing storage across many machines while ensuring fault tolerance and high availability.

<h2><b>GFS</b></h2>

<ins>Key Features of the GFS</ins>
* GFS is a file system that is distributed in nature, and tailored for handling large files and batch-processing workloads.
* Multiple machines store copies of every file and multiple machines try to read/write the same file.
* Its architecture consists of a single master server and multiple chunkservers, where data is stored in 64MB chunks.
* The master server manages metadata, including file and chunk namespaces, chunk locations, and replica information.
* To maintain high availability, GFS replicates data across multiple chunkservers and server racks, ensuring that data remains accessible even if individual machines or disks fail.
* The system is optimized for reading (specifically large streaming reads) or appending because web crawling and indexing heavily rely on these operations.


<ins>GFS Architecture</ins>

<img width="825" alt="image" src="https://github.com/user-attachments/assets/f0a0baff-2ef1-4b4e-a647-804d8380a445">

<ins>Challenges and Evolution</ins>

Despite its robust design, GFS faced scalability challenges as Google’s data needs grew. The single master server became a bottleneck, and the system struggled with memory limitations and increased latency for user-facing applications. These limitations led to the development of successor technologies like Colossus, which introduced distributed metadata management using BigTable, enhancing scalability and performance.

<h2><b>YaDFS: Building on GFS Principles</b></h2>

YaDFS leverages the foundational principles of GFS. Developed using Python, Flask, MongoDB, and Docker, YaDFS facilitates efficient file operations, health monitoring, and metadata persistence. Docker containers ensure portability and scalability, making it easier to manage distributed files across various environments.

<ins>YaDFS Features</ins>

1. Upload file (upload_file)
2. Download file (get_file)
3. Both upload_file and get_file are coordinated by the NameNode in YaDFS (in Hadoop/GFS, NameNode handles only metadata operations)
4. Multithreaded NameNode with the capability of monitoring the health status of the DataNodes/chunkservers with a heartbeat mechanism.
5. Metadata persistence ensured using MongoDB.
6. File system commands are supported: list_directories (ls), create_directory (mkdir), get_directory, delete_file, delete_folder, move_file, move_folder, copy_file.
7. A custom CLI is developed using Python. This is the client-side interface to send instructions like create_directory, upload_file, and get_file to the NameNode.
8. get_info: Gives info on the distributed chunk organization.
9. datanode_status: Gives the status of all the DataNodes present in the system.
10. Replication of chunks is made to ensure High Availability of chunks and faster, parallel chunk reads. During the FileWrite process: replication of each chunk and its distribution is done on a completely different thread. (Replication Factor (rf) = 3).
11. chunks and replication_chunks are collections: that hold the metadata related to chunk storage. it stores all of them in a linear fashion. one chunk after the other regardless of the file.
   -future improvement: tree like database storage sturcture for faster retrieval of chunk metaData.
12. re_replicate : is a manual way to re-replicate chunks; in case there is under-replication, especially when multiple DataNodes fail; and the get_file endpoint fails
13. delete_folder has a recursive deletion capability: deleting all files within it, the file metadata and file chunks and replicated chunks located in different DataNodes.
14. All this has been dockerized. A custom number of DataNodes can be churned up just by adding another service in docker-compose.
15. Variable chunk-size: determined based on the "Number of chunks" parameter requested by the user.
16. M chunks are mapped onto N DataNodes using a simple Round Robin Algorithm

<ins>YaDFS Architecture</ins>

![YaDFS](https://github.com/user-attachments/assets/37e245ed-a363-42b3-968e-27259428b9f6)

<ins>ScreenShots</ins>

<table>
  <tr>
    <td><img src="https://github.com/user-attachments/assets/6a71227f-0f51-4ae5-afd5-fa3729fa41b3" alt="Screenshot 1"><br>NameNode and DataNodes as Docker Containers</td>
    <td><img src="https://github.com/user-attachments/assets/42154183-334a-480d-971e-c2a77d47e41d" alt="Screenshot 2"><br>DataNode Health Check by NN Thread</td>
  </tr>
   <tr>
    <td><img src="https://github.com/user-attachments/assets/c424b4ff-e5ed-4e44-8482-fbca19cc4bc7" alt="Screenshot 3"><br>mkdir and Other File System Ops</td>
    <td><img src="https://github.com/user-attachments/assets/9d57aeba-8734-43e2-bfbe-3fb8a263c878" alt="Screenshot 4"><br>mongosh logs</td>
  </tr>
   <tr>
    <td><img src="https://github.com/user-attachments/assets/12e0238b-2da5-4534-b8cc-68ac54235351" alt="Screenshot 5"><br>Another dir created</td>
    <td><img src="https://github.com/user-attachments/assets/b1eb09e7-72b7-44f3-8ba9-9a3ce5281660" alt="Screenshot 6"><br>DataNode status when all nodes are alive Vs when some are dead</td>
  </tr>
   <tr>
    <td><img src="https://github.com/user-attachments/assets/b2a56987-2ad3-4db2-9982-deedbc459b30" alt="Screenshot 7"><br>upload_file</td>
    <td><img src="https://github.com/user-attachments/assets/b11b53ca-3676-4bf6-a1f8-22a464f7333e" alt="Screenshot 8"><br>get_info after upload_file</td>
  </tr>
   <tr>
    <td><img src="https://github.com/user-attachments/assets/4c3097d6-cfbb-4538-b533-6529a3e25af4" alt="Screenshot 9"><br>get_file</td>
    <td><img src="https://github.com/user-attachments/assets/9286a5b0-1ca6-41a1-97fd-9b1e6236241b" alt="Screenshot 10"><br></td>
  </tr>
    <tr>
    <td><img src="https://github.com/user-attachments/assets/579f556e-893b-4246-b567-9cb37b423296" alt="Screenshot 10"><br>New file uploaded</td>
    <td><img src="https://github.com/user-attachments/assets/9ff83482-ba16-4577-b45d-4f746ebe97ee" alt="Screenshot 11"><br>Recursive File and Chunk Deletion</td>
  </tr>
   <tr>
    <td><img src="https://github.com/user-attachments/assets/ffcd576a-ec14-42b1-b03d-be5ac3fa59fb" alt="Screenshot 12"><br>2 DataNodes are down</td>
    <td><img src="https://github.com/user-attachments/assets/ce672e54-f697-4cfb-b38c-e73ff2d73b92" alt="Screenshot 13"><br>Fault Tolerant Download despite 2 DNs being down</td>
  </tr>
   <tr>
    <td><img src="https://github.com/user-attachments/assets/2299707b-b104-4d99-a192-af2044950345" alt="Screenshot 14"><br>dummy2.txt uploaded to /Sowmesh_BigData</td>
    <td><img src="https://github.com/user-attachments/assets/3b53935f-164a-49af-82a9-0828cff4c10f" alt="Screenshot 15"><br>Recursive Folder Deletion: Including deleting all files within it and all their chunks from all DNs</td>
  </tr>
</table>
