# RR-Team-19-Yet-Another-Distributed-File-System-YADFS

Repository for RR-Team 19-Yet Another Distributed File System (YADFS)

1. Clone the Repository
2. Use postman/ curl / Our CLI on command line to send http requests.
3. Test features.

Features:

1. Upload file
2. Download file
3. NameNode/DataNode health status check with heartbeats
4. Metadata persistence is ensured using mongoDB.
5. list_directories : lists all the present directories in the DFS.
6. create_directory : u can create a dir.
7. chunks and replication_chunks are collections: which hold the metadata related to chunk storage. it stoes all of them in a linear fashion. one chunk after the other regardless of the file.
   -future improvement: tree like database storage sturcture for faster retrieval of chunk metaData.
8. re_replicate : is a manual way to re-replicate chunks; in case there is under-replication; and the get_file enpoint fails
