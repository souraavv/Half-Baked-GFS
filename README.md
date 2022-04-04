# Half-baked-GFS

# Instructions to execute different modules:

## Master: 
python master.py <master_url> <master_port>

## Chunkserver: 
python chunkserver.py <chunkserver_url> <chunkserver_port> <master_url> <master_port> <storage_dir>

## Client: 
python client.py <client_url> <client_port> <master_url> <master_port>

## Test script for client
python test_client.py <client_url> <client_port>