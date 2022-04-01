import sys
import random
import rpyc
from rpyc.utils.server import ThreadedServer
import time
import threading
import copy

# class for storing file's metadata
class FileMeta:
    def __init__(self, chunks):
        self.chunks = chunks # list of  chunk_ids = [chunk_id1, ..., chunk_idn ... ]
        self.deleted_time = 0 # set when file name set to TRASHFILE
        
# class for storing chunk's metadata
class ChunkMeta:
    def __init__(self, file_name, primary = [None, 0], replicas = [], version = 0):
        self.file_name = file_name
        self.primary = primary #(primary_url, lease_expiry_time)
        self.replicas = replicas
        self.version = version

class ChunkserverMeta:
    def __init__(self, chunk_list, heartbeat_time):
        self.chunk_list = chunk_list # list of chunk_ids = [chunk_id1, ..., chunk_idn ... ]
        self.heartbeat_time = heartbeat_time # time when last heartbeat is received
        
class MasterService(rpyc.Service):
    def __init__(self):
        self.files_metadata = {} # file_name -> FileMeta
        self.chunks_metadata = {} # chunk_id -> ChunkMeta
        self.chunkserver_url_to_meta = {} # chunkserver_url -> ChunkserverMeta
        self.lease_expiration_timeout = 60
        self.delete_timeout = 40
        self.heartbeat_interval = 30
        self.rereplicate_chunks_interval = 50
        self.latest_chunk_id = 1000 
        self.start_background_threads()
        self.replication_factor = 3
        random.seed(0)
        
    # initialize the master data structures
    def on_connect(self, conn):
        # print("Connected")
        pass

    def start_background_threads(self):
        garbage_collect_thread = threading.Thread(target=self.garbage_collection, args=(), daemon=True)
        check_heartbeat_thread = threading.Thread(target=self.check_heartbeats, args=(), daemon=True)
        rereplicate_chunks_thread = threading.Thread(target=self.rereplicate_chunks, args=(), daemon=True)
        garbage_collect_thread.start()
        check_heartbeat_thread.start()
        rereplicate_chunks_thread.start()

    def check_heartbeats(self):
        while True:
            chunkservers_dict = copy.deepcopy(self.chunkserver_url_to_meta)
            for url, chunkserver_meta in chunkservers_dict.items():
                last_heartbeat_time = chunkserver_meta.heartbeat_time
                heartbeat_expiration = last_heartbeat_time + self.heartbeat_interval + 10
                if time.time() > heartbeat_expiration:
                    print(f'heartbeat expired for chunkserver {url}')
                    self.exposed_remove_chunkserver(url)
            time.sleep(self.heartbeat_interval + 10)
    
    def exposed_remove_chunkserver(self, url):
        print(f'remove chunkservers called for {url}')
        if url not in self.chunkserver_url_to_meta:
            return "Chunkserver already removed"
        del self.chunkserver_url_to_meta[url]
        for chunk_id, chunk_meta in self.chunks_metadata.items():
            # if this chunkserver is primary for some chunk then set its lease_expiry_time to 0
            if url == chunk_meta.primary[0]:
                # lease_expiry_time = 0, which will force re-election of new primary
                chunk_meta.primary[1] = 0
            # if this url was present as replicas of some chunks
            if url in chunk_meta.replicas:
                chunk_meta.replicas.remove(url)
                # If this was the only replica of the chunk, remove its chunks_metadata information of chunk_id 
                # if len(chunk_meta.replicas) == 0:
                #     del self.chunks_metadata[chunk_id]

    # Background thread - checking for the (number of replicas == replication factor) for each chunk
    def rereplicate_chunks(self):
        while True:
            for chunk_id, chunk_meta in self.chunks_metadata.items():
                replicas = chunk_meta.replicas # These replicas has the chunks
                # print(f'{chunk_id} has {len(replicas)} replicas')
                replicas_required = self.replication_factor - len(replicas)
                if replicas_required > 0:
                    print(f'less replica count {len(replicas)}, for chunk id {chunk_id} ')
                    all_urls = self.chunkserver_url_to_meta.keys()
                    urls_without_replicas = [url for url in all_urls if url not in replicas]
                    # if no other chunkservers available for further replication
                    if len(urls_without_replicas) != 0:
                        print(f'urls without replicas {len(urls_without_replicas)}, for chunk id {chunk_id} ')
                        replicas_required = min(len(urls_without_replicas), replicas_required)
                        random_urls = random.sample(urls_without_replicas, replicas_required)
                        for url in random_urls:
                            res = rpyc.connect(*url).root.replicate_chunk(chunk_id, chunk_meta.version, replicas)
                            if res != "success":
                                print(f"no replica has correct data for chunk id {chunk_id}" )
                            else:
                                # updating master data structures, after successful replication
                                self.chunks_metadata[chunk_id].replicas.append(url)
                                self.chunkserver_url_to_meta[url].chunk_list.append(chunk_id)
                    else:
                        print('no new chunkservers found for re-replication')
            time.sleep(self.rereplicate_chunks_interval)
    
    # def replicate_single_chunk(self):
    #     # TODO make replicate chunk function in chunkserver, this function will call it
    #     pass

    def exposed_delete(self, file_name):
        # if file which is requested to be deleted is not present on master, might be calling delete after delete
        # this call will be idempotent
        print(f'delete request for file {file_name} received')
        if file_name not in self.files_metadata:
            return "file not found"
        trash_file_name = 'TRASHFILE_' + file_name
        self.files_metadata[trash_file_name] = copy.deepcopy(self.files_metadata[file_name])
        self.files_metadata[trash_file_name].deleted_time = time.time()
        del self.files_metadata[file_name]
        return "success"

    def exposed_restore(self, file_name):
        print(f'trying to restore {file_name}')
        trash_file_name = 'TRASHFILE_' + file_name
        if trash_file_name not in self.files_metadata:
            return "file removed from trash"
        self.files_metadata[file_name] = copy.deepcopy(self.files_metadata[trash_file_name])
        self.files_metadata[file_name].deleted_time = 0
        del self.files_metadata[trash_file_name]
        return "successfully restored"
        
    def garbage_collection(self):
        # If prefix with TRASHFILE_Originalfilename the we need to remove it from the master metadata.
        # Need to remove from file_metadata the FileMeta of that file. But before that take "all the chunks" related to that file
        # and remove them first from the ChunkMeta and then remove find which ChunkServer contain those chunks and also remove
        # that from the Chunkserver Meta data.
        while True:
            files_metadata = copy.deepcopy(self.files_metadata)
            for filename, file_meta in files_metadata.items():
                # Checking whether filename starts with TRASHFILE and deleted time has exceeded delete timeout
                if filename.startswith('TRASHFILE_') and (self.files_metadata[filename].deleted_time + self.delete_timeout) < time.time():
                    # Remove chunk_metadata corresponding to chunk_ids of the file
                    to_delete = {}
                    for chunk_id in file_meta.chunks:
                        if chunk_id in self.chunks_metadata:
                            # Add chunk_id to list associated with chunkserver url in to_delete dict
                            for replica_url in self.chunks_metadata[chunk_id].replicas:
                                if replica_url not in to_delete:
                                    to_delete[replica_url] = []
                                to_delete[replica_url].append(chunk_id)
                            del self.chunks_metadata[chunk_id]

                    # Remove chunk_ids from chunkserver_meta corresponding to the file
                    for replica_url, chunk_list in to_delete.items():
                        print('chunkserver chunk list', self.chunkserver_url_to_meta[replica_url].chunk_list)
                        print('local chunk list', chunk_list)
                        for chunk_id in chunk_list:
                            self.chunkserver_url_to_meta[replica_url].chunk_list.remove(chunk_id)

                    del self.files_metadata[filename]
            time.sleep(self.delete_timeout + 10)

    # check stale replicas in the chunkserver on coming out of dead state
    def exposed_sync_chunkserver(self, url, chunk_list):
        # chunk_list is a list of tuples containing chunk_id and version
        chunk_list = list(chunk_list)
        if url not in self.chunkserver_url_to_meta:
            self.chunkserver_url_to_meta[url] = ChunkserverMeta([], time.time())
        chunkserver_meta = self.chunkserver_url_to_meta[url]
        chunkserver_meta.heartbeat_time = time.time()
        stale_chunks = []

        for chunk_id, version in chunk_list:
            # case 1: if chunk_id not present in chunks_metadata
            if chunk_id not in self.chunks_metadata:
                stale_chunks.append(chunk_id)
                continue
            chunk_meta = self.chunks_metadata[chunk_id]
            # case 2: version with chunkserver is less than version with master i.e stale replica
            if version < chunk_meta.version:
                stale_chunks.append(chunk_id)
                continue
            # master assume that it crashed while assigning lease, and update its metadata from information received from chunkserver
            if version > chunk_meta.version:
                chunk_meta.version = version
            # if replicas not contain url in list of replicas then append url
            if url not in chunk_meta.replicas:
                chunk_meta.replicas.append(url)
            # if chunk_id not present in list of chunkserver metadata
            if chunk_id not in chunkserver_meta.chunk_list:
                chunkserver_meta.chunk_list.append(chunk_id)
        return stale_chunks

    # Send by the chunkserver
    def exposed_heartbeat(self, url, chunk_list):
        print(f"heartbeat received from {url} at {time.time()}")
        stale_replicas = []

        # print('chunk list heartbeat', chunk_list)
        for (chunk_id, version) in chunk_list:
            if chunk_id not in self.chunks_metadata:
                # in case if the chunk is deleted, we remove its metadata from master
                # otherwise, master will always have data about every chunk present in system
                stale_replicas.append(chunk_id)
                continue
            chunk_meta = self.chunks_metadata[chunk_id]

            # if the chunkserver is the primary of the chunk, extend its lease and also ping the chunkserver to tell it to update the lease
            if chunk_meta.primary[0] == url:
                updated_lease_time = time.time() + self.lease_expiration_timeout
                self.chunks_metadata[chunk_id].primary[1] = updated_lease_time
                rpyc.connect(*url).root.select_primary(chunk_id, updated_lease_time)

            if version < chunk_meta.version:
                # Stale replica
                stale_replicas.append(chunk_id)
            else:
                # Append url of chunkserver into list of replicas associated with chunk metadata
                if url not in chunk_meta.replicas and (len(chunk_meta.replicas) < self.replication_factor):
                    # Updating chunks_metadata and chunkserver_url
                    print(f'found new replica {url} with version no {version} adding to chunk id {chunk_id}')
                    self.chunks_metadata[chunk_id].replicas.append(url)
                    self.chunkserver_url_to_meta[url].chunk_list.append(chunk_id)
                
        self.chunkserver_url_to_meta[url].heartbeat_time = time.time()

        """
        # When master has mapping for the chunkserver url in list of replicas for a chunk_id, and chunkserver list doesn't have that chunk_id ??? -> Think of a case
        # If master has some chunks containing chunkserver url as replicas and chunk_list not contains them
        # Remove them from master chunk metadata and also remove if it's primary
        chunkserver_chunk_id_list = [chunk_id for chunk_id, _ in chunk_list]
        # List of chunk_ids which are only present on master and not on the chunkserver which has supplied with the chunk_list
        remaining_chunks = [chunk_id for chunk_id in self.chunk_metadata  if chunk_id not in chunkserver_chunk_id_list]
        
        for chunk_id in remaining_chunks:

            if url == self.chunks_metadata[chunk_id].primary[0]:
                self.chunks_metadata[chunk_id].primary[1] = 0

            if url in self.chunks_metadata[chunk_id].replicas:
                self.chunks_metadata[chunk_id].replicas.remove(url)
        """
        return stale_replicas

    """
    Used by client to create a file and first chunk, is file not exists
    And if file exists then this function will simply create a single chunk and add to the file
    """
    def exposed_create(self, file_name):
        # select three random chunkservers to create file on
        # make rpc calls to all the three random chunkservers to create the file on their own storage
        # create the FileMeta and ChunkMeta instances for the file and its first chunk
        print(f'creating new chunk for file {file_name}')
        chunkserver_urls = self.chunkserver_url_to_meta.keys() 
        new_chunk_id = self.latest_chunk_id + 1
        replicas = []

        # adding new chunk metadata in cache
        self.chunks_metadata[new_chunk_id] = ChunkMeta(file_name, [None, 0], replicas, 1)
        if file_name not in self.files_metadata:
            self.files_metadata[file_name] = FileMeta([])
        self.files_metadata[file_name].chunks.append(new_chunk_id)

        while len(replicas) < self.replication_factor:
            # Prevent it from selecting the same urls on next iteration
            remaining_urls = [url for url in chunkserver_urls if url not in replicas]
            
            if len(remaining_urls) == 0:
                return "failed to create chunk"

            random_urls = random.sample(remaining_urls, min(len(remaining_urls), self.replication_factor - len(replicas)))
            
            # If there are no chunkservers registered with master 
            # if len(random_urls) == 0:
            #     return "failed to create chunk"

            for url in random_urls:
                try:
                    rpyc.connect(*url).root.create(file_name, new_chunk_id) 
                    replicas.append(url)
                except Exception as e:
                    print(e)
                    # Remove chunkserver if its not responding to request
                    self.exposed_remove_chunkserver(url)
                    continue
            print(f'replicas created by master in create: {len(replicas)}')
        for replica in replicas:
            self.chunkserver_url_to_meta[replica].chunk_list.append(new_chunk_id)
        self.chunks_metadata[new_chunk_id].replicas = replicas
        self.latest_chunk_id = self.latest_chunk_id + 1
        return "success"

    def exposed_read(self, file_name, chunk_num):
        print(f'read requested for file {file_name}, chunk num {chunk_num}')
        if file_name not in self.files_metadata:
            # file_name is not present.
            return (-1, []) # Returns chunk_id = -1(means invalid) and replicas = []
        else:
            if len(self.files_metadata[file_name].chunks) < chunk_num:
                return "requested chunk num not found"

            # Return the chunk_id corresponding to the chunk_num in given filename and it's replica
            chunk_id = self.files_metadata[file_name].chunks[chunk_num]
            
            # get the repilca corresponding to the given chunk_id
            replicas = self.chunks_metadata[chunk_id].replicas
            return (chunk_id, replicas)

    def exposed_invalid_checksum(self, chunk_id, chunkserver):
        if self.chunks_metadata[chunk_id].primary[0] == chunkserver:
            self.chunks_metadata[chunk_id].primary = [None, 0]
        if chunkserver in self.chunks_metadata[chunk_id].replicas:
            self.chunks_metadata[chunk_id].replicas.remove(chunkserver)
        if chunk_id in self.chunkserver_url_to_meta[chunkserver].chunk_list:
            self.chunkserver_url_to_meta[chunkserver].chunk_list.remove(chunk_id)

    def exposed_get_primary(self, file_name, chunk_idx, force_primary = False):
        # get the chunk id corresponding to the chunk_idx of the file
        chunk_id = self.files_metadata[file_name].chunks[chunk_idx]

        # get the chunk metadata
        replicas = self.chunks_metadata[chunk_id].replicas
        primary_url = self.chunks_metadata[chunk_id].primary[0]
        select_new_primary = False
        
        # check if lease is expired for the current primary for the chunk
        if time.time() > self.chunks_metadata[chunk_id].primary[1]:
            # Select new primary if lease of current primary is expired
            select_new_primary = True
        
        # Elect new primary
        # if force_primary sent by client or lease is expired elect new primary or chunk has no current primary assigned
        if primary_url == None or force_primary or select_new_primary:
            # random shuffle the replicas list and try to make first reachable chunkserver as primary
            new_replicas = replicas
            random.shuffle(new_replicas)
            primary_elected = False
            for url in new_replicas:
                try:
                    # if we are able to connect to chunk server, tell it to increment the version number of the chunk
                    #------------------- not increment chunk version here, should be done once the primary is assigned
                    rpyc.connect(*url).root.increment_chunk_version(chunk_id, self.chunks_metadata[chunk_id].version + 1)
                    # if we are unable to increment the chunk version number we will remove that chunkserver from list of replicas
                    if not primary_elected:
                        primary_url = url
                        rpyc.connect(*primary_url).root.select_primary(chunk_id, time.time() + self.lease_expiration_timeout)
                        primary_elected = True
                except:
                    # if not able to connect then remove chunkserver from the master data structures
                    self.exposed_remove_chunkserver(url)
                    replicas.remove(url)
 
            # if replicas is empty, we are not able to restore the data
            if len(replicas) == 0:
                primary_url = None
                return "all chunkservers down"

            # update the chunk metadata corresponding to the chunk id
            self.chunks_metadata[chunk_id].primary = [primary_url, time.time() + self.lease_expiration_timeout]
            self.chunks_metadata[chunk_id].replicas = replicas
            self.chunks_metadata[chunk_id].version += 1
        return (chunk_id, primary_url, replicas)

if __name__ == "__main__":
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    t = ThreadedServer(MasterService(), hostname = hostname, port = port)
    t.start()