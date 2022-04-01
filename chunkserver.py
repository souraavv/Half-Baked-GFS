import chunk
import sys
import time
import random
import hashlib as hash
import pickle
import rpyc
from rpyc.utils.server import ThreadedServer
import threading 
import os

class ChunkMeta:
    def __init__(self, file_name, version, lease_expiry_time = 0, checksum = None, offset = 0):
        self.file_name = file_name
        self.version = version
        self.lease_expiry_time = lease_expiry_time
        self.offset = offset
        self.checksum = checksum

class ChunkServerService(rpyc.Service):
    def __init__(self, hostname: str, port: int, master_hostname: str, master_port: int, storage_dir: str):
        # TODO What if master is alive but there is a network partition between master and chunkserver
        self.master_url = (master_hostname, master_port)
        self.chunks_metadata = {} # chunk_id -> ChunkMeta
        self.chunk_size = 64
        self.storage_dir = storage_dir
        self.backup_dir = f'{storage_dir}/dump/'
        self.heartbeat_interval = 30
        self.checksum_check_interval = 120
        self.url = (hostname, port)
        self.to_commit = {} # (chunk_id, client_url) => {Data in string}
        self.ack_commit = {} # (chunk_id, client_url) => {Data in string}
        self.init_from_storage() # Initialize from storage
        random.seed(0)

        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir, exist_ok=True)

        chunk_list = self.get_chunks_to_version()
        while True:
            try:
                stale_replicas = rpyc.connect(*self.master_url).root.sync_chunkserver(self.url, chunk_list) # stale_replicas = Chunk_ids
                break
            except Exception as e:
                print('failed to connect to master, retrying in 10 sec', e)
                time.sleep(10)
        if len(stale_replicas) > 0:
            print("stale replicas found, deleting")
            self.remove_chunks(stale_replicas)
        background_heartbeat_thread = threading.Thread(target=self.background_heartbeat_thread, args=(), daemon=True)
        background_heartbeat_thread.start()
        background_checksum_thread = threading.Thread(target=self.background_checksum_thread, args=(), daemon=True)
        background_checksum_thread.start()
        print("chunkserver initialised and connected with master")

    def get_chunks_to_version(self):
        return [ (chunk_id, self.chunks_metadata[chunk_id].version) for chunk_id in self.chunks_metadata ]

    def remove_chunks(self, stale_replicas):
        print('stale replicas', stale_replicas)
        for chunk_id in stale_replicas:
            file_name = self.chunks_metadata[chunk_id].file_name + '_chunk_id_' + str(chunk_id)
            file_path = self.storage_dir + '/' + file_name
            if os.path.exists(file_path):
                os.remove(file_path)
            del self.chunks_metadata[chunk_id]
        self.flush_to_storage()

    def init_from_storage(self):
        if os.path.exists(f'{self.backup_dir}dump.pickle'):
            self.chunks_metadata = None
            with open(f'{self.backup_dir}dump.pickle', 'rb') as f:
                self.chunks_metadata = pickle.load(f)

    def flush_to_storage(self):
        with open(f'{self.backup_dir}dump.pickle', 'wb') as f:
            pickle.dump(self.chunks_metadata, f)

    def on_connect(self, conn):
        pass

    def is_checksum_valid(self, chunk_id):
        # print("stored checksum", self.chunks_metadata[chunk_id].checksum)
        file_name = self.chunks_metadata[chunk_id].file_name + '_chunk_id_' + str(chunk_id)

        res =  (os.path.exists(self.storage_dir+'/'+file_name)) and ((hash.md5(open(self.storage_dir+'/'+file_name, 'rb').read()).hexdigest() == self.chunks_metadata[chunk_id].checksum) or (self.chunks_metadata[chunk_id].checksum == None))
        if not res:
            # Checksum validation failed for the chunk_id
            print(f'checksum validation failed for chunk id {chunk_id}')
            # Delete data from own storage
            del self.chunks_metadata[chunk_id]
            # Calling master to remove the chunk metadata whose checksum is invalid
            rpyc.connect(*self.master_url).root.invalid_checksum(chunk_id, self.url)
        
        self.flush_to_storage()
        return res
    
    def background_checksum_thread(self):
        while True:
            curr_chunks = list(self.chunks_metadata.keys())[:]
            for chunk_id in curr_chunks:
                self.is_checksum_valid(chunk_id)
            # wait before checking for checksum again
            time.sleep(self.checksum_check_interval)

    # Send heartbeat to master and remove stale replicas, if any
    def background_heartbeat_thread(self):
        while True:
            chunk_list = self.get_chunks_to_version()
            stale_replicas = []
            while True:
                try:
                    # Check for stale replicas if any
                    master_conn = rpyc.connect(*self.master_url).root
                    stale_replicas = master_conn.heartbeat(self.url, chunk_list)
                    break
                except Exception as e:
                    print('failed to connect to master, retrying in 10 sec', e)
                    time.sleep(10)

            # Remove stale replicas if any
            if len(stale_replicas) > 0:
                self.remove_chunks(stale_replicas)

            time.sleep(self.heartbeat_interval)

    def exposed_replicate_chunk(self, chunk_id, version, replicas):
        # Random shuffle replicas and iterate over it to get data from one of those
        print(f"replicate chunk data on chunkserver for chunk id {chunk_id}")
        replicas = list(replicas)
        random.shuffle(replicas)
        success = 0
        file_name = None
        data = None
        for replica_url in replicas:
            # get the chunk data from the replica
            # Unable to connect to the replica
            try:
                res = rpyc.connect(*replica_url).root.get_chunk_data(chunk_id, version)
                version, file_name, data = res
            except Exception as e:
                print('call get chunk data exception', e)
                continue

            if res == "invalid checksum" or res == "stale replica":
                continue
            success = 1
            break

        # check whether it is successful in writing to atleast one replica
        if success == 0:
            return "no replica has correct data"

        # write the obtained data to the local storage
        chunk_file_name = file_name + '_chunk_id_' + str(chunk_id)
        with open(self.storage_dir +'/'+ chunk_file_name, 'w') as f:
            f.write(data)

        # update  the file name and version for chunk in the chunks_metadata
        self.chunks_metadata[chunk_id] = ChunkMeta(file_name, version, offset = len(data))

        self.flush_to_storage()
        return "success"

    def exposed_get_chunk_data(self, chunk_id, version):
        # Check for the version number - stale replica
        # Check for the checksum - invalid checksum
        file_name = self.chunks_metadata[chunk_id].file_name + '_chunk_id_'+ str(chunk_id)
        curr_version = self.chunks_metadata[chunk_id].version
        if curr_version < version:
            return "stale replica"
        if not self.is_checksum_valid(chunk_id):
            return "invalid checksum"
        with open(self.storage_dir + '/' + file_name, 'r') as f:
            data = f.read()
        # print('data to be sent', data)
        # supply version number as curr chunkserver may have more 
        return (curr_version, self.chunks_metadata[chunk_id].file_name, data)
        
    # master will make rpc to the create function to tell the chunkserver to create the file on its storage
    def exposed_create(self, file_name, chunk_id):
        # create a file in local storage and initialize metadata for the chunk
        print(f"create chunk on chunkserver for {file_name} with {chunk_id}")
        chunk_filename = file_name + "_chunk_id_"+ str(chunk_id)
        with open(self.storage_dir + '/' + chunk_filename, 'w') as f:
            pass
        self.chunks_metadata[chunk_id] = ChunkMeta(file_name, 1)
        self.flush_to_storage()
        
    # client will make rpc to append data to one of the replicas for the chunk which will then send to the next one
    # and serially will reach all the replicas
    # another chunkserver can also make rpc to append data to the next replica
    def exposed_append(self, chunk_id, data, client_url, replicas):
        # store the data in self.chunks_metadata[chunk_id].to_commit\
        # if chunk id not exists in the chunkserver, it might be removed by the chunkserver due to invalid checksum
        # and client cache is not refreshed
        replicas = list(replicas)

        # chunk not 
        if chunk_id not in self.chunks_metadata:
            return 'chunkserver_failure_' + str(self.url[0]) + '_' + str(self.url[1])

        self.to_commit[(chunk_id, client_url)] = data
        
        # remove itself from the replicas list
        # print("Replicas: ", list(replicas), type(list(replicas))," URL: ",self.url, type(self.url))
        replicas.remove(self.url)
        
        # make the same append call to random replica from the new replicas list if list size > 0
        if len(replicas) > 0:
            replica = random.choice(replicas)
            cnt = 0
            while True:
                try:
                    res = rpyc.connect(*replica).root.append(chunk_id, data, client_url, replicas)
                    # print("append res:", res, self.url)
                    if res != "success":
                        del self.to_commit[(chunk_id, client_url)]
                        return res
                    break
                except:
                    cnt += 1
                    if cnt == 10:
                        del self.to_commit[(chunk_id, client_url)]
                        return 'chunkserver_failure_' + str(replica[0]) + '_' + str(replica[1])
                    time.sleep(10)
        return "success"

    # client will make rpc to the primary chunkserver to apply latest mutations on itself as well as all the secondaries
    def exposed_commit_append(self, chunk_id, client_url, secondary_urls, primary_url):
        # in case of stale replica and corrupted replica
        secondary_urls = list(secondary_urls)
        if chunk_id not in self.chunks_metadata:
            return "chunk not present"
        

        if (chunk_id, client_url) not in self.to_commit:
            return "data not present to commit"

        # check the lease timeout corresponding to the chunk whether the lease has expired, if yes return "not primary" to the client
        if time.time() > self.chunks_metadata[chunk_id].lease_expiry_time:
            return "not primary"
        
        data_to_commit = self.to_commit[(chunk_id, client_url)]
        if (chunk_id, client_url) not in self.ack_commit:
            file_name = self.chunks_metadata[chunk_id].file_name + '_chunk_id_' + str(chunk_id)
            with open(self.storage_dir+'/'+file_name, 'a') as f:
                # get the data from to_commit[(chunk_id, client_url)] and append it to the file filename_chunk_chunkid
                # read the data which is in temp log storage
                f.seek(self.chunks_metadata[chunk_id].offset)
                f.write(data_to_commit)
            # setting ack to true to prevent double writing and ensuring idempotence
            self.ack_commit[(chunk_id, client_url)] = True
            # reset the temporary log storage i.e to_commit
            del self.to_commit[(chunk_id, client_url)]
            # update the checksum which is a md5 hash :: String
            self.chunks_metadata[chunk_id].checksum = hash.md5(open(self.storage_dir+'/'+file_name, 'rb').read()).hexdigest()
            self.flush_to_storage()
        # then make rpc to commit_secondary_append for the secondary chunkservers by removing primary from replicas along with the chunk offset
        # wait for reply from the secondary chunkservers, on receiving reply from all, return success to the client
        acks = []
        for url in secondary_urls:
            cnt = 0
            # Retry if secondary replicas is unreachable at an interval of 10 sec, and atmost 10 retries
            while True:
                try:
                    ack = rpyc.connect(*url).root.commit_secondary_append(chunk_id, client_url, self.chunks_metadata[chunk_id].offset)
                    # print('commit append ack', ack)
                    if(ack != "success"):
                        # Secondary chunkserver is unable to commit
                        return "commit_failed_" + str(url[0]) + '_' + str(url[1])
                    acks.append(ack)
                    break
                except Exception as e:
                    print("commit append exception", e)
                    cnt = cnt + 1
                    if(cnt == 10):
                        # Unable to commit to secondary chunkserver even after retries
                        return "commit_failed_" + str(url[0]) + '_' + str(url[1])
                    time.sleep(10)
        
        commit_offset = self.chunks_metadata[chunk_id].offset
        self.chunks_metadata[chunk_id].offset = self.chunks_metadata[chunk_id].offset + len(data_to_commit)

        # Accepted success from all chunkservers remove ack_commit from all chunkservers
        self.exposed_remove_commit_ack(chunk_id, client_url)
        for url in secondary_urls:
            # Retry if secondary replicas is unreachable at an interval of 10 sec, and atmost 10 retries            
            ack = rpyc.connect(*url).root.remove_commit_ack(chunk_id, client_url)
        self.flush_to_storage()
        return commit_offset

    def exposed_remove_commit_ack(self, chunk_id, client_url):
        del self.ack_commit[(chunk_id, client_url)]
    
    def exposed_commit_secondary_append(self, chunk_id, client_url, offset):
        # Already ack, make commit apend idempotent
        if (chunk_id, client_url) in self.ack_commit:
            return "success"
            
        file_name = self.chunks_metadata[chunk_id].file_name+'_chunk_id_'+ str(chunk_id)
        with open(self.storage_dir+'/'+file_name, 'a') as f:
            # read the data which is in temp log storage
            f.seek(offset)
            data_to_commit = self.to_commit[(chunk_id, client_url)]
            self.chunks_metadata[chunk_id].offset = offset + len(data_to_commit)
            f.write(data_to_commit)
            del self.to_commit[(chunk_id, client_url)]
            # update the checksum which is a md5 hash :: String
            self.ack_commit[(chunk_id, client_url)] = True
        self.chunks_metadata[chunk_id].checksum = hash.md5(open(self.storage_dir+'/'+file_name, 'rb').read()).hexdigest()
        self.flush_to_storage()
        return "success"

    # client will make rpc to any of the chunkserver holding the chunk 
    def exposed_read(self, chunk_id, chunk_offset, amount_to_read):
        # Check for checksum (see if it is corrupted or not ?) and also send data.
        if chunk_id not in self.chunks_metadata:
            return "chunk not found"
        file_name = self.storage_dir + '/' + self.chunks_metadata[chunk_id].file_name + '_chunk_id_' + str(chunk_id)
        # print(file_name)
        with open(file_name, 'r') as f:
            f.seek(chunk_offset)
            data = f.read(amount_to_read)
            is_checksum_valid = self.is_checksum_valid(chunk_id)
            if not is_checksum_valid:
                return "data corrupted"
            return data

    def exposed_increment_chunk_version(self, chunk_id, version):
        self.chunks_metadata[chunk_id].version += 1
        self.flush_to_storage()

    # master will make rpc to tell the chunkserver it is the new primary for given chunk
    def exposed_select_primary(self, chunk_id, lease_expiry_time):
        self.chunks_metadata[chunk_id].lease_expiry_time = lease_expiry_time
        self.flush_to_storage()

if __name__ == "__main__":
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    master_hostname = sys.argv[3]
    master_port = int(sys.argv[4])
    storage_dir = sys.argv[5]
    print("check")
    ThreadedServer(ChunkServerService(hostname, port, master_hostname, master_port, storage_dir), hostname = hostname, port = port).start()