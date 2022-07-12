"""
Client.py: Interface between end application and Half Baked GFS
- Read data.
- Write data.
- Delete and Restore data.
"""
import sys
import random
import time
import rpyc
from rpyc.utils.server import ThreadedServer

class CacheMeta:
    def __init__(self, chunk_id, primary_url, replica_urls, cache_timeout = 0):
        self.chunk_id = chunk_id
        self.primary_url = primary_url
        self.replica_urls = replica_urls
        self.cache_timeout = cache_timeout

class ClientService(rpyc.Service):
    def __init__(self, master_hostname, master_port, client_hostname, client_port):
        self.master_url = (master_hostname, master_port)        # Hostname and port at which the master listens
        self.url = (client_hostname, client_port)               # Hostname and port at which the client listens
        self.cache_data = {}                                    # Store : (file_name, chunk_num) -> CacheMeta(chunk_id, primary_url, replica_urls)
        self.cache_timeout = 30                                 # Timeout after which primary chunkserver information corresponding to a chunk gets expired
        self.chunk_size = 64 * 1024                             # Size of chunks  
        random.seed(0)


    """
    exposed_create: 
    RPC called by end application to create a file. 
    Initiates RPC to master to create the file.

    @param file_name : string
    """
    def exposed_create(self, file_name):
        cnt = 0
        while True:
            try:
                res = rpyc.connect(*self.master_url).root.create(file_name)
                if res == "failed to create chunk":
                    return "failed to create file"
                break
            except Exception as e:
                print("except", e)
            cnt += 1
            if cnt == 10:
                return "failed to create file"
        return "success"

    """
    exposed_read:
    Reads the given amount of data starting from offset, for the supplied file name.
    Initiates RPC to master to fetch chunk id and replicas corresponding to file name and chunk number.
    Then reads corresponding chunk from any randomly choosen replica supplied by the master.

    @param file_name : string
    @param offset : int
    @param total_bytes_to_read : int
    """
    def exposed_read(self, file_name, offset, total_bytes_to_read):
        chunk_num = offset // self.chunk_size    # chunk_num from where we need to check chunk_id
        chunk_offset = offset % self.chunk_size  # with in the chunk at chunk_num, offset.
        data = ""
        while (total_bytes_to_read > 0):
            # For the current chunk check the amount of data we need to read.
            rem_to_read = self.chunk_size - chunk_offset
            amount_to_read = min(total_bytes_to_read, rem_to_read)
            get_fresh_data = True
            # Now check whether that chunk_num is present or not in the cache for the given file_name
            if (file_name, chunk_num) in self.cache_data:
                # Try to get the data from the cache.
                cache_data = self.cache_data[(file_name, chunk_num)]
                chunk_id, replicas, cache_timeout = cache_data.chunk_id, cache_data.replica_urls, cache_data.cache_timeout
                # If it is cache-timeout data then we need to bring fresh cache data, for that we need to connect to master
                # and ask it to read a file_name for given chunk_num
                if time.time() < cache_timeout:
                    get_fresh_data = False
            # Ask the master for the data for this file_name and chunk_num in case cache_timeout is happen for the entry.
            if (get_fresh_data):
                # Call the Master's read to get the data
                chunk_id, replicas = rpyc.connect(*self.master_url).root.read(file_name, chunk_num)
                replicas = list(replicas)
                if chunk_id == -1:
                    return 'file not found'
                # Setup the cache and expire_time
                self.cache_data[(file_name, chunk_num)] = CacheMeta(chunk_id, None, replicas, time.time() + self.cache_timeout)
            cache_data = self.cache_data[(file_name, chunk_num)]
            chunk_id, replicas = cache_data.chunk_id, cache_data.replica_urls
            if (len(replicas) == 0):
                # There is no replica for the given chunk return simply
                return 'file not found'
            # Try to get the data from the replica, 
            while len(replicas) > 0:
                replica_url = random.choice(replicas)
                # Get to that replica and in read request send the chunk_id and amount of data to read from that chunk, and
                # from where to start read to that chunk.
                try:
                    res = rpyc.connect(*replica_url).root.read(chunk_id, chunk_offset, amount_to_read)
                    # In case of corrupt_data : invalid checksum we are removing that replica and then will retry on any other.
                    if res == 'data corrupted' or res == 'chunk not found':
                        replicas.remove(replica_url)
                        continue
                    data += res
                    break
                except Exception as e:
                    print("read exception client", e)
                    replicas.remove(replica_url)
            # Reset & Update all these value for the next round.
            total_bytes_to_read -= amount_to_read
            chunk_num += 1
            chunk_offset = 0 
        return data

    """
    exposed_write:
    Writes the given data to the desired file. 
    Splits writes that overlap chunks into multiple writes.

    @param file_name : string
    @param data : string
    @param offset : int
    """
    def exposed_write(self, file_name, data, offset):
        # offset is the offset within file_name from which data is to be written
        chunk_num = offset // self.chunk_size # get the last chunk number
        last_chunk_offset = offset % self.chunk_size # get the offset in the last chunk from which we can write
        initial_chunk = chunk_num
        data_size = len(data)
        data_offset = 0 # offset within data from which next chunk should be written
        res = 0
        while data_size > 0:
            # if the chunk is full and data is remaining to write, create new chunk
            if chunk_num != initial_chunk:
                rpyc.connect(*self.master_url).root.create(file_name)
            
            # size of data to be written in chunk, if initial chunk, will be chunk_size-curr_offset, else chunk_size
            data_size_in_curr_chunk = min(self.chunk_size - last_chunk_offset, data_size)
            
            # get current data and call write_chunk
            curr_data = data[data_offset:data_offset+data_size_in_curr_chunk]
            
            write_offset = self.write_chunk(file_name, curr_data, chunk_num)
            
            if not isinstance(write_offset, int) and write_offset == "all chunkservers down":
                return write_offset

            if chunk_num == initial_chunk:
                res = write_offset

            # increment data offset and chunk number
            data_offset += data_size_in_curr_chunk
            chunk_num += 1

            # decrement data_size as data_size_in_curr_chunk amount of data written in curr_chunk
            data_size -= data_size_in_curr_chunk

            # set chunk offset to 0 as new chunk will be created in next iteration
            last_chunk_offset = 0
        return res
    

    """
    write_chunk:
    Initiates RPC call to one of the chunkservers holding the chunk to append the data to the chunk and 
    on success, initiates RPC call to commit the data.

    @param file_name : string
    @param data : string
    @param chunk_num : int
    """
    def write_chunk(self, file_name, data, chunk_num):
        # Check primary is present in cache, if present return from cache, (chunk_id, primary and replica urls).
        # Send the data to first replica, which will send it to all the replicas.
        # Try sending data to each replica, if in respose we gets not successful for any of the chunkserver then delete that chunkserver.
        # Need to relay the failure message from any of the replica to client so that client can remove the chunkserver and request new primary, replicas from master.

        if (file_name, chunk_num) in self.cache_data and self.cache_data[(file_name, chunk_num)].primary_url:
            # Read from cache if present
            res = self.cache_data[(file_name, chunk_num)]
            chunk_id, primary_url, replica_urls = res.chunk_id, res.primary_url, res.replica_urls
        else:
            # Get information on replicas from master and save in local cache
            res = rpyc.connect(*self.master_url).root.get_primary(file_name, chunk_num, False)
            if res == "all chunkservers down":
                return res
            self.cache_data[(file_name, chunk_num)] = CacheMeta(res[0], res[1], list(res[2]), time.time() + self.cache_timeout)
            chunk_id, primary_url, replica_urls = res
        replica_urls = list(replica_urls) 

        # Requesting primary to commit all the appends
        # Checking whether the urls returned by get_primary are not empty
        while len(replica_urls) > 0:
            try:
                to_send_replica = random.choice(replica_urls)
                append_res = rpyc.connect(*to_send_replica).root.append(chunk_id, data, self.url, replica_urls)
            except Exception as e:
                print("exception", e)
                append_res = 'chunkserver_failure_' + str(to_send_replica[0]) + '_' + str(to_send_replica[1])

            # If success then break
            if append_res == 'success':
                break
            
            hostname, port = append_res.split('_')[-2], int(append_res.split('_')[-1])
            failed_url = (hostname, port)
            # Removing failed chunkserver from master metadata, this failed_url 
            rpyc.connect(*self.master_url).root.remove_chunkserver(failed_url)
            
            # Request new lease information
            new_res = rpyc.connect(*self.master_url).root.get_primary(file_name, chunk_num, False)
            if new_res == "all chunkservers down":
                return new_res
            chunk_id, primary_url, replica_urls = new_res
            replica_urls = list(replica_urls)
            self.cache_data[(file_name, chunk_num)] = CacheMeta(chunk_id, primary_url, replica_urls, time.time() + self.cache_timeout)
            
        # call commit on primary, not be done without appending to all the chunkservers
        commit_append_res = 0
        while append_res and append_res == 'success':
            secondary_urls = replica_urls[:]
            secondary_urls.remove(primary_url)
            try:
                commit_append_res = rpyc.connect(*primary_url).root.commit_append(chunk_id, self.url, secondary_urls, primary_url)
            except Exception as e:
                print("commit append call exception", e)
                # If not able to connect to primary itelf
                commit_append_res = "commit_failed_" + str(primary_url[0]) + '_' + str(primary_url[1])

            if not isinstance(commit_append_res, int):
                # Remove chunkserver if its unable to respond
                if commit_append_res.startswith("commit_failed_"):
                    failed_url = commit_append_res.split('_')[-2], int(commit_append_res.split('_')[-1])
                    try:
                        rpyc.connect(*self.master_url).root.remove_chunkserver(failed_url)
                    except Exception as e:
                        print("remove chunkserver call exception", e)
                
                # Request new primary from master, as the expiry time of current primary is expired, or if primary was not responding
                # Remove chunkserver will also remove primary
                new_res = rpyc.connect(*self.master_url).root.get_primary(file_name, chunk_num, False)
                if new_res == "all chunkservers down":
                    return new_res
                old_urls = replica_urls 
                chunk_id, primary_url, replica_urls = new_res
                replica_urls = list(replica_urls)
                # updating local cache with newly fetched information from master
                self.cache_data[(file_name, chunk_num)] = CacheMeta(chunk_id, primary_url, replica_urls, time.time() + self.cache_timeout)

                to_send_urls = [url for url in replica_urls if url not in old_urls]
                if len(to_send_urls) > 0:
                    # resend the data to these urls
                    try:
                        append_res = rpyc.connect(*to_send_urls[0]).root.append(chunk_id, data, to_send_urls)
                    except:
                        append_res = 'chunkserver_failure_' + str(to_send_urls[0])
                    
                    if append_res != 'success':
                        hostname, port = append_res.split('_')[-2], int(append_res.split('_')[-1])
                        rpyc.connect(*self.master_url).root.remove_chunkserver(failed_url)
                continue
            else:
                break
        
        return commit_append_res

    """
    exposed_delete:
    Initiate RPC call to master for moving supplied file to trash.

    @param file_name : string
    """
    def exposed_delete(self, file_name):
        # delete from cache time before calling master
        to_delete = []
        for filename, chunk_num in self.cache_data:
            if filename == file_name:
                to_delete.append((filename, chunk_num))
        
        for t in to_delete:
            del self.cache_data[t]

        # call delete on master which will replace file name with the deleted file name
        return rpyc.connect(*self.master_url).root.delete(file_name)
        
    """
    exposed_restore:
    Initiates RPC call to master in order to restore file from trash before expiry of trash timeout.

    @param file_name : string
    """
    def exposed_restore(self, file_name):
        return rpyc.connect(*self.master_url).root.restore(file_name)
    
if __name__ == "__main__":
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    master_hostname = sys.argv[3]
    master_port = int(sys.argv[4])
    ThreadedServer(ClientService(master_hostname, master_port, hostname, port), hostname = hostname, port = port).start()