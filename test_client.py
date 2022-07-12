import math
import sys
import time
import rpyc
import random
from pexpect import pxssh

# chunkserver_1 for IP[1] port 3001
# chunkserver_2 for IP[1] port 3002
# chunkserver_3 for IP[2] port 3003
IPS = ["IP1", "IP2", "IP3"]

class BasicTests():

    def __init__(self, client_url):
        self.client_url = client_url


    def create_data(self, repeat = 1):
        data = self.data
        for i in range(0, repeat - 1):
            data += data
        return data


    def create(self, file_name):
        try:
            rpyc.connect(*self.client_url).root.create(file_name)
            return "Created successfully"
        except Exception as e:
            print(e)
            return "File creation failed"
    
    def read(self, file_name, offset, size):
        try:
            res = rpyc.connect(*self.client_url).root.read(file_name, offset, size)
            if res == 'file not found':
               return "Failed reading file"
            else:
               return res
        except:
            return "Failed reading file"

    def write(self, file_name, data, offset):
        try:
            offset = rpyc.connect(*self.client_url).root.write(file_name, data, offset)
            curr_len = offset + len(data)
            if not isinstance(offset, int):
                return f"Error received from write: {offset}"
        except Exception as e:
            return f"Error received from write: {offset}"
        return curr_len

    def delete(self, file_name):
        try:
            res = rpyc.connect(*self.client_url).root.delete(file_name)
            if res == 'file not found':
                return "Failed deleting file"
            else:
                return res
        except:
            return "Failed deleting file"

    def restore(self, file_name):
        try:
            res = rpyc.connect(*self.client_url).root.restore(file_name)
            if res == 'file removed from trash':
                return "Failed restoring file"
            else:
                return res
        except:
            return "Failed restoring file" 

# small file test
def write_test(client_url, input_file_name, create_file_name):
    test1 = BasicTests(client_url)

    # create new file
    input('Press enter to create a file')
    create_res = test1.create(create_file_name)
    print('Create result: '+ create_res)


    # write to file
    input('Press enter to write to the file')
    data = ''
    curr_len = 0
    with open(input_file_name, 'r') as f:
        data += f.read()
    write_res = test1.write(create_file_name, data, curr_len)
    if isinstance(write_res, int):
        curr_len = write_res + len(data)
        write_res = "Successfully written"
    print("Write result: " + write_res)

def read_test(client_url, input_file_name, create_file_name):
    test1 = BasicTests(client_url)
    # read from file
    data = ''
    with open(input_file_name, 'r') as f:
        data = f.read()
    read_res = test1.read(create_file_name, 0, len(data))
    print('Read result: ' + read_res)

def invalid_checksum_test():
    s = pxssh.pxssh()
    s.login(IPS[1], 'student', 'guess123')

    # write file on gfs

    # Case 1: file contents are manipulated
    
    # 1. modify the contents of the file at chunkserver
    input('Press enter to modify the contents of the file on one chunkserver')
    s.sendline("echo 'garbage' >> /home/student/Half-baked-GFS/chunkserver_1/hello.txt_chunk_id_1015")
    s.prompt()
    print(s.before)
    
    # 2. wait till the chunk is rereplicated
    input('Press enter after invalid checksum is detected and file is rereplicated onto another or same chunkserver')
    
    # Case 2: chunk files are deleted
    # 1. delete the file from gfs chunkserver
    s.sendline('rm -rf /home/student/Half-baked-GFS/chunkserver_1/hello.txt_chunk_id_1010')
    s.prompt()
    print(s.before)
    
    # 2. wait till the chunk is rereplicated
    input('Press enter after invalid checksum is detected and file is rereplicated onto another or same chunkserver')

def rereplicate_chunks_test():
    s = pxssh.pxssh()
    s.login(IPS[1], 'student', 'guess123')

    # crash one chunkserver and start another
    input('Press enter to crash one chunkserver')
    s.sendline('kill -9 $(lsof -t -i tcp:3001)')
    s.prompt()
    print(s.before)

    input('Press enter after starting new chunkserver at port 3004')

def delete_and_restore_file_test(client_url, input_file_name, create_file_name, restore_flag):
    delete_test = BasicTests(client_url)
    delete_res = delete_test.delete(create_file_name)
    print('Delete result: ' + delete_res)

    input('Press enter to read from file')
    read_test(client_url, input_file_name, create_file_name)
    
    if not restore_flag:
        time.sleep(60)

    input('Press enter to restore the file')
    restore_res = delete_test.restore(create_file_name)
    print('Restore result: ' + restore_res)

    if restore_flag:
        input('Press enter to read from file')
        read_test(client_url, input_file_name, create_file_name)

def persistent_chunkserver_test():
    s = pxssh.pxssh()
    s.login(IPS[1], 'student', 'guess123')

    # crash one chunkserver and start another
    input('Press enter to crash one chunkserver')
    s.sendline('kill -9 $(lsof -t -i tcp:3002)')
    s.prompt()
    print(s.before)

    input('Press enter after starting same chunkserver at port 3002')


if __name__ == '__main__':
    random.seed(0)
    client_url = (sys.argv[1], int(sys.argv[2]))
    # testcase 1
    print('Test case 1 : Write')
    write_test(client_url, 'test.txt', 'hello.txt') 
    input("Press enter to proceed to next case")

    # testcase 2
    print('Test case 2 : Read')
    read_test(client_url, 'test.txt', 'hello.txt') 
    input("Press enter to proceed to next case")

    # testcase 3
    print('Test case 3: Invalid Checksum')
    invalid_checksum_test()
    input("Press enter to proceed to next case")
    
    # testcase 4
    print('Test case 4: Chunkserver crash')
    rereplicate_chunks_test()
    input("Press enter to proceed to next case")

    # testcase 5
    print('Test case 5: Delete and restore file')
    delete_and_restore_file_test(client_url, 'test.txt', 'hello.txt', True)
    input("Press enter to proceed to next case")

     # testcase 6
    print('Test case 6: Delete file')
    delete_and_restore_file_test(client_url, 'test.txt', 'hello.txt', False)
    input("Press enter to proceed to next case")

     # testcase 7
    print('Test case 7: Persistent storage for chunkserver')
    persistent_chunkserver_test()
    input("Press enter to proceed to next case")

    print('all tests passed!')

# large file test
def large_file_test(input_file_name, create_file_name):
    test2 = BasicTests()

    # create new file
    input('Press enter to create a file')
    create_res = test2.create(create_file_name)
    print('Create result: '+ create_res)


    # write to file
    input('Press enter to write to the file')
    data = ''
    curr_len = 0
    with open(input_file_name, 'r') as f:
        data += f.read()
    write_res = test2.write(create_file_name, data, curr_len)
    if isinstance(write_res, int):
        curr_len = write_res + len(data)
        write_res = "Successfully written"
    print("Write result: " + write_res)
    
    
    # read from file
    input("Press enter to read from the file")
    data = ''
    with open(input_file_name, 'r') as f:
        data = f.read()
    read_res = test2.read(create_file_name, 0, len(data))
    print('Read result: ' + read_res)

    # delete a file
    input("Press enter to delete a file")
    delete_res = test2.delete(create_file_name)
    print("Delete Result: ", delete_res)

    # restore a file
    input('Press enter to restore the file')
    restore_result = test2.restore(create_file_name)
    print("Restore result: ", restore_result)


# write rate
def test_write_rate(input_file_name, create_file_name):
    data = ''
    with open(input_file_name, 'r') as f:
        data = f.read()
    test4 = BasicTests()
    file_sizes = [x for x in range(0, 256 * 1024 * 1024) if x+1 >= (1024 * 1024) and math.log(x+1, 2) == int(math.log(x+1, 2))]
    for size in file_sizes:
        create_res = test4.create(f'{create_file_name}_{size}')
        write_data = test4.create_data()
        start_time = time.time()
        write_res = test4.write(f'{create_file_name}_{size}', write_data, size)
        end_time = time.time()
        time_taken = end_time - start_time
        print(f'Time taken to write file of {size}: {time_taken}')

# read rate
def test_read_rate(read_file_name):
    test3 = BasicTests()
    file_sizes = [x for x in range(0, 256 * 1024 * 1024) if x+1 >= (1024 * 1024) and math.log(x+1, 2) == int(math.log(x+1, 2))]
    for size in file_sizes:
        start_time = time.time()
        read_res = test3.read(read_file_name, 0, size)
        end_time = time.time()
        time_taken = end_time - start_time
        print(f'Time taken to read file of {size}: {time_taken}')
