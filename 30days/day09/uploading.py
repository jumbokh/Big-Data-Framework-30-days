# make connections
from hdfs import InsecureClient
client = InsecureClient("http://localhost:9870/", user='mengchiehliu')

# make directory
client.delete('day09', recursive=True)
client.makedirs('day09/input')

# upload testing files
import os
local_dir = os.path.dirname(__file__)
client.upload(hdfs_path="day09/input/test_text_1.txt", local_path=f'{local_dir}/test_text_1.txt', overwrite=True)
client.upload(hdfs_path="day09/input/test_text_2.txt", local_path=f'{local_dir}/test_text_2.txt', overwrite=True)
client.upload(hdfs_path="day09/input/test_text_3.txt", local_path=f'{local_dir}/test_text_3.txt', overwrite=True)