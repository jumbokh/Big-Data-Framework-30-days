# make connections
from hdfs import InsecureClient
client = InsecureClient("http://localhost:9870/", user='mengchiehliu')


# ---------- Create ----------
# make directory
client.makedirs('day08')

# upload local files to hdfs
import os
local_path = os.path.join(os.path.dirname(__file__), "test_writing.txt")
return_path = client.upload(hdfs_path="day08/test_writing.txt", local_path=local_path, overwrite=True)
print("成功後回傳 hdfs_path:", return_path)

# list dirs
dirs = client.list('day08')
print("查看目錄內容:", dirs)


# ---------- Read ----------
# read file content
with client.read('day08/test_writing.txt') as reader:
    content = reader.read()
    print('文件內容:', content)

# download files from hdfs
new_local_path = os.path.join(os.path.dirname(__file__), "get_test_writing.txt")
return_path = client.download(hdfs_path="day08/test_writing.txt", local_path=new_local_path, overwrite=True)
print("成功後回傳 local_path:", return_path)


# ---------- Delete ----------
# Delete a file
print('刪除文件?', client.delete('day08/test_writing.txt'))

# Recursuvely delete dir content
print('遞迴刪除目錄與內容?', client.delete('day08', recursive=True))


# ---------- Others ---------- (try it yourself)
# Retrieve a file or folder content summary.
# content = client.content('<hdfs/path>')

# Retrieve a file or folder status.
# status = client.status('<hdfs/path>')

# Rename (move) a file.
# client.rename('<hdfs/path>', '<hdfs/new/path>')