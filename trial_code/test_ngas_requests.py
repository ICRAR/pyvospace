# Function to test NGAS requests
import requests

payload={'file', "file.img"}
base_string="http://localhost:7777"

archive_req_string=base_string+"/ARCHIVE"
retrieve_req_string=base_string+"/RETRIEVE"

upload_param={"filename": "file.img",
       "mime_type":"application/octet-stream"}

# POST the file and then retrieve it

# Upload the data
with open(upload_param["filename"],"rb") as fd:
    upload = requests.post(archive_req_string, params=upload_param, data={upload_param["filename"]: fd})
    print(upload.url)
    print(upload.text)

# Now try to download the data
download_param={"file_id": "file.img"}
download = requests.get(retrieve_req_string, params=download_param, stream=True)

with open("file_out.img","wb") as fd:
    for chunk in download.iter_content(chunk_size=1024):
        if chunk:
            fd.write(chunk)

print(download.url)





















