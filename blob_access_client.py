import yaml
from azure.storage.blob import BlobClient
from azure.storage.blob import ContainerClient
import aiohttp


inputYaml = open("wounds.yml", "r")
yaml2json = yaml.safe_load(inputYaml)
inputYaml.close()

blob_connection_string = yaml2json["storage_blob"]["azure_storage_blob_connection_string"]
containerName = yaml2json["storage_blob"]["blob_container"]

def blob_download(containerName, blobName):
    """
      Download blobName from containerName
      Output = a local version of blobName is saved to current directory
      Return = name of local file, which is just blobName
    """


    #
    # download blob from container
    #

    blob = BlobClient.from_connection_string(conn_str=blob_connection_string, container_name=containerName, blob_name=blobName)

    with open(blobName, "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)
    
    return blobName

def blob_upload(containerName, blobName, localName):


    #
    # upload a blob
    #

    blob = BlobClient.from_connection_string(conn_str=blob_connection_string, container_name=containerName, blob_name=blobName)

    with open(localName, "rb") as data:
        blob.upload_blob(data, overwrite=True)

def latestBlob_get(containerName):
    """
      enumerate the container
      Output = return latest blob [name, creation_time]

      Note: Assumption is the container is not very busy with new blobs
    """

    w_blobs= []

    container = ContainerClient.from_connection_string(conn_str=blob_connection_string, container_name=containerName)

    blob_list = container.list_blobs().by_page().next()

    for _, blob in enumerate(blob_list):
        w_blobs.append((blob["last_modified"], blob["name"]))

    latest_blob = sorted(w_blobs)[-1]
    return [latest_blob[1], latest_blob[0]]
