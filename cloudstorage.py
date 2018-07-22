# compare files in 2 different blobs and check if their name and count match
# get the leaf files in each of the sub folders

import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess
import azure.common
from azure.storage.common import CloudStorageAccount
import azure.batch.models as batchmodels
import azure.storage.blob as azureblob
import config



def file_list_blob00(container_name,blob_path):
    storageName = 'dataplatdevblob00'
    storageKey = config.storage[storageName]
    storage = CloudStorageAccount(account_name=storageName, account_key=storageKey, sas_token=None, is_emulated=None)
    # Create a Block Blob Service object
    bbs = storage.create_block_blob_service()
    return(bbs.list_blobs(container_name,blob_path))


def file_list_blob03(container_name,blob_path):
    storageName = 'dataplatdevblob03'
    storageKey = config.storage[storageName]
    storage = CloudStorageAccount(account_name=storageName, account_key=storageKey, sas_token=None, is_emulated=None)
    # Create a Block Blob Service object
    bbs = storage.create_block_blob_service()
    return(bbs.list_blobs(container_name,blob_path)) 

# compare files in 2 different blobs and check if their name and count match
def compare_blob_structure(file_list_blob00,file_list_blob03):
    if file_list_blob00 == file_list_blob03 :
        """
        for i in file_list:
             print(i.name)
        """
    else:
        print("Blob structure mismatch.....")

# get the leaf files in each of the sub folders
def get_leaf_files(file_list):
    name_split = []
    name_d = {}
    # split on the last occurence of the delimiter "/" to get the file names and the parent folder
    # to find latest schema version 
    for i in file_list:
        name_split.append(i.name.rsplit('/',1))
    
    for i in range(0,len(name_split)):
        if name_split[i][0] in name_d:
            #add leaf file name to the parent folder
            name_d[name_split[i][0]].append(name_split[i][1])
        else:
            #create  new parent folder
            name_d.update({name_split[i][0]:[name_split[i][1]]})
    
    for i in name_d:
        print("Parent Folder: ", i ,":\n")
        print("Leaf Files:\n")
        for files in name_d[i]:
            print(files)
        print("\n\n")
    
   
    
#####################################
#####           MAIN            #####
#####################################

blob_path = 'source1/data_sub_type/'
blob03_path = 'source1/data_sub_type/'
blob00_path = 'source1/data_sub_type/'

file_list_blob00 = file_list_blob00('raw' , blob_path)
file_list_blob03 = file_list_blob03('raw' , blob_path)
compare_blob_structure(file_list_blob00,file_list_blob03)

file_list_blob00 = file_list_blob00('raw' , blob00_path)
file_list_blob03 = file_list_blob03('raw' , blob03_path)

# compare_blob_structure(file_list_blob00,file_list_blob03)
get_leaf_files(file_list_blob03)
get_leaf_files(file_list_blob00)
