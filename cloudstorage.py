cloudstorage.py



import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess
import azure.common
from azure.storage.common import CloudStorageAccount
import azure.batch.models as batchmodels
import azure.storage.blob as azureblob
import config


# STORAGE_ACCOUNT_NAME = "dataplatdevblob00"
# STORAGE_ACCOUNT_KEY = "sqI+1LLilEvjdGSgumF85X8zlgTLYHmg0JUbPew2akdl/Vc93B1ceRouIGQqbwndkFnGETgnRiJ3GRWcRDlBiw=="

# storageName = "dataplatdevblob03"
# storageKey = "/7fNjm9gJYMNWS4DexNvDrnHgMUJOJWY6EoJYMs3YuWZcWp+jFZyDXHTpaymm9Gt+C6Z6uGitrRV6MCS9oCz7A=="

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


def compare_blob_structure(file_list_blob00,file_list_blob03):
    if file_list_blob00 == file_list_blob03 :
        """
        for i in file_list:
             print(i.name)
        #get_leaf_files(file_list_blob00)
        """
    else:
        print("Blob structure mismatch.....")


def get_leaf_files(file_list):
    name_split = []
    name_d = {}
    # split on the last occurence of the delimiter "/" to get the file names and the parent folder to find latest schema version 
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

blob_path = 'nbv/in_nbv_azure_net_transaction/'
blob03_path = 'nbv/in_nbv_azure_net_transaction/'
blob00_path = 'nbv/net_transaction/'
"""
file_list_blob00 = file_list_blob00('raw' , blob_path)
file_list_blob03 = file_list_blob03('raw' , blob_path)

compare_blob_structure(file_list_blob00,file_list_blob03)
"""
file_list_blob00 = file_list_blob00('raw' , blob00_path)
file_list_blob03 = file_list_blob03('raw' , blob03_path)

# compare_blob_structure(file_list_blob00,file_list_blob03)
get_leaf_files(file_list_blob03)
get_leaf_files(file_list_blob00)
