{\rtf1\ansi\ansicpg1252\cocoartf1561\cocoasubrtf400
{\fonttbl\f0\fmodern\fcharset0 CourierNewPSMT;}
{\colortbl;\red255\green255\blue255;\red157\green0\blue210;\red255\green255\blue255;\red0\green0\blue0;
\red15\green112\blue1;\red0\green0\blue255;\red101\green76\blue29;\red0\green0\blue109;\red144\green1\blue18;
\red16\green60\blue192;\red19\green120\blue72;}
{\*\expandedcolortbl;;\cssrgb\c68627\c0\c85882;\cssrgb\c100000\c100000\c100000;\cssrgb\c0\c0\c0;
\cssrgb\c0\c50196\c0;\cssrgb\c0\c0\c100000;\cssrgb\c47451\c36863\c14902;\cssrgb\c0\c6275\c50196;\cssrgb\c63922\c8235\c8235;
\cssrgb\c6667\c33333\c80000;\cssrgb\c3529\c53333\c35294;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11900\viewh12720\viewkind1
\deftab720
\pard\pardeftab720\sl320\partightenfactor0

\f0\fs28 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 import\cf4 \strokec4  os, uuid, sys\
\cf2 \strokec2 from\cf4 \strokec4  azure.storage.blob \cf2 \strokec2 import\cf4 \strokec4  BlockBlobService, PublicAccess\
\cf2 \strokec2 import\cf4 \strokec4  azure.common\
\cf2 \strokec2 from\cf4 \strokec4  azure.storage.common \cf2 \strokec2 import\cf4 \strokec4  CloudStorageAccount\
\cf2 \strokec2 import\cf4 \strokec4  azure.batch.models \cf2 \strokec2 as\cf4 \strokec4  batchmodels\
\cf2 \strokec2 import\cf4 \strokec4  azure.storage.blob \cf2 \strokec2 as\cf4 \strokec4  azureblob\
\cf2 \strokec2 import\cf4 \strokec4  config\
\
\
\pard\pardeftab720\sl320\partightenfactor0
\cf5 \strokec5 # STORAGE_ACCOUNT_NAME = "dataplatdevblob00"\cf4 \strokec4 \
\cf5 \strokec5 # STORAGE_ACCOUNT_KEY = "sqI+1LLilEvjdGSgumF85X8zlgTLYHmg0JUbPew2akdl/Vc93B1ceRouIGQqbwndkFnGETgnRiJ3GRWcRDlBiw=="\cf4 \strokec4 \
\
\cf5 \strokec5 # storageName = "dataplatdevblob03"\cf4 \strokec4 \
\cf5 \strokec5 # storageKey = "/7fNjm9gJYMNWS4DexNvDrnHgMUJOJWY6EoJYMs3YuWZcWp+jFZyDXHTpaymm9Gt+C6Z6uGitrRV6MCS9oCz7A=="\cf4 \strokec4 \
\
\pard\pardeftab720\sl320\partightenfactor0
\cf6 \strokec6 def\cf4 \strokec4  \cf7 \strokec7 file_list_blob00\cf4 \strokec4 (\cf8 \strokec8 container_name\cf4 \strokec4 ,\cf8 \strokec8 blob_path\cf4 \strokec4 ):\
    storageName = \cf9 \strokec9 'dataplatdevblob00'\cf4 \strokec4 \
    storageKey = config.storage[storageName]\
    storage = CloudStorageAccount(\cf8 \strokec8 account_name\cf4 \strokec4 =storageName, \cf8 \strokec8 account_key\cf4 \strokec4 =storageKey, \cf8 \strokec8 sas_token\cf4 \strokec4 =\cf6 \strokec6 None\cf4 \strokec4 , \cf8 \strokec8 is_emulated\cf4 \strokec4 =\cf6 \strokec6 None\cf4 \strokec4 )\
    \cf5 \strokec5 # Create a Block Blob Service object\cf4 \strokec4 \
    bbs = storage.create_block_blob_service()\
    \cf2 \strokec2 return\cf4 \strokec4 (bbs.list_blobs(container_name,blob_path))\
\
\
\cf6 \strokec6 def\cf4 \strokec4  \cf7 \strokec7 file_list_blob03\cf4 \strokec4 (\cf8 \strokec8 container_name\cf4 \strokec4 ,\cf8 \strokec8 blob_path\cf4 \strokec4 ):\
    storageName = \cf9 \strokec9 'dataplatdevblob03'\cf4 \strokec4 \
    storageKey = config.storage[storageName]\
    storage = CloudStorageAccount(\cf8 \strokec8 account_name\cf4 \strokec4 =storageName, \cf8 \strokec8 account_key\cf4 \strokec4 =storageKey, \cf8 \strokec8 sas_token\cf4 \strokec4 =\cf6 \strokec6 None\cf4 \strokec4 , \cf8 \strokec8 is_emulated\cf4 \strokec4 =\cf6 \strokec6 None\cf4 \strokec4 )\
    \cf5 \strokec5 # Create a Block Blob Service object\cf4 \strokec4 \
    bbs = storage.create_block_blob_service()\
    \cf2 \strokec2 return\cf4 \strokec4 (bbs.list_blobs(container_name,blob_path)) \
\
\
\cf6 \strokec6 def\cf4 \strokec4  \cf7 \strokec7 compare_blob_structure\cf4 \strokec4 (\cf8 \strokec8 file_list_blob00\cf4 \strokec4 ,\cf8 \strokec8 file_list_blob03\cf4 \strokec4 ):\
    \cf2 \strokec2 if\cf4 \strokec4  file_list_blob00 == file_list_blob03 :\
        \cf9 \strokec9 """\cf4 \strokec4 \
\pard\pardeftab720\sl320\partightenfactor0
\cf9 \strokec9         for i in file_list:\cf4 \strokec4 \
\cf9 \strokec9              print({\field{\*\fldinst{HYPERLINK "http://i.name/"}}{\fldrslt \cf10 \ul \ulc10 \strokec10 i.name}})\cf4 \strokec4 \
\cf9 \strokec9         #get_leaf_files(file_list_blob00)\cf4 \strokec4 \
\cf9 \strokec9         """\cf4 \strokec4 \
    \cf2 \strokec2 else\cf4 \strokec4 :\
        \cf7 \strokec7 print\cf4 \strokec4 (\cf9 \strokec9 "Blob structure mismatch....."\cf4 \strokec4 )\
\
\
\pard\pardeftab720\sl320\partightenfactor0
\cf6 \strokec6 def\cf4 \strokec4  \cf7 \strokec7 get_leaf_files\cf4 \strokec4 (\cf8 \strokec8 file_list\cf4 \strokec4 ):\
    name_split = []\
    name_d = \{\}\
    \cf5 \strokec5 # split on the last occurence of the delimiter "/" to get the file names and the parent folder to find latest schema version \cf4 \strokec4 \
    \cf2 \strokec2 for\cf4 \strokec4  i \cf6 \strokec6 in\cf4 \strokec4  file_list:\
        name_split.append(i.name.rsplit(\cf9 \strokec9 '/'\cf4 \strokec4 ,\cf11 \strokec11 1\cf4 \strokec4 ))\
    \
        \
    \
    \cf2 \strokec2 for\cf4 \strokec4  i \cf6 \strokec6 in\cf4 \strokec4  \cf7 \strokec7 range\cf4 \strokec4 (\cf11 \strokec11 0\cf4 \strokec4 ,\cf7 \strokec7 len\cf4 \strokec4 (name_split)):\
        \cf2 \strokec2 if\cf4 \strokec4  name_split[i][\cf11 \strokec11 0\cf4 \strokec4 ] \cf6 \strokec6 in\cf4 \strokec4  name_d:\
            \cf5 \strokec5 #add leaf file name to the parent folder\cf4 \strokec4 \
            name_d[name_split[i][\cf11 \strokec11 0\cf4 \strokec4 ]].append(name_split[i][\cf11 \strokec11 1\cf4 \strokec4 ])\
        \cf2 \strokec2 else\cf4 \strokec4 :\
            \cf5 \strokec5 #create  new parent folder\cf4 \strokec4 \
            name_d.update(\{name_split[i][\cf11 \strokec11 0\cf4 \strokec4 ]:[name_split[i][\cf11 \strokec11 1\cf4 \strokec4 ]]\})\
    \
    \cf2 \strokec2 for\cf4 \strokec4  i \cf6 \strokec6 in\cf4 \strokec4  name_d:\
        \cf7 \strokec7 print\cf4 \strokec4 (\cf9 \strokec9 "Parent Folder: "\cf4 \strokec4 , i ,\cf9 \strokec9 ":\\n"\cf4 \strokec4 )\
        \cf7 \strokec7 print\cf4 \strokec4 (\cf9 \strokec9 "Leaf Files:\\n"\cf4 \strokec4 )\
        \cf2 \strokec2 for\cf4 \strokec4  files \cf6 \strokec6 in\cf4 \strokec4  name_d[i]:\
            \cf7 \strokec7 print\cf4 \strokec4 (files)\
        \cf7 \strokec7 print\cf4 \strokec4 (\cf9 \strokec9 "\\n\\n"\cf4 \strokec4 )\
    \
   \
    \
\pard\pardeftab720\sl320\partightenfactor0
\cf5 \strokec5 #####################################\cf4 \strokec4 \
\cf5 \strokec5 #####           MAIN            #####\cf4 \strokec4 \
\cf5 \strokec5 #####################################\cf4 \strokec4 \
\
blob_path = \cf9 \strokec9 'nbv/in_nbv_azure_net_transaction/'\cf4 \strokec4 \
blob03_path = \cf9 \strokec9 'nbv/in_nbv_azure_net_transaction/'\cf4 \strokec4 \
blob00_path = \cf9 \strokec9 'nbv/net_transaction/'\cf4 \strokec4 \
\pard\pardeftab720\sl320\partightenfactor0
\cf9 \strokec9 """\cf4 \strokec4 \
\cf9 \strokec9 file_list_blob00 = file_list_blob00('raw' , blob_path)\cf4 \strokec4 \
\cf9 \strokec9 file_list_blob03 = file_list_blob03('raw' , blob_path)\cf4 \strokec4 \
\
\cf9 \strokec9 compare_blob_structure(file_list_blob00,file_list_blob03)\cf4 \strokec4 \
\cf9 \strokec9 """\cf4 \strokec4 \
file_list_blob00 = file_list_blob00(\cf9 \strokec9 'raw'\cf4 \strokec4  , blob00_path)\
file_list_blob03 = file_list_blob03(\cf9 \strokec9 'raw'\cf4 \strokec4  , blob03_path)\
\
\pard\pardeftab720\sl320\partightenfactor0
\cf5 \strokec5 # compare_blob_structure(file_list_blob00,file_list_blob03)\cf4 \strokec4 \
get_leaf_files(file_list_blob03)\
get_leaf_files(file_list_blob00)\
\
\pard\pardeftab720\sl320\partightenfactor0
\cf4 \cb1 \
}