#!/usr/bin/python 
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import AnsibleModule

import mysql.connector

from os import walk as Walk

from os.path import exists as Exists
from os.path import isfile as Isfile
from os.path import isdir as Isdir
from os.path import join as Join
from os.path import getsize as GetSizeOfThis

from ntpath import split as SplitFile

from hashlib import md5

from datetime import datetime as Datetime


DOCUMENTATION = """
module: saver
author: Martin Bonneau
description : Allow to save a file or a folder to another storage server. Can do full or incremental save.

options:
    path_to_save:
        description: The file or folder path to save
        required : yes
"""

EXAMPLES = """
- name : "Example task name"
  saver:
    path_to_save: "/var/www/"
"""

RETURN = """
results:
    description: describe what module return
"""



class DB:
    mydb = None
    save_id = 0
    

    def __init__(self):
        self.mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="root",
            database="save"
        )

    
    def create_save(self, name:str, date:str):
        '''
        @param name : The name of the save
        @param date : The date of the save
        @return : The id if save was create or False
        '''

        query = "INSERT INTO SAVES (name, date) VALUES (%s, %s)"
        values = (name, date)

        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        self.mydb.commit()

        if(cursor.rowcount == 1):
            self.save_id = cursor.lastrowid
            return cursor.lastrowid
        else :
            return False
    

    def create_file(self, name:str, size:int, hash:str, location:str):
        '''
        @param name : The name of the save
        @param date : The date of the save
        @return : The id if save was create or False
        '''

        query = "INSERT INTO FILES (name, size, hash) VALUES (%s, %s, %s)"
        values = (name, str(size), hash)

        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        self.mydb.commit()

        if(cursor.rowcount == 1):
            fileid = cursor.lastrowid

            if( self.create_file_references(fileid, location) > 0):
                return fileid

        return False


    def create_block(self, blocknumber:int, value:str, hash:str, fileid:int):

        query = "INSERT INTO BLOCKS (blocknumber, value, hash) VALUES (%s, %s, %s)"
        values = (str(blocknumber), value, hash)

        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        self.mydb.commit()

        if(cursor.rowcount == 1):
            blockid = cursor.lastrowid

            #insert into savedFiles
            query = "INSERT INTO BLOCKSFILES (fileid, blockid) VALUES (%s, %s);"
            values = (str(fileid), blockid)

            cursor.execute( query, values )

            self.mydb.commit()


            if(cursor.rowcount == 1):
                return blockid

        return False


    def create_file_references(self, fileid:int, location:str, saveid:int=0 ):
        if not saveid : saveid = self.save_id

        #insert into savedFiles
        query = "INSERT INTO SAVEDFILES (fileid, saveid, location) VALUES (%s, %s, %s);"
        values = (str(fileid), str(saveid), location)

        cursor = self.mydb.cursor()
        cursor.execute( query, values )

        self.mydb.commit()

        if(cursor.rowcount == 1):
            return fileid
        
        return False



    def get_files_of_save(self, saveid:int=0):

        if not saveid : saveid = self.save_id

        query = """SELECT files.*, savedfiles.location
                   FROM savedfiles, files
                   WHERE savedfiles.SAVEID = %s
                   AND files.id = savedfiles.FILEID;
        """

        values = (str(saveid),)
        
        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        res = cursor.fetchall()

        if len(res):
            return res
        else:
            return False


    def get_locations_by_fileid(self, fileid:int):


        query = """ SELECT savedfiles.location
                    FROM savedfiles
                    WHERE savedfiles.FILEID = %s;
        """

        values = (str(fileid),)
        
        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        res = cursor.fetchall()

        if len(res):
            return res
        else:
            return False


    def get_hashblocks_of_file(self, fileid:int):

        query = """SELECT blocks.BLOCKNUMBER, blocks.HASH
                   FROM blocksfiles, blocks
                   WHERE blocksfiles.FILEID = %s
                   and  blocks.ID = blocksfiles.BLOCKID;
        """
        values = (str(fileid),)
        
        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        res = cursor.fetchall()

        return res

    def get_blocks_of_file(self, fileid:int, start:int, end:int):

        query = """SELECT blocks.BLOCKNUMBER, blocks.value
                   FROM blocksfiles, blocks
                   WHERE blocksfiles.FILEID = %s
                   and  blocks.ID = blocksfiles.BLOCKID
                   and blocks.BLOCKNUMBER >= """ + str(start) + """
                   and blocks.BLOCKNUMBER < """  + str(end)   + """
                   ORDER BY blocks.BLOCKNUMBER
        """
        values = (str(fileid),)
        
        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        res = cursor.fetchall()

        return res


    def get_saveid_by_savedate(self, date:str):

        query = """SELECT id
                   FROM saves
                   WHERE saves.DATE = %s;
        """
        values = (date,)
        
        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        res = cursor.fetchall()

        if len(res):
            return res
        else:
            return False

    def get_last_saveid_by_savename(self, saveName:str):

        query = """SELECT max(id)
                   FROM saves
                   WHERE saves.NAME = %s;
        """
        values = (saveName,)
        
        cursor = self.mydb.cursor(dictionary=True)
        cursor.execute(query, values)

        res = cursor.fetchall()

        if len(res):
            return res
        else:
            return False

#endclass db




def main():
    module = AnsibleModule(
        argument_spec = dict(
            #arguments here
            path_to_save = dict(required=True, type='str'),
        )
    )
    
    #get params
    #path_to_save = module.params.get("path_to_save")

    #mock data
    path_to_save = r'E:\Shared\p6\save_dir_ansible\example\simple'
    save_name = "MysuperSave1"
    blockSize = 64000
    action = "restore"
    restore_date = "2020-05-13 10:38:28.044540"

    output=""

    db = DB()

    if(action == "save"):

        #check if the path exists and if it's a file or a directory
        if(Exists(path_to_save)):

            if (Isfile(path_to_save)):
                #path is a single file
                files = [path_to_save]

            else:
                #path is a directory
                
                #get files of dir
                files = walkInDir(path_to_save)

            
            lastSaveId = db.get_last_saveid_by_savename(save_name)[0]["max(id)"]

            #mockup
            #lastSaveId = 1

            if (db.create_save(save_name, str(Datetime.now()))):

                db_files = db.get_files_of_save(lastSaveId)


                for file in files:
                    #compute actual md5 of the file
                    hashfile = md5()
                    with open(file, 'rb') as fopen:

                        sliced_content = fopen.read(blockSize)
                        while sliced_content:
                            hashfile.update(sliced_content)
                            sliced_content = fopen.read(blockSize)


                    #get both name and directory of the file
                    file_dir, file_name = SplitFile(file)
                    compute_blocks_flag = True
                    db_file_id = -1


                    #check hash of files
                    for db_file in db_files:
                        
                        if ( db_file["NAME"] == file_name ):

                            #check hashes
                            if ( db_file["HASH"] == hashfile.hexdigest() ):

                                #build array of all locations file
                                file_locations = []
                                for loc in db.get_locations_by_fileid(db_file["ID"]):
                                    file_locations.append(loc["location"])

                                #check if location exists
                                if ( file_dir in file_locations ) :
                                    #no changes for this file
                                    #just insert references for the current save and continue
                                    db.create_file_references(db_file["ID"], db_file["location"])
                                    compute_blocks_flag = False


                                else :
                                    #file already exists but has been moved or copied
                                    #update location
                                    db.create_file_references(db_file["ID"], file_dir)
                                    compute_blocks_flag = False

                            #else, file has been modified, we have to create file and compute blocks
                            db_file_id = db_file["ID"]

                            break

                    #no changes for the current file, continue to the next file_to_save
                    if not compute_blocks_flag : continue
                    else:
                        fileid = db.create_file(file_name, GetSizeOfThis(file), hashfile.hexdigest(), file_dir)


                    #get stored hash_blocks
                    db_hashes = {}
                    for db_hash in db.get_hashblocks_of_file(db_file_id):
                        db_hashes[db_hash["BLOCKNUMBER"]] = db_hash["HASH"]

                    #compute hash of each block
                    with open(file, 'rb') as fopen:

                        #read file and slice it in blockSize
                        block = fopen.read(blockSize)
                        block_number = 0

                        while block:
                            hash_block = md5(block)

                            if( not len(db_hashes) or ( len(db_hashes) < block_number+1 or hash_block.hexdigest() != db_hashes[block_number])):
                                #hash are differents, we have to reupload the block
                                db.create_block(block_number, block, hash_block.hexdigest(), fileid)
                            else:
                                db.create_block_references(hash_block.hexdigest(), fileid)

                            block_number += 1
                            block = fopen.read(blockSize)
                    
                    output = 'saved 100 per 100 ok'
                    
            
            else:
                output = "Can't create save object in database"

        else:
            output = "The given path doesn't exist on the host."


    elif (action == "restore"):

        if (restore_date == None)   : lastSaveId = db.get_last_saveid_by_savename(save_name)[0]["max(id)"]
        else                        : lastSaveId = db.get_saveid_by_savedate(restore_date)[0]["id"]

        for restore_file in db.get_files_of_save(lastSaveId):
            
            #if folder doesn't exists, create it
            if ( not (Exists(restore_file["location"]) and Isdir(restore_file["location"]))): makedirs(restore_file["location"])

            #erase / create file
            restored_file = open(Join(restore_file["location"] + '/', restore_file["NAME"]), 'wb')
            restored_file.close()

            block_index = 0
            blocks = db.get_blocks_of_file(restore_file["ID"], block_index, block_index+5)

            while (blocks):
                with open(Join(restore_file["location"] + '/', restore_file["NAME"]), 'ab+') as restored_file:
                    for block in blocks:
                        restored_file.write(bytes(block["value"], encoding="utf8"))
                
                block_index += 5
                blocks = db.get_blocks_of_file(restore_file["ID"], block_index, block_index+5)




    else:
        output = "Unknow action \"" + action + "\""


    #export something to ansible output
    module.exit_json(changed=True, ansible_module_results=output)


def walkInDir(dir_path):
    files_export = []

    if (Exists(dir_path) and Isdir(dir_path)):

        for root, dirs, files in Walk(dir_path, topdown=False):
            for name in files:
                #append the filepath to the array
                files_export.append(Join(root, name))
    
    return files_export


if __name__ == "__main__":
    main()