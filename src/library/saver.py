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

    #check if the path exists and if it's a file or a directory
    if(Exists(path_to_save)):

        if (Isfile(path_to_save)):
            #path is a single file
            files = [path_to_save]

        elif (Isdir(path_to_save)):
            #path is a directory
            
            #get files of dir
            files = walkInDir(path_to_save)
        

        db = DB()
        lastSaveId = db.get_last_saveid_by_savename(save_name)[0]["max(id)"]

        #mockup
        #lastSaveId = 1

        if (db.create_save(save_name, str(Datetime.now()))):
        #mockup
        #if True:

            
            if(lastSaveId) :
                db_files = db.get_files_of_save(lastSaveId)
            else:
                db_files = []

            
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

                        break

                #no changes for the current file, continue to the next file_to_save
                if not compute_blocks_flag : continue
                else:
                    fileid = db.create_file(file_name, GetSizeOfThis(file), hashfile.hexdigest(), file_dir)



        else:
            output = "Can't create save object in database"

    else:
        output = "The given path doesn't exist on the host."

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