#!/usr/bin/python 
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import AnsibleModule

from os import walk as Walk

from os.path import exists as Exists
from os.path import isfile as Isfile
from os.path import isdir as Isdir
from os.path import join as Join


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



def main():
    module = AnsibleModule(
        argument_spec = dict(
            #arguments here
            path_to_save = dict(required=True, type='str'),
        )
    )

    #get params
    path_to_save = module.params.get("path_to_save")

    #check if the path exists and if it's a file or a directory
    if(Exists(path_to_save)):

        if (Isfile(path_to_save)):
            #path is a single file
            output = "single file"

        elif (Isdir(path_to_save)):
            #path is a directory
            
            #walk inside it
            files = walkInDir(path_to_save)

        else:
            output = "The given path is not a file or a directory."

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