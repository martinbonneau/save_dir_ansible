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