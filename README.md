# Save_dir_ansible module
This project is an ansible module that permits to save and restore a file or directory by the incremental method.
You can change the size of saved blocks.

## How does it works ?

Save :
1. The script scan files of the directory (or skip this step if you just want to save a file)
2. For each file, the script computes the hash of the entire file
3. If file is already saved or if the last saved hash is equal, continue throught the next file
4. If the file is different then the script compute the hash of each block of the file and compares it with the last saved one
5. If the hashes are different the blocks will be saved

Restoration :
It just recreates files and replaces the old with the new


### More in depht

Logical stuff is done by using a SGBD (here, mysql)
The blocks content is stored throught FTP

There are two ways to use this module :
- save content
- restore content

In "save" mode, the script saves (recursively) all the files of the given folder (or just the file if the path targets a single file). If you save it with the same name, the script only saves the modified content.
For example, let's suppose that you have 2 files, the one containing 112KiB and the other one 212KiB : if the latter block changes then the script only saves this block and skips the other one.
Finally, it costs 408KiB to the storage (the first save costs 112 + 212 = 324 ; the last block of the second file costs 84KiB)

Note that if you have just moved or copied a file, it will not be saved again (only the path is updated).


In "restore" mode, the script recreates all the files of the saves section in the same location.
If other files exist they are not erased.
If a saved file is modified then it's totally erased during the process.
By default, the script restores the last performed save, but you can specify a date in order to restore a specific save.


## Prerequists

- SGBD (I used MySQL but all should works, by adapting sql requests)
- FTP Server
- Ansible
- Python3 (in both ansible server and clients)
- Create the schemes of the database, found in the "diagrams" folder


You should have to install this module :
```bash
    pip3 install mysql-connector-python
```

Be carefull with the user who installs the python module. Maybe you have to install it globally with sudo (if ansible uses your root account) :
```
    sudo -H pip3 install mysql-connector-python
```


# How to use it ?

There are two ways to use this module :
- save content
- restore content



## How to perform a Save ?

1. Open the `save.yml`
2. Change values, but keep the `action: "save"`
3. `block_size` is optionnal (default is 4096). It works in Bytes. Remove this argument if not used.
4. Construct an inventory file and change both `hosts` and `connection` arguments by your conveniencs if need be.
5. Launch the module (and force ansible to use python3) with :

`ansible-playbook [-i your_inventory_file.yml] save.yml -e "ansible_python_interpreter=/usr/bin/python3"`


## How to perform a Restoration ?

1. Open the `restore.yml`
2. Change values, but keep the `action: "restore"`
3. `restore_date` is only needed if you want to restore at a specific date, remove it if not.
4. Construct an inventory file and change both `hosts` and `connection` arguments by your conveniencs if need be.
5. Launch the module (and force ansible to use python3) with :

`ansible-playbook [-i your_inventory_file.yml] restore.yml -e "ansible_python_interpreter=/usr/bin/python3"`
