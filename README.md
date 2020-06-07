# Save_dir_ansible module
This is an ansible module that permit to save and restore a file or directory in incremental method.
You can change the size of saved blocks.

## How does it work ?

Save :
1. The script scan files of the directory (or skip this step if you just want to save a file)
2. For each file, the script compute the hash of the entire file
3. If file is already saved or if the last saved hash is equal, continue throught next file
4. The file is different : the script compute the hash of each block of the file and compare it with the last saved one
5. If hashes are differents, it'll save blocks

Restoration :
It just recreate files and replace old by new


### More in depht

Logical stuff is done by using a SGBD (here, mysql) .
Blocks content are stored throught FTP.

There's two ways to use this module :
- save content
- restore content

In "save" mode, the script will save all files (recursively) of the given folder (or just the file if the path target a single file). If you perform another save with the same name, the script will only save the modified content.
For example, if you have 2 files, one of 112KiB and the other of 212KiB, and that the last block of the 212KiB's file is changed : the script will only save this block and skip other.
Finally, it will cost 408KiB of storage (first save of 112 + 212 = 324 ; and the last block of the second file : 84KiB)

Note that if you just move or copy a file, it will not be save again (just the path is updated).


In "restore" mode, the script will recreate all files of the save, in the same location as saved.
If other files exists, they will be not erased.
If a saved file is modified, it will be totally erased in the process.
By default, the script will restore the last performed save, but you can specify a date in order to restore a specific save.


## Prerequists

- SGBD (I used MySQL but all should works, by adapting sql requests)
- FTP Server
- Ansible
- Python3 (in both ansible server and clients)
- Create the schemas of the database, found in the "diagrams" folder


You should have to install this module :
```bash
    pip3 install mysql-connector-python
```

Be carefull with the user who install the python module. Maybe you have to install globally with sudo (if ansible use your root account) :
```
    sudo -H pip3 install mysql-connector-python
```


# How to use ?

There's two ways to use this module :
- save content
- restore content



## How to perform a Save ?

1. Open the `save.yml`
2. Change values, but keep the `action: "save"`
3. `block_size` is optionnal (default is 4096). It works in Bytes. Remove this argument if not used.
4. If needed, construct an inventory file and change both `hosts` and `connection` argument by your conveniences
5. Launch the module (and force ansible to use python3) with :

`ansible-playbook [-i your_inventory_file.yml] save.yml -e "ansible_python_interpreter=/usr/bin/python3"`


## How to perform a Restoration ?

1. Open the `restore.yml`
2. Change values, but keep the `action: "restore"`
3. `restore_date` is only needed if you want to restore at a specific date, remove it if not.
3. If needed, construct an inventory file and change hosts and connection argument by your conveniences
4. Launch the module (and force ansible to use python3) with :

`ansible-playbook [-i your_inventory_file.yml] restore.yml -e "ansible_python_interpreter=/usr/bin/python3"`
