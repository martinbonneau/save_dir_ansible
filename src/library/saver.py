#!/usr/bin/python 
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import AnsibleModule

DOCUMENTATION = """
module: module_name
author: author_name
description : description

options:
    arg:
        description: description of arg
        required : yes/no
"""

EXAMPLES = """
- name : "Task name example"
  module_name:
    arg: "value"
"""

RETURN = """
results:
    description: describe what module return
"""


def main():
    module = AnsibleModule(
        argument_spec = dict(
            #arguments here
            arg = dict(required=True, type='str'),
        )
    )

    #get params
    arg = module.params.get("arg")

    #do something
    output = arg

    #export something to ansible output
    module.exit_json(changed=True, ansible_module_results=output)


if __name__ == "__main__":
    main()