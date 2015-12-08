import json
import os
from enum import Enum

class Inventory:

    _FILENAME = 'pyback_inventory.json'

    def __init__(self, dir_path):
        self.dir_path = dir_path = unicode(dir_path)
        self.file_path = os.path.join(dir_path, Inventory._FILENAME)
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as infile:
                self._entries = json.load(infile, object_hook=as_enum)
        else:
            self._entries = {}

        contents_list = os.listdir(dir_path)
        for entry in contents_list:
            if (os.path.isfile(os.path.join(dir_path, entry))
            and entry not in self._entries):
                self._entries[entry] = {
                    u'state': FileState.NEW
                }


    def save(self):
        with open(self.file_path, 'w') as outfile:
            json.dump(self._entries, outfile, cls=EnumEncoder)


    def set_state(self, fileName, new_state):
        self._entries[fileName]['state'] = new_state


    def get_state(self, fileName):
        return self._entries[fileName]['state']


class FileState(Enum):
    MISSING = -1
    NEW = 0
    IN_PROGRESS = 1
    UPLOADED = 2
    VERIFIED = 3 # Appears in an inventory

# Stolen from
# http://stackoverflow.com/questions/24481852/serialising-an-enum-member-to-json
class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return {"__enum__": str(obj)}
        return json.JSONEncoder.default(self, obj)

def as_enum(d):
    if "__enum__" in d:
        name, member = d["__enum__"].split(".")
        return getattr(globals()[name], member)
    else:
        return d
