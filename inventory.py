import json
import os
import enum

class Inventory:

    _FILENAME = 'pyback_inventory.json'

    def __init__(self, dir_path):
        self.dir_path = dir_path = unicode(dir_path)
        self.file_path = os.path.join(dir_path, Inventory._FILENAME)
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as infile:
                self.entries = json.load(infile)
        else:
            self.entries = {}

        contents_list = os.listdir(dir_path)
        for entry in contents_list:
            if (os.path.isfile(os.path.join(dir_path, entry))
            and entry not in self.entries):
                self.entries[entry] = {}


    def save(self):
        with open(self.file_path, 'w') as outfile:
            json.dump(self.entries, outfile)
