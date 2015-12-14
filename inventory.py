from enum import Enum
# import cli
import json
import os


class Inventory:

    _FILENAME = u'pyback_inventory.json'

    def __init__(self, dir_path):
        self.dir_path = dir_path = unicode(dir_path)
        self.file_path = os.path.join(dir_path, Inventory._FILENAME)
        self._entries = {}
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as infile:
                deserialized = json.load(infile,
                                         object_hook=EnumEncoder.as_enum)
                for key, obj in deserialized.iteritems():
                    self._entries[key] = InventoryEntry.deserialize(
                        self, key, obj)
        else:
            self._add_new_file(Inventory._FILENAME)

        contents_list = os.listdir(dir_path)
        for name in contents_list:
            if (os.path.isfile(os.path.join(dir_path, name))
                    and name not in self._entries):
                self._add_new_file(name)

    def save(self):
        with open(self.file_path, 'w') as outfile:
            outfile.write("{")
            for index, key in enumerate(self._entries):
                if index:
                    outfile.write(",")
                outfile.write("\"%s\": " % key)
                self._entries[key].serialize(outfile)
            outfile.write("}")

    def get_by_state(self, state):
        for key, entry in self._entries.iteritems():
            if (entry.get_state() == state
                    and key != Inventory._FILENAME):
                yield entry

    def get_inventory_file_entry(self):
        return self._entries[Inventory._FILENAME]

    def get_full_path(self, filename):
        return os.path.join(self.dir_path, filename)

    def _add_new_file(self, filename):
        self._entries[filename] = InventoryEntry(
            self, filename, FileState.NEW)


class InventoryEntry:

    _in_progress_attrs = ('_uploadId', '_partSize',
                          '_partsUploaded')

    def __init__(self, inventory, fileName, state=None):
        self._inventory = inventory
        self._fileName = fileName
        if state is None:
            state = FileState.NEW
        self._state = state
        self._uploads = []

    def get_state(self):
        return self._state

    def get_fileName(self):
        return self._fileName

    def get_filePath(self):
        return self._inventory.get_full_path(self._fileName)

    def get_part_size(self):
        return self._partSize

    def get_upload_id(self):
        return self._uploadId

    def get_parts_uploaded(self):
        return self._partsUploaded

    def set_state_from_upload(self, file_upload, state):
        if state == FileState.IN_PROGRESS:
            self._uploadId = file_upload.get_upload_id()
            self._partSize = file_upload.get_part_size()
            self._partsUploaded = file_upload.get_parts_uploaded()
            self._state = FileState.IN_PROGRESS
        elif state == FileState.UPLOADED:
            self._add_upload(file_upload)
            for attr_name in self._in_progress_attrs:
                del self.__dict__[attr_name]
            self._state = FileState.UPLOADED
        else:
            raise Exception("Unhandled file state: " + file_upload.get_state())

        self._inventory.save()

    def serialize(self, outfile):
        me = self.__dict__.copy()
        del me['_inventory']
        del me['_fileName']
        if len(self._uploads) == 0:
            del me['_uploads']
        json.dump(me, outfile, cls=EnumEncoder)

    @classmethod
    def deserialize(cls, inventory, fileName, obj):
        me = cls(inventory, fileName, obj['_state'])
        if '_uploads' in obj and len(obj['_uploads']):
            me._uploads = obj['_uploads']
        for attr_name in cls._in_progress_attrs:
            if attr_name in obj:
                setattr(me, attr_name, obj[attr_name])
        return me

    def _add_upload(self, file_upload):
        self._uploads.append({
            'end_time': file_upload.get_end_time(),
            'checksum': file_upload.get_checksum(),
            'archiveId': file_upload.get_archive_id(),
            'location': file_upload.get_upload_location()
        })


class FileState(Enum):
    MISSING = -1    # uploaded but now missing from local dir
    NEW = 0         # in local dir but not uploaded
    IN_PROGRESS = 1
    UPLOADED = 2
    MODIFIED = 3    # modified locally since uploading


# Stolen from
# http://stackoverflow.com/questions/24481852/serialising-an-enum-member-to-json
class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return {"__enum__": str(obj)}
        return json.JSONEncoder.default(self, obj)

    @staticmethod
    def as_enum(d):
        if "__enum__" in d:
            name, member = d["__enum__"].split(".")
            return getattr(globals()[name], member)
        else:
            return d
