from inventory import FileState
from treehash import TreeHash
import botocore
import cli
import os
import time


class FileUpload:

    def __init__(self, vaultName, inventory_entry):
        self._startTime = 0
        self._vaultName = vaultName
        self._inventory_entry = inventory_entry
        self._fileSizeBytes = os.path.getsize(
            self._inventory_entry.get_filePath())

        if inventory_entry.get_state() == FileState.IN_PROGRESS:
            self._upload_id = inventory_entry.get_upload_id()
            self._partSize = inventory_entry.get_part_size()
            self._partNumUploading = inventory_entry.get_parts_uploaded()
        else:
            self._partSize = self._get_best_part_size(self._fileSizeBytes)
            self._partNumUploading = 0

    def get_state(self):
        return self._inventory_entry.get_state()

    def get_parts_uploaded(self):
        return self._partNumUploading

    def get_part_size(self):
        return self._partSize

    def get_upload_id(self):
        return self._upload_id

    def get_end_time(self):
        return self._endTime

    def get_checksum(self):
        return self._checksum

    def get_http_status(self):
        return self._http_status

    def get_archive_id(self):
        return self._archive_id

    def get_upload_location(self):
        return self._upload_location

    def formattedFileSize(self):
        if not hasattr(self, '_formattedFileSize'):
            self._formattedFileSize = cli.format_filesize(self._fileSizeBytes)
        return self._formattedFileSize

    def formattedPartSize(self):
        if not hasattr(self, '_formattedPartSize'):
            self._formattedPartSize = cli.format_filesize(self._partSize, 0)
        return self._formattedPartSize

    def upload(self, client):

        if (self._inventory_entry.get_state() == FileState.IN_PROGRESS):
            self._upload_id = self._inventory_entry.get_upload_id()
        else:
            tmp_upload = client.initiate_multipart_upload(
                vaultName=self._vaultName,
                archiveDescription=self._inventory_entry.get_fileName(),
                partSize=str(self._partSize))
            self._upload_id = tmp_upload['uploadId']

        if self._partSize < self._fileSizeBytes:
            self._inventory_entry.set_state_from_upload(
                self, FileState.IN_PROGRESS)

        partBegin = self._partNumUploading * self._partSize
        data = b""
        with open(self._inventory_entry.get_filePath(), "rb") as f:
            if partBegin:
                data = f.read(partBegin)
            treehash = TreeHash(data=data, block_size=self._partSize)
            while partBegin < self._fileSizeBytes:
                partEnd = partBegin + self._partSize - 1
                if partEnd > self._fileSizeBytes:
                    partEnd = self._fileSizeBytes - 1

                part = f.read(self._partSize)
                treehash.update(part)

                if not self._startTime:
                    self._startTime = time.time()

                self._upload_part(client, part, partBegin, partEnd)
                partBegin = partEnd + 1
                self._partNumUploading += 1

                if partEnd < self._fileSizeBytes:
                    self._inventory_entry.set_state_from_upload(
                        self, FileState.IN_PROGRESS)

        completed_treehash = treehash.hexdigest()
        response = client.complete_multipart_upload(
            vaultName=self._vaultName,
            uploadId=self._upload_id,
            archiveSize=str(self._fileSizeBytes),
            checksum=completed_treehash)

        self._endTime = time.time()

        cli.cli_progress(self._inventory_entry.get_fileName(),
                         self.formattedFileSize(),
                         self.formattedPartSize(),
                         self._startTime,
                         self._fileSizeBytes-1,
                         self._fileSizeBytes-1)

        # Sanity check that's probably unnecessary.
        if treehash.hexdigest() != response['checksum']:
            raise Exception('checksum mismatch')

        self._checksum = response['checksum']
        self._http_status = response['ResponseMetadata']['HTTPStatusCode']
        self._archive_id = response['archiveId']
        self._upload_location = response['location']
        # cli.pp(json.dumps(self, default=lambda o: o.__dict__))

        self._inventory_entry.set_state_from_upload(self, FileState.UPLOADED)

    def _upload_part(self,
                     client,
                     part,
                     partBegin,
                     partEnd):

        cli.cli_progress(self._inventory_entry.get_fileName(),
                         self.formattedFileSize(),
                         self.formattedPartSize(),
                         self._startTime,
                         partBegin,
                         self._fileSizeBytes-1)

        for upload_attempt in range(0, 2):
            print '\nUploading bytes %d through %d (%d%%)...' % (
                partBegin, partEnd,
                float(partEnd)/(self._fileSizeBytes-1)*100)
            try:
                response = client.upload_multipart_part(
                    vaultName=self._vaultName,
                    uploadId=self._upload_id,
                    range='bytes %d-%d/*' % (partBegin, partEnd),
                    body=part)
                return response

            except botocore.exceptions.ClientError, e:
                print "\n"
                print e
                print "Retrying..."

            print "\nFAILED"

    def _get_best_part_size(self, fileSizeBytes):
        # We want the smallest possible part size. Maximum parts is 10,000.
        # So we find the first part size larger than file_len/10,000.
        targetSize = fileSizeBytes / 10000
        partSize = 1048576  # min size 1 MB
        while partSize < targetSize:
            partSize *= 2
            if partSize > targetSize or partSize == 4294967296:  # max size 4GB
                break
        return partSize
