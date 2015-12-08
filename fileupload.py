from treehash import TreeHash
import botocore
import cli
import os
import time

class FileUpload:

    def __init__(self, vaultName, filePath):
        self._filePath = filePath
        self._vaultName = vaultName
        self._fileName = os.path.basename(filePath)
        self._fileSizeBytes = os.path.getsize(self._filePath)
        self._partSize = get_best_part_size(self._fileSizeBytes)
        self._partNumUploading = 0


    def formattedFileSize(self):
        if not hasattr(self, '_formattedFileSize'):
            self._formattedFileSize = cli.format_filesize(self._fileSizeBytes)
        return self._formattedFileSize


    def formattedPartSize(self):
        if not hasattr(self, '_formattedPartSize'):
            self._formattedPartSize = cli.format_filesize(self._partSize, 0)
        return self._formattedPartSize


    def upload(self, client):

        self._upload = client.initiate_multipart_upload(
            vaultName=self._vaultName,
            archiveDescription=self._fileName,
            partSize=str(self._partSize))

        treehash = TreeHash()
        partBegin = 0
        self._partNumUploading = 0
        with open(self._filePath, "rb") as f:
            while partBegin < self._fileSizeBytes:
                partEnd = partBegin + self._partSize - 1
                if partEnd > self._fileSizeBytes:
                    partEnd = self._fileSizeBytes - 1

                part = f.read(self._partSize)
                treehash.update(part)

                if partBegin == 0:
                    self._startTime = time.time()
                self._upload_part(client, part, partBegin, partEnd)
                partBegin = partEnd + 1
                self._partNumUploading += 1

        response = client.complete_multipart_upload(
            vaultName=self._vaultName,
            uploadId=self._upload['uploadId'],
            archiveSize=str(self._fileSizeBytes),
            checksum=treehash.hexdigest())

        cli.cli_progress(self._fileName,
            self.formattedFileSize(),
            self.formattedPartSize(),
            self._startTime,
            self._fileSizeBytes-1,
            self._fileSizeBytes-1)

        return response


    def _upload_part(self,
                     client,
                     part,
                     partBegin,
                     partEnd):

        cli.cli_progress(self._fileName,
            self.formattedFileSize(),
            self.formattedPartSize(),
            self._startTime,
            partBegin,
            self._fileSizeBytes-1)

        for upload_attempt in range(0, 2):
            # print 'Uploading bytes %d through %d (%d%%)...' % (
            #     partBegin, partEnd, float(partEnd)/(self._fileSizeBytes-1)*100)
            try:
                response = client.upload_multipart_part(
                    vaultName=self._vaultName,
                    uploadId=self._upload['uploadId'],
                    range='bytes %d-%d/*' % (partBegin, partEnd),
                    body=part)
                return response
            except botocore.exceptions.ClientError, e:
                print "\n" + e
                print "Retrying..."

            print "\nFAILED"

def get_best_part_size(fileSizeBytes):
    # We want the smallest possible part size. Maximum parts is 10,000.
    # So we find the first part size larger than file_len/10,000.
    targetSize = fileSizeBytes / 10000
    partSize = 1048576  # min size 1 MB
    while partSize < targetSize:
        partSize *= 2
        if partSize > targetSize or partSize == 4294967296:  # max size 4GB
            break
    return partSize
