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

    def upload(self, client):

        self._fileSizeBytes = os.path.getsize(self._filePath)
        self._partSize = self._get_best_part_size()
        print '%s is %d MB. Using part size of %d MB.\n' % (
            self._fileName, self._fileSizeBytes/1024/1024, self._partSize/1024)

        self._upload = client.initiate_multipart_upload(
            vaultName=self._vaultName,
            archiveDescription=self._fileName,
            partSize=str(self._partSize))

        treehash = TreeHash()
        partBegin = 0
        with open(self._filePath, "rb") as f:
            self._startTime = time.time()
            while partBegin < self._fileSizeBytes:
                partEnd = partBegin + self._partSize - 1
                if partEnd > self._fileSizeBytes:
                    partEnd = self._fileSizeBytes - 1

                part = f.read(self._partSize)
                treehash.update(part)

                self._upload_part(client, part, partBegin, partEnd)
                partBegin = partEnd + 1

        response = client.complete_multipart_upload(
            vaultName=self._vaultName,
            uploadId=self._upload['uploadId'],
            archiveSize=str(self._fileSizeBytes),
            checksum=treehash.hexdigest())

        return response


    def _upload_part(self,
                     client,
                     part,
                     partBegin,
                     partEnd):

        for upload_attempt in range(0, 2):
            # print 'Uploading bytes %d through %d (%d%%)...' % (
            #     partBegin, partEnd, float(partEnd)/(self._fileSizeBytes-1)*100)
            cli.cli_progress(self._fileName, self._startTime, partEnd, self._fileSizeBytes-1)
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

    def _get_best_part_size(self):
        # We want the smallest possible part size. Maximum parts is 10,000.
        # So we find the first part size larger than file_len/10,000.
        targetSize = self._fileSizeBytes / 10000
        self._partSize = 1048576  # min size 1 MB
        while self._partSize < targetSize:
            self._partSize *= 2
            if self._partSize > targetSize or self._partSize == 4294967296:  # max size 4GB
                break
        return self._partSize
