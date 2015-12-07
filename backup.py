from inventory import Inventory
from treehash import TreeHash
import boto3
import botocore
import json
import os
import pdb
import pprint
import sys
import time
import yaml


config = yaml.safe_load(open('config.yaml'))

client = boto3.client('glacier')
rows, columns = os.popen('stty size', 'r').read().split()


def get_rate(startTime, val):
    elapsed = time.time() - startTime
    if elapsed:
        rate = val / 1024 / elapsed
    else:
        rate = 0
    return "{1:.2f} KB/sec".format(elapsed, rate)


def cli_progress(filename, startTime, current_val, end_val, bar_length=20):
    percent = float(current_val) / end_val
    hashes = '#' * int(round(percent * bar_length))
    spaces = ' ' * (bar_length - len(hashes))
    output = "\r[{0}] {1} {2}/{3} {4}% ({5})".format(
        hashes + spaces, filename, current_val, end_val,
        int(round(percent * 100)), get_rate(startTime, current_val))
    sys.stdout.write(output.ljust(int(columns)))
    if current_val == end_val:
        sys.stdout.write('\n')
    sys.stdout.flush()


def get_best_part_size(archiveLenBytes):
    # We want the smallest possible part size. Maximum parts is 10,000.
    # So we find the first part size larger than file_len/10,000.
    targetSize = archiveLenBytes / 10000
    partSize = 1048576  # min size 1 MB
    while partSize < targetSize:
        partSize *= 2
        if partSize > targetSize or partSize == 4294967296:  # max size 4GB
            break
    return partSize


def upload_file_part(vaultName,
                     uploadId,
                     fileName,
                     startTime,
                     part,
                     partBegin,
                     partEnd,
                     fileSizeBytes):

    for upload_attempt in range(0, 2):
        # print 'Uploading bytes %d through %d (%d%%)...' % (
        #     partBegin, partEnd, float(partEnd)/(fileSizeBytes-1)*100)
        cli_progress(fileName, startTime, partEnd, fileSizeBytes-1)
        try:
            response = client.upload_multipart_part(
                vaultName=config['vaultName'],
                uploadId=uploadId,
                range='bytes %d-%d/*' % (partBegin, partEnd),
                body=part)
            return response
        except botocore.exceptions.ClientError, e:
            print "\n" + e
            print "Retrying..."

        print "\nFAILED"


def pp(data):
    pp = pprint.PrettyPrinter(indent=4, width=columns)
    pp.pprint(data)


def upload_file(vaultName, filePath):

    treehash = TreeHash()
    fileSizeBytes = os.path.getsize(filePath)
    partSize = get_best_part_size(fileSizeBytes)
    fileName = os.path.basename(filePath)
    print '%s is %d MB. Using part size of %d MB.\n' % (
        fileName, fileSizeBytes/1024/1024, partSize/1024)

    upload = client.initiate_multipart_upload(
        vaultName=config['vaultName'],
        archiveDescription=fileName,
        partSize=str(partSize))

    startTime = time.time()
    partBegin = 0
    with open(filePath, "rb") as f:
        while partBegin < fileSizeBytes:
            partEnd = partBegin + partSize - 1
            if partEnd > fileSizeBytes:
                partEnd = fileSizeBytes - 1

            part = f.read(partSize)
            treehash.update(part)

            upload_file_part(vaultName, upload['uploadId'], fileName,
                             startTime, part, partBegin, partEnd,
                             fileSizeBytes)
            partBegin = partEnd + 1

    response = client.complete_multipart_upload(
        vaultName=config['vaultName'],
        uploadId=upload['uploadId'],
        archiveSize=str(fileSizeBytes),
        checksum=treehash.hexdigest())

    if response:
        pp(response)


def list_jobs(vaultName):

    response = client.list_jobs(vaultName=vaultName)
    pp(response)


def perform_inventory(vaultName):

    glacier = boto3.resource('glacier')
    vault = glacier.Vault(config['accountId'], vaultName)
    job = vault.initiate_inventory_retrieval()
    pp(job)


def fetch_inventory(vaultName, jobId):

    glacier = boto3.resource('glacier')
    job = glacier.Job(config['accountId'], vaultName, jobId)
    output = job.get_output()
    pp(output)
    inventory = json.loads(output['body'].read())
    pp(inventory)


def sync(vaultName):

    # glacier = boto3.resource('glacier')
    # vault = glacier.Vault(config['accountId'], vaultName)
    inventory = Inventory('/home/iolsen/test_pyback')
    pp(inventory._entries)

    inventory.save()

# perform_inventory(config['vaultName'])
# list_jobs(config['vaultName'])
# fetch_inventory(config['vaultName'], 'WzsxmkG8F0Vca-cxMNuqkBCxgpHP4a-aHaGW2a5bw2yG_MlNKOittFhg2sJEiADSafdZsBIWPNEMQUejNVqDYHc6tK2z')

# upload_file(config['vaultName'], config['filePath'])

sync(config['vaultName'])
