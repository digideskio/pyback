from fileupload import FileUpload
from inventory import Inventory
from inventory import FileState
import boto3
import cli
import json
import yaml


config = yaml.safe_load(open('config.yaml'))
client = boto3.client('glacier')


def upload_file(inventory_entry):
    file_upload = FileUpload(config['vaultName'], inventory_entry)
    file_upload.upload(client)


def list_jobs(vaultName):

    response = client.list_jobs(vaultName=vaultName)
    cli.pp(response)


def perform_inventory(vaultName):

    glacier = boto3.resource('glacier')
    vault = glacier.Vault(config['accountId'], vaultName)
    job = vault.initiate_inventory_retrieval()
    cli.pp(job)


def fetch_inventory(vaultName, jobId):

    glacier = boto3.resource('glacier')
    job = glacier.Job(config['accountId'], vaultName, jobId)
    output = job.get_output()
    cli.pp(output)
    inventory = json.loads(output['body'].read())
    cli.pp(inventory)


def sync(vaultName):

    inventory = Inventory('/home/iolsen/test_pyback')
    # cli.pp(inventory._entries)
    inventory.save()
    for entry in inventory.get_by_state(FileState.IN_PROGRESS):
        upload_file(entry)
    for entry in inventory.get_by_state(FileState.NEW):
        upload_file(entry)
    entry = inventory.get_inventory_file_entry()
    if entry.get_state() != FileState.UPLOADED:
        upload_file(entry)

# perform_inventory(config['vaultName'])
# list_jobs(config['vaultName'])
# fetch_inventory(config['vaultName'],
# '-YDD4AVvtcn6rn7zEYz8SF2HzNdLqqIhRnduONtSTz40jOBfAvIvycrfGJNijSefJHDS8D8A9tOCNxv6akFckF81Z493')

# upload_file(config['filePath'])

sync(config['vaultName'])

# inventory = Inventory('/home/iolsen/test_pyback')
# for entry in inventory._entries.itervalues():
#     cli.pp(entry.__dict__)
