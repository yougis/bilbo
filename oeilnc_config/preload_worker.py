from oeilnc_config import settings
from dask.distributed import  Variable
import dask
import logging

configFile = Variable(name="configFile")

def custom_startup(worker):
    print(f"Worker {worker.address} connected to the scheduler.")
    client = settings.getDaskClient()
    client.run(settings.initializeWorkers)
    print(f"Executing command on worker {worker.id}")

logging.info("preload script for worker")
