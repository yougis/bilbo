import dask.distributed
from oeilnc_config import settings
from distributed import Worker
import dask
import logging

def custom_startup(worker):
    print(f"Worker {worker.address} connected to the scheduler.")
    client = settings.getDaskClient()
    configFile = settings.initializeBilboProject('.env')
    client.run(settings.initializeWorkers, configFile)
    print(f"Executing command on worker {worker.id}")

logging.info("preload script for worker")
