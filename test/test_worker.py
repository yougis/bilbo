import unittest
from oeilnc_config import settings


class TestWorker(unittest.TestCase):

    def custom_startup(worker):
        print(f"Worker {worker} connected to the scheduler.")
        #client = settings.getDaskClient()
        #configFile = settings.initializeBilboProject('.env')
        #client.run(settings.initializeWorkers, configFile)
        print(f"Executing command on worker {worker}")



    custom_startup(None)

    if __name__ == '__main__':

        unittest.main()