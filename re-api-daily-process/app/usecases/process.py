# pylint: disable=no-member
# utf-8
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from .re_queries import InmoAPI
from time import time
import psutil


class Process:
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.logger.info("All good")
        cpu_usage = psutil.cpu_percent(interval=0.5)
        memory_usage = 100*int(psutil.virtual_memory().total - psutil.virtual_memory().available)/int(psutil.virtual_memory().total)
        self.logger.info(
            "Total % memory use before ETL: {} - Total % CPU use before ETL: {}".format(memory_usage, cpu_usage))
        begin = time()
        self.real_state_api_data = InmoAPI(self.config,
                                           self.params,
                                           self.logger).generate()
        delta = time() - begin
        self.logger.info(f"----- Total runtime of the option is {delta}")
        del cpu_usage
        del memory_usage
        del begin
        del delta

