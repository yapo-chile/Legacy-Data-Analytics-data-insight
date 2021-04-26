# pylint: disable=no-member
# utf-8
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from .re_queries import InmoAPI3
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

    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.execute_command(query.delete_base())
        db.insert_data(self.data_athena)
        db.insert_data(self.data_dwh)
        db.close_connection()

    # Query data from data warehouse
    @property
    def data_dwh(self):
        return self.__data_dwh

    @data_dwh.setter
    def data_dwh(self, config):
        query = Query(config, self.params)
        db_source = Database(conf=config)
        data_dwh = db_source.select_to_dict(query\
                                            .query_base_postgresql())
        db_source.close_connection()
        self.__data_dwh = data_dwh

    # Query data from Pulse bucket
    @property
    def data_athena(self):
        return self.__data_athena

    @data_athena.setter
    def data_athena(self, config):
        athena = Athena(conf=config)
        query = Query(config, self.params)
        data_athena = athena.get_data(query.query_base_athena())
        athena.close_connection()
        self.__data_athena = data_athena

    def generate(self):
        self.logger.info("All good")
        cpu_usage = psutil.cpu_percent(interval=0.5)
        memory_usage = 100*int(psutil.virtual_memory().total - psutil.virtual_memory().available)/int(psutil.virtual_memory().total)
        self.logger.info(
            "Total % memory use before ETL: {} - Total % CPU use before ETL: {}".format(memory_usage, cpu_usage))
        begin = time()
        self.real_state_api_data = InmoAPI3(self.config,
                                           self.params,
                                           self.logger).generate()
        delta = time() - begin
        self.logger.info(f"----- Total runtime of the option is {delta}")
        del cpu_usage
        del memory_usage
        del begin
        del delta

