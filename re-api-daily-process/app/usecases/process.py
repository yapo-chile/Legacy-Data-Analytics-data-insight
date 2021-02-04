# pylint: disable=no-member
# utf-8
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from .re_queries_op2 import InmoAPI2
from .re_queries_op3 import InmoAPI3
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
        # self.logger.info(str(config))
        # JUST FOR DEBUGGING, IT'S RISKY TO TRY TO PRINT SECRETS (they don't appear, but still)

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
        # for option in [1, 2, 3]:
        cpu_usage = psutil.cpu_percent(interval=0.5)
        memory_usage = int(psutil.virtual_memory().total - psutil.virtual_memory().available)
        if cpu_usage <= 60 and memory_usage <= 60:  # is the machine ready for multiprocessing?
            option = 2
        else:  # choose sequential option is the machine is busy
            option = 3
        option = 2  # OPTION FIXATED FOR TESTING PURPOSES
        # bump
        begin = time()
        self.logger.info("----- Applying option {}".format(str(option)))
        if option == 2:  # parallel option
            self.real_state_api_data = InmoAPI2(self.config,
                                               self.params,
                                               self.logger).generate()
        elif option == 3:  # sequential option
            self.real_state_api_data = InmoAPI3(self.config,
                                               self.params,
                                               self.logger).generate()
        delta = time() - begin
        self.logger.info(f"----- Total runtime of the option is {delta}")
        self.logger.info("Total memory use of ETL: {} - Total CPU use of ETL: {}".format(memory_usage, cpu_usage))
        del cpu_usage
        del memory_usage
        del option
        del begin
        del delta
