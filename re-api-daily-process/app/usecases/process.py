# pylint: disable=no-member
# utf-8
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from .re_queries_op1 import InmoAPI1
from .re_queries_op2 import InmoAPI2
from .re_queries_op3 import InmoAPI3
from time import time


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
        rank = {}
        # for option in [1, 2, 3]:
        option = 2 # OPTION FIXATED FOR TESTING PURPOSES
        begin = time()
        self.logger.info("Applying option {}".format(str(option)))
        if option == 1:
            self.real_state_api_data = InmoAPI1(self.config,
                                             self.params,
                                             self.logger).generate(True)
        elif option == 2:
            self.real_state_api_data = InmoAPI2(self.config,
                                               self.params,
                                               self.logger).generate(True)
        elif option == 3:
            self.real_state_api_data = InmoAPI3(self.config,
                                               self.params,
                                               self.logger).generate(True)
        delta = time() - begin
        self.logger.info(f"Total runtime of the option is {delta}")
        rank[option] = delta
        sorted_tuples = sorted(rank.items(), key=lambda item: item[1])
        sorted_rank = {k: v for k, v in sorted_tuples}
        for item in sorted_rank.items():
            self.logger.info("Option {} got runtime of {}".format(str(item[0]), str(item[1])))
