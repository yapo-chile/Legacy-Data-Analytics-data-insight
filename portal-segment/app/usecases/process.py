# pylint: disable=no-member
# utf-8
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from .segment import PortalSegment, CarsSegment


class Process():
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.real_state_data = PortalSegment(self.config,
                                            self.params,
                                            self.logger).generate()
        self.cars_state_data = CarsSegment(self.config,
                                            self.params,
                                            self.logger).generate()

        # End