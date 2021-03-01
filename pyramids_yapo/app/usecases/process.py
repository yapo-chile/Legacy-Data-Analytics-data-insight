# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import RePyramidsYapoQuery
from utils.query import CarsPyramidsYapoQuery
from utils.read_params import ReadParams
from usecases.re_segment import RePyramidsYapo
from usecases.cars_segment import CarsPyramidsYapo


class Process():
    """
    class Process
    """
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger


    def generate(self):
        """
        generate
        """
        self.re_pyramid_yapo=RePyramidsYapo(self.config,
                                        self.params,
                                        self.logger).generate()

        self.cars_pyramid_yapo= CarsPyramidsYapo(self.config,
                                self.params,
                                self.logger).generate()