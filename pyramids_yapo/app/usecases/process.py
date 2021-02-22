# pylint: disable=no-member
# utf-8
import logging
# from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import RePyramidsYapoQuery
from utils.query import CarsPyramidsYapoQuery
from utils.read_params import ReadParams
from .re_segment import RePyramidsYapo
from .cars_segment import CarsPyramidsYapo


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
        self.re.pyramid_yapo=RePyramidsYapo(self.config,
                                        self.params,
                                        self.logger).generate()

        self.cars.pyramid_yapo= CarsPyramidsYapo(self.config,
                                self.params,
                                self.logger).generate()
        
