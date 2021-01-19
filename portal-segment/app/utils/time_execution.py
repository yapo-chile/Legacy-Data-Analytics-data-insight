import logging
import datetime


class TimeExecution:
    """
    Class that allows to measure execution time.
    """
    def __init__(self) -> None:
        self.start = datetime.datetime.now()
        self.end = datetime.datetime.now()
        self.logger = logging.getLogger('timeExecution')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)

    def get_time(self) -> datetime:
        """
        Method [ getTime ] Returns time execution.
        """
        self.end = datetime.datetime.now()
        difference = self.end - self.start
        self.logger.info('Time Execution : %s ', difference)
