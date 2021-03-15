import logging
from pyathena import connect
import pandas as pd

class Athena:
    """
    Class that extract data from Athena database
    """
    def __init__(self, conf) -> None:
        self.log = logging.getLogger('athena')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.conf = conf
        self.connection = None
        self.get_connection()

    def get_connection(self) -> None:
        """
        Method that get connection to S3 Bucket.
        """
        self.log.info('get_connection S3 %s%s',
                      self.conf.s3_bucket,
                      self.conf.user)
        s3_staging_dir = self.conf.s3_bucket + self.conf.user
        self.connection = connect(aws_access_key_id=self.conf.access_key,
                                  aws_secret_access_key=self.conf.secret_key,
                                  s3_staging_dir=s3_staging_dir,
                                  region_name=self.conf.region)

    def get_data(self, query: str) -> pd.DataFrame:
        """
        Method that returns pandas DataFrame from query to S3 Bucket
        """
        self.log.info('Run query : %s',
                      query.replace('\n', ' ').replace('\t', ' '))
        df_result = pd.io.sql.read_sql(query, self.connection)
        return df_result

    def close_connection(self) -> None:
        """
        Method that close connection to S3 Bucket
        """
        self.log.info('Close Athena %s%s', self.conf.s3_bucket, self.conf.user)
        self.connection.close()
