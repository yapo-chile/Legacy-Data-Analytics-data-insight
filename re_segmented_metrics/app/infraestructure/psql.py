import csv
import logging
from io import StringIO
import psycopg2
import psycopg2.extras
import pandas as pd
from sqlalchemy import create_engine


class Database:
    """
    Class that allow do operations with postgresql database.
    """
    def __init__(self, conf) -> None:
        self.log = logging.getLogger('psql')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.conf = conf
        self.connection = None
        self.get_connection()

    def database_conf(self) -> dict:
        """
        Method that return dict with database credentials.
        """
        return {"host": self.conf.host,
                "port": self.conf.port,
                "user": self.conf.user,
                "password": self.conf.password,
                "dbname": self.conf.name}

    def get_connection(self) -> None:
        """
        Method that returns database connection.
        """
        self.log.info('get_connection DB %s/%s', self.conf.host, self.conf.name)
        self.connection = psycopg2.connect(**self.database_conf())
        self.connection.set_client_encoding('UTF-8')

    def execute_command(self, command) -> None:
        """
        Method that allow execute sql commands such as DML commands.
        """
        self.log.info('execute_command : %s',
                      command.replace('\n', ' ').replace('\t', ' '))
        cursor = self.connection.cursor()
        cursor.execute(command)
        self.connection.commit()
        cursor.close()

    def select_to_dict(self, query) -> pd.DataFrame:
        """
        Method that from query transform raw data into dict.
        """
        self.log.info('Query : %s', query.replace(
            '\n', ' ').replace('    ', ' '))
        cursor = self.connection.cursor()
        cursor.execute(query)
        fieldnames = [name[0] for name in cursor.description]
        result = []
        for row in cursor.fetchall():
            rowset = []
            for field in zip(fieldnames, row):
                rowset.append(field)
            result.append(dict(rowset))
        pd_result = pd.DataFrame(result)
        cursor.close()
        return pd_result

    def close_connection(self):
        """
        Method that close connection to postgresql database.
        """
        self.log.info('Close connection DB : %s/%s',
                      self.conf.host,
                      self.conf.name)
        self.connection.close()

    def get_sqlalchemy_conn(self):
        return create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(
                self.conf.user,
                self.conf.password,
                self.conf.host,
                self.conf.port,
                self.conf.name
            )
        )

    def insert_copy(self, schema, table, df, index=False):
        def psql_insert_copy(table, conn, keys, data_iter):
            db_conn = conn.connection
            with db_conn.cursor() as cur:
                s_buf = StringIO()
                writer = csv.writer(s_buf)
                writer.writerows(data_iter)
                s_buf.seek(0)
                columns = ', '.join('"{}"'.format(k) for k in keys)
                if table.schema:
                    table_name = '{}.{}'.format(table.schema, table.name)
                else:
                    table_name = table.name

                sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                    table_name, columns)
                cur.copy_expert(sql=sql, file=s_buf)
                # close conn is called automatically, no needed to code

        df.to_sql("{}.{}".format(schema, table),
                  self.get_sqlalchemy_conn(),
                  index=index,
                  if_exists='append',  # Do not remove this,
                  # otherwise it would be failing or truncating the whole table
                  method=psql_insert_copy
                  )
