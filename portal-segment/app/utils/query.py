from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_base_postgresql(self) -> str:
        """
        Method return str with query
        """
        query = """
                select cast((now() - interval '1 day')::date as varchar)
                    as timedate,
	            version()  as current_version;
            """
        return query

    def query_base_athena(self) -> str:
        """
        Method return str with query
        """
        query = """
                select substr(
                        cast((cast(now() as timestamp) - interval '1' day)
                    as varchar), 1, 10) as timedate,
                'Athena' as current_version
            """
        return query

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.db_version where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
