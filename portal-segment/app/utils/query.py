from infraestructure.conf import getConf
from utils.read_params import ReadParams
import pandas as pd


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_get_emails(self) -> str:
        """
        Method return str with query of big seller emails
        """
        query = """
                select cast((now() - interval '1 day')::date as varchar)
                    as timedate,
	            version()  as current_version;
            """
        return query

    def query_get_athena_performance(self) -> str:
        """
        Method return str with query of athena ad performance data
        """
        query = """
                select substr(
                        cast((cast(now() as timestamp) - interval '1' day)
                    as varchar), 1, 10) as timedate,
                'Athena' as current_version
            """
        return query

    def query_ads_users(self, mail) -> str:
        """
        Method return str with query of daily ads for each big seller
        """
        command = """
                    delete from dm_analysis.db_version where 
                    timedate::date = 
                    '""" + mail + """'::date """

        return command

    def query_ads_params(self) -> str:
        """
        Method return str with query of enriched ads parameters
        """
        query = """
                    select substr(
                            cast((cast(now() as timestamp) - interval '1' day)
                        as varchar), 1, 10) as timedate,
                    'Athena' as current_version
                """
        return query

    def joined_params(self, ads, performance, ad_params) -> pd.Dataframe:
        """
        Method return Pandas Dataframe of joined tables
        """
        final_df = ads.set_index('key').join(performance.set_index('key')).set_index('key').join(ad_params.set_index('key'))
        return final_df