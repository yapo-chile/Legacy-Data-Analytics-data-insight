# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from joblib import Parallel, delayed
from infraestructure.athena import Athena
import gc
import pandas as pd


class InmoAPI1(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
        self.performance = ''
        self.ad_params = ''
        self.emails = ''
        self.dm_table = "dm_analysis"
        self.target_table = "real_estate_api_daily_yapo"
        self.final_format = {"email": "str",
                               "date": "str",
                               "number_of_views": "Int64",
                               "number_of_calls": "Int64",
                               "number_of_call_whatsapp": "Int64",
                               "number_of_show_phone": "Int64",
                               "number_of_ad_replies": "Int64",
                               "estate_type_name": "str",
                               "rooms": "Int64",
                               "bathrooms": "Int64",
                               "currency": "str",
                               "price": "Int64"}

    def joined_params(self, EMAIL_LISTID, PERFORMANCE, PARAMS):
        """
        Method return Pandas Dataframe of joined tables
        fecha
        email
        list_id
        state_type
        price
        bathroom
        room
        currency
        number_of_views
        number_of_calls
        number_of_whatsapp
        number_of_show_phone
        number_of_ad_reply
        """
        EMAIL_LISTID["list_id"] = EMAIL_LISTID["list_id"].apply(pd.to_numeric)
        PERFORMANCE["list_id"] = PERFORMANCE["list_id"].apply(pd.to_numeric)
        PARAMS["list_id"] = PARAMS["list_id"].apply(pd.to_numeric)
        # PARAMS['rooms'] = PARAMS['rooms'].where(pd.notnull(PARAMS['rooms']), None)
        # PARAMS['bathrooms'] = PARAMS['bathrooms'].where(pd.notnull(PARAMS['bathrooms']), None)
        final_df = EMAIL_LISTID.merge(PERFORMANCE, left_on='list_id', right_on='list_id').merge(PARAMS, left_on='list_id', right_on='list_id').drop_duplicates(keep='last')
        self.logger.info("CURRENT OUTPUT ROW:")
        self.logger.info(str(final_df))
        return final_df

    def dwh_re_api(self, config):
        self.__dwh_re_api = []
        db_source = Database(conf=self.config.db)
        self.emails = db_source.select_to_dict(self.query_ads_users())
        # Parallel mail data insert
        Parallel(n_jobs=2)(delayed(self.mail_iterations)(self.emails["list_id"][i], db_source, config) for i in range(len(self.emails["list_id"])))
        db_source.close_connection()
        del db_source

    def mail_iterations(self, listid, db_source, config):
        db_athena = Athena(conf=self.config.athenaConf)
        performance = db_athena.get_data(self.query_get_athena_performance(listid))
        db_athena.close_connection()
        del db_athena
        ad_params = db_source.select_to_dict(self.query_ads_params(listid))
        self.logger.info("PERFORMANCE DF HEAD:")
        self.logger.info(performance.head())
        self.logger.info("PARAMS DF HEAD:")
        self.logger.info(ad_params.head())
        # ---- JOIN ALL ----
        self.__dwh_re_api.append(self.joined_params(self.emails, performance, ad_params))
        del performance
        del ad_params

    def insert_to_dwh_batch(self):
        dwh = Database(conf=self.config.db)
        for data in self.dwh_re_api:
            data = data.astype(self.final_format)
            self.logger.info("First records as evidence to DM ANALISYS - Parallel email loop")
            self.logger.info(data.head())
            dwh.insert_copy(data, self.dm_table, self.target_table)
        dwh.close_connection()
        del dwh

    def generate(self):
        # List id level parallelism
        self.dwh_re_api()
        self.insert_to_dwh_batch()
        self.logger.info("Succesfully saved")

        gc.collect()
        self.logger.info("Uncollectable memory garbage: {}. If empty, all memory of the current "
                         "run was succesfully freed. Be free, memory!".format(str(gc.garbage)))
        return True