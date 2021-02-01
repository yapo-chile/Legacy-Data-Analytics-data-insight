# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from joblib import Parallel, delayed
from multiprocessing import Process
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

    def joined_params(self, EMAIL_LISTID, PERFORMANCE, PARAMS) -> pd.Dataframe:
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
        return final_df

    @property
    def dwh_re_api(self):
        return self.__dwh_re_api

    @dwh_re_api.setter
    def dwh_re_api(self, config):
        self.__dwh_re_api = []
        db_source = Database(conf=config)
        self.emails = db_source.select_to_dict(self.query_ads_users())
        listid = self.emails["list_id"]
        # Parallel mail data insert
        Parallel(n_jobs=2)(delayed(self.mail_iterations)(listid[i], db_source) for i in range(len(listid)))
        db_source.close_connection()
        del listid
        del db_source

    def mail_iterations(self, listid, db_source):
        performance = db_source.select_to_dict(self.query_get_athena_performance(listid))
        ad_params = db_source.select_to_dict(self.query_ads_params(listid))
        self.logger.info("PERFORMANCE DF HEAD:")
        self.logger.info(performance.head())
        self.logger.info("PARAMS DF HEAD:")
        self.logger.info(ad_params.head())
        # ---- JOIN ALL ----
        dwh_re_api = self.joined_params(self.emails, performance, ad_params)
        self.__dwh_re_api.append(dwh_re_api)
        del dwh_re_api
        del performance
        del ad_params

    def insert_to_dwh_batch(self):
        cleaned_data = self.dwh_re_api
        astypes = self.final_format
        dwh = Database(conf=self.config.db)
        for data in cleaned_data:
            data = data.astype(astypes)
            self.logger.info("First records as evidence to DM ANALISYS - Parallel email loop")
            self.logger.info(data.head())
            dwh.insert_copy(data, self.dm_table, self.target_table)
        dwh.close_connection()
        del cleaned_data
        del astypes
        del dwh

    def generate(self):
        # List id level parallelism
        self.dwh_re_api = self.config.db
        self.insert_to_dwh_batch()
        self.logger.info("Succesfully saved")

        gc.collect()
        self.logger.info("Uncollectable memory garbage: {}. If empty, all memory of the current "
                         "run was succesfully freed. Be free, memory!".format(str(gc.garbage)))
        return True