# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from joblib import Parallel, delayed
from multiprocessing import Process
import gc
import pandas as pd


class InmoAPI2(Query):
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
    def dwh_re_api_parallel_queries(self):
        return self.__dwh_re_api_parallel_queries

    @dwh_re_api_parallel_queries.setter
    def dwh_re_api_parallel_queries(self, config):
        db_source = Database(conf=config)
        self.emails = db_source.select_to_dict(self.query_ads_users())
        listid = self.emails["list_id"]
        for i in range(len(listid)):
            # ---- PARALLEL ----
            performance = Process(target=self.performance_query, args=(db_source, listid[i], ))
            performance.start()
            ad_params = Process(target=self.ad_params_query, args=(db_source, listid[i], ))
            ad_params.start()
            performance.join()
            ad_params.join()
            # ---- JOIN ALL ----
            self.logger.info("PERFORMANCE DF HEAD:")
            self.logger.info(self.performance.head())
            self.logger.info("PARAMS DF HEAD:")
            self.logger.info(self.ad_params.head())
            self.__dwh_re_api_parallel_queries = self.joined_params(self.emails, self.performance, self.ad_params)
            self.insert_to_dwh_parallel(db_source)
        db_source.close_connection()
        del listid
        del db_source

    def performance_query(self, db_source, listid):
        self.performance = db_source.select_to_dict(self.query_get_athena_performance(listid))

    def ad_params_query(self, db_source, listid):
        self.ad_params = db_source.select_to_dict(self.query_ads_params(listid))

    def insert_to_dwh_parallel(self, db_source):
        cleaned_data = self.dwh_re_api_parallel_queries
        astypes = self.final_format
        cleaned_data = cleaned_data.astype(astypes)
        self.logger.info("First records as evidence to DM ANALISYS - Parallel queries")
        self.logger.info(cleaned_data.head())
        db_source.insert_copy(cleaned_data, self.dm_table, self.target_table)
        self.logger.info("Succesfully saved")
        del cleaned_data
        del astypes

    def generate(self):
        # Query level parallelism
        self.dwh_re_api_parallel_queries = self.config.db

        gc.collect()
        self.logger.info("Uncollectable memory garbage: {}. If empty, all memory of the current "
                         "run was succesfully freed. Be free, memory!".format(str(gc.garbage)))
        return True