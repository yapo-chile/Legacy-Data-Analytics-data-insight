# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from joblib import Parallel, delayed
from multiprocessing import Process


class InmoAPI(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
        self.performance = ''
        self.ad_params = ''

    # Query data from data blocket
    @property
    def dwh_re_api(self):
        return self.__dwh_re_api

    @dwh_re_api.setter
    def dwh_re_api(self, config):
        self.__dwh_re_api = []
        db_source = Database(conf=config)
        emails = db_source.select_to_dict(self.query_get_emails())
        # Parallel mail data insert
        Parallel(n_jobs=2)(delayed(self.mail_iterations)(emails[i], db_source) for i in range(len(emails)))

    def mail_iterations(self, mail, db_source):
        ads = db_source.select_to_dict(self.query_ads_users(mail))
        performance = db_source.select_to_dict(self.query_get_athena_performance())
        ad_params = db_source.select_to_dict(self.query_ads_params())
        # ---- JOIN ALL ----
        dwh_re_api = self.joined_params(ads, performance, ad_params)
        db_source.close_connection()
        self.__dwh_re_api.append(dwh_re_api)

    def insert_to_dwh_batch(self):
        cleaned_data = self.dwh_re_api
        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        for data in cleaned_data:
            data = data.astype(astypes)
            dwh = Database(conf=self.config.db)
            self.logger.info("First records as evidence to DM ANALISYS - Parallel email loop")
            self.logger.info(data.head())
            dwh.insert_copy(data, "dm_analysis", "real_estate_pyramids_yapo")

    # Query data from data blocket
    @property
    def dwh_re_api_parallel_queries(self):
        return self.__dwh_re_api_parallel_queries

    @dwh_re_api_parallel_queries.setter
    def dwh_re_api_parallel_queries(self, config):
        db_source = Database(conf=config)
        emails = db_source.select_to_dict(self.query_get_emails())
        for i in range(len(emails)):
            ads = db_source.select_to_dict(self.query_ads_users(emails[i]))
            # ---- PARALLEL ----
            performance = Process(target=self.performance_query, args=(db_source,))
            performance.start()
            ad_params = Process(target=self.ad_params_query, args=(db_source,))
            ad_params.start()
            performance.join()
            ad_params.join()
            # ---- JOIN ALL ----
            dwh_re_api_parallel = self.joined_params(ads, self.performance, self.ad_params)
            db_source.close_connection()
            self.__dwh_re_api_parallel_queries = dwh_re_api_parallel
            self.insert_to_dwh_parallel()

    def performance_query(self, db_source):
        self.performance = db_source.select_to_dict(self.query_get_athena_performance())

    def ad_params_query(self, db_source):
        self.ad_params = db_source.select_to_dict(self.query_ads_params())

    def insert_to_dwh_parallel(self):
        cleaned_data = self.dwh_re_api_parallel_queries
        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        dwh = Database(conf=self.config.db)
        self.logger.info("First records as evidence to DM ANALISYS - Parallel queries")
        self.logger.info(cleaned_data.head())
        dwh.insert_copy(cleaned_data, "dm_analysis", "real_estate_pyramids_yapo")
        self.logger.info("Succesfully saved")

    # Query data from data blocket
    @property
    def dwh_re_api_vanilla(self):
        return self.__dwh_re_api_vanilla

    @dwh_re_api_vanilla.setter
    def dwh_re_api_vanilla(self, config):
        db_source = Database(conf=config)
        emails = db_source.select_to_dict(self.query_get_emails())
        for i in range(len(emails)):
            ads = db_source.select_to_dict(self.query_ads_users(emails[i]))
            performance = db_source.select_to_dict(self.query_get_athena_performance())
            ad_params = db_source.select_to_dict(self.query_ads_params())
            # ---- JOIN ALL ----
            dwh_re_api_vanilla = self.joined_params(ads, performance, ad_params)
            db_source.close_connection()
            self.__dwh_re_api_vanilla = dwh_re_api_vanilla

    def insert_to_dwh_vanilla(self):
        cleaned_data = self.dwh_re_api_vanilla
        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        dwh = Database(conf=self.config.db)
        self.logger.info("First records as evidence to DM ANALISYS - Sequential loop")
        self.logger.info(cleaned_data.head())
        dwh.insert_copy(cleaned_data, "dm_analysis", "real_estate_pyramids_yapo")

    def generate(self, option):
        if option == 1: # Email level parallelism
            self.dwh_re_api = self.config.db
            self.insert_to_dwh_batch()
            self.logger.info("Succesfully saved")
        elif option == 2: # Query level parallelism
            self.dwh_re_api_parallel_queries = self.config.db
        elif option == 3: # Basic sequential case
            self.dwh_re_api_vanilla = self.config.db
            self.insert_to_dwh_vanilla()
            self.logger.info("Succesfully saved")
        return True