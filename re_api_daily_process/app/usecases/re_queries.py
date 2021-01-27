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
        self.emails = ''
        self.dm_table = "dm_analysis"
        self.target_table = "real_estate_api_daily_yapo"

    # Query data from data blocket
    @property
    def dwh_re_api(self):
        return self.__dwh_re_api

    @dwh_re_api.setter
    def dwh_re_api(self, config):
        self.__dwh_re_api = []
        db_source = Database(conf=config)
        df = db_source.select_to_dict(self.query_ads_users())
        listid = df["list_id"]
        self.emails = df['email']
        # Parallel mail data insert
        Parallel(n_jobs=2)(delayed(self.mail_iterations)(listid[i], db_source) for i in range(len(listid)))
        db_source.close_connection()
        del listid
        del df

    def mail_iterations(self, listid, db_source):
        performance = db_source.select_to_dict(self.query_get_athena_performance(listid))
        ad_params = db_source.select_to_dict(self.query_ads_params(listid))
        # ---- JOIN ALL ----
        dwh_re_api = self.joined_params(self.emails, performance, ad_params)
        self.__dwh_re_api.append(dwh_re_api)
        del dwh_re_api

    def insert_to_dwh_batch(self):
        cleaned_data = self.dwh_re_api
        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        dwh = Database(conf=self.config.db)
        for data in cleaned_data:
            data = data.astype(astypes)
            self.logger.info("First records as evidence to DM ANALISYS - Parallel email loop")
            self.logger.info(data.head())
            dwh.insert_copy(data, self.dm_table, self.target_table)
        dwh.close_connection()
        del cleaned_data
        del astypes

    # Query data from data blocket
    @property
    def dwh_re_api_parallel_queries(self):
        return self.__dwh_re_api_parallel_queries

    @dwh_re_api_parallel_queries.setter
    def dwh_re_api_parallel_queries(self, config):
        db_source = Database(conf=config)
        df = db_source.select_to_dict(self.query_ads_users())
        listid = df["list_id"]
        self.emails = df['email']
        for i in range(len(listid)):
            # ---- PARALLEL ----
            performance = Process(target=self.performance_query, args=(db_source, listid[i], ))
            performance.start()
            ad_params = Process(target=self.ad_params_query, args=(db_source, listid[i], ))
            ad_params.start()
            performance.join()
            ad_params.join()
            # ---- JOIN ALL ----
            dwh_re_api_parallel = self.joined_params(self.emails, self.performance, self.ad_params)
            self.__dwh_re_api_parallel_queries = dwh_re_api_parallel
            self.insert_to_dwh_parallel(db_source)
            del dwh_re_api_parallel
        db_source.close_connection()
        del listid
        del df

    def performance_query(self, db_source, listid):
        self.performance = db_source.select_to_dict(self.query_get_athena_performance(listid))

    def ad_params_query(self, db_source, listid):
        self.ad_params = db_source.select_to_dict(self.query_ads_params(listid))

    def insert_to_dwh_parallel(self, db_source):
        cleaned_data = self.dwh_re_api_parallel_queries
        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        self.logger.info("First records as evidence to DM ANALISYS - Parallel queries")
        self.logger.info(cleaned_data.head())
        db_source.insert_copy(cleaned_data, self.dm_table, self.target_table)
        self.logger.info("Succesfully saved")
        del cleaned_data
        del astypes

    # Query data from data blocket
    @property
    def dwh_re_api_vanilla(self):
        return self.__dwh_re_api_vanilla

    @dwh_re_api_vanilla.setter
    def dwh_re_api_vanilla(self, config):
        db_source = Database(conf=config)
        df = db_source.select_to_dict(self.query_ads_users())
        listid = df["list_id"]
        self.emails = df['email']
        for i in range(len(listid)):
            performance = db_source.select_to_dict(self.query_get_athena_performance(listid[i]))
            ad_params = db_source.select_to_dict(self.query_ads_params(listid[i]))
            # ---- JOIN ALL ----
            dwh_re_api_vanilla = self.joined_params(self.emails, performance, ad_params)
            db_source.close_connection()
            self.__dwh_re_api_vanilla = dwh_re_api_vanilla
            self.insert_to_dwh_vanilla(db_source)
            self.logger.info("Succesfully saved")
            del dwh_re_api_vanilla
        db_source.close_connection()
        del listid
        del df

    def insert_to_dwh_vanilla(self, db_source):
        cleaned_data = self.dwh_re_api_vanilla
        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        self.logger.info("First records as evidence to DM ANALISYS - Sequential loop")
        self.logger.info(cleaned_data.head())
        db_source.insert_copy(cleaned_data, self.dm_table, self.target_table)
        del cleaned_data
        del astypes

    def generate(self, option):
        if option == 1: # Email level parallelism
            self.dwh_re_api = self.config.db
            self.insert_to_dwh_batch()
            self.logger.info("Succesfully saved")
        elif option == 2: # Query level parallelism
            self.dwh_re_api_parallel_queries = self.config.db
        elif option == 3: # Basic sequential case
            self.dwh_re_api_vanilla = self.config.db
        return True