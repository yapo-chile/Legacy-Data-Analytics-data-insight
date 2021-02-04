# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from multiprocessing import Process
from infraestructure.athena import Athena
import gc
import pandas as pd
from joblib import Parallel, delayed


class InmoAPI2(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
        self.performance = pd.DataFrame()
        self.ad_params = pd.DataFrame()
        self.emails = ''
        self.dm_table = "dm_analysis"
        self.target_table = "real_estate_api_daily_yapo"
        self.performance_dummy = {'date': str(self.params.get_date_from()), 'list_id': [0], 'number_of_views': [0],
                                  'number_of_calls': [0],
                                  'number_of_call_whatsapp': [0], 'number_of_show_phone': [0],
                                  'number_of_ad_replies': [0]}
        self.performance_dummy = pd.DataFrame.from_dict(self.performance_dummy)
        self.performance_dummy_dict = {'date': str(self.params.get_date_from()), 'list_id': 0, 'number_of_views': 0,
                                       'number_of_calls': 0,
                                       'number_of_call_whatsapp': 0, 'number_of_show_phone': 0,
                                       'number_of_ad_replies': 0}
        self.params_dummy = {'list_id': [0], 'estate_type_name': ["NULL"], 'rooms': [0],
                             'bathrooms': [0], 'currency': ["NULL"],
                             'price': [0], 'link_type': ["NULL"]}
        self.params_dummy = pd.DataFrame.from_dict(self.params_dummy)
        self.params_dummy_dict = {'list_id': 0, 'estate_type_name': "NULL", 'rooms': 0,
                                  'bathrooms': 0, 'currency': "NULL",
                                  'price': 0, 'link_type': "NULL"}
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
                             "price": "Int64",
                             "link_type": "str"}

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

    def chunkIt(self, seq, num):
        avg = len(seq) / float(num)
        out = []
        last = 0.0

        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out

    def dwh_re_api_parallel_queries(self):
        db_source = Database(conf=self.config.db)
        self.emails = db_source.select_to_dict(self.query_ads_users())
        self.logger.info("Information about emails table:")
        self.logger.info(str(self.emails))
        listid = self.emails["list_id"].tolist()
        chunks = 8 + int(len(listid) / 10000)
        listid = self.chunkIt(listid, chunks)
        self.logger.info("Batch size: 1/{}".format(str(len(listid))))
        del chunks
        for ls in listid:
            self.magnum_bullet(ls)
        del listid

    def magnum_bullet(self, ls):
        db_source = Database(conf=self.config.db)
        db_athena = Athena(conf=self.config.athenaConf)
        # ---- PARALLEL ----
        performance = Process(target=self.performance_query, args=(db_athena, ls))
        performance.start()
        ad_params = Process(target=self.ad_params_query, args=(db_source, ls))
        ad_params.start()
        performance.join()
        ad_params.join()
        # ---- JOIN ALL ----
        if self.performance.empty:
            self.performance = self.performance_dummy
            self.performance["list_id"] = ls[0]
            for i in range(1, len(ls)):
                dummy = self.performance_dummy_dict
                dummy['list_id'] = ls[i]
                self.performance = self.performance.append(dummy, ignore_index=True)
        else:
            self.check_performance(ls)
        self.performance = self.performance.dropna(subset=['list_id'])
        if self.ad_params.empty:
            self.ad_params = self.params_dummy
            self.ad_params["list_id"] = ls[0]
            for i in range(1, len(ls)):
                dummy = self.params_dummy_dict
                dummy['list_id'] = ls[i]
                self.ad_params = self.ad_params.append(dummy, ignore_index=True)
        else:
            self.check_params(ls)
        self.ad_params["link_type"].fillna("NULL", inplace=True)
        self.ad_params = self.ad_params.dropna(subset=['list_id'])
        self.logger.info("PERFORMANCE DF:")
        self.logger.info(self.performance)
        self.logger.info("PARAMS DF:")
        self.logger.info(self.ad_params)
        self.dwh_re_api_parallel_queries = self.joined_params(self.emails, self.performance, self.ad_params)
        self.insert_to_dwh_parallel(db_source)
        db_source.close_connection()
        db_athena.close_connection()
        del db_source
        del db_athena

    def check_params(self, ls):
        params = self.ad_params['list_id'].tolist()
        Parallel(n_jobs=2)(delayed(self.check_params_aux)(ls[i], params) for i in range(len(ls)))
        del params

    def check_params_aux(self, inp, params):
        if inp not in params:
            dummy = self.params_dummy_dict
            dummy['list_id'] = inp
            self.ad_params = self.ad_params.append(dummy, ignore_index=True)

    def check_performance(self, ls):
        performance = self.performance['list_id'].tolist()
        Parallel(n_jobs=2)(delayed(self.check_performance_aux)(ls[i], performance) for i in range(len(ls)))
        del performance

    def check_performance_aux(self, inp, performance):
        if inp not in performance:
            dummy = self.params_dummy_dict
            dummy['list_id'] = inp
            self.performance = self.performance.append(dummy, ignore_index=True)

    def performance_query(self, db_source, listid):
        self.performance = db_source.get_data(self.query_get_athena_performance(listid))

    def ad_params_query(self, db_source, listid):
        self.ad_params = db_source.select_to_dict(self.query_ads_params(listid))

    def insert_to_dwh_parallel(self, db_source):
        self.dwh_re_api_parallel_queries = self.dwh_re_api_parallel_queries.astype(self.final_format)
        db_source.insert_copy(self.dm_table, self.target_table, self.dwh_re_api_parallel_queries)
        self.logger.info("Succesfully saved")

    def generate(self):
        # Query level parallelism
        self.dwh_re_api_parallel_queries()

        gc.collect()
        self.logger.info("Uncollectable memory garbage: {}. If empty, all memory of the current "
                         "run was succesfully freed. Be free, memory!".format(str(gc.garbage)))
        return True