# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from infraestructure.athena import Athena
import gc
import pandas as pd
from joblib import Parallel, delayed
import psutil


class InmoAPI3(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
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
        self.params_dummy = {'list_id': [0], 'estate_type_name': [""], 'rooms': [0],
                                  'bathrooms': [0], 'currency': [""],
                                  'price': [0], 'link_id': [""]}
        self.params_dummy = pd.DataFrame.from_dict(self.params_dummy)
        self.params_dummy_dict = {'list_id': 0, 'estate_type_name': "", 'rooms': 0,
                                  'bathrooms': 0, 'currency': "",
                                  'price': 0, 'link_id': ""}
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

    def dwh_re_api_vanilla(self):
        db_source = Database(conf=self.config.db)
        db_athena = Athena(conf=self.config.athenaConf)
        self.emails = db_source.select_to_dict(self.query_ads_users())
        self.logger.info("Information about emails table:")
        self.logger.info(str(self.emails))
        listid = self.emails["list_id"].tolist()
        chunks = 8 + int(len(listid)/10000)
        listid = self.chunkIt(listid, chunks)
        self.logger.info("Batch size: 1/{}".format(str(len(listid))))
        del chunks
        for ls in listid:
            performance = db_athena.get_data(self.query_get_athena_performance(ls))
            if performance.empty:
                performance = self.performance_dummy
                performance["list_id"] = ls[0]
                for i in range(1, len(ls)):
                    dummy = self.performance_dummy_dict
                    dummy['list_id'] = ls[i]
                    performance = performance.append(dummy, ignore_index=True)
                del dummy
            else:
                perf = performance['list_id'].tolist()
                for i in range(len(ls)):
                    if ls[i] not in perf:
                        dummy = self.performance_dummy_dict
                        dummy['list_id'] = ls[i]
                        performance = performance.append(dummy, ignore_index=True)
                del dummy
                del perf
            self.logger.info("PERFORMANCE DF HEAD:")
            self.logger.info(performance.head())
            ad_params = db_source.select_to_dict(self.query_ads_params(ls))
            # ---- JOIN ALL ----
            if ad_params.empty:
                ad_params = self.params_dummy
                ad_params["list_id"] = ls[0]
                for i in range(1, len(ls)):
                    dummy = self.params_dummy_dict
                    dummy['list_id'] = ls[i]
                    ad_params = ad_params.append(dummy, ignore_index=True)
                del dummy
            else:
                params = ad_params['list_id'].tolist()
                for i in range(len(ls)):
                    if ls[i] not in params:
                        dummy = self.params_dummy_dict
                        dummy['list_id'] = ls[i]
                        ad_params = ad_params.append(dummy, ignore_index=True)
                del dummy
                del params
            ad_params["rooms"].fillna("NULL", inplace=True)
            ad_params["bathrooms"].fillna("NULL", inplace=True)
            self.logger.info("PARAMS DF HEAD:")
            self.logger.info(ad_params.head())
            self.dwh_re_api_vanilla = self.joined_params(self.emails, performance, ad_params)
            self.insert_to_dwh_vanilla(db_source)
            self.logger.info("Succesfully saved")
            del ad_params
            del performance
        del listid
        db_source.close_connection()
        db_athena.close_connection()
        del db_source
        del db_athena

    def insert_to_dwh_vanilla(self, db_source):
        self.dwh_re_api_vanilla = self.dwh_re_api_vanilla.astype(self.final_format)
        db_source.insert_copy(self.dm_table, self.target_table, self.dwh_re_api_vanilla)

    def generate(self):
        # Basic sequential case
        self.dwh_re_api_vanilla()
        gc.collect()
        self.logger.info("Uncollectable memory garbage: {}. If empty, all memory of the current "
                         "run was succesfully freed. Be free, memory!".format(str(gc.garbage)))
        return True