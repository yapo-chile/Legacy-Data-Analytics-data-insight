# pylint: disable=no-member
# utf-8
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from infraestructure.athena import Athena
import gc
import pandas as pd


class InmoAPI3(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
        self.emails = ''
        self.total_rows = 0
        self.dm_table = "dm_analysis"
        self.target_table = "real_estate_api_daily_yapo"
        self.target_table_emails = "inmo_pro_user_emails"
        self.target_table_emails_input = "pro_user_mail_performance"
        self.target_table_emails_historical = "inmo_pro_user_emails_backup"
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
        final_df = EMAIL_LISTID.merge(PERFORMANCE, left_on='list_id', right_on='list_id').drop_duplicates(keep='last')
        self.logger.info("CURRENT OUTPUT INTERMEDIATE ROWS:")
        self.logger.info(str(final_df))
        self.logger.info("CURRENT OUTPUT INTERMEDIATE ROW DUPLICATES:")  # bump
        self.logger.info(str(final_df[final_df[['email', 'list_id']].duplicated(keep="first")]))

        final_df = final_df.merge(PARAMS, left_on='list_id', right_on='list_id').drop_duplicates(keep='last')
        self.logger.info("CURRENT OUTPUT ROWS:")
        self.logger.info(str(final_df))
        self.logger.info("CURRENT OUTPUT ROW DUPLICATES:") #bump
        self.logger.info(str(final_df[final_df[['email', 'list_id']].duplicated(keep="first")]))
        self.total_rows += len(final_df)
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

        # Update emails table in DW for the next query result to be updated as well
        db_source.execute_command('TRUNCATE TABLE {}.{};'.format(self.dm_table, self.target_table_emails_input))
        self.logger.info("Truncated {}.{}".format(self.dm_table, self.target_table_emails_input))
        input_emails = db_source.select_to_dict(self.query_pro_user_mail_performance())
        self.logger.info("Information about input emails table:")
        self.logger.info(str(input_emails))
        input_emails = input_emails.sort_values(by=['type'])
        self.logger.info("Information about input emails table's duplicated values:")
        self.logger.info(str(input_emails[input_emails["email"].duplicated(keep="first")]))
        input_emails = input_emails.drop_duplicates(subset=['email'], keep='first')
        self.logger.info("Information about input emails table without duplicates:")
        self.logger.info(str(input_emails))
        db_source.insert_copy(self.dm_table, self.target_table_emails_input, input_emails)
        del input_emails

        # Get emails and list_ids, write them to DW
        db_source.execute_command('TRUNCATE TABLE {}.{};'.format(self.dm_table, self.target_table_emails))
        self.logger.info("Truncated {}.{}".format(self.dm_table, self.target_table_emails))
        self.emails = db_source.select_to_dict(self.query_ads_users())
        self.logger.info("Information about emails table:")
        self.logger.info(str(self.emails))
        db_source.insert_copy(self.dm_table, self.target_table_emails, self.emails)

        emails = self.emails.copy()
        emails['date'] = self.params.get_date_from()
        db_source.insert_copy(self.dm_table, self.target_table_emails_historical, emails)
        self.logger.info("Information about backup emails table inserted rows:")
        self.logger.info(str(emails))
        del emails

        # bump
        listid = self.emails["list_id"].tolist()
        chunks = 8 + int(len(listid)/10000)
        listid = self.chunkIt(listid, chunks)
        self.logger.info("Batch size: 1/{}".format(str(len(listid))))
        del chunks
        for ls in listid:
            performance = db_athena.get_data(self.query_get_athena_performance(ls))
            #if performance.empty:
            #    performance = self.performance_dummy
            #    performance["list_id"] = ls[0]
            #    for i in range(1, len(ls)):
            #        dummy = self.performance_dummy_dict
            #        dummy['list_id'] = ls[i]
            #        performance = performance.append(dummy, ignore_index=True)
            #else:
            perf = performance['list_id'].tolist()
            self.logger.info('List ids performance')
            self.logger.info(perf)
            self.logger.info(ls)
            for i in range(len(ls)):
                if int(ls[i]) not in perf:
                    dummy = self.performance_dummy_dict
                    dummy['list_id'] = ls[i]
                    performance = performance.append(dummy, ignore_index=True)

            performance = performance.dropna(subset=['list_id'])
            self.logger.info("PERFORMANCE DF:")
            self.logger.info(performance)
            ad_params = db_source.select_to_dict(self.query_ads_params(ls))
            # ---- JOIN ALL ----
            #if ad_params.empty:
            #    ad_params = self.params_dummy
            #    ad_params["list_id"] = ls[0]
            #    for i in range(1, len(ls)):
            #        dummy = self.params_dummy_dict
            #        dummy['list_id'] = ls[i]
            #        ad_params = ad_params.append(dummy, ignore_index=True)
            #else:

            params = ad_params['list_id'].tolist()
            self.logger.info('List ids params')
            self.logger.info(params)
            self.logger.info(ls)
            for i in range(len(ls)):
                if int(ls[i]) not in params:
                    dummy = self.params_dummy_dict
                    dummy['list_id'] = ls[i]
                    ad_params = ad_params.append(dummy, ignore_index=True)

            ad_params["link_type"].fillna("NULL", inplace=True)
            ad_params = ad_params.dropna(subset=['list_id'])
            self.logger.info("PARAMS DF:")
            self.logger.info(ad_params)
            self.logger.info("CURRENT PERFORMANCE DUPLICATES:")
            self.logger.info(str(performance[performance['list_id'].duplicated(keep="first")]))
            self.logger.info("CURRENT AD PARAMS DUPLICATES:")
            self.logger.info(str(ad_params[ad_params['list_id'].duplicated(keep="first")]))
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
        self.logger.info('TOTAL INSERTED OUTPUT ROWS: ' + str(self.total_rows))
        gc.collect()
        self.logger.info("Uncollectable memory garbage: {}. If empty, all memory of the current "
                         "run was succesfully freed. Be free, memory!".format(str(gc.garbage)))
        return True