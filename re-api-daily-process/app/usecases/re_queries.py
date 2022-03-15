# pylint: disable=no-member
# utf-8
from google.oauth2 import service_account
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class InmoAPI(Query):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger
        self.emails = ''
        self.total_rows = 0
        self.schema = "dm_analysis"
        self.target_table = "real_estate_api_daily_yapo"

    @property
    def gbq_data(self):
        return self.__gbq_data
    
    @gbq_data.setter
    def gbq_data(self, date):

        credentials = service_account.Credentials.from_service_account_info(
            {
                "type": self.config.gbq.type,
                "project_id": self.config.gbq.project_id,
                "private_key_id": self.config.gbq.private_key_id,
                "private_key": self.config.gbq.private_key.replace('\\n', '\n'),
                "client_email": self.config.gbq.client_email,
                "client_id": self.config.gbq.client_id,
                "auth_uri": self.config.gbq.auth_uri,
                "token_uri": self.config.gbq.token_uri,
                "auth_provider_x509_cert_url": self.config.gbq.auth_provider_x509_cert_url,
                "client_x509_cert_url": self.config.gbq.client_x509_cert_url
            }
        )
        data = pd.read_gbq(self.query_ads_params_pro(date),
                                      project_id=self.config.gbq.project_id,
                                      credentials=credentials,
                                      dialect='standard')
        for column in ['list_id', 'count']:
            data[column] = data[column].astype('Int64')
        self.__gbq_data = data

    @property
    def ads_params(self):
        return self.__ads_params
    
    @ads_params.setter
    def ads_params(self, config):
        db_source = Database(conf=config)
        output_df = db_source \
            .select_to_dict(self.query_ads_params())
        db_source.close_connection()
        if output_df.empty:
            raise Exception("users account etl got empty dataframe")
        for column in ["list_id",
                        "price",
                        "rooms",
                        "bathrooms"]:
            output_df[column] = output_df[column].astype('Int64')
        self.__ads_params = output_df

    def insert_to_dwh(self, data):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.schema, self.target_table, data)

    def clean_data(self):
        dwh = Database(conf=self.config.db)
        self.logger.info("Cleaning data")
        dwh.execute_command(self.clean_data_by_dates())
        dwh.close_connection()
        return True

    def generate(self):
        self.ads_params = self.config.db
        self.clean_data()
        self.gbq_data = self.params.date_from.strftime("%Y%m%d")
        data = pd.merge(self.ads_params, self.gbq_data, how='inner', on = 'list_id')
        data['external_ad_id'] = None
        data['number_of_views'] = 0
        data['number_of_calls'] = 0
        data['number_of_call_whatsapp'] = 0
        data['number_of_show_phone'] = 0
        data['number_of_ad_replies'] = 0

        def parse_counts(row):
            if row['event_name'] == 'Ad detail viewed':
                row['number_of_views'] = row['count']
            elif row['event_name'] == 'Ad phone number called':
                row['number_of_calls'] = row['count']
            elif row['event_name'] == 'Ad phone whatsapp number contacted':
                row['number_of_call_whatsapp'] = row['count']
            elif row['event_name'] == 'Ad phone number displayed':
                row['number_of_show_phone'] = row['count']
            elif row['number_of_ad_replies'] == 'Ad reply submitted':
                row['number_of_ad_replies'] = row['count']
            return row
        data = data.apply(parse_counts, axis=1)
        data = data[['email', 'list_id', 'date', 'estate_type_name', 'rooms',
                    'bathrooms', 'currency', 'price',
                    'link_type', 'number_of_views','number_of_calls',
                    'number_of_call_whatsapp', 'number_of_show_phone', 'number_of_ad_replies',
                    'external_ad_id' ]]
        data = data.drop_duplicates()

        self.insert_to_dwh(data)
        print(data[['email', 'list_id', 'date', 'event_name', 'estate_type_name', 'rooms',
                    'bathrooms', 'currency', 'price',
                    'link_type', 'number_of_views','number_of_calls',
                    'number_of_call_whatsapp', 'number_of_show_phone', 'number_of_ad_replies',
                    'external_ad_id' ]].head())
        return True
