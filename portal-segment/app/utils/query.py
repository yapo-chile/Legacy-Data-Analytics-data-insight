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
                SELECT
                    CAST(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) AS "date",
                    cast(split_part(ad_id,':',4) as varchar) AS list_id,
                    count(distinct case when event_type = 'View' and object_type = 'ClassifiedAd' then event_id end) number_of_views,
                    count(distinct case when event_type = 'Call' then event_id end)-count(distinct case when event_name = 'Ad phone whatsapp number contacted' then event_id end) number_of_calls,
                    count(distinct case when event_name = 'Ad phone whatsapp number contacted' then event_id end) number_of_call_whatsapp,
                    count(distinct case when event_type = 'Show' then event_id end) number_of_show_phone,
                    count(distinct case when event_type = 'Send' then environment_id end) number_of_ad_replies
                FROM
                    yapocl_databox.insights_events_behavioral_fact_layer_365d
                WHERE
                    ad_id NOT IN ('sdrn:yapocl:classified:', 'sdrn:yapocl:classified:0', 'unknown')
                AND
                    CAST(split_part(ad_id,':',4) AS varchar) IN ('{}')
                AND
                   date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') = CAST('2021-01-24' as date)
                GROUP BY 1,2
            """
        return query

    def query_ads_users(self, mail) -> str:
        """
        Method return str with query of daily ads for each big seller
        """
        command = """
                    SELECT
                        cast(list_id as varchar),
                        email,
                        link_type
                    FROM
                        stg.big_sellers_detail bs
                    INNER JOIN
                        ods.active_ads aa USING (ad_id_nk)
                    LEFT JOIN
                        ods.ad a USING (ad_id_nk)
                    LEFT JOIN
                            ods.seller s ON a.seller_id_fk =s.seller_id_pk
                """

        return command

    def query_ads_params(self) -> str:
        """
        Method return str with query of enriched ads parameters
        """
        query = """
                    select
                        a.ad_id_nk,
                        CASE
                            WHEN aip.estate_type = '1' then 'Departamento'
                            WHEN aip.estate_type = '2' then 'Casa'
                            WHEN aip.estate_type = '3' then 'Oficina'
                            WHEN aip.estate_type = '4' then 'Comercial e industrial'
                            WHEN aip.estate_type = '5' then 'Terreno'
                            WHEN aip.estate_type = '6' then 'Estacionamiento, bodega u otro'
                            WHEN aip.estate_type = '7' then 'Pieza'
                            WHEN aip.estate_type = '8' then 'Cabaña'
                            WHEN aip.estate_type = '9' then 'Habitacion'
                            END AS estate_type_name,
                        aip.rooms,
                            aip.bathrooms,
                            aip.currency,
                        a.price
                    FROM
                        ods.ads_inmo_params aip
                    LEFT JOIN
                        ods.ad a using (ad_id_nk)
                    WHERE
                        a.ad_id_nk in ('{}') 
                """
        return query

    def joined_params(self, ads, performance, ad_params) -> pd.Dataframe:
        """
        Method return Pandas Dataframe of joined tables
        """
        final_df = ads.set_index('key').join(performance.set_index('key')).set_index('key').join(ad_params.set_index('key'))
        return final_df