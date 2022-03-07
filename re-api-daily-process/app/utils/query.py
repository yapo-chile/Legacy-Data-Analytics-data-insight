from infraestructure.conf import getConf
from utils.read_params import ReadParams
import math


class Query:

    def clean_data_by_dates(self):
        """
        Method returns str with query used to data between specified dates
        """
        return """delete from "{}".{} 
            where date::date = '{}'::date""".format(self.schema,
                                                  self.target_table,
                                                  self.params.date_from)

    def query_ads_params(self) -> str:
        """
        Method return str with query of ads params
        """
        return  """
            select
                '{}' as date,
                CASE
                    WHEN a.action_type = 'import' THEN bsd.list_id
                    ELSE a.list_id_nk
                    END AS list_id,
                a.ad_id_nk,
                s.email,
                CASE
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Bigseller'
                    WHEN spd.seller_id_fk IS NULL THEN 'Pri'
                    ELSE 'Pro'
                    END AS type,
                link_type,
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
            from
                ods.active_ads aa
            inner join
                ods.ad a using(ad_id_nk)
            left join
                ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
            left join
                ods.seller s on a.seller_id_fk = s.seller_id_pk
            left join
                stg.big_sellers_detail bsd on bsd.ad_id_nk::int = a.ad_id_nk
            left join ods.ads_inmo_params aip on aip.ad_id_nk=aa.ad_id_nk 
            where
                a.category_id_fk in (47,48)
            and 
                (CASE
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Pro'
                    WHEN spd.seller_id_fk IS NULL THEN 'Pri'
                    ELSE 'Pro'
                    end) != 'Pri'
                """.format(self.params.date_from)

    def query_ads_params_pro(self, date) -> str:
        """
        Method return str with query of enriched ads parameters
        """
        return """select 
            PARSE_DATE("%Y%m%d", event_date) as timedate,
            (select value.int_value FROM UNNEST (event_params) WHERE key ='object_ad_id') AS list_id,
            event_name,
            count(*) count
        FROM
            `{ANALYTICS_SCHEMA}.events_{DATE}` a
        where
            event_name='Ad detail viewed'
        and (`{ANALYTICS_SCHEMA}`.GetCategory((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'object_categories_string'and value.string_value!='[{{"level":0,"id":null}},{{"level":1,"id":null}}]'),1))in (1220,1240)
        group by 1,2,3
        union all
        select 
            PARSE_DATE("%Y%m%d", event_date) as timedate,
            (select value.int_value FROM UNNEST (event_params) WHERE key ='object_ad_id') AS list_id,
            event_name,
            count(*) count
        FROM
            `{ANALYTICS_SCHEMA}.events_{DATE}` a
        INNER JOIN `meta.active_lead_name_events` e on event_name = e.name
        where 
            (`{ANALYTICS_SCHEMA}`.GetCategory((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'object_categories_string'and value.string_value!='[{{"level":0,"id":null}},{{"level":1,"id":null}}]'),1))in (1220,1240)
        group by 1,2,3""".format(ANALYTICS_SCHEMA=self.config.gbq.analytics_schema, DATE=date)