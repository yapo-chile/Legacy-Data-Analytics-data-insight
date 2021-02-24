from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_base_postgresql(self) -> str:
        """
        Method return str with query
        """
        query = """
                select cast((now() - interval '1 day')::date as varchar)
                    as timedate,
	            version()  as current_version;
            """
        return query

    def query_base_athena(self) -> str:
        """
        Method return str with query
        """
        query = """
                select substr(
                        cast((cast(now() as timestamp) - interval '1' day)
                    as varchar), 1, 10) as timedate,
                'Athena' as current_version
            """
        return query

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.db_version where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command


class AdViewsRE:

    def __init__(self, conf: getConf, params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def ad_views(self) -> str:

        query = """
        select
            cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) timedate,
            split_part(ad_id,':',4) list_id,
            case
                when (device_type = 'desktop' and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'Web' or object_url like '%www.yapo.cl%' or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet') and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'M-Site' or object_url like '%m.yapo.cl%' or ((device_type = 'mobile' or device_type = 'tablet') and product_type = 'unknown') then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'AndroidApp') or product_type = 'AndroidApp' then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'iOSApp') or product_type = 'iOSApp' or product_type = 'iPadApp' then 'iOSApp'
            end platform,
            count(distinct row(ad_id, event_id)) ad_views
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            CAST(date_parse(CAST(year AS varchar) || '-' || CAST(month AS varchar) || '-' || CAST(day AS varchar),'%Y-%c-%e') AS date)
        BETWEEN DATE('{}') AND DATE('{}')
            and event_name = 'Ad detail viewed'
            and (local_category_level1 in ('arrendar','arriendo','comprar') and local_main_category in ('inmuebles'))
        group by 1,2,3
        """
        return query

class UniqueLeadsWithOutShowPhoneRE:

    def __init__(self, conf: getConf, params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def unique_leads(self) -> str:

        query = """
        SELECT
            CAST(date_parse(CAST(year AS varchar) || '-' || CAST(month AS varchar) || '-' || CAST(day AS varchar),'%Y-%c-%e') AS date) timedate,
            split_part(ad_id,':',4) list_id,
            CASE
                WHEN (device_type = 'desktop' AND (object_url like '%www2.yapo.cl%' OR object_url like '%//yapo.cl%')) 
                    OR product_type = 'Web' OR object_url like '%www.yapo.cl%' 
                    OR (device_type = 'desktop' AND product_type = 'unknown') then 'Web'
                WHEN ((device_type = 'mobile' OR device_type = 'tablet') AND (object_url like '%www2.yapo.cl%' OR object_url like '%//yapo.cl%')) 
                    OR product_type = 'M-Site' OR object_url like '%m.yapo.cl%' 
                    OR ((device_type = 'mobile' OR device_type = 'tablet') AND product_type = 'unknown') then 'MSite'
                WHEN ((device_type = 'mobile' OR device_type = 'tablet') AND object_url IS NOT NULL AND product_type = 'AndroidApp') 
                    OR product_type = 'AndroidApp' then 'AndroidApp'
                WHEN ((device_type = 'mobile' OR device_type = 'tablet') AND object_url IS NOT NULL AND product_type = 'iOSApp') 
                    OR product_type = 'iOSApp' OR product_type = 'iPadApp' then 'iOSApp'
            END platform,
            COUNT(distinct row(ad_id, environment_id)) unique_leads
        FROM
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        WHERE
            CAST(date_parse(CAST(year AS varchar) || '-' || CAST(month AS varchar) || '-' || CAST(day AS varchar),'%Y-%c-%e') AS date)
                BETWEEN DATE('{}') AND DATE('{}')
            AND ad_id != 'sdrn:yapocl:classified:' AND ad_id != 'sdrn:yapocl:classified:0'
            AND event_type IN ('Call','SMS','Send')
            AND (local_category_level1 IN ('arrendar','arriendo','comprar') AND local_main_category IN ('inmuebles'))
            AND lead_id != 'unknown'
        GROUP BY  1,2,3
        """
        return query
