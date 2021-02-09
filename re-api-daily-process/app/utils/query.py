from infraestructure.conf import getConf
from utils.read_params import ReadParams
import math


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_get_athena_performance(self, list_id) -> str:
        """
        Method return str with query of athena ad performance data
        """
        st = "("
        for l in range(len(list_id)):
            if list_id[l] is not None and str(list_id[l]) != "None":
                if l == len(list_id) - 1:
                    st += "'" + str(list_id[l]) + "'"
                else:
                    st += "'" + str(list_id[l]) + "',"
        st += ")"
        list_id = st
        del st

        query = """
                    SELECT
                        CAST(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) AS "date",
                        cast(split_part(ad_id,':',4) as integer) AS list_id,
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
                        CAST(split_part(ad_id,':',4) AS varchar) IN """ + list_id + """
                    AND
                       date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') = CAST('""" + self.params.get_date_from() + """' as date)
                    GROUP BY 1,2
                """
        return query

    def query_pro_user_mail_performance(self) -> str:
        """
        Method return str with query of emails per type of user
        """
        command = """
                    SELECT
                        email,
                        type
                        From(
                        SELECT distinct
                            s.email,
                            case
                                when spd.seller_id_fk is null then 'PRI'
                                else 'pro'
                            end as type
                        from
                            ods.active_ads aa
                        inner join
                            ods.ad a using(ad_id_nk)
                        left join
                            ods.seller_pro_details spd using(seller_id_fk, category_id_fk)
                        left join ods.seller s
                            on a.seller_id_fk =s.seller_id_pk
                        left join stg.big_sellers_detail bs 
                        on (bs.ad_id_nk::int = a.ad_id_nk and bs.list_id is null)
                        and
                            a.category_id_fk in (47,48)
                        and
                            bs.list_id is null
                        union all
                        SELECT distinct
                            s.email,
                            'bigseller' as type
                        from
                            ods.active_ads aa
                        inner join
                            ods.ad a using(ad_id_nk)
                        inner join
                            stg.big_sellers_detail bs on bs.ad_id_nk::int = a.ad_id_nk
                        left join ods.seller s
                            on a.seller_id_fk =s.seller_id_pk
                        where
                            a.category_id_fk in (47,48))aa
                """

        return command

    def query_ads_users(self) -> str:
        """
        Method return str with query of daily ads for each big seller
        """
        command = """
                    select
                        aa.email,
                        aa.list_id
                    from(
                        select
                            s.email,
                            cast(a.list_id_nk as varchar) as list_id
                        From
                            ods.active_ads aa
                        inner join
                            ods.ad a using(ad_id_nk)
                        left join
                            ods.seller s
                            on a.seller_id_fk =s.seller_id_pk
                        left join
                            dm_analysis.pro_user_mail_performance pro
                            on s.email=pro.email
                        where
                            pro.type='pro'
                        union all select
                            s.email,
                            cast(bs.list_id as varchar) as list_id
                        From
                            ods.ad a
                        inner join
                            ods.active_ads aa using(ad_id_nk)
                        left join
                            ods.seller s
                            on a.seller_id_fk =s.seller_id_pk
                        inner join
                            stg.big_sellers_detail bs
                        on
                            bs.ad_id_nk::int = a.ad_id_nk
                        left join
                            dm_analysis.pro_user_mail_performance pro
                            on s.email=pro.email
                        where
                            pro.type='bigseller') aa
                """

        return command

    def query_ads_params(self, list_id) -> str:
        """
        Method return str with query of enriched ads parameters
        """
        st = "("
        for l in range(len(list_id)):
            if list_id[l] is not None and str(list_id[l]) != "None":
                if l == len(list_id) - 1:
                    st += str(list_id[l])
                else:
                    st += str(list_id[l]) + ","
        st += ")"
        list_id = st
        del st

        query = """
                    select
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
                        a.price,
                        aa.list_id as list_id,
                        link_type
                    from ods.ads_inmo_params aip
                    inner join (
                        select
                            ad_id_nk,
                            cast(a.list_id_nk as varchar) as list_id,
                            'NULL' as link_type
                        from
                            ods.ad a
                        where
                            list_id_nk in """ + list_id + """
                        union all
                        select
                            ad_id_nk,
                            cast(bs.list_id as varchar) as list_id,
                            link_type
                        from
                            stg.big_sellers_detail bs
                        where
                            list_id in """ + list_id + """) aa
                    on
                        aip.ad_id_nk=aa.ad_id_nk
                    left join
                        ods.ad a
                    on
                        a.ad_id_nk=aip.ad_id_nk
                """

        return query

