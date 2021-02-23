from infraestructure.conf import getConf
from utils.read_params import ReadParams


class RePyramidsYapoQuery:
    """
    Class that store all query
    """

    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def re_segment_pyramid_yapo(self) -> str:
        """
        Real estate Pyramid
        """
        query = """
            select
            a.status_date,
            a.ad_id_nk,
            a.email,
            a.price::bigint,
            a.uf_price,
            case
            when a.uf_price >= 1 AND a.uf_price < 3000 then '0-3000UF'
            when a.uf_price >= 3000 AND a.uf_price < 5000 then '3000-5000UF'
            when a.uf_price >= 5000 AND a.uf_price < 7000 then '5000-7000UF'
            when a.uf_price >= 7000 AND a.uf_price < 9000 then '7000-9000UF'
            when a.uf_price >= 9000 then '9000UF+'
            else null
            end as price_interval,
            a.category_id_fk,
            a.doc_num,
            a.integrador,
            /*case
                when a.integrador is not null and a.automotora is null then 'UNKNOWN'
                else a.automotora
            end as automotora,*/
            case
                when a.integrador is not null then 'Integrador'
                when a.pack_id is not null then 'Pack'
                when a.insfee_buyer = 'if_buyer' then 'Insertion Fee'
                else null
            end as ad_type,
            case
                when split_part(split_part(a.email,'@',2),'.',1) in ('gmail','hotmail','icloud','live','outlook','yahoo') then 'Publico'
                else 'Privado'
            end as email_provider
            from(
                select
                    aa.status_date,
                    a.ad_id_nk,
                    s.email,
                    a.price::bigint,
                    case
                        when a.currency = 'peso' or a.currency is null then a.price / (select a.value from stg.currency a where date(date_time::date) = date(now()) and a.money = 'UF')
                        else a.price/100
                    end as uf_price,
                    a.category_id_fk,
                    p.doc_num,
                    p.pack_id,
                    bsd.link_type as integrador,
                    --bcs.automotora,
                    case when i.ad_id_fk is not null then 'if_buyer' else null end insfee_buyer
                from
                    ods.active_ads aa
                inner join
                    (select
                        a.ad_id_pk,
                        a.ad_id_nk,
                        a.seller_id_fk,
                        a.price::bigint,
                        ap.currency,
                        a.category_id_fk,
                        'real_estate'::text as pack_vertical
                    from
                        ods.ad a
                    left join
                        ods.ads_inmo_params ap on ap.ad_id_nk = a.ad_id_nk
                    where
                        category_id_fk::int in (47,48)
                    ) a on aa.ad_id_nk = a.ad_id_nk
                left join
                    ods.packs p on ((a.seller_id_fk = p.seller_id_fk) and (aa.status_date between p.date_start and p.date_end) and (a.pack_vertical = p.category))
                left join
                    (select
                        ad_id_fk
                    from
                        ods.product_order po
                    where
                        product_id_fk in (23)
                        and status in ('confirmed','paid','sent','failed')) i on a.ad_id_pk = i.ad_id_fk
                left join
                    ods.seller s on a.seller_id_fk = s.seller_id_pk
                left join
                    stg.big_sellers_detail bsd on a.ad_id_nk = bsd.ad_id_nk
                --left join
                --	ods.big_car_sellers_data bcs on s.email = bcs.email
                ) a
            where
                (pack_id is not null
                or insfee_buyer is not null
                or integrador is not null)
            group by 1,2,3,4,5,6,7,8,9,10,11
                    """
        return query


class CarsPyramidsYapoQuery:
    """
    Query pyramids cars
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def cars_segment_pyramid_yapo(self) -> str:
        """
        Cars Pyramid Yapo
        """
        query = """
            select
            a.status_date,
            a.ad_id_nk,
            a.email,
            a.price::bigint,
            a.price_interval,
            a.doc_num,
            a.integrador,
            case
                when a.integrador is not null and a.automotora is null then 'UNKNOWN'
                else a.automotora
            end as automotora,
            case
                when a.integrador is not null then 'Integrador'
                when a.pack_id is not null then 'Pack'
                when a.insfee_buyer = 'if_buyer' then 'Insertion Fee'
                else null
            end as ad_type,
            case
                when split_part(split_part(a.email,'@',2),'.',1) in ('gmail','hotmail','icloud','live','outlook','yahoo') then 'Publico'
                else 'Privado'
            end as email_provider
        from(
            select
                aa.status_date,
                a.ad_id_nk,
                s.email,
                a.price::bigint,
                a.price_interval,
                p.doc_num,
                p.pack_id,
                bsd.link_type as integrador,
                bcs.automotora,
                case when i.ad_id_fk is not null then 'if_buyer' else null end insfee_buyer
            from
                ods.active_ads aa
            inner join
                (select
                    a.ad_id_pk,
                    a.ad_id_nk,
                    a.seller_id_fk,
                    a.price::bigint,
                    case
                        when a.price >= 1 AND a.price < 3000000 then '0-2MM'
                        when a.price >= 3000000 AND a.price < 5000000 then '3-4MM'
                        when a.price >= 5000000 AND a.price < 10000000 then '5-9MM'
                        when a.price >= 10000000 AND a.price < 20000000 then '10-19MM'
                        when a.price >= 20000000 then '20MM+'
                        else null
                    end as price_interval,
                    'car'::text as pack_vertical
                from 
                    ods.ad a
                where 
                    category_id_fk::int in (7,8)
                ) a on aa.ad_id_nk = a.ad_id_nk
            left join
                ods.packs p on ((a.seller_id_fk = p.seller_id_fk) and (aa.status_date between p.date_start and p.date_end) and (a.pack_vertical = p.category))
            left join
                (select
                    ad_id_fk
                from
                    ods.product_order po
                where
                    product_id_fk in (22,421)
                    and status in ('confirmed','paid','sent','failed')) i on a.ad_id_pk = i.ad_id_fk
            left join
                ods.seller s on a.seller_id_fk = s.seller_id_pk
            left join
                stg.big_sellers_detail bsd on a.ad_id_nk = bsd.ad_id_nk
            left join
                ods.big_car_sellers_data bcs on s.email = bcs.email) a
        where
            (pack_id is not null
            or insfee_buyer is not null
            or integrador is not null)
        group by 1,2,3,4,5,6,7,8,9
        """
        return query
