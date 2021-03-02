from infraestructure.conf import getConf
from utils.read_params import ReadParams


class AdViewsQuery:

    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def get_ad_views(self) -> str:

        query = """
        select
            cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) AS event_date,
            split_part(ad_id,':',4) list_id,
            case
                when (device_type = 'desktop' and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'Web' or object_url like '%www.yapo.cl%' or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet') and (object_url like '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or product_type = 'M-Site' or object_url like '%m.yapo.cl%' or ((device_type = 'mobile' or device_type = 'tablet') and product_type = 'unknown') then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'AndroidApp') or product_type = 'AndroidApp' then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet') and object_url is not null and product_type = 'iOSApp') or product_type = 'iOSApp' or product_type = 'iPadApp' then 'iOSApp'
            end platform,
            count(distinct row(ad_id, event_id)) AS ad_views
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            CAST(date_parse(CAST(year AS varchar) || '-' || CAST(month AS varchar) || '-' || CAST(day AS varchar),'%Y-%c-%e') AS date)
                = current_date - interval '1' day
            and event_name = 'Ad detail viewed'
            and (local_category_level1 in ('arrendar','arriendo','comprar') and local_main_category in ('inmuebles'))
        group by 1,2,3
        """
        return query

    def get_segmented_ads(self) -> str:

        query = """
        SELECT
            "date",
            list_id,
            category,
            region,
            commune,
            case
                WHEN category = 'Arrendar' THEN 'Arriendo'
                when uf_price >= 0 AND uf_price < 3000 THEN '0-3000UF'
                when uf_price >= 3000 AND uf_price < 5000 THEN '3000-5000UF'
                when uf_price >= 5000 AND uf_price < 7000 THEN '5000-7000UF'
                when uf_price >= 7000 AND uf_price < 9000 THEN '7000-9000UF'
                when uf_price >= 9000 THEN '9000UF+'
            END AS price_interval,
            estate_type,
            pri_pro
        FROM
            (
            SELECT
               CASE
                    WHEN a.action_type = 'import' THEN bsd.list_time
                    ELSE a.approval_date::date
                END AS "date",
                CASE
                    WHEN a.action_type = 'import' THEN bsd.list_id
                    ELSE a.list_id_nk
                END AS list_id,
                c.category_name AS category,
                r.region_name AS region,
                -- Price in UF
                CASE 
                    WHEN ip.currency = 'uf' THEN a.price::float / 100.0
                    ELSE a.price::float / 
                        (SELECT c.value FROM stg.currency AS c WHERE c.money = 'UF' ORDER BY date_time DESC LIMIT 1)
                END AS uf_price,
                co.comuna_name AS commune,
                CASE
                    WHEN ip.estate_type = '1' THEN 'Departamento'
                    WHEN ip.estate_type = '2' THEN 'Casa'
                    WHEN ip.estate_type = '3' THEN 'Oficina'
                    WHEN ip.estate_type = '4' THEN 'Comercial e industrial'
                    WHEN ip.estate_type = '5' THEN 'Terreno'
                    WHEN ip.estate_type = '6' THEN 'Estacionamiento, bodega u otro'
                    WHEN ip.estate_type = '7' THEN 'Pieza'
                    WHEN ip.estate_type = '8' THEN 'Cabaña'
                    WHEN ip.estate_type = '9' THEN 'Habitacion'
                END AS estate_type,
                CASE
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Pro'
                    WHEN spd.seller_id_fk IS NULL THEN 'Pri'
                    ELSE 'Pro'
                END AS pri_pro
            FROM
                ods.ad AS a
                LEFT JOIN
                    ods.category AS c
                    ON a.category_id_fk = c.category_id_pk
                LEFT JOIN 
                    ods.region AS r
                    ON a.region_id_fk = r.region_id_pk
                LEFT JOIN
                    stg.dim_communes_blocket AS co
                    ON a.communes_id_nk::int = co.comuna_id::int
                INNER JOIN
                    ods.ads_inmo_params AS ip 
                    ON a.ad_id_nk = ip.ad_id_nk
                LEFT JOIN 
                    stg.big_sellers_detail AS bsd
                    ON a.ad_id_nk = bsd.ad_id_nk
                 LEFT JOIN
                    ods.seller_pro_details AS spd
                        ON a.seller_id_fk = spd.seller_id_fk
                            AND a.category_id_fk = spd.category_id_fk
            WHERE
                a.category_id_fk IN (47,48)
            ) AS tmp
        """
        return query

    class UniqueLeadsWithoutShowPhoneQuery:

        def __init__(self,
                     conf: getConf,
                     params: ReadParams) -> None:
            self.params = params
            self.conf = conf

    def get_unique_leads(self) -> str:

        query = """
        SELECT
            CAST(date_parse(CAST(year AS varchar) || '-' || CAST(month AS varchar) || '-' || CAST(day AS varchar),'%Y-%c-%e') AS date) AS event_date,
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
                = current_date - interval '1' day
            AND ad_id != 'sdrn:yapocl:classified:' AND ad_id != 'sdrn:yapocl:classified:0'
            AND event_type IN ('Call','SMS','Send')
            AND (local_category_level1 IN ('arrendar','arriendo','comprar') AND local_main_category IN ('inmuebles'))
            AND lead_id != 'unknown'
        GROUP BY  1,2,3
        """
        return query

    def get_segmented_ads(self) -> str:
        query = """
        SELECT
            "date",
            list_id,
            category,
            region,
            commune,
            case
                WHEN category = 'Arrendar' THEN 'Arriendo'
                when uf_price >= 0 AND uf_price < 3000 THEN '0-3000UF'
                when uf_price >= 3000 AND uf_price < 5000 THEN '3000-5000UF'
                when uf_price >= 5000 AND uf_price < 7000 THEN '5000-7000UF'
                when uf_price >= 7000 AND uf_price < 9000 THEN '7000-9000UF'
                when uf_price >= 9000 THEN '9000UF+'
            END AS price_interval,
            estate_type,
            pri_pro
        FROM
            (
            SELECT
               CASE
                    WHEN a.action_type = 'import' THEN bsd.list_time
                    ELSE a.approval_date::date
                END AS "date",
                CASE
                    WHEN a.action_type = 'import' THEN bsd.list_id
                    ELSE a.list_id_nk
                END AS list_id,
                c.category_name AS category,
                r.region_name AS region,
                -- Price in UF
                CASE 
                    WHEN ip.currency = 'uf' THEN a.price::float / 100.0
                    ELSE a.price::float / 
                        (SELECT c.value FROM stg.currency AS c WHERE c.money = 'UF' ORDER BY date_time DESC LIMIT 1)
                END AS uf_price,
                co.comuna_name AS commune,
                CASE
                    WHEN ip.estate_type = '1' THEN 'Departamento'
                    WHEN ip.estate_type = '2' THEN 'Casa'
                    WHEN ip.estate_type = '3' THEN 'Oficina'
                    WHEN ip.estate_type = '4' THEN 'Comercial e industrial'
                    WHEN ip.estate_type = '5' THEN 'Terreno'
                    WHEN ip.estate_type = '6' THEN 'Estacionamiento, bodega u otro'
                    WHEN ip.estate_type = '7' THEN 'Pieza'
                    WHEN ip.estate_type = '8' THEN 'Cabaña'
                    WHEN ip.estate_type = '9' THEN 'Habitacion'
                END AS estate_type,
                CASE
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Pro'
                    WHEN spd.seller_id_fk IS NULL THEN 'Pri'
                    ELSE 'Pro'
                END AS pri_pro
            FROM
                ods.ad AS a
                LEFT JOIN
                    ods.category AS c
                    ON a.category_id_fk = c.category_id_pk
                LEFT JOIN 
                    ods.region AS r
                    ON a.region_id_fk = r.region_id_pk
                LEFT JOIN
                    stg.dim_communes_blocket AS co
                    ON a.communes_id_nk::int = co.comuna_id::int
                INNER JOIN
                    ods.ads_inmo_params AS ip 
                    ON a.ad_id_nk = ip.ad_id_nk
                LEFT JOIN 
                    stg.big_sellers_detail AS bsd
                    ON a.ad_id_nk = bsd.ad_id_nk
                 LEFT JOIN
                    ods.seller_pro_details AS spd
                        ON a.seller_id_fk = spd.seller_id_fk
                            AND a.category_id_fk = spd.category_id_fk
            WHERE
                a.category_id_fk IN (47,48)
            ) AS tmp
        """
        return query

    class NewApprovedAdsQuery:

        def __init__(self,
                     conf: getConf,
                     params: ReadParams) -> None:
            self.params = params
            self.conf = conf

    def get_new_approved_ads(self) -> str:

        query = """
        SELECT
            "date",
            list_id,
            category,
            region,
            commune,
            case
                WHEN category = 'Arrendar' THEN 'Arriendo'
                when uf_price >= 0 AND uf_price < 3000 THEN '0-3000UF'
                when uf_price >= 3000 AND uf_price < 5000 THEN '3000-5000UF'
                when uf_price >= 5000 AND uf_price < 7000 THEN '5000-7000UF'
                when uf_price >= 7000 AND uf_price < 9000 THEN '7000-9000UF'
                when uf_price >= 9000 THEN '9000UF+'
            END AS price_interval,
            estate_type,
            platform,
            pri_pro
        FROM
            (
            SELECT
               CASE
                    WHEN a.action_type = 'import' THEN bsd.list_time
                    ELSE a.approval_date::date
                END AS "date",
                CASE
                    WHEN a.action_type = 'import' THEN bsd.list_id
                    ELSE a.list_id_nk
                END AS list_id,
                c.category_name AS category,
                r.region_name AS region,
                -- Price in UF
                CASE 
                    WHEN ip.currency = 'uf' THEN (CAST(a.price AS float)/100.0) 
                    ELSE CAST(a.price AS float) / 
                        (SELECT a.value FROM stg.currency a WHERE date_time::date = CURRENT_DATE AND a.money = 'UF')
                END AS uf_price,
                co.comuna_name AS commune,
                CASE
                    WHEN ip.estate_type = '1' THEN 'Departamento'
                    WHEN ip.estate_type = '2' THEN 'Casa'
                    WHEN ip.estate_type = '3' THEN 'Oficina'
                    WHEN ip.estate_type = '4' THEN 'Comercial e industrial'
                    WHEN ip.estate_type = '5' THEN 'Terreno'
                    WHEN ip.estate_type = '6' THEN 'Estacionamiento, bodega u otro'
                    WHEN ip.estate_type = '7' THEN 'Pieza'
                    WHEN ip.estate_type = '8' THEN 'Cabaña'
                    WHEN ip.estate_type = '9' THEN 'Habitacion'
                END AS estate_type,
                CASE 
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Web'
                    when p.platform_name = 'Unknown' then 'Web'
                    when p.platform_name = 'M Site' then 'MSite'
                    when p.platform_name = 'NGA Android' then 'AndroidApp'
                    when p.platform_name = 'NGA Ios' then 'iOSApp'
                    ELSE p.platform_name 
                END AS platform,
                -- Aviso Pri/Pro
                CASE
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Pro'
                    WHEN spd.seller_id_fk IS NULL THEN 'Pri'
                    ELSE 'Pro'
                END AS pri_pro,
                a.deletion_date::date AS deletion_date,
                CASE 
                    WHEN a.reason_removed_detail_id_fk = 1  THEN 'yes'
                    WHEN a.deletion_date IS NOT NULL THEN 'no'
                END AS sold_on_site
            FROM
                ods.ad AS a
                LEFT JOIN
                    ods.category AS c
                    ON a.category_id_fk = c.category_id_pk
                LEFT JOIN 
                    ods.region AS r
                    ON a.region_id_fk = r.region_id_pk
                LEFT JOIN
                    stg.dim_communes_blocket AS co
                    ON a.communes_id_nk::int = co.comuna_id::int
                INNER JOIN
                    ods.ads_inmo_params AS ip 
                    ON a.ad_id_nk = ip.ad_id_nk
                LEFT JOIN 
                    stg.big_sellers_detail AS bsd
                    ON a.ad_id_nk = bsd.ad_id_nk
                LEFT JOIN 
                    ods.platform AS p 
                    ON a.platform_id_fk = p.platform_id_pk
                 LEFT JOIN
                    ods.seller_pro_details AS spd
                        ON a.seller_id_fk = spd.seller_id_fk
                            AND a.category_id_fk = spd.category_id_fk
            WHERE
                a.category_id_fk IN (47,48)
                AND (CASE WHEN a.action_type = 'import' THEN bsd.list_time 
                    ELSE a.approval_date::date END) = current_date - interval '1' day
            ) AS tmp
        """
        return query

    class DeletedAdsQuery:

        def __init__(self,
                     conf: getConf,
                     params: ReadParams) -> None:
            self.params = params
            self.conf = conf

    def get_deleted_ads(self) -> str:

        query = """
        SELECT
            deletion_date,
            sold_on_site,
            list_id,
            category,
            region,
            commune,
            case
                WHEN category = 'Arrendar' THEN 'Arriendo'
                when uf_price >= 0 AND uf_price < 3000 THEN '0-3000UF'
                when uf_price >= 3000 AND uf_price < 5000 THEN '3000-5000UF'
                when uf_price >= 5000 AND uf_price < 7000 THEN '5000-7000UF'
                when uf_price >= 7000 AND uf_price < 9000 THEN '7000-9000UF'
                when uf_price >= 9000 THEN '9000UF+'
            END AS price_interval,
            estate_type,
            platform,
            pri_pro      
        FROM
            (
            SELECT
                CASE
                    WHEN a.action_type = 'import' THEN bsd.list_id
                    ELSE a.list_id_nk
                END AS list_id,
                c.category_name AS category,
                r.region_name AS region,
                -- Price in UF
                CASE 
                    WHEN ip.currency = 'uf' THEN (CAST(a.price AS float)/100.0) 
                    ELSE CAST(a.price AS float) / 
                        (SELECT a.value FROM stg.currency a WHERE date_time::date = CURRENT_DATE AND a.money = 'UF')
                END AS uf_price,
                co.comuna_name AS commune, 
                CASE
                    WHEN ip.estate_type = '1' THEN 'Departamento'
                    WHEN ip.estate_type = '2' THEN 'Casa'
                    WHEN ip.estate_type = '3' THEN 'Oficina'
                    WHEN ip.estate_type = '4' THEN 'Comercial e industrial'
                    WHEN ip.estate_type = '5' THEN 'Terreno'
                    WHEN ip.estate_type = '6' THEN 'Estacionamiento, bodega u otro'
                    WHEN ip.estate_type = '7' THEN 'Pieza'
                    WHEN ip.estate_type = '8' THEN 'Cabaña'
                    WHEN ip.estate_type = '9' THEN 'Habitacion'
                END AS estate_type,
                CASE 
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Web'
                    when p.platform_name = 'Unknown' then 'Web'
                    when p.platform_name = 'M Site' then 'MSite'
                    when p.platform_name = 'NGA Android' then 'AndroidApp'
                    when p.platform_name = 'NGA Ios' then 'iOSApp'
                    ELSE p.platform_name 
                END AS platform,
                -- Aviso Pri/Pro
                CASE
                    WHEN bsd.ad_id_nk IS NOT NULL THEN 'Pro'
                    WHEN spd.seller_id_fk IS NULL THEN 'Pri'
                    ELSE 'Pro'
                END AS pri_pro,
                a.deletion_date::date AS deletion_date,
                CASE 
                    WHEN a.reason_removed_detail_id_fk = 1  THEN 'yes'
                    WHEN a.deletion_date IS NOT NULL THEN 'no'
                END AS sold_on_site
            FROM
                ods.ad AS a
                LEFT JOIN
                    ods.category AS c
                    ON a.category_id_fk = c.category_id_pk
                LEFT JOIN 
                    ods.region AS r
                    ON a.region_id_fk = r.region_id_pk
                LEFT JOIN
                    stg.dim_communes_blocket AS co
                    ON a.communes_id_nk::int = co.comuna_id::int
                INNER JOIN
                    ods.ads_inmo_params AS ip 
                    ON a.ad_id_nk = ip.ad_id_nk
                LEFT JOIN 
                    stg.big_sellers_detail AS bsd
                    ON a.ad_id_nk = bsd.ad_id_nk
                LEFT JOIN 
                    ods.platform AS p 
                    ON a.platform_id_fk = p.platform_id_pk
                 LEFT JOIN
                    ods.seller_pro_details AS spd
                        ON a.seller_id_fk = spd.seller_id_fk
                            AND a.category_id_fk = spd.category_id_fk
            WHERE
                a.category_id_fk IN (47,48)
                AND a.deletion_date::date = current_date - interval '1' day
            ) AS tmp
        """
        return query

    class ActiveAdsQuery:

        def __init__(self,
                     conf: getConf,
                     params: ReadParams) -> None:
            self.params = params
            self.conf = conf

        def get_active_ads(self):

            query = """
            SELECT
                aa.status_date,
                aa.ad_id_nk,
                aa.ad_id_fk
            FROM
                ods.active_ads AS aa
                LEFT JOIN
                    ods.ad AS a
                        using(ad_id_nk)
                WHERE
                    a.category_id_fk in (47, 48)
            """
            return query
