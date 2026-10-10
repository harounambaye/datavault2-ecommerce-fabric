-- =====================================================================
--  Teranga Shop · Data Vault 2.0 — Requêtes de démonstration (T-SQL)
--  À exécuter dans : LH_EcomVault > Point de terminaison d'analytique SQL
-- =====================================================================

-- Q1. HISTORISATION — la cliente C003 a déménagé.
--     Le HUB ne change pas (1 seule clé), le SATELLITE garde les 2 versions.
SELECT h.customer_id, s.city, s.segment, s.load_date, s.record_source, s.hashdiff
FROM dbo.hub_customer AS h
JOIN dbo.sat_customer_crm AS s ON s.hk_customer = h.hk_customer
WHERE h.customer_id = 'C003'
ORDER BY s.load_date;

-- Q2. HISTORIQUE DES PRIX — le produit P007.
--     La date de fin n'est jamais stockée : elle est calculée avec LEAD() (insert-only).
SELECT h.sku, s.product_name, s.unit_price,
       s.load_date AS valid_from,
       LEAD(s.load_date) OVER (PARTITION BY s.hk_product ORDER BY s.load_date) AS valid_to
FROM dbo.hub_product AS h
JOIN dbo.sat_product AS s ON s.hk_product = h.hk_product
WHERE h.sku = 'P007'
ORDER BY s.load_date;

-- Q3. CYCLE DE VIE D'UNE COMMANDE — O0039 : EN_PREPARATION puis LIVREE.
SELECT h.order_id, s.status, s.load_date, s.record_source
FROM dbo.hub_order AS h
JOIN dbo.sat_order AS s ON s.hk_order = h.hk_order
WHERE h.order_id = 'O0039'
ORDER BY s.load_date;

-- Q4. POURQUOI C'EST IMPORTANT — CA par ville :
--     "au moment de la commande" (historique) vs "situation actuelle" (écrasée).
SELECT COALESCE(a.city, b.city) AS ville,
       a.ca_au_moment_commande,
       b.ca_situation_actuelle
FROM (SELECT city_at_order AS city, SUM(revenue) AS ca_au_moment_commande
      FROM dbo.fact_sales GROUP BY city_at_order) AS a
FULL JOIN (SELECT d.city, SUM(f.revenue) AS ca_situation_actuelle
           FROM dbo.fact_sales AS f JOIN dbo.dim_customer AS d ON d.customer_id = f.customer_id
           GROUP BY d.city) AS b
       ON a.city = b.city
ORDER BY ca_au_moment_commande DESC;

-- Q5. AUDIT — d'où vient chaque donnée et quand est-elle arrivée ?
SELECT 'sat_customer_crm' AS table_name, record_source, load_date, COUNT(*) AS nb_lignes
FROM dbo.sat_customer_crm GROUP BY record_source, load_date
UNION ALL
SELECT 'sat_customer_loyalty', record_source, load_date, COUNT(*)
FROM dbo.sat_customer_loyalty GROUP BY record_source, load_date
UNION ALL
SELECT 'sat_product', record_source, load_date, COUNT(*)
FROM dbo.sat_product GROUP BY record_source, load_date
UNION ALL
SELECT 'sat_order', record_source, load_date, COUNT(*)
FROM dbo.sat_order GROUP BY record_source, load_date
ORDER BY table_name, load_date;
