# %% [markdown]
# # NB_DataVault — Teranga Shop · Data Vault 2.0
# Charge **un lot** de fichiers sources dans le Data Vault, puis reconstruit les tables de restitution.
#
# | Partie | Couche | Rôle |
# |---|---|---|
# | A | Staging | lecture des CSV + colonnes techniques (hash keys, load_date, record_source) |
# | B | Raw Vault | Hubs, Links, Satellites — **insert-only** (jamais d'UPDATE ni de DELETE) |
# | C | Business Vault | règle métier : le CA exclut les commandes annulées |
# | D | Information Mart | schéma en étoile pour Power BI (dimensions + faits) |
# | E | Audit | journal du chargement |
#
# **Paramètre** : `lot` = `"20260901"` (lot 1) ou `"20260908"` (lot 2).

# %% PARAMETERS
lot = "20260901"   # "20260901" = lot 1 (chargement initial) · "20260908" = lot 2 (changements)

# %%
# ============================ PARTIE 0 : outils communs ============================
from pyspark.sql import functions as F, Window

LOAD_DATE = F.to_timestamp(F.lit(f"{lot[:4]}-{lot[4:6]}-{lot[6:]} 00:00:00"))  # date de chargement simulée
LANDING = f"Files/landing/lot_{lot}"
journal = []  # (table, lignes insérées) pour ce chargement


def table_exists(name):
    try:
        spark.table(name).limit(0).collect()
        return True
    except Exception:
        return False


def hash_key(*cols):
    """Hash key DV 2.0 : MD5 de la (des) clé(s) métier normalisée(s) -> même clé calculée partout, sans lookup."""
    return F.md5(F.concat_ws("||", *[F.upper(F.trim(F.col(c).cast("string"))) for c in cols]))


def hash_diff(*cols):
    """Empreinte de tous les attributs descriptifs : si elle change, la donnée a changé."""
    return F.md5(F.concat_ws("||", *[F.coalesce(F.trim(F.col(c).cast("string")), F.lit("")) for c in cols]))


def read_source(file, source):
    """Lit un CSV du lot et ajoute les 2 colonnes d'audit obligatoires : load_date et record_source."""
    try:
        df = spark.read.option("header", True).option("encoding", "UTF-8").csv(f"{LANDING}/{file}")
    except Exception:
        print(f"  (pas de fichier {file} dans ce lot)")
        return None
    return df.withColumn("load_date", LOAD_DATE).withColumn("record_source", F.lit(source))


def append(df, table):
    n = df.count()
    if n > 0 or not table_exists(table):
        df.write.format("delta").mode("append").saveAsTable(table)
    journal.append((table, n))
    print(f"  {table:<22} +{n} ligne(s)")


def overwrite(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"  {table:<22} reconstruite ({df.count()} lignes)")


# ---- Les 3 patterns de chargement Data Vault ------------------------------------
def load_hub(stg, table, hk, bk):
    """HUB : on insère uniquement les clés métier jamais vues."""
    rows = stg.select(hk, bk, "load_date", "record_source").dropDuplicates([hk])
    if table_exists(table):
        rows = rows.join(spark.table(table).select(hk), hk, "left_anti")
    append(rows, table)


def load_link(stg, table, hk, parents, extra=()):
    """LINK : on insère uniquement les relations jamais vues."""
    rows = stg.select(hk, *parents, *extra, "load_date", "record_source").dropDuplicates([hk])
    if table_exists(table):
        rows = rows.join(spark.table(table).select(hk), hk, "left_anti")
    append(rows, table)


def load_sat(stg, table, hk, attrs):
    """SATELLITE : on insère une nouvelle version seulement si le hashdiff diffère de la dernière version connue."""
    rows = (stg.withColumn("hashdiff", hash_diff(*attrs))
               .select(hk, "load_date", "hashdiff", "record_source", *attrs)
               .dropDuplicates([hk, "hashdiff"]))
    if table_exists(table):
        w = Window.partitionBy(hk).orderBy(F.col("load_date").desc())
        last = (spark.table(table).where(F.col("load_date") <= LOAD_DATE)
                     .withColumn("rn", F.row_number().over(w)).where("rn = 1")
                     .select(hk, F.col("hashdiff").alias("last_hashdiff")))
        rows = (rows.join(last, hk, "left")
                    .where(F.col("last_hashdiff").isNull() | (F.col("last_hashdiff") != F.col("hashdiff")))
                    .drop("last_hashdiff"))
    append(rows, table)


print(f"Chargement du lot {lot} depuis {LANDING}")

# %%
# ============================ PARTIE A : STAGING ============================
# Seulement des "hard rules" : typage + calcul des hash keys. Aucune règle métier ici.
stg_customer = (read_source("crm_customers.csv", "CRM.customers")
                .withColumn("hk_customer", hash_key("customer_id")))

stg_product = (read_source("products.csv", "ERP.products")
               .withColumn("unit_price", F.col("unit_price").cast("int"))
               .withColumn("hk_product", hash_key("sku")))

stg_order = (read_source("orders.csv", "WEB.orders")
             .withColumn("order_date", F.to_date("order_date"))
             .withColumn("hk_order", hash_key("order_id"))
             .withColumn("hk_customer", hash_key("customer_id"))
             .withColumn("hk_order_customer", hash_key("order_id", "customer_id")))

stg_line = (read_source("order_lines.csv", "WEB.order_lines")
            .withColumn("line_no", F.col("line_no").cast("int"))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("int"))
            .withColumn("hk_order", hash_key("order_id"))
            .withColumn("hk_product", hash_key("sku"))
            .withColumn("hk_order_line", hash_key("order_id", "sku", "line_no")))

stg_loyalty = read_source("loyalty_members.csv", "FID.loyalty")  # n'existe qu'à partir du lot 2
if stg_loyalty is not None:
    stg_loyalty = (stg_loyalty.withColumn("points", F.col("points").cast("int"))
                              .withColumn("enrolled_at", F.to_date("enrolled_at"))
                              .withColumn("hk_customer", hash_key("customer_id")))

display(stg_customer.select("customer_id", "hk_customer", "city", "load_date", "record_source"))

# %%
# ============================ PARTIE B : RAW VAULT ============================
print("HUBS (entités métier) — alimentés par toutes les sources qui connaissent la clé")
load_hub(stg_customer, "hub_customer", "hk_customer", "customer_id")
load_hub(stg_order,    "hub_customer", "hk_customer", "customer_id")
load_hub(stg_product,  "hub_product",  "hk_product",  "sku")
load_hub(stg_line,     "hub_product",  "hk_product",  "sku")
load_hub(stg_order,    "hub_order",    "hk_order",    "order_id")
load_hub(stg_line,     "hub_order",    "hk_order",    "order_id")
if stg_loyalty is not None:
    load_hub(stg_loyalty, "hub_customer", "hk_customer", "customer_id")

print("LINKS (relations entre hubs)")
load_link(stg_order, "lnk_order_customer", "hk_order_customer", ["hk_order", "hk_customer"])
load_link(stg_line,  "lnk_order_line",     "hk_order_line",     ["hk_order", "hk_product"], extra=["line_no"])

print("SATELLITES (attributs descriptifs + historique)")
load_sat(stg_customer, "sat_customer_crm", "hk_customer",
         ["first_name", "last_name", "email", "phone", "city", "country", "segment"])
load_sat(stg_product, "sat_product", "hk_product", ["product_name", "category", "unit_price"])
load_sat(stg_order, "sat_order", "hk_order", ["order_date", "status", "channel", "payment_method"])
load_sat(stg_line, "sat_order_line", "hk_order_line", ["quantity", "unit_price"])
if stg_loyalty is not None:  # NOUVELLE SOURCE = NOUVEAU SATELLITE, aucune table existante n'est modifiée
    load_sat(stg_loyalty, "sat_customer_loyalty", "hk_customer", ["loyalty_tier", "points", "enrolled_at"])

# %%
# ============================ PARTIE C : BUSINESS VAULT ============================
# Règles métier ("soft rules"), recalculables à tout moment à partir du Raw Vault.
def current(sat, hk):
    """Dernière version connue de chaque clé d'un satellite."""
    w = Window.partitionBy(hk).orderBy(F.col("load_date").desc())
    return spark.table(sat).withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


order_lines = (spark.table("lnk_order_line").select("hk_order_line", "hk_order", "hk_product", "line_no")
               .join(current("sat_order_line", "hk_order_line").select("hk_order_line", "quantity", "unit_price"),
                     "hk_order_line"))

bsat_order_totals = (order_lines.groupBy("hk_order")
                     .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("order_total"),
                          F.count("*").alias("nb_lines"))
                     .join(current("sat_order", "hk_order").select("hk_order", "status"), "hk_order")
                     .withColumn("is_revenue", F.col("status") != "ANNULEE")  # règle : une commande annulée n'est pas du CA
                     .withColumn("load_date", LOAD_DATE)
                     .withColumn("record_source", F.lit("BV.regle_ca_commande")))
overwrite(bsat_order_totals, "bsat_order_totals")

# %%
# ============================ PARTIE D : INFORMATION MART (étoile pour Power BI) ============================
def versions(sat, hk):
    """Satellite -> historique SCD2. Les dates de fin sont CALCULÉES (LEAD), jamais stockées : le vault reste insert-only."""
    w = Window.partitionBy(hk).orderBy("load_date")
    return (spark.table(sat)
            .withColumn("version", F.row_number().over(w))
            .withColumn("valid_from", F.when(F.col("version") == 1, F.to_timestamp(F.lit("1900-01-01 00:00:00")))
                                       .otherwise(F.col("load_date")))
            .withColumn("valid_to", F.coalesce(F.lead("load_date").over(w), F.to_timestamp(F.lit("9999-12-31 00:00:00"))))
            .withColumn("is_current", F.lead("load_date").over(w).isNull()))


hub_customer = spark.table("hub_customer").select("hk_customer", "customer_id")
hub_product = spark.table("hub_product").select("hk_product", "sku")
hub_order = spark.table("hub_order").select("hk_order", "order_id")

# --- Clients : historique complet + version courante
dim_customer_history = (versions("sat_customer_crm", "hk_customer").join(hub_customer, "hk_customer")
                        .select("customer_id", "version",
                                F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
                                "city", "segment", "load_date", "valid_from", "valid_to", "is_current", "record_source"))

if table_exists("sat_customer_loyalty"):
    loyalty = current("sat_customer_loyalty", "hk_customer").join(hub_customer, "hk_customer").select("customer_id", "loyalty_tier")
else:
    loyalty = spark.createDataFrame([], "customer_id string, loyalty_tier string")

dim_customer = (dim_customer_history.where("is_current").select("customer_id", "customer_name", "city", "segment")
                .join(loyalty, "customer_id", "left")
                .join(dim_customer_history.groupBy("customer_id").agg(F.count("*").alias("nb_versions")), "customer_id")
                .fillna({"loyalty_tier": "Non membre"}))

# --- Produits : historique des prix + version courante
dim_product_history = (versions("sat_product", "hk_product").join(hub_product, "hk_product")
                       .select("sku", "version", "product_name", "category", "unit_price",
                               "load_date", "valid_from", "valid_to", "is_current"))
dim_product = dim_product_history.where("is_current").select("sku", "product_name", "category", "unit_price")

# --- Calendrier (août-septembre 2026)
mois = F.array(*[F.lit(m) for m in ["janvier", "février", "mars", "avril", "mai", "juin", "juillet",
                                    "août", "septembre", "octobre", "novembre", "décembre"]])
jours = F.array(*[F.lit(j) for j in ["dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"]])
dim_date = (spark.sql("SELECT explode(sequence(to_date('2026-08-01'), to_date('2026-09-30'), interval 1 day)) AS date")
            .withColumn("year", F.year("date"))
            .withColumn("month_no", F.month("date"))
            .withColumn("month_name", F.element_at(mois, F.month("date")))
            .withColumn("day_name", F.element_at(jours, F.dayofweek("date"))))

# --- Faits : une ligne par ligne de commande
city_hist = dim_customer_history.select(F.col("customer_id").alias("h_customer_id"),
                                        F.col("city").alias("city_at_order"), "valid_from", "valid_to")
fact_sales = (order_lines
              .join(hub_order, "hk_order").join(hub_product, "hk_product")
              .join(current("sat_order", "hk_order").select("hk_order", "order_date", "status", "channel", "payment_method"), "hk_order")
              .join(spark.table("lnk_order_customer").select("hk_order", "hk_customer"), "hk_order")
              .join(hub_customer, "hk_customer")
              .join(spark.table("bsat_order_totals").select("hk_order", "is_revenue"), "hk_order")
              .withColumn("amount", F.col("quantity") * F.col("unit_price"))
              .withColumn("revenue", F.when(F.col("is_revenue"), F.col("amount")).otherwise(F.lit(0)))
              # ville où habitait le client AU MOMENT de la commande (vision "as-was")
              .join(city_hist, (F.col("customer_id") == F.col("h_customer_id"))
                    & (F.col("order_date") >= F.col("valid_from")) & (F.col("order_date") < F.col("valid_to")), "left")
              .select("order_id", "line_no", "order_date", "customer_id", "sku", "quantity", "unit_price",
                      "amount", "revenue", "status", "channel", "payment_method", "city_at_order"))

overwrite(dim_customer, "dim_customer")
overwrite(dim_customer_history, "dim_customer_history")
overwrite(dim_product, "dim_product")
overwrite(dim_product_history, "dim_product_history")
overwrite(dim_date, "dim_date")
overwrite(fact_sales, "fact_sales")

# %%
# ============================ PARTIE E : AUDIT DU CHARGEMENT ============================
bilan = (spark.createDataFrame(journal, "table_name string, rows_inserted int")
         .groupBy("table_name").agg(F.sum("rows_inserted").cast("int").alias("rows_inserted"))
         .withColumn("lot", F.lit(lot))
         .withColumn("load_date", LOAD_DATE)
         .withColumn("run_at", F.current_timestamp()))
bilan.write.format("delta").mode("append").saveAsTable("meta_load_log")
display(bilan.select("lot", "table_name", "rows_inserted").orderBy("table_name"))
