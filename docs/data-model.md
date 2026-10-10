# Dictionnaire du modèle de données

## Conventions

| Convention | Règle appliquée |
|---|---|
| Préfixes | `stg_` staging, `hub_`, `lnk_`, `sat_`, `bsat_` (business vault), `dim_` / `fact_` (restitution), `meta_` (technique) |
| Clé de hachage | `hk_<entité>` = `MD5(UPPER(TRIM(clé métier)))`, les clés multiples étant concaténées par `||` |
| Empreinte de changement | `hashdiff` = `MD5` de tous les attributs descriptifs du satellite, valeurs nulles remplacées par une chaîne vide |
| Colonnes d'audit | `load_date` (horodatage du lot) et `record_source` (système source, ex. `CRM.customers`) sur **toutes** les tables du vault |
| Mode d'écriture | `append` uniquement : aucune instruction `UPDATE` ni `DELETE` dans le Raw Vault |
| Dates de validité | Jamais stockées : `valid_from` et `valid_to` sont calculées à la lecture avec `LEAD()` sur `load_date` |

Le format Delta Lake est utilisé pour toutes les tables, ce qui apporte les transactions ACID et le
versionnement des fichiers dans OneLake.

## Raw Vault

### Hubs

Une ligne par clé métier, insérée une seule fois.

| Table | Clé de hachage | Clé métier | Alimenté par |
|---|---|---|---|
| `hub_customer` | `hk_customer` | `customer_id` | CRM, commandes du site web, programme de fidélité |
| `hub_product` | `hk_product` | `sku` | Catalogue ERP, lignes de commande |
| `hub_order` | `hk_order` | `order_id` | Commandes et lignes de commande du site web |

Colonnes : `hk_*`, clé métier, `load_date`, `record_source`.

Un hub est alimenté par **toutes** les sources qui connaissent la clé : la première qui la fournit laisse
sa trace dans `record_source`, les suivantes n'insèrent rien.

### Links

Une ligne par relation observée, en plusieurs-à-plusieurs.

| Table | Clé de hachage | Hubs reliés | Particularité |
|---|---|---|---|
| `lnk_order_customer` | `hk_order_customer` | `hub_order`, `hub_customer` | Relation simple |
| `lnk_order_line` | `hk_order_line` | `hub_order`, `hub_product` | Porte `line_no`, clé dépendante qui distingue deux lignes d'une même commande |

Colonnes : `hk_<link>`, clés de hachage des hubs, éventuelle clé dépendante, `load_date`, `record_source`.

### Satellites

Une nouvelle ligne uniquement lorsque le `hashdiff` change.

| Table | Parent | Attributs | Source |
|---|---|---|---|
| `sat_customer_crm` | `hub_customer` | `first_name`, `last_name`, `email`, `phone`, `city`, `country`, `segment` | `CRM.customers` |
| `sat_customer_loyalty` | `hub_customer` | `loyalty_tier`, `points`, `enrolled_at` | `FID.loyalty` (apparaît au lot 2) |
| `sat_product` | `hub_product` | `product_name`, `category`, `unit_price` | `ERP.products` |
| `sat_order` | `hub_order` | `order_date`, `status`, `channel`, `payment_method` | `WEB.orders` |
| `sat_order_line` | `lnk_order_line` | `quantity`, `unit_price` | `WEB.order_lines` |

Colonnes : clé de hachage du parent, `load_date`, `hashdiff`, `record_source`, puis les attributs.

Les satellites sont découpés **par source** : c'est ce qui permet de raccorder le programme de fidélité en
créant une table, sans toucher au satellite CRM existant.

`sat_order_line` est un satellite de link : il décrit une relation (la ligne de commande) et non une entité.

## Business Vault

| Table | Clé | Colonnes | Règle métier |
|---|---|---|---|
| `bsat_order_totals` | `hk_order` | `order_total`, `nb_lines`, `status`, `is_revenue`, `load_date`, `record_source` | Le montant d'une commande est la somme des `quantity × unit_price` de ses lignes ; une commande au statut `ANNULEE` est exclue du chiffre d'affaires (`is_revenue = false`) |

Cette table est entièrement recalculée à chaque exécution : si la règle du chiffre d'affaires évolue, il
suffit de relancer le notebook, sans recharger aucune source.

## Information Mart (schéma en étoile)

| Table | Grain | Contenu |
|---|---|---|
| `fact_sales` | Une ligne de commande | `order_id`, `line_no`, `order_date`, `customer_id`, `sku`, `quantity`, `unit_price`, `amount`, `revenue`, `status`, `channel`, `payment_method`, `city_at_order` |
| `dim_customer` | Un client (version courante) | `customer_name`, `city`, `segment`, `loyalty_tier`, `nb_versions` |
| `dim_customer_history` | Une version de client | `version`, `city`, `segment`, `load_date`, `valid_from`, `valid_to`, `is_current`, `record_source` |
| `dim_product` | Un produit (version courante) | `product_name`, `category`, `unit_price` |
| `dim_product_history` | Une version de produit | `version`, `unit_price`, `load_date`, `valid_from`, `valid_to`, `is_current` |
| `dim_date` | Un jour | `date`, `year`, `month_no`, `month_name`, `day_name` |

Deux colonnes de `fact_sales` méritent une attention particulière :

- `unit_price` est le prix **réellement pratiqué** au moment de la vente, issu de la ligne de commande, et non
  le prix catalogue courant ;
- `city_at_order` est la ville du client **à la date de la commande**, obtenue en joignant l'historique du
  satellite sur l'intervalle `valid_from` / `valid_to`. Comparée à `dim_customer[city]` (la ville actuelle),
  elle rend visible l'apport de l'historisation.

La colonne `revenue` vaut `amount` lorsque la commande compte dans le chiffre d'affaires, et `0` sinon : elle
applique la règle du Business Vault et simplifie les mesures DAX.

## Table technique

| Table | Contenu |
|---|---|
| `meta_load_log` | Une ligne par table et par exécution : `lot`, `table_name`, `rows_inserted`, `load_date`, `run_at` |

Ce journal sert de preuve d'exécution : il montre notamment qu'une relance du même lot n'insère aucune ligne.
