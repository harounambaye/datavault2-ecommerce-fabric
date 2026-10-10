# Power BI — Modèle sémantique `SM_TerangaShop` et rapport `RPT_TerangaShop`

Le rapport ne lit jamais le Data Vault directement : il s'appuie sur les tables de restitution
(`fact_sales` et ses dimensions), exposées en **Direct Lake**, c'est-à-dire lues directement dans les
fichiers Delta de OneLake, sans import ni actualisation planifiée.

## 1. Tables à inclure (mode Direct Lake)

`fact_sales`, `dim_customer`, `dim_product`, `dim_date`, `dim_customer_history`, `dim_product_history`

## 2. Relations (toutes « plusieurs à un », filtrage simple)

| De (côté plusieurs) | Vers (côté un) |
|---|---|
| `fact_sales[customer_id]` | `dim_customer[customer_id]` |
| `fact_sales[sku]` | `dim_product[sku]` |
| `fact_sales[order_date]` | `dim_date[date]` |
| `dim_customer_history[customer_id]` | `dim_customer[customer_id]` |
| `dim_product_history[sku]` | `dim_product[sku]` |

## 3. Mesures DAX (à créer dans la table `fact_sales`)

```dax
CA = SUM ( fact_sales[revenue] )
```

```dax
Nb commandes = CALCULATE ( DISTINCTCOUNT ( fact_sales[order_id] ), fact_sales[status] <> "ANNULEE" )
```

```dax
Panier moyen = DIVIDE ( [CA], [Nb commandes] )
```

```dax
Clients actifs = DISTINCTCOUNT ( fact_sales[customer_id] )
```

Format des 4 mesures : **Nombre entier**, séparateur des milliers activé.

## 4. Page unique « Teranga Shop — Data Vault 2.0 »

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Teranga Shop — Data Vault 2.0 sur Microsoft Fabric        [Segment : Client ▾] │
├──────────────┬──────────────┬──────────────┬─────────────────────────────┤
│   CA (FCFA)  │ Nb commandes │ Panier moyen │ Clients actifs              │
├──────────────┴───────────┬──┴──────────────┴──────────┬──────────────────┤
│ CA par ville             │ CA par ville               │ CA par catégorie │
│ AU MOMENT DE LA COMMANDE │ SITUATION ACTUELLE         │ (anneau)         │
│ axe: fact_sales          │ axe: dim_customer[city]    │ axe: dim_product │
│      [city_at_order]     │                            │      [category]  │
├──────────────────────────┴────────────────────────────┴──────────────────┤
│ Historique des clients (dim_customer_history)                            │
│ customer_name · version · city · segment · load_date · valid_to · is_current │
└──────────────────────────────────────────────────────────────────────────┘
```

| Visuel | Type | Champs |
|---|---|---|
| Titre | Zone de texte | « Teranga Shop — Data Vault 2.0 sur Microsoft Fabric » |
| Segment | Segment (liste déroulante) | `dim_customer[customer_name]` |
| 4 cartes | Carte | `[CA]`, `[Nb commandes]`, `[Panier moyen]`, `[Clients actifs]` |
| CA par ville (au moment de la commande) | Graphique à barres groupées | Axe Y : `fact_sales[city_at_order]` · Axe X : `[CA]` |
| CA par ville (situation actuelle) | Graphique à barres groupées | Axe Y : `dim_customer[city]` · Axe X : `[CA]` |
| CA par catégorie | Graphique en anneau | Légende : `dim_product[category]` · Valeurs : `[CA]` |
| Historique des clients | Table | `dim_customer_history` : customer_name, version, city, segment, load_date, valid_to, is_current |

Les deux graphiques « CA par ville » sont volontairement côte à côte : le premier utilise la ville
historisée dans le satellite au moment de la commande, le second la ville actuelle du client. Leur écart
rend visible, pour un utilisateur métier, ce que l'historisation apporte.
