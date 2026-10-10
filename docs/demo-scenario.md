# Scénario de démonstration : deux lots et une relance

Le jeu de données est conçu pour qu'un simple enchaînement de trois exécutions rende visibles les trois
promesses de Data Vault 2.0 : historisation, agilité et auditabilité.

| Exécution | Paramètre | Attendu |
|---|---|---|
| 1 | `lot = "20260901"` | Création des 10 tables du Raw Vault et chargement initial |
| 2 | `lot = "20260908"` | Insertion des seuls changements, création d'un satellite supplémentaire |
| 3 | `lot = "20260908"` (relance) | **0 ligne insérée** dans toutes les tables |

## Lot 1 — chargement initial (01/09/2026)

Extraction complète du CRM et du catalogue, commandes du mois d'août.

| Table | Lignes insérées |
|---|---|
| `hub_customer` | 12 |
| `hub_product` | 15 |
| `hub_order` | 40 |
| `lnk_order_customer` | 40 |
| `lnk_order_line` | 87 |
| `sat_customer_crm` | 12 |
| `sat_product` | 15 |
| `sat_order` | 40 |
| `sat_order_line` | 87 |

## Lot 2 — changements (08/09/2026)

Le lot rejoue l'intégralité des clients et des produits, mais n'apporte que les commandes nouvelles ou
modifiées. Le vault détecte seul ce qui a changé.

| Événement métier | Table concernée | Lignes insérées | Observation |
|---|---|---|---|
| La cliente **C003** déménage de Dakar à Thiès | `sat_customer_crm` | +2 (avec le nouveau client) | La version « Dakar » reste consultable |
| Nouveau client **C013** | `hub_customer` | +1 | Seule clé inédite ; les 12 autres ne produisent rien |
| Le produit **P007** passe de 25 000 à 22 500 FCFA | `sat_product` | +1 | Les 14 autres produits, inchangés, n'insèrent rien |
| 15 nouvelles commandes et 4 changements de statut (dont **O0039**, passée à `LIVREE`) | `hub_order` / `sat_order` | +15 / +19 | Le satellite conserve chaque étape du cycle de vie |
| 15 nouvelles commandes | `lnk_order_customer` / `lnk_order_line` / `sat_order_line` | +15 / +23 / +23 | |
| Raccordement du **programme de fidélité** | `sat_customer_loyalty` | +6 (table créée) | **Aucune table existante n'est modifiée** |

## Relance du lot 2 — idempotence

La troisième exécution ne modifie rien : les clés existent déjà dans les hubs et les links, et chaque
`hashdiff` est identique à la dernière version connue. Le journal `meta_load_log` affiche `0` pour chaque
table. C'est cette propriété qui permet de relancer un chargement après un incident, sans contrôle manuel
ni risque de doublon.

## Requêtes de vérification

À exécuter sur le point de terminaison d'analytique SQL du Lakehouse
([`fabric/sql/demo_queries.sql`](../fabric/sql/demo_queries.sql)).

| Requête | Résultat attendu |
|---|---|
| **Q1** Historique de C003 | 2 lignes : Dakar (chargée le 01/09) puis Thiès (chargée le 08/09), avec leur `record_source` et leur `hashdiff` |
| **Q2** Historique de prix de P007 | 25 000 valable du 01/09 au 08/09, puis 22 500 ; la date de fin est calculée par `LEAD()`, jamais stockée |
| **Q3** Cycle de vie de la commande O0039 | `EN_PREPARATION` puis `LIVREE` |
| **Q4** CA par ville | Dakar : 1 823 000 FCFA au moment de la commande contre 1 240 500 FCFA en situation actuelle ; Thiès : 638 500 contre 1 221 000 |
| **Q5** Audit des chargements | 7 lignes : pour chaque satellite, la source et la date de chargement de ses lignes |

L'écart de 582 500 FCFA entre les deux lectures de la requête Q4 correspond exactement au chiffre d'affaires
de la cliente C003 : c'est la part du chiffre d'affaires qu'un modèle sans historisation attribuerait à la
mauvaise ville.

## Chiffres clés après les deux lots

| Indicateur | Valeur |
|---|---|
| Chiffre d'affaires (commandes annulées exclues) | 3 028 000 FCFA |
| Commandes retenues | 54 sur 55 (la commande O0015 est annulée) |
| Panier moyen | 56 074 FCFA |
| Clients actifs | 12 sur 13 (le client C008 n'a jamais commandé) |
| Part de la catégorie Électronique | 48 % du chiffre d'affaires |

## Régénérer le jeu de données

```bash
python data/generate_sample_data.py
```

La graine aléatoire est fixée dans le script : les fichiers produits sont identiques à ceux du dépôt, donc
les chiffres ci-dessus restent reproductibles.
