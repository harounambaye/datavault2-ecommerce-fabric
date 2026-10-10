"""
Génère les fichiers sources de l'étude de cas « Teranga Shop » (Data Vault 2.0).

Lot 1 (2026-09-01) : chargement initial (extraction complète CRM + catalogue, commandes d'août).
Lot 2 (2026-09-08) : changements pour démontrer l'historisation et l'agilité :
  - C003 déménage de Dakar à Thiès              -> nouvelle ligne dans sat_customer_crm
  - P007 passe de 25 000 à 22 500 FCFA          -> nouvelle ligne dans sat_product
  - O0039 passe de EN_PREPARATION à LIVREE      -> nouvelle ligne dans sat_order
  - nouveau client C013 + 15 nouvelles commandes
  - NOUVELLE SOURCE : programme de fidélité     -> nouveau satellite, rien d'autre ne change

Exécution : python generate_sample_data.py
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
BASE = Path(__file__).parent / "landing"
LOT1, LOT2 = "lot_20260901", "lot_20260908"

# ---------------------------------------------------------------- Clients (CRM)
CUSTOMERS = [
    ("C001", "Mamadou", "Diallo", "Dakar", "Particulier"),
    ("C002", "Fatou", "Sow", "Dakar", "Particulier"),
    ("C003", "Awa", "Ndiaye", "Dakar", "Particulier"),
    ("C004", "Ousmane", "Fall", "Thiès", "Professionnel"),
    ("C005", "Aminata", "Ba", "Saint-Louis", "Particulier"),
    ("C006", "Cheikh", "Diop", "Dakar", "Professionnel"),
    ("C007", "Mariama", "Sarr", "Kaolack", "Particulier"),
    ("C008", "Ibrahima", "Gueye", "Ziguinchor", "Particulier"),
    ("C009", "Khady", "Faye", "Dakar", "Particulier"),
    ("C010", "Moussa", "Cissé", "Thiès", "Particulier"),
    ("C011", "Ndeye", "Mbaye", "Saint-Louis", "Professionnel"),
    ("C012", "Abdoulaye", "Seck", "Mbour", "Particulier"),
]
NEW_CUSTOMER = ("C013", "Seynabou", "Diagne", "Dakar", "Particulier")


def customer_row(c, idx):
    cid, fn, ln, city, seg = c
    email = f"{fn.lower()}.{ln.lower()}@mail.example".replace("é", "e")
    phone = f"+221 77 {100 + idx:03d} {10 + idx:02d} {20 + idx:02d}"
    return [cid, fn, ln, email, phone, city, "Sénégal", seg]


# ---------------------------------------------------------------- Produits (catalogue)
PRODUCTS = [
    ("P001", "T-shirt wax", "Mode", 8500),
    ("P002", "Boubou brodé", "Mode", 35000),
    ("P003", "Sandales cuir", "Mode", 12000),
    ("P004", "Smartphone A15", "Électronique", 95000),
    ("P005", "Power bank 20000 mAh", "Électronique", 15000),
    ("P006", "Montre connectée", "Électronique", 30000),
    ("P007", "Écouteurs Bluetooth", "Électronique", 25000),
    ("P008", "Ventilateur sur pied", "Maison", 22000),
    ("P009", "Set de casseroles", "Maison", 28000),
    ("P010", "Lampe solaire", "Maison", 9500),
    ("P011", "Beurre de karité 250 g", "Beauté", 4500),
    ("P012", "Thiouraye parfumé", "Beauté", 6000),
    ("P013", "Savon noir artisanal", "Beauté", 2000),
    ("P014", "Café Touba 500 g", "Alimentation", 3500),
    ("P015", "Bissap séché 1 kg", "Alimentation", 5000),
]
PRICE_LOT2 = {"P007": 22500}  # promotion à partir du lot 2

CHANNELS = ["Web", "Mobile"]
PAYMENTS = ["Wave", "Orange Money", "Carte bancaire", "Paiement à la livraison"]


def gen_orders(first_no, n, start, days, price_of, forced_customers, forced_skus, customer_ids):
    """Génère n commandes + lignes. forced_* : {order_no: valeur} pour garantir le scénario."""
    orders, lines = [], []
    for i in range(n):
        no = first_no + i
        oid = f"O{no:04d}"
        odate = start + timedelta(days=int(i * days / n))
        cid = forced_customers.get(no, random.choice(customer_ids))
        orders.append([oid, cid, odate.isoformat(), None, random.choice(CHANNELS), random.choice(PAYMENTS)])
        skus = random.sample([p[0] for p in PRODUCTS], random.randint(1, 3))
        if no in forced_skus and forced_skus[no] not in skus:
            skus[0] = forced_skus[no]
        for ln, sku in enumerate(skus, start=1):
            cat = next(p[2] for p in PRODUCTS if p[0] == sku)
            qty = random.randint(1, 4) if cat in ("Beauté", "Alimentation") else random.randint(1, 2)
            lines.append([oid, ln, sku, qty, price_of(sku)])
    return orders, lines


def write(folder, name, header, rows):
    path = BASE / folder / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {folder}/{name:<22} {len(rows):>3} lignes")


H_CUST = ["customer_id", "first_name", "last_name", "email", "phone", "city", "country", "segment"]
H_PROD = ["sku", "product_name", "category", "unit_price"]
H_ORD = ["order_id", "customer_id", "order_date", "status", "channel", "payment_method"]
H_LINE = ["order_id", "line_no", "sku", "quantity", "unit_price"]

price1 = {p[0]: p[3] for p in PRODUCTS}
price2 = {**price1, **PRICE_LOT2}
ids1 = [c[0] for c in CUSTOMERS]

# ---------------------------------------------------------------- LOT 1
o1, l1 = gen_orders(1, 40, date(2026, 8, 1), 31, price1.get,
                    forced_customers={5: "C003", 21: "C003", 33: "C003"},
                    forced_skus={10: "P007", 21: "P007", 28: "P007"},
                    customer_ids=ids1)
status1 = {15: "ANNULEE", 37: "EXPEDIEE", 38: "EXPEDIEE", 39: "EN_PREPARATION", 40: "EN_PREPARATION"}
for i, o in enumerate(o1, start=1):
    o[3] = status1.get(i, "LIVREE")

print("Lot 1 :")
write(LOT1, "crm_customers.csv", H_CUST, [customer_row(c, i) for i, c in enumerate(CUSTOMERS)])
write(LOT1, "products.csv", H_PROD, [[p[0], p[1], p[2], price1[p[0]]] for p in PRODUCTS])
write(LOT1, "orders.csv", H_ORD, o1)
write(LOT1, "order_lines.csv", H_LINE, l1)

# ---------------------------------------------------------------- LOT 2
cust2 = [(c[0], c[1], c[2], "Thiès" if c[0] == "C003" else c[3], c[4]) for c in CUSTOMERS] + [NEW_CUSTOMER]
o2, l2 = gen_orders(41, 15, date(2026, 9, 1), 7, price2.get,
                    forced_customers={43: "C003", 47: "C013", 52: "C013"},
                    forced_skus={45: "P007", 50: "P007"},
                    customer_ids=ids1 + ["C013"])
for i, o in enumerate(o2):
    o[3] = "LIVREE" if i < 6 else ("EXPEDIEE" if i < 11 else "EN_PREPARATION")
# Extraction incrémentale : les commandes du lot 1 dont le statut a changé sont ré-extraites
updated = {37: "LIVREE", 38: "LIVREE", 39: "LIVREE", 40: "EXPEDIEE"}
o2_changed = [[*o1[no - 1][:3], st, *o1[no - 1][4:]] for no, st in updated.items()]

print("Lot 2 :")
write(LOT2, "crm_customers.csv", H_CUST, [customer_row(c, i) for i, c in enumerate(cust2)])
write(LOT2, "products.csv", H_PROD, [[p[0], p[1], p[2], price2[p[0]]] for p in PRODUCTS])
write(LOT2, "orders.csv", H_ORD, o2_changed + o2)
write(LOT2, "order_lines.csv", H_LINE, l2)
write(LOT2, "loyalty_members.csv", ["customer_id", "loyalty_tier", "points", "enrolled_at"], [
    ["C001", "Or", 5200, "2025-11-03"],
    ["C003", "Argent", 2100, "2026-01-15"],
    ["C006", "Or", 4800, "2025-09-20"],
    ["C009", "Bronze", 600, "2026-05-02"],
    ["C011", "Argent", 1500, "2026-03-11"],
    ["C013", "Bronze", 100, "2026-09-02"],
])
