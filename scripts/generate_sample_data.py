"""
Tạo dữ liệu mẫu (sample) để test pipeline
Không cần tài khoản Kaggle
"""

import pandas as pd
import numpy as np
import os

np.random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

N_ORDERS = 5000
N_CUSTOMERS = 3000
N_PRODUCTS = 500
N_SELLERS = 50

print("=" * 60)
print("  GENERATING SAMPLE DATA")
print("=" * 60)

# ====== 1. Customers ======
states = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "CE", "DF", "GO",
          "PE", "ES", "MA", "MT", "PA", "PB", "AM", "RN", "PI", "AL"]
cities = {
    "SP": ["sao paulo", "campinas", "guarulhos", "santos"],
    "RJ": ["rio de janeiro", "niteroi", "petropolis"],
    "MG": ["belo horizonte", "uberlandia", "juiz de fora"],
    "RS": ["porto alegre", "caxias do sul", "pelotas"],
    "PR": ["curitiba", "londrina", "maringa"],
    "SC": ["florianopolis", "joinville", "blumenau"],
    "BA": ["salvador", "feira de santana"],
    "CE": ["fortaleza", "juazeiro do norte"],
    "DF": ["brasilia"],
    "GO": ["goiania", "anapolis"],
}

customer_ids = [f"cust_{i:05d}" for i in range(N_CUSTOMERS)]
customer_unique_ids = [f"custuniq_{i:05d}" for i in range(N_CUSTOMERS)]
customer_states = np.random.choice(states, N_CUSTOMERS, p=[
    0.25, 0.12, 0.10, 0.07, 0.06, 0.05, 0.05, 0.04, 0.04, 0.03,
    0.03, 0.03, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.01, 0.03
])

customer_cities_list = []
customer_zips = []
for st in customer_states:
    city_list = cities.get(st, [st.lower()])
    customer_cities_list.append(np.random.choice(city_list))
    customer_zips.append(np.random.randint(10000, 99999))

df_customers = pd.DataFrame({
    "customer_id": customer_ids,
    "customer_unique_id": customer_unique_ids,
    "customer_zip_code_prefix": customer_zips,
    "customer_city": customer_cities_list,
    "customer_state": customer_states
})
df_customers.to_csv(f"{DATA_DIR}/olist_customers_dataset.csv", index=False)
print(f"  customers: {len(df_customers)} rows")

# ====== 2. Sellers ======
seller_ids = [f"seller_{i:04d}" for i in range(N_SELLERS)]
seller_states = np.random.choice(["SP", "RJ", "MG", "PR", "SC", "RS", "BA", "CE"], N_SELLERS,
                                  p=[0.35, 0.15, 0.12, 0.10, 0.08, 0.08, 0.06, 0.06])
seller_cities_list = []
seller_zips = []
for st in seller_states:
    city_list = cities.get(st, [st.lower()])
    seller_cities_list.append(np.random.choice(city_list))
    seller_zips.append(np.random.randint(10000, 99999))

df_sellers = pd.DataFrame({
    "seller_id": seller_ids,
    "seller_zip_code_prefix": seller_zips,
    "seller_city": seller_cities_list,
    "seller_state": seller_states
})
df_sellers.to_csv(f"{DATA_DIR}/olist_sellers_dataset.csv", index=False)
print(f"  sellers: {len(df_sellers)} rows")

# ====== 3. Products ======
categories_en = [
    "health_beauty", "computers_accessories", "furniture_decor",
    "housewares", "watches_gifts", "sports_leisure", "bed_bath_table",
    "telephony", "auto", "toys", "electronics", "baby",
    "fashion_bags_accessories", "cool_stuff", "garden_tools",
    "perfumery", "stationery", "computers", "pet_shop", "office_furniture"
]
categories_pt = [
    "beleza_saude", "informatica_acessorios", "moveis_decoracao",
    "utilidades_domesticas", "relogios_presentes", "esporte_lazer",
    "cama_mesa_banho", "telefonia", "automotivo", "brinquedos",
    "eletronicos", "bebes", "fashion_bolsas_e_acessorios",
    "cool_stuff", "ferramentas_jardim", "perfumaria",
    "papelaria", "pcs", "pet_shop", "moveis_escritorio"
]

product_ids = [f"prod_{i:05d}" for i in range(N_PRODUCTS)]
prod_categories = np.random.choice(categories_pt, N_PRODUCTS)

df_products = pd.DataFrame({
    "product_id": product_ids,
    "product_category_name": prod_categories,
    "product_name_lenght": np.random.randint(20, 80, N_PRODUCTS),
    "product_description_lenght": np.random.randint(50, 3000, N_PRODUCTS),
    "product_photos_qty": np.random.randint(1, 8, N_PRODUCTS),
    "product_weight_g": np.random.exponential(2000, N_PRODUCTS).astype(int).clip(50, 30000),
    "product_length_cm": np.random.randint(10, 100, N_PRODUCTS),
    "product_height_cm": np.random.randint(5, 60, N_PRODUCTS),
    "product_width_cm": np.random.randint(10, 80, N_PRODUCTS)
})
df_products.to_csv(f"{DATA_DIR}/olist_products_dataset.csv", index=False)
print(f"  products: {len(df_products)} rows")

# ====== Category translation ======
df_translation = pd.DataFrame({
    "product_category_name": categories_pt,
    "product_category_name_english": categories_en
})
df_translation.to_csv(f"{DATA_DIR}/product_category_name_translation.csv", index=False)
print(f"  category_translation: {len(df_translation)} rows")

# ====== 4. Orders ======
order_ids = [f"order_{i:06d}" for i in range(N_ORDERS)]
order_customers = np.random.choice(customer_ids, N_ORDERS)

# Ngày đặt hàng: 2017-01 đến 2018-08
base_date = pd.Timestamp("2017-01-01")
purchase_dates = [base_date + pd.Timedelta(days=int(d))
                  for d in np.random.uniform(0, 600, N_ORDERS)]

statuses = np.random.choice(
    ["delivered", "shipped", "canceled", "processing"],
    N_ORDERS, p=[0.90, 0.05, 0.03, 0.02]
)

approved_dates = [d + pd.Timedelta(hours=np.random.uniform(0.5, 48)) for d in purchase_dates]
carrier_dates = [d + pd.Timedelta(days=np.random.uniform(1, 5)) for d in purchase_dates]
delivered_dates = [d + pd.Timedelta(days=np.random.uniform(3, 30)) for d in purchase_dates]
estimated_dates = [d + pd.Timedelta(days=np.random.uniform(10, 40)) for d in purchase_dates]

df_orders = pd.DataFrame({
    "order_id": order_ids,
    "customer_id": order_customers,
    "order_status": statuses,
    "order_purchase_timestamp": purchase_dates,
    "order_approved_at": approved_dates,
    "order_delivered_carrier_date": carrier_dates,
    "order_delivered_customer_date": delivered_dates,
    "order_estimated_delivery_date": estimated_dates
})

# Null ra delivered dates cho orders chưa giao
mask_not_delivered = df_orders["order_status"] != "delivered"
df_orders.loc[mask_not_delivered, "order_delivered_customer_date"] = None
df_orders.loc[mask_not_delivered, "order_delivered_carrier_date"] = None

df_orders.to_csv(f"{DATA_DIR}/olist_orders_dataset.csv", index=False)
print(f"  orders: {len(df_orders)} rows")

# ====== 5. Order Items ======
items = []
for oid in order_ids:
    n_items = np.random.choice([1, 1, 1, 2, 2, 3], 1)[0]
    for seq in range(1, n_items + 1):
        price = round(np.random.exponential(120) + 10, 2)
        freight = round(np.random.exponential(20) + 5, 2)
        purchase_ts = df_orders.loc[df_orders["order_id"] == oid, "order_purchase_timestamp"].values[0]
        ship_limit = pd.Timestamp(purchase_ts) + pd.Timedelta(days=np.random.uniform(3, 10))
        items.append({
            "order_id": oid,
            "order_item_id": seq,
            "product_id": np.random.choice(product_ids),
            "seller_id": np.random.choice(seller_ids),
            "shipping_limit_date": ship_limit,
            "price": price,
            "freight_value": freight
        })

df_items = pd.DataFrame(items)
df_items.to_csv(f"{DATA_DIR}/olist_order_items_dataset.csv", index=False)
print(f"  order_items: {len(df_items)} rows")

# ====== 6. Payments ======
payments = []
for oid in order_ids:
    n_payments = np.random.choice([1, 1, 1, 2], 1)[0]
    total_order = df_items[df_items["order_id"] == oid]["price"].sum()
    for seq in range(1, n_payments + 1):
        ptype = np.random.choice(["credit_card", "boleto", "debit_card", "voucher"],
                                  p=[0.7, 0.15, 0.10, 0.05])
        installments = np.random.choice([1, 2, 3, 4, 6, 10], 1)[0] if ptype == "credit_card" else 1
        value = round(total_order / n_payments, 2)
        payments.append({
            "order_id": oid,
            "payment_sequential": seq,
            "payment_type": ptype,
            "payment_installments": installments,
            "payment_value": value
        })

df_payments = pd.DataFrame(payments)
df_payments.to_csv(f"{DATA_DIR}/olist_order_payments_dataset.csv", index=False)
print(f"  payments: {len(df_payments)} rows")

# ====== 7. Reviews ======
reviews = []
for oid in order_ids[:int(N_ORDERS * 0.7)]:
    reviews.append({
        "review_id": f"rev_{len(reviews):06d}",
        "order_id": oid,
        "review_score": np.random.choice([1, 2, 3, 4, 5], p=[0.05, 0.05, 0.10, 0.25, 0.55]),
        "review_comment_title": "",
        "review_comment_message": "",
        "review_creation_date": "2018-01-01",
        "review_answer_timestamp": "2018-01-02"
    })

df_reviews = pd.DataFrame(reviews)
df_reviews.to_csv(f"{DATA_DIR}/olist_order_reviews_dataset.csv", index=False)
print(f"  reviews: {len(df_reviews)} rows")

# ====== 8. Geolocation ======
geo = []
for zip_code in np.random.randint(10000, 99999, 500):
    st = np.random.choice(states)
    city = np.random.choice(cities.get(st, [st.lower()]))
    geo.append({
        "geolocation_zip_code_prefix": zip_code,
        "geolocation_lat": np.random.uniform(-30, -5),
        "geolocation_lng": np.random.uniform(-55, -35),
        "geolocation_city": city,
        "geolocation_state": st
    })

df_geo = pd.DataFrame(geo)
df_geo.to_csv(f"{DATA_DIR}/olist_geolocation_dataset.csv", index=False)
print(f"  geolocation: {len(df_geo)} rows")

print(f"\n{'=' * 60}")
print(f"  ALL SAMPLE DATA GENERATED!")
print(f"  Location: {os.path.abspath(DATA_DIR)}")
print(f"{'=' * 60}")
