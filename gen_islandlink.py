import sqlite3
import random
import string
from datetime import datetime, timedelta

# Налаштування бази даних
DB_NAME = 'islandlink_analysis.db'
TOTAL_HOUSEHOLDS = 28000
DUPLICATES_COUNT = 400

# Тарифи
PLANS = [
    {'name': 'Essential 100', 'rate': 19.95, 'weight': 10},
    {'name': 'Full-Fibre 150', 'rate': 30.95, 'weight': 15},
    {'name': 'Full-Fibre 300', 'rate': 31.95, 'weight': 40},
    {'name': 'Full-Fibre 500', 'rate': 35.95, 'weight': 15},
    {'name': 'Full-Fibre 900', 'rate': 41.95, 'weight': 10},
    {'name': 'Full-Fibre 900 - All In', 'rate': 48.95, 'weight': 10}
]

# Географія
POSTCODES_CONFIG = {
    'PO30': {'town': 'Newport', 'weight': 25, 'coverage': 0.90},
    'PO33': {'town': 'Ryde', 'weight': 20, 'coverage': 0.92},
    'PO36': {'town': 'Sandown', 'weight': 10, 'coverage': 0.85},
    'PO35': {'town': 'Bembridge', 'weight': 5, 'wealthy': True, 'coverage': 0.95},
    'PO31': {'town': 'Cowes', 'weight': 10, 'wealthy': True, 'coverage': 0.98}, # Максимальне покриття
    'PO40': {'town': 'Freshwater', 'weight': 2, 'coverage': 0.70},
    'PO38': {'town': 'Ventnor', 'weight': 2, 'coverage': 0.75},
    'PO32': {'town': 'East Cowes', 'weight': 8, 'coverage': 0.90},
    'PO34': {'town': 'Seaview', 'weight': 4, 'coverage': 0.95},
    'PO37': {'town': 'Shanklin', 'weight': 8, 'coverage': 0.88},
    'PO39': {'town': 'Totland Bay', 'weight': 3, 'coverage': 0.70},
    'PO41': {'town': 'Yarmouth', 'weight': 3, 'coverage': 0.75}
}

STREETS = ['High St', 'Main Rd', 'Victoria Ave', 'Church Rd', 'Broadway', 'Station Rd', 'Queens Rd', 'Mill Hill Rd', 'Tithe Barn', 'Arctic Rd']
NAMES = ['Smith', 'Jones', 'Taylor', 'Brown', 'Williams', 'Wilson', 'Johnson', 'Davies', 'Robinson', 'Wright']
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth']

def generate_random_postcode(district):
    # Формат: PO31 7AA
    sector = random.randint(1, 9)
    unit = ''.join(random.choices(string.ascii_uppercase, k=2))
    return f"{district} {sector}{unit}"

def get_random_date(start_year=2023):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(2026, 4, 19)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return start_date + timedelta(days=random_days)

def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS customers')
    cursor.execute('DROP TABLE IF EXISTS subscriptions')
    cursor.execute('DROP TABLE IF EXISTS billing')
    cursor.execute('DROP TABLE IF EXISTS infrastructure')

    cursor.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, street_address TEXT, town TEXT, postcode TEXT, join_date TEXT, status TEXT, churn_date TEXT)')
    cursor.execute('CREATE TABLE subscriptions (customer_id INTEGER, plan_name TEXT, monthly_rate REAL)')
    cursor.execute('CREATE TABLE billing (id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER, invoice_date TEXT, amount_paid REAL, payment_method TEXT)')
    cursor.execute('CREATE TABLE infrastructure (postcode TEXT PRIMARY KEY, is_serviceable BOOLEAN)')

    print("Generating infrastructure (full postcodes)...")
    infra_data = {}
    pc_districts = list(POSTCODES_CONFIG.keys())
    
    # Генеруємо пул з ~2000 унікальних посткодів для острова
    for _ in range(2500):
        district = random.choice(pc_districts)
        full_pc = generate_random_postcode(district)
        if full_pc not in infra_data:
            coverage_chance = POSTCODES_CONFIG[district]['coverage']
            is_svc = random.random() < coverage_chance
            infra_data[full_pc] = is_svc
    
    infra_list = [(pc, svc) for pc, svc in infra_data.items()]
    cursor.executemany('INSERT INTO infrastructure VALUES (?, ?)', infra_list)

    print(f"Generating {TOTAL_HOUSEHOLDS} customers...")
    customer_records = []
    sub_records = []
    
    all_pcs = list(infra_data.keys())
    # Ваги для вибору району
    districts = list(POSTCODES_CONFIG.keys())
    weights = [POSTCODES_CONFIG[d]['weight'] for d in districts]

    for i in range(TOTAL_HOUSEHOLDS):
        # Вибираємо район згідно з вагою
        district = random.choices(districts, weights=weights)[0]
        
        # Знаходимо посткоди цього району з нашого пулу
        district_pcs = [pc for pc in all_pcs if pc.startswith(district)]
        if not district_pcs: # Якщо раптом пустий (малоімовірно)
            pc = generate_random_postcode(district)
            infra_data[pc] = random.random() < POSTCODES_CONFIG[district]['coverage']
            cursor.execute('INSERT OR IGNORE INTO infrastructure VALUES (?, ?)', (pc, infra_data[pc]))
        else:
            pc = random.choice(district_pcs)

        town = POSTCODES_CONFIG[district]['town']
        name = f"{random.choice(FIRST_NAMES)} {random.choice(NAMES)}"
        address = f"{random.randint(1, 150)} {random.choice(STREETS)}"
        join_date = get_random_date()
        
        status = 'Active'
        churn_date = None
        if random.random() < (0.18 if district == 'PO36' else 0.10):
            status = 'Churned'
            c_date = join_date + timedelta(days=random.randint(90, 365))
            if c_date < datetime(2026, 4, 19):
                churn_date = c_date.strftime('%Y-%m-%d')
            else:
                status = 'Active'

        customer_records.append((name, address, town, pc, join_date.strftime('%Y-%m-%d'), status, churn_date))
        
        plan_weights = [p['weight'] for p in PLANS]
        if POSTCODES_CONFIG[district].get('wealthy'):
            plan_weights = [5, 5, 20, 20, 25, 25]
        
        selected_plan = random.choices(PLANS, weights=plan_weights)[0]
        sub_records.append((i+1, selected_plan['name'], selected_plan['rate']))

    cursor.executemany('INSERT INTO customers (name, street_address, town, postcode, join_date, status, churn_date) VALUES (?,?,?,?,?,?,?)', customer_records)
    cursor.executemany('INSERT INTO subscriptions VALUES (?,?,?)', sub_records)

    print("Generating billing...")
    # (Логіка білінгу спрощена для швидкості, але зберігає структуру)
    billing_records = []
    for i in range(200000): # Генеруємо 200к записів
        c_id = random.randint(1, TOTAL_HOUSEHOLDS)
        date = get_random_date().strftime('%Y-%m-%d')
        amount = random.choice([19.95, 30.95, 31.95, 35.95, 41.95, 48.95])
        billing_records.append((c_id, date, amount, 'Direct Debit'))
        if len(billing_records) > 50000:
            cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records)
            billing_records = []
    
    if billing_records:
        cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records)

    conn.commit()
    conn.close()
    print("Database updated with full postcodes and realistic infrastructure!")

if __name__ == '__main__':
    create_db()
