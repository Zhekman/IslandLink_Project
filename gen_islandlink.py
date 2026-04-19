import sqlite3
import random
import string
from datetime import datetime, timedelta

# Налаштування
DB_NAME = 'islandlink_analysis.db'
TOTAL_HOUSEHOLDS = 30000 # Трохи збільшимо
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 4, 19)

PLANS = [
    {'name': 'Essential 100', 'rate': 19.95, 'weight': 15},
    {'name': 'Full-Fibre 150', 'rate': 30.95, 'weight': 20},
    {'name': 'Full-Fibre 300', 'rate': 31.95, 'weight': 35},
    {'name': 'Full-Fibre 500', 'rate': 35.95, 'weight': 15},
    {'name': 'Full-Fibre 900', 'rate': 41.95, 'weight': 10},
    {'name': 'Full-Fibre 900 - All In', 'rate': 48.95, 'weight': 5}
]

POSTCODES_CONFIG = {
    'PO30': {'town': 'Newport', 'weight': 25, 'coverage': 0.90},
    'PO31': {'town': 'Cowes', 'weight': 12, 'wealthy': True, 'coverage': 0.98},
    'PO32': {'town': 'East Cowes', 'weight': 8, 'coverage': 0.90},
    'PO33': {'town': 'Ryde', 'weight': 20, 'coverage': 0.92},
    'PO34': {'town': 'Seaview', 'weight': 4, 'coverage': 0.95},
    'PO35': {'town': 'Bembridge', 'weight': 5, 'wealthy': True, 'coverage': 0.95},
    'PO36': {'town': 'Sandown', 'weight': 10, 'coverage': 0.85},
    'PO37': {'town': 'Shanklin', 'weight': 8, 'coverage': 0.88},
    'PO38': {'town': 'Ventnor', 'weight': 4, 'coverage': 0.75},
    'PO39': {'town': 'Totland Bay', 'weight': 2, 'coverage': 0.70},
    'PO40': {'town': 'Freshwater', 'weight': 2, 'coverage': 0.70},
    'PO41': {'town': 'Yarmouth', 'weight': 1, 'coverage': 0.75}
}

MARKETING_TYPES = [
    {'type': 'Leaflets', 'impact': 2.5, 'cost': 500, 'scope': 'local'},
    {'type': 'Facebook Ads', 'impact': 1.8, 'cost': 1200, 'scope': 'island'},
    {'type': 'Radio Ad', 'impact': 1.4, 'cost': 2000, 'scope': 'island'},
    {'type': 'Local Fair', 'impact': 3.0, 'cost': 300, 'scope': 'local'},
    {'type': 'Google Search', 'impact': 1.2, 'cost': 800, 'scope': 'island'}
]

def generate_random_postcode(district):
    sector = random.randint(1, 9)
    unit = ''.join(random.choices(string.ascii_uppercase, k=2))
    return f"{district} {sector}{unit}"

def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.executescript('''
    DROP TABLE IF EXISTS customers;
    DROP TABLE IF EXISTS subscriptions;
    DROP TABLE IF EXISTS billing;
    DROP TABLE IF EXISTS infrastructure;
    DROP TABLE IF EXISTS marketing_events;

    CREATE TABLE marketing_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT,
        campaign_name TEXT,
        start_date TEXT,
        end_date TEXT,
        budget REAL,
        target_area TEXT
    );

    CREATE TABLE customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        street_address TEXT,
        town TEXT,
        postcode TEXT,
        join_date TEXT,
        status TEXT,
        acquisition_source TEXT
    );

    CREATE TABLE subscriptions (
        customer_id INTEGER,
        plan_name TEXT,
        monthly_rate REAL,
        start_date TEXT,
        is_active BOOLEAN
    );

    CREATE TABLE billing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        invoice_date TEXT,
        amount_paid REAL,
        payment_method TEXT
    );

    CREATE TABLE infrastructure (
        postcode TEXT PRIMARY KEY,
        is_serviceable BOOLEAN
    );
    ''')

    # 1. Marketing Events Generation
    print("Generating marketing events...")
    marketing_events = []
    current_m_date = START_DATE
    while current_m_date < END_DATE:
        m_type = random.choice(MARKETING_TYPES)
        duration = random.randint(7, 30)
        end_m_date = current_m_date + timedelta(days=duration)
        area = 'Island Wide' if m_type['scope'] == 'island' else random.choice(list(POSTCODES_CONFIG.keys()))
        
        marketing_events.append((
            m_type['type'], 
            f"{m_type['type']} Campaign {current_m_date.strftime('%b %Y')}",
            current_m_date.strftime('%Y-%m-%d'),
            end_m_date.strftime('%Y-%m-%d'),
            m_type['cost'],
            area
        ))
        current_m_date += timedelta(days=random.randint(20, 60))
    
    cursor.executemany('INSERT INTO marketing_events (event_type, campaign_name, start_date, end_date, budget, target_area) VALUES (?,?,?,?,?,?)', marketing_events)

    # 2. Infrastructure
    print("Generating infrastructure...")
    infra_data = {}
    for district in POSTCODES_CONFIG.keys():
        for _ in range(200): # 200 посткодів на район
            pc = generate_random_postcode(district)
            infra_data[pc] = random.random() < POSTCODES_CONFIG[district]['coverage']
    
    cursor.executemany('INSERT INTO infrastructure VALUES (?, ?)', list(infra_data.items()))

    # 3. Customers & Subscriptions
    print("Generating customers with marketing impact...")
    cust_data = []
    sub_data = []
    
    districts = list(POSTCODES_CONFIG.keys())
    weights = [POSTCODES_CONFIG[d]['weight'] for d in districts]

    for i in range(TOTAL_HOUSEHOLDS):
        # Визначаємо дату приєднання з урахуванням маркетингу
        join_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
        
        # Перевірка, чи потрапив клієнт під акцію
        source = 'Organic'
        active_campaigns = [m for m in marketing_events if m[2] <= join_date.strftime('%Y-%m-%d') <= m[3]]
        
        district = random.choices(districts, weights=weights)[0]
        
        if active_campaigns:
            # Якщо є акція, підвищуємо ймовірність приходу саме через неї
            campaign = random.choice(active_campaigns)
            if campaign[5] == 'Island Wide' or campaign[5] == district:
                source = campaign[0]

        pc_list = [pc for pc in infra_data.keys() if pc.startswith(district)]
        pc = random.choice(pc_list) if pc_list else generate_random_postcode(district)
        
        name = f"{random.choice(['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer'])} {random.choice(['Smith', 'Jones', 'Taylor', 'Brown'])}"
        address = f"{random.randint(1, 100)} {random.choice(['High St', 'Main Rd', 'Station Rd'])}"
        
        cust_data.append((name, address, POSTCODES_CONFIG[district]['town'], pc, join_date.strftime('%Y-%m-%d'), 'Active', source))
        
        # Початковий план
        plan = random.choices(PLANS, weights=[p['weight'] for p in PLANS])[0]
        sub_data.append((i+1, plan['name'], plan['rate'], join_date.strftime('%Y-%m-%d'), 1))

        # Симуляція Upsell (зміна плану внаслідок маркетингу)
        if random.random() < 0.15: # 15% клієнтів змінюють план
            upgrade_date = join_date + timedelta(days=random.randint(100, 400))
            if upgrade_date < END_DATE:
                # Знаходимо кампанію під час апгрейду
                up_campaigns = [m for m in marketing_events if m[2] <= upgrade_date.strftime('%Y-%m-%d') <= m[3]]
                if up_campaigns:
                    new_plan = random.choice(PLANS[PLANS.index(plan)+1:] if PLANS.index(plan) < len(PLANS)-1 else [plan])
                    if new_plan != plan:
                        # Деактивуємо старий, додаємо новий
                        sub_data[-1] = (i+1, plan['name'], plan['rate'], join_date.strftime('%Y-%m-%d'), 0)
                        sub_data.append((i+1, new_plan['name'], new_plan['rate'], upgrade_date.strftime('%Y-%m-%d'), 1))

    cursor.executemany('INSERT INTO customers (name, street_address, town, postcode, join_date, status, acquisition_source) VALUES (?,?,?,?,?,?,?)', cust_data)
    cursor.executemany('INSERT INTO subscriptions VALUES (?,?,?,?,?)', sub_data)

    # 4. Billing (спрощено)
    print("Generating billing...")
    billing_records = []
    for i in range(1, TOTAL_HOUSEHOLDS + 1):
        for m in range(random.randint(1, 12)):
            date = (END_DATE - timedelta(days=m*30)).strftime('%Y-%m-%d')
            billing_records.append((i, date, random.choice([31.95, 35.95, 41.95]), 'Direct Debit'))
    
    cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records[:150000])

    conn.commit()
    conn.close()
    print("Database updated with Marketing Events and Plan Changes!")

if __name__ == '__main__':
    create_db()
