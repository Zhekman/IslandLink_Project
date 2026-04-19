import sqlite3
import random
import string
from datetime import datetime, timedelta

# Налаштування
DB_NAME = 'islandlink_analysis.db'
TOTAL_HOUSEHOLDS = 30000
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
    {'type': 'Google Search', 'impact': 1.2, 'cost': 800, 'scope': 'island'},
    {'type': 'Door-to-Door', 'impact': 4.0, 'cost': 600, 'scope': 'local'},
    {'type': 'Billboards', 'impact': 1.5, 'cost': 1500, 'scope': 'island'},
    {'type': 'Sponsorship', 'impact': 2.0, 'cost': 1000, 'scope': 'local'},
    {'type': 'Instagram Influencer', 'impact': 2.2, 'cost': 900, 'scope': 'island'}
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

    # 1. Marketing Events Generation (More events!)
    print("Generating a large number of marketing events...")
    marketing_events = []
    # Генеруємо приблизно 200-300 івентів за весь період
    for _ in range(250):
        m_type = random.choice(MARKETING_TYPES)
        # Рандомна дата початку протягом всього періоду
        start_ts = random.randint(int(START_DATE.timestamp()), int(END_DATE.timestamp()))
        start_m_date = datetime.fromtimestamp(start_ts)
        duration = random.randint(3, 21) # Коротші, але інтенсивніші кампанії
        end_m_date = start_m_date + timedelta(days=duration)
        if end_m_date > END_DATE: end_m_date = END_DATE
        
        area = 'Island Wide' if m_type['scope'] == 'island' else random.choice(list(POSTCODES_CONFIG.keys()))
        
        marketing_events.append((
            m_type['type'], 
            f"{m_type['type']} {random.choice(['Promo', 'Drive', 'Blast', 'Sale', 'Special'])} {start_m_date.strftime('%b %Y')}",
            start_m_date.strftime('%Y-%m-%d'),
            end_m_date.strftime('%Y-%m-%d'),
            m_type['cost'] + random.randint(-100, 500), # Варіативність бюджету
            area
        ))
    
    # Сортуємо за датою для зручності
    marketing_events.sort(key=lambda x: x[2])
    cursor.executemany('INSERT INTO marketing_events (event_type, campaign_name, start_date, end_date, budget, target_area) VALUES (?,?,?,?,?,?)', marketing_events)

    # 2. Infrastructure
    print("Generating infrastructure...")
    infra_data = {}
    for district in POSTCODES_CONFIG.keys():
        for _ in range(200):
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
        join_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
        join_date_str = join_date.strftime('%Y-%m-%d')
        
        district = random.choices(districts, weights=weights)[0]
        
        # Перевірка активних кампаній для цього дня та району
        active_campaigns = [m for m in marketing_events if m[2] <= join_date_str <= m[3] and (m[5] == 'Island Wide' or m[5] == district)]
        
        source = 'Organic'
        if active_campaigns:
            # Чим більше кампаній, тим вищий шанс не-органічного входу
            if random.random() < 0.6: # 60% шанс, що маркетинг спрацював, якщо він був
                source = random.choice(active_campaigns)[0]

        pc_list = [pc for pc in infra_data.keys() if pc.startswith(district)]
        pc = random.choice(pc_list) if pc_list else generate_random_postcode(district)
        
        name = f"{random.choice(['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer'])} {random.choice(['Smith', 'Jones', 'Taylor', 'Brown'])}"
        address = f"{random.randint(1, 100)} {random.choice(['High St', 'Main Rd', 'Station Rd', 'Queens Rd'])}"
        
        cust_data.append((name, address, POSTCODES_CONFIG[district]['town'], pc, join_date_str, 'Active', source))
        
        plan = random.choices(PLANS, weights=[p['weight'] for p in PLANS])[0]
        sub_data.append((i+1, plan['name'], plan['rate'], join_date_str, 1))

        # Upsell Logic
        if random.random() < 0.2: # 20% клієнтів змінюють план (частіше завдяки маркетингу)
            upgrade_date = join_date + timedelta(days=random.randint(60, 500))
            if upgrade_date < END_DATE:
                up_date_str = upgrade_date.strftime('%Y-%m-%d')
                up_campaigns = [m for m in marketing_events if m[2] <= up_date_str <= m[3] and (m[5] == 'Island Wide' or m[5] == district)]
                
                if up_campaigns: # Якщо була акція, апгрейд імовірніший
                    new_plan = random.choice(PLANS[PLANS.index(plan)+1:] if PLANS.index(plan) < len(PLANS)-1 else [plan])
                    if new_plan != plan:
                        sub_data[-1] = (i+1, plan['name'], plan['rate'], join_date_str, 0)
                        sub_data.append((i+1, new_plan['name'], new_plan['rate'], up_date_str, 1))

    cursor.executemany('INSERT INTO customers (name, street_address, town, postcode, join_date, status, acquisition_source) VALUES (?,?,?,?,?,?,?)', cust_data)
    cursor.executemany('INSERT INTO subscriptions VALUES (?,?,?,?,?)', sub_data)

    # 4. Billing
    print("Generating billing...")
    billing_records = []
    for i in range(1, TOTAL_HOUSEHOLDS + 1):
        for m in range(random.randint(1, 15)):
            date = (END_DATE - timedelta(days=m*30)).strftime('%Y-%m-%d')
            billing_records.append((i, date, random.choice([31.95, 35.95, 41.95, 48.95]), 'Direct Debit'))
            if len(billing_records) > 50000:
                cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records)
                billing_records = []
    
    if billing_records:
        cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records)

    conn.commit()
    conn.close()
    print(f"Database created with {len(marketing_events)} marketing events!")

if __name__ == '__main__':
    create_db()
