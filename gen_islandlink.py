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

STREETS = ['High St', 'Main Rd', 'Victoria Ave', 'Church Rd', 'Broadway', 'Station Rd', 'Queens Rd', 'Mill Hill Rd', 'Tithe Barn', 'Arctic Rd', 'Park Rd', 'London Rd', 'York St']
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth']
LAST_NAMES = ['Smith', 'Jones', 'Taylor', 'Brown', 'Williams', 'Wilson', 'Johnson', 'Davies', 'Robinson', 'Wright']

FAILURE_REASONS = ['Insufficient Funds', 'Card Expired', 'Bank Declined', 'System Error']

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
        acquisition_source TEXT,
        churn_date TEXT
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
        payment_method TEXT,
        payment_status TEXT,
        failure_reason TEXT
    );

    CREATE TABLE infrastructure (
        postcode TEXT PRIMARY KEY,
        is_serviceable BOOLEAN
    );
    ''')

    # 1. Marketing Events
    marketing_events = []
    m_types = ['Leaflets', 'Facebook Ads', 'Radio Ad', 'Local Fair', 'Google Search', 'Door-to-Door', 'Billboards', 'Sponsorship', 'Instagram Influencer']
    for _ in range(250):
        m_type = random.choice(m_types)
        start_ts = random.randint(int(START_DATE.timestamp()), int(END_DATE.timestamp()))
        start_m_date = datetime.fromtimestamp(start_ts)
        end_m_date = start_m_date + timedelta(days=random.randint(3, 21))
        area = 'Island Wide' if random.random() > 0.6 else random.choice(list(POSTCODES_CONFIG.keys()))
        marketing_events.append((m_type, f"{m_type} Campaign", start_m_date.strftime('%Y-%m-%d'), end_m_date.strftime('%Y-%m-%d'), random.randint(300, 2000), area))
    cursor.executemany('INSERT INTO marketing_events (event_type, campaign_name, start_date, end_date, budget, target_area) VALUES (?,?,?,?,?,?)', marketing_events)

    # 2. Infrastructure
    infra_data = {}
    for district in POSTCODES_CONFIG.keys():
        for _ in range(200):
            pc = generate_random_postcode(district)
            infra_data[pc] = random.random() < POSTCODES_CONFIG[district]['coverage']
    cursor.executemany('INSERT INTO infrastructure VALUES (?, ?)', list(infra_data.items()))

    # 3. Customers & Subscriptions
    cust_list = []
    sub_inserts = []
    districts = list(POSTCODES_CONFIG.keys())
    weights = [POSTCODES_CONFIG[d]['weight'] for d in districts]

    for i in range(TOTAL_HOUSEHOLDS):
        c_id = i + 1
        join_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
        join_date_str = join_date.strftime('%Y-%m-%d')
        district = random.choices(districts, weights=weights)[0]
        
        status = 'Active'
        churn_date = None
        if random.random() < (0.18 if district == 'PO36' else 0.12):
            c_date = join_date + timedelta(days=random.randint(90, 600))
            if c_date < END_DATE:
                status = 'Churned'
                churn_date = c_date.strftime('%Y-%m-%d')

        source = 'Organic'
        active_campaigns = [m for m in marketing_events if m[2] <= join_date_str <= m[3] and (m[5] == 'Island Wide' or m[5] == district)]
        if active_campaigns and random.random() < 0.6:
            source = random.choice(active_campaigns)[0]

        pc_list = [pc for pc in infra_data.keys() if pc.startswith(district)]
        pc = random.choice(pc_list) if pc_list else generate_random_postcode(district)
        
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        address = f"{random.randint(1, 150)} {random.choice(STREETS)}"
        
        cust_list.append({
            'id': c_id,
            'name': name,
            'address': address,
            'town': POSTCODES_CONFIG[district]['town'],
            'postcode': pc,
            'join_date': join_date_str,
            'status': status,
            'source': source,
            'churn_date': churn_date
        })
        
        # Initial Plan
        initial_plan = random.choices(PLANS[:3], weights=[p['weight'] for p in PLANS[:3]])[0]
        
        # Upsell Logic (20% chance to have an old plan)
        if random.random() < 0.20:
            upgrade_date = join_date + timedelta(days=random.randint(60, 400))
            if upgrade_date < END_DATE:
                new_plan = random.choice(PLANS[PLANS.index(initial_plan)+1:])
                # Add old plan as inactive
                sub_inserts.append((c_id, initial_plan['name'], initial_plan['rate'], join_date_str, 0))
                # Add new plan as active
                sub_inserts.append((c_id, new_plan['name'], new_plan['rate'], upgrade_date.strftime('%Y-%m-%d'), 1))
            else:
                sub_inserts.append((c_id, initial_plan['name'], initial_plan['rate'], join_date_str, 1))
        else:
            sub_inserts.append((c_id, initial_plan['name'], initial_plan['rate'], join_date_str, 1))

    cursor.executemany('INSERT INTO customers (name, street_address, town, postcode, join_date, status, acquisition_source, churn_date) VALUES (?,?,?,?,?,?,?,?)', 
                       [(c['name'], c['address'], c['town'], c['postcode'], c['join_date'], c['status'], c['source'], c['churn_date']) for c in cust_list])
    cursor.executemany('INSERT INTO subscriptions VALUES (?,?,?,?,?)', sub_inserts)

    # 4. Billing
    print("Generating billing...")
    billing_records = []
    # Create a lookup for active rates
    active_plans = {s[0]: s[2] for s in sub_inserts if s[4] == 1}

    for i, cust in enumerate(cust_list):
        c_id = cust['id']
        last_date = END_DATE
        if cust['status'] == 'Churned' and cust['churn_date']:
            last_date = datetime.strptime(cust['churn_date'], '%Y-%m-%d')
        
        curr = datetime.strptime(cust['join_date'], '%Y-%m-%d')
        rate = active_plans.get(c_id, 30.95)
        
        while curr <= last_date:
            is_success = random.random() > 0.015
            status = 'Success' if is_success else 'Failed'
            reason = 'None' if is_success else random.choice(FAILURE_REASONS)
            amount = rate if is_success else 0.0
            
            billing_records.append((c_id, curr.strftime('%Y-%m-%d'), amount, 'Direct Debit', status, reason))
            curr += timedelta(days=30)
            
            if len(billing_records) > 50000:
                cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method, payment_status, failure_reason) VALUES (?,?,?,?,?,?)', billing_records)
                billing_records = []
    
    if billing_records:
        cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method, payment_status, failure_reason) VALUES (?,?,?,?,?,?)', billing_records)

    conn.commit()
    conn.close()
    print("Database finalized: Subscriptions history added.")

if __name__ == '__main__':
    create_db()
