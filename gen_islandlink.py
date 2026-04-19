import sqlite3
import random
from datetime import datetime, timedelta

# Налаштування бази даних
DB_NAME = 'islandlink_analysis.db'
TOTAL_HOUSEHOLDS = 28000
DUPLICATES_COUNT = 400

# Тарифи та їх базові ваги
PLANS = [
    {'name': 'Essential 100', 'rate': 19.95, 'weight': 10},
    {'name': 'Full-Fibre 150', 'rate': 30.95, 'weight': 15},
    {'name': 'Full-Fibre 300', 'rate': 31.95, 'weight': 40}, # Найпопулярніший
    {'name': 'Full-Fibre 500', 'rate': 35.95, 'weight': 15},
    {'name': 'Full-Fibre 900', 'rate': 41.95, 'weight': 10},
    {'name': 'Full-Fibre 900 - All In', 'rate': 48.95, 'weight': 10}
]

# Географія та поштові індекси
POSTCODES = {
    'PO30': {'town': 'Newport', 'weight': 25},
    'PO33': {'town': 'Ryde', 'weight': 20},
    'PO36': {'town': 'Sandown', 'weight': 10}, # Високий відтік
    'PO35': {'town': 'Bembridge', 'weight': 5, 'wealthy': True},
    'PO31': {'town': 'Cowes', 'weight': 10, 'wealthy': True},
    'PO40': {'town': 'Freshwater', 'weight': 2}, # Мало даних
    'PO38': {'town': 'Ventnor', 'weight': 2},    # Мало даних
    'PO32': {'town': 'East Cowes', 'weight': 8},
    'PO34': {'town': 'Seaview', 'weight': 4},
    'PO37': {'town': 'Shanklin', 'weight': 8},
    'PO39': {'town': 'Totland Bay', 'weight': 3},
    'PO41': {'town': 'Yarmouth', 'weight': 3}
}

STREETS = ['High St', 'Main Rd', 'Victoria Ave', 'Church Rd', 'Broadway', 'Station Rd', 'Queens Rd']
NAMES = ['Smith', 'Jones', 'Taylor', 'Brown', 'Williams', 'Wilson', 'Johnson', 'Davies', 'Robinson', 'Wright']
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth']

def get_random_date(start_year=2023):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(2026, 4, 19)
    days_between = (end_date - start_date).days
    
    random_days = random.randint(0, days_between)
    res_date = start_date + timedelta(days=random_days)
    
    # Симуляція сезонних спайків (подвійний шанс)
    if (res_date.year == 2024 and res_date.month == 9) or (res_date.year == 2025 and res_date.month == 3):
        if random.random() > 0.5:
            return res_date
        else:
            return get_random_date(start_year)
    return res_date

def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Створення таблиць
    cursor.execute('DROP TABLE IF EXISTS customers')
    cursor.execute('DROP TABLE IF EXISTS subscriptions')
    cursor.execute('DROP TABLE IF EXISTS billing')
    cursor.execute('DROP TABLE IF EXISTS infrastructure')

    cursor.execute('''
    CREATE TABLE customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        street_address TEXT,
        town TEXT,
        postcode TEXT,
        join_date TEXT,
        status TEXT,
        churn_date TEXT
    )''')

    cursor.execute('''
    CREATE TABLE subscriptions (
        customer_id INTEGER,
        plan_name TEXT,
        monthly_rate REAL,
        FOREIGN KEY(customer_id) REFERENCES customers(id)
    )''')

    cursor.execute('''
    CREATE TABLE billing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        invoice_date TEXT,
        amount_paid REAL,
        payment_method TEXT,
        FOREIGN KEY(customer_id) REFERENCES customers(id)
    )''')

    cursor.execute('''
    CREATE TABLE infrastructure (
        postcode TEXT PRIMARY KEY,
        is_serviceable BOOLEAN
    )''')

    # Заповнення інфраструктури
    for pc in POSTCODES.keys():
        is_svc = random.random() > 0.15 # 15% не обслуговується
        cursor.execute('INSERT INTO infrastructure VALUES (?, ?)', (pc, is_svc))

    print("Generating customers...")
    customer_data = []
    sub_data = []
    
    pc_list = list(POSTCODES.keys())
    pc_weights = [POSTCODES[p]['weight'] for p in pc_list]

    for i in range(TOTAL_HOUSEHOLDS):
        pc = random.choices(pc_list, weights=pc_weights)[0]
        town_orig = POSTCODES[pc]['town']
        
        if town_orig == 'Newport':
            town = random.choices(['Newport', 'NPT', 'Newport, IOW'], weights=[70, 15, 15])[0]
        elif town_orig == 'Ryde':
            town = random.choices(['Ryde', 'Ryde, Isle of Wight'], weights=[80, 20])[0]
        else:
            town = town_orig

        clean_pc = pc
        if random.random() < 0.03:
            clean_pc = None
        elif random.random() < 0.02:
            clean_pc = pc.replace('O', '0')

        name = f"{random.choice(FIRST_NAMES)} {random.choice(NAMES)}"
        address = f"{random.randint(1, 150)} {random.choice(STREETS)}"
        join_date = get_random_date()
        
        status = 'Active'
        churn_date = None
        churn_prob = 0.18 if pc == 'PO36' else 0.10
        if random.random() < churn_prob:
            status = 'Churned'
            churn_days = random.randint(90, 365)
            c_date = join_date + timedelta(days=churn_days)
            if c_date < datetime(2026, 4, 19):
                churn_date = c_date.strftime('%Y-%m-%d')
            else:
                status = 'Active'

        customer_data.append((name, address, town, clean_pc, join_date.strftime('%Y-%m-%d'), status, churn_date))

        plan_weights = [p['weight'] for p in PLANS]
        if POSTCODES[pc].get('wealthy'):
            plan_weights = [5, 5, 20, 20, 25, 25]
        
        selected_plan = random.choices(PLANS, weights=plan_weights)[0]
        sub_data.append((i+1, selected_plan['name'], selected_plan['rate']))

    cursor.executemany('INSERT INTO customers (name, street_address, town, postcode, join_date, status, churn_date) VALUES (?,?,?,?,?,?,?)', customer_data)
    cursor.executemany('INSERT INTO subscriptions VALUES (?,?,?)', sub_data)

    print("Adding duplicates...")
    for _ in range(DUPLICATES_COUNT):
        orig_idx = random.randint(0, TOTAL_HOUSEHOLDS-1)
        orig = customer_data[orig_idx]
        new_addr = orig[1] + " "
        cursor.execute('INSERT INTO customers (name, street_address, town, postcode, join_date, status, churn_date) VALUES (?,?,?,?,?,?,?)', 
                       (orig[0], new_addr, orig[2], orig[3], orig[4], orig[5], orig[6]))

    print("Generating billing records...")
    billing_records = []
    methods = ['Direct Debit', 'Credit Card', 'Bank Transfer']
    
    for i in range(len(customer_data)):
        c_id = i + 1
        j_date = datetime.strptime(customer_data[i][4], '%Y-%m-%d')
        status = customer_data[i][5]
        c_date_str = customer_data[i][6]
        
        last_date = datetime(2026, 4, 19)
        if status == 'Churned' and c_date_str:
            last_date = datetime.strptime(c_date_str, '%Y-%m-%d')
        
        curr_date = j_date
        plan_rate = sub_data[i][2]
        
        while curr_date <= last_date:
            if random.random() > 0.5:
                date_str = curr_date.strftime('%Y-%m-%d')
            else:
                date_str = curr_date.strftime('%d/%m/%Y')
            
            amount = plan_rate if random.random() > 0.05 else 0.0
            method = random.choice(methods)
            
            billing_records.append((c_id, date_str, amount, method))
            curr_date += timedelta(days=30)
            
            if len(billing_records) > 50000:
                cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records)
                billing_records = []

    if billing_records:
        cursor.executemany('INSERT INTO billing (customer_id, invoice_date, amount_paid, payment_method) VALUES (?,?,?,?)', billing_records)

    conn.commit()
    conn.close()
    print(f"Database '{DB_NAME}' created successfully!")

if __name__ == '__main__':
    create_db()
