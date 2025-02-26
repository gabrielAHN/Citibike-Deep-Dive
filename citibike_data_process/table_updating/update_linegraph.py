import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_linegraph(**kwargs):
    main_table = kwargs.get('name')
    conn = kwargs.get('conn')

    temp_table = f'{main_table}_temp'
    new_table = f'ImportedTable'

    create_temp_table(conn, new_table, temp_table)

    conn.execute(f""" 
        INSERT INTO {main_table}
        SELECT * FROM {temp_table};
    """)

def create_temp_table(conn, new_table, temp_table):
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    conn.execute(f'''
    CREATE TEMP TABLE {temp_table} (
        year TEXT,
        month TEXT,
        subscriber_count INTEGER,
        customer_count INTEGER
    )
    ''')

    conn.execute(f'''
        INSERT INTO {temp_table}
            SELECT 
                year,
                month,
                COUNT(CASE WHEN user_type = 'subscriber' THEN 1 END) AS subscriber_count,
                COUNT(CASE WHEN user_type = 'customer' THEN 1 END) AS customer_count
            FROM "{new_table}"
        GROUP BY year, month
        ORDER BY year, month;
    ''')
