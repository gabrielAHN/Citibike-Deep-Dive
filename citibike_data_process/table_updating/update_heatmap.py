import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_heatmap(**kwargs):
    main_table = kwargs.get('name', [])
    conn = kwargs.get('conn', [])

    temp_table = f'{main_table}_temp'
    new_table = f'ImportedTable'

    create_new_table(conn, new_table, temp_table)

    conn.execute(f""" 
        UPDATE {main_table} AS main
        SET total_count = main.total_count + temp.total_count
        FROM {temp_table} AS temp
        WHERE main.year = temp.year 
          AND main.month = temp.month 
          AND main.hour = temp.hour;
    """)

    conn.execute(f""" 
        INSERT INTO {main_table} (year, month, hour, total_count)
        SELECT temp.year, temp.month, temp.hour, temp.total_count
        FROM {temp_table} AS temp
        LEFT JOIN {main_table} AS main
        ON main.year = temp.year 
           AND main.month = temp.month 
           AND main.hour = temp.hour
        WHERE main.year IS NULL;
    """)

def create_new_table(conn, new_table, temp_table_name):
    conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    conn.execute(f'''
    CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} (
        year TEXT,
        month TEXT,
        hour INTEGER,
        total_count INTEGER
    )
    ''')

    conn.execute(f'''
        INSERT INTO {temp_table_name}
        WITH transformed AS (
            SELECT 
                strftime('%H', start_time) AS hour,
                *
            FROM "{new_table}"
        )
        SELECT 
            year,
            month,
            hour,
            COUNT(*) AS total_count
        FROM transformed
        GROUP BY year, month, hour
        ORDER BY year, month, hour;
    ''')
