import logging
from ..shared_util.parser import parse_file_date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_data_table(**kwargs):
    new_files = kwargs.get('new_files')
    main_table = kwargs.get('name')
    conn = kwargs.get('conn')

    for i in new_files:
        year, month = parse_file_date(i.table_name)
        if not year:
            continue

        row_exists = conn.execute(f"""
            SELECT 1
            FROM {main_table}
            WHERE year = '{year}'
            LIMIT 1
        """).fetchone()

        if month:
            if row_exists:
                conn.execute(f"""
                    UPDATE {main_table}
                    SET month = '{month}',
                        complete = false
                    WHERE year = '{year}'
                """)
            else:
                conn.execute(f"""
                    INSERT INTO {main_table} (year, month, complete)
                    VALUES ('{year}', '{month}', false)
                """)
        else:
            if row_exists:
                conn.execute(f"""
                    UPDATE {main_table}
                    SET month = NULL,
                        complete = true
                    WHERE year = '{year}'
                """)
            else:
                conn.execute(f"""
                    INSERT INTO {main_table} (year, month, complete)
                    VALUES ('{year}', NULL, true)
                """)
