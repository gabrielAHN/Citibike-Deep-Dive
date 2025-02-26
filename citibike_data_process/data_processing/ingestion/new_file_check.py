from ...shared_util.parser import parse_file_name


def new_file_check(conn, args, file_list, table_name='StatusDataTable'):
    table_check_query = f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_name = '{table_name}'
    """

    result = conn.execute(table_check_query).fetchone()

    if not result:
        return file_list

    valid_files = [
        (file, year, month)
        for file in file_list
        if (result := parse_file_name(file)) is not None
        for year, month in [result]
    ]

    if not valid_files:
        return file_list

    existing_records = get_existing_table(conn, args, valid_files)

    filtered_files = []
    for file, new_year, new_month in valid_files:
        new_year_int = int(new_year)
        new_month_int = int(new_month) if new_month else None
        record_exists = False
        for rec_year, rec_month in existing_records:
            if new_year_int == rec_year:
                if new_month_int is None:
                    if rec_month is None:
                        record_exists = True
                        break
                else:
                    if rec_month is not None and new_month_int == rec_month:
                        record_exists = True
                        break
        if not record_exists:
            filtered_files.append(file)

    return filtered_files if filtered_files else False


def get_existing_table(conn, args, valid_files):
    conditions_by_year = {}
    for _, year, month in valid_files:
        if year not in conditions_by_year or month is None:
            conditions_by_year[year] = month

    conditions = []
    for year, month in conditions_by_year.items():
        if month is None:
            conditions.append(f"(year = {year} AND complete = true)")
        else:
            conditions.append(
                f"(year = {int(year)} AND month <= {int(month)} AND complete = false)"
            )

    query_conditions = " OR ".join(conditions)

    query = f"""
        SELECT DISTINCT year, month
        FROM {'CitibikeData.StatusDataTable' if args.read_remote else 'StatusDataTable'}
        WHERE {query_conditions}
    """
    existing_records = conn.execute(query).fetchall()

    normalized_records = []
    for y, m in existing_records:
        normalized_records.append((int(y), int(m) if m is not None else None))
    return normalized_records
