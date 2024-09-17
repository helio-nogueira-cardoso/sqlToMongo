from utilities import getColumns, getKeyColumns, getNotKeys, getRefContraints, formatField, formatFieldSingleQuote, condComma

def mqlTableEmbedded(con, table_name):
    mql = ""
    cur = con.cursor()
    cur.execute(f"""
        SELECT * 
        FROM {table_name}
    """)
    res = cur.fetchall()
    col_names, col_types, col_index = getColumns(cur)
    keys = getKeyColumns(con, table_name)
    not_keys = getNotKeys(keys, col_names)
    ref_constraints = getRefContraints(con, table_name)

    mql += f'db.{table_name}.insertMany([\n'

    if len(res) == 0:
        raise Exception('Tabela vazia. Não há o que inserir.')
    
    for i, row in enumerate(res):
        mql += makeDocument(con, keys, not_keys, col_index, col_types, row, ref_constraints)
        mql += condComma(i, res)

    mql += '])\n'
    
    cur.close()
    return mql

def makeDocument(con, keys, not_keys, col_index, col_types, row, ref_constraints):
    mql = "{\n"
    mql += f'"_id": {makeId(keys, col_index, col_types, row)}'
    mql += makeNotId(not_keys, col_index, col_types, row)
    mql += makeRefs(con, col_index, col_types, row, ref_constraints)
    mql += "}"
    return mql

def makeRefs(con, col_index, col_types, row, ref_constraints):
    mql = ""
    for constraint_name in ref_constraints:
        mql += ",\n"
        mql += makeRef(con, col_index, col_types, row, constraint_name)
    mql += "\n"
    return mql

def makeRef(con, col_index, col_types, row, constraint_name):
    mql = ""
    n, child_cols, parent_cols, parent_table = getRefConstraintInfo(con, constraint_name)

    for i in range(n):
        if row[col_index[child_cols[i]]] is None:
            return f'"{constraint_name}": null'

    query = f"SELECT * FROM {parent_table} WHERE "
    for i in range(n):
        query += f"{parent_cols[i]} = {formatFieldSingleQuote(child_cols[i], col_index, col_types, row[col_index[child_cols[i]]])} "
        if i < n - 1:
            query += "AND "

    cur = con.cursor()
    cur.execute(query)
    res = cur.fetchall()
    ref_row = res[0]
    ref_col_names, ref_col_types, ref_col_index  = getColumns(cur)
    ref_keys = getKeyColumns(con, parent_table)
    ref_not_keys = getNotKeys(ref_keys, ref_col_names)
    ref_ref_constraints = getRefContraints(con, parent_table)

    cur.close()
    mql += f'"{constraint_name}": ' 
    mql += makeDocument(con, ref_keys, ref_not_keys, ref_col_index, ref_col_types, ref_row, ref_ref_constraints)

    return mql
        
def getRefConstraintInfo(con, constraint_name):
    cur = con.cursor()
    cur.execute(f"""
        SELECT 
            a.table_name child_table, 
            a.column_name child_column, 
            a.constraint_name, 
            b.table_name parent_table, 
            b.column_name parent_column 
        FROM all_cons_columns a 
            JOIN all_constraints c 
            ON a.owner = c.owner AND a.constraint_name = c.constraint_name 
            JOIN all_cons_columns b 
            ON c.owner = b.owner and c.r_constraint_name = b.constraint_name AND a.position = b.position 
        WHERE a.constraint_name = '{constraint_name}'
    """)

    res = cur.fetchall()
    n = len(res)
    child_cols = [row[1] for row in res]
    parent_cols = [row[4] for row in res]
    parent_table = res[0][3]

    cur.close()
    return n, child_cols, parent_cols, parent_table

def makeNotId(not_keys, col_index, col_types, row):
    mql = ""
    for not_key in not_keys:
        mql += ",\n"
        mql += f'"{not_key}": {formatField(not_key, col_index, col_types, row[col_index[not_key]])}'
    mql += "\n"
    return mql

def makeId(keys, col_index, col_types, row):
    mql = "{\n"
    for i, key in enumerate(keys):
        mql += f'"{key}": {formatField(key, col_index, col_types, row[col_index[key]])}'
        if i < len(keys) - 1:
            mql += ","
        mql += "\n"
    mql += "}"
    return mql