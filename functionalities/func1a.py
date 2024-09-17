from utilities import getColumns, getKeyColumns, getNotKeys, condComma, formatField

def mqlTable(con, table_name):
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
    
    mql += f'db.{table_name}.insertMany([\n'

    if len(res) == 0:
        raise Exception('Tabela vazia. Não há o que inserir.')
    
    for i, row in enumerate(res):
        mql += makeDocument(keys, not_keys, col_index, col_types, row)
        mql += condComma(i, res)

    mql += '])\n'
    
    cur.close()
    return mql

def makeDocument(keys, not_keys, col_index, col_types, row):
    mql = "{\n"
    mql += f'"_id": {makeId(keys, col_index, col_types, row)}'
    mql += makeNotId(not_keys, col_index, col_types, row)
    mql += "}"
    return mql

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