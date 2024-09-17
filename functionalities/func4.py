from utilities import getTables, getColumns, getKeyColumns, getNotKeys, getRefContraints, formatField, formatFieldSingleQuote, condComma
from func1a import mqlTable

_NO_TABLE_ = "_&*_NO_TABLE_&*_"

def mqlFutebol(con):
    tables = getTables(con)
    mql = ""
    for table in tables:
        if table in ['F01_ESTADO', "F14_TRANSMITE"]: # Não serão inseridas
            continue
        print(f"Iniciando tabela {table}.")
        try:
            if table in ["F15_GOLS_CARTOES", "F12_PATROCINA", "F13_APITA"]: # N-N linking com atributos
                mql += mqlTable(con, table)
            else:
                mql += mqlTableEmbedded(con, table)
        except Exception as e:
            print(str(e))
        mql += "\n"
        print(f"Tabela {table} finalizada.")
    return mql

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
        mql += makeDocument(con, table_name, keys, not_keys, col_index, col_types, row, ref_constraints)
        mql += condComma(i, res)

    mql += '])\n'
    
    cur.close()
    return mql

def makeDocument(con, table_name, keys, not_keys, col_index, col_types, row, ref_constraints):
    mql = "{\n"
    mql += f'"_id": {makeId(keys, col_index, col_types, row)}'
    mql += makeNotId(not_keys, col_index, col_types, row)
    mql += makeRefs(con, table_name, col_index, col_types, row, ref_constraints)
    if table_name == _NO_TABLE_:
        mql += ""
    else:
        mql += makeManyToMany(con, table_name, col_index, col_types, row)
    mql += "}"
    return mql

def makeRefs(con, table_name, col_index, col_types, row, ref_constraints):
    mql = ""
    for constraint_name in ref_constraints:
        mql += ",\n"
        mql += makeRef(con, table_name, col_index, col_types, row, constraint_name)
    mql += "\n"
    return mql

def makeRef(con, table_name, col_index, col_types, row, constraint_name):
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

    mql += f'"{constraint_name}": ' 
    mql += makeDocument(con, _NO_TABLE_, ref_keys, ref_not_keys, ref_col_index, ref_col_types, ref_row, ref_ref_constraints)

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

def makeManyToMany(con, table_name, col_index, col_types, row):    
    if table_name == 'F04_TIME':
        return makeF04_TIMEManyToMany(con, col_index, col_types, row)
    if table_name == 'F05_JOGADOR':
        return makeF05_JOGADORManyToMany(con, col_index, col_types, row)
    if table_name == 'F06_PATROCINADOR':
        return makeF06_PATROCINADORManyToMany(con, col_index, col_types, row)
    if table_name == 'F07_ARBITRO':
        return makeF07_ARBITROManyToMany(con, col_index, col_types, row)
    if table_name == 'F10_EMISSORA':
        return makeF10_EMISSORAManyToMany(con, col_index, col_types, row)
    if table_name == 'F11_PARTIDA':
        return makeF11_PARTIDAManyToMany(con, col_index, col_types, row)
    return ""

def makeF04_TIMEManyToMany(con, col_index, col_types, row):
    mql = ",\n"
    mql += '"PATROCINADORES": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT IDPAT FROM F12_PATROCINA
        WHERE TTIME = {formatFieldSingleQuote("TTIME", col_index, col_types, row[col_index["TTIME"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"IDPAT": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += ']\n'

    return mql

def makeF05_JOGADORManyToMany(con, col_index, col_types, row):
    mql = ",\n"
    mql += '"PARTIDAS_GOLS_CARTOES": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT IDPARTIDA FROM F15_GOLS_CARTOES
        WHERE CPFJ = {formatFieldSingleQuote("CPFJ", col_index, col_types, row[col_index["CPFJ"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"IDPARTIDA": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += ']\n'

    return mql

def makeF06_PATROCINADORManyToMany(con, col_index, col_types, row):
    mql = ",\n"
    mql += '"TIMES_PATROCINADOS": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT TTIME FROM F12_PATROCINA
        WHERE IDPAT = {formatFieldSingleQuote("IDPAT", col_index, col_types, row[col_index["IDPAT"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"TTIME": "{result_row[0]}"' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += ']\n'

    return mql

def makeF07_ARBITROManyToMany(con, col_index, col_types, row):
    mql = ",\n"
    mql += '"PARTIDAS_APITADAS": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT IDPARTIDA FROM F13_APITA
        WHERE CPFA = {formatFieldSingleQuote("CPFA", col_index, col_types, row[col_index["CPFA"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"IDPARTIDA": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += ']\n'

    return mql

def makeF10_EMISSORAManyToMany(con, col_index, col_types, row):
    mql = ",\n"
    mql += '"PARTIDAS_TRANSMITIDAS": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT IDPARTIDA FROM F14_TRANSMITE
        WHERE IDEMISSORA = {formatFieldSingleQuote("IDEMISSORA", col_index, col_types, row[col_index["IDEMISSORA"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"IDPARTIDA": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += ']\n'

    return mql

def makeF11_PARTIDAManyToMany(con, col_index, col_types, row):
    mql = ",\n"
    mql += '"ARBITROS_QUE_APITARAM": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT CPFA FROM F13_APITA
        WHERE IDPARTIDA = {formatFieldSingleQuote("IDPARTIDA", col_index, col_types, row[col_index["IDPARTIDA"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"CPFA": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += '],\n'
    
    mql += '"JOGADORES_QUE_MARCARAM": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT CPFJ FROM F15_GOLS_CARTOES
        WHERE IDPARTIDA = {formatFieldSingleQuote("IDPARTIDA", col_index, col_types, row[col_index["IDPARTIDA"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"CPFJ": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += '],\n'
    
    mql += '"EMISSORAS_QUE_TRANSMITIRAM": [\n'

    cur = con.cursor()
    cur.execute(f"""
        SELECT IDEMISSORA FROM F14_TRANSMITE
        WHERE IDPARTIDA = {formatFieldSingleQuote("IDPARTIDA", col_index, col_types, row[col_index["IDPARTIDA"]])}
    """)
    res = cur.fetchall()
    for i, result_row in enumerate(res):
        mql += '{' + f'"IDEMISSORA": {result_row[0]}' + '}'
        mql += condComma(i, res)    
    cur.close()
    mql += ']\n'

    return mql