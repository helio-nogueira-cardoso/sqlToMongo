from utilities import getColumns, condComma, formatField

def mqlQuery(con, query, seq):
    mql = ""
    cur = con.cursor()
    cur.execute(query)
    res = cur.fetchall()
    col_names, col_types, col_index = getColumns(cur)
    
    mql += f'db.consulta{seq[0]}.insertMany([\n'

    if len(res) == 0:
        raise Exception('Resultado da consulta é vazio')
    
    for i, row in enumerate(res):
        mql += makeDocument(col_names, col_index, col_types, row)
        mql += condComma(i, res)

    mql += '])\n'
    
    cur.close()

    seq[0] += 1

    return mql

def makeDocument(col_names, col_index, col_types, row):
    mql = "{\n"
    for i, col in enumerate(col_names):
        mql += f'"{col}": {formatField(col, col_index, col_types, row[col_index[col]])}'
        mql += condComma(i, col_names)
    mql += "}"
    return mql