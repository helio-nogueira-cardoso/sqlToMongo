from utilities import getKeyColumns, condComma

def mqlIndexesFutebol(con):
    mql = ""

    constraint_names = getConstraintNames(con)

    for constraint in constraint_names:
        table_name, constraint_fields = getConstraintFields(con, constraint)
        mql += makeIndex(con, table_name, constraint_fields)
        mql += "\n"
    
    return mql

def makeIndex(con, table_name, constraint_fields):
    mql = ""
    mql += f"db.{table_name}." + "createIndex(\n{\n"

    keys = getKeyColumns(con, table_name)
    for i, field in enumerate(constraint_fields):
        if field in keys:
            mql += '"_id.'
        else:
            mql += '"'
        mql += f'{field}": 1'
        mql += condComma(i, constraint_fields)

    mql += "\n}, {unique: true}\n)\n"

    return mql

def getConstraintNames(con):
    cur = con.cursor()
    cur.execute("""
        SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS
        WHERE CONSTRAINT_TYPE IN ('U')
        AND TABLE_NAME NOT IN ('F01_ESTADO', 'F14_TRANSMITE')
    """)

    res = cur.fetchall()
    constraint_names = [row[0] for row in res]
    cur.close()

    return constraint_names

def getConstraintFields(con, constraint_name):
    cur = con.cursor()
    cur.execute(f"""
        SELECT TABLE_NAME, COLUMN_NAME FROM USER_CONS_COLUMNS
        WHERE CONSTRAINT_NAME = '{constraint_name}'
    """)

    res = cur.fetchall()
    table_name = res[0][0]
    constraint_fields = [row[1] for row in res]

    cur.close()

    return (table_name, constraint_fields)