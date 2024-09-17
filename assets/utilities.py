from dotenv import load_dotenv
from datetime import datetime
import cx_Oracle
import os
import getpass
import re

def init():
    load_dotenv()
    oracle_lib_dir = os.getenv("ORACLE_LIB_DIR")
    try:
        cx_Oracle.init_oracle_client(lib_dir=oracle_lib_dir)
    except Exception as e:
        print(str(e))

def connect():
    connection = None

    load_dotenv()
    oracle_host = os.getenv("HOST")
    oracle_port = os.getenv("PORT")
    oracle_service_name = os.getenv("SERVICE_NAME")

    while True:
        try:
            dsn_str = cx_Oracle.makedsn(host=oracle_host, port=oracle_port, service_name=oracle_service_name)
            us = input("Digite seu nome de usuário: ")
            pw = getpass.getpass("Digite sua senha: ")

            connection = cx_Oracle.connect(user=us, password=pw, dsn=dsn_str)

            print("Conectado com sucesso!\n")
            return connection
        except Exception as e:
            print("Erro ao se conectar:")
            print(str(e))
            print("Tente novamente\n")

def getTables(con):
    cur = con.cursor()
    cur.execute("SELECT TABLE_NAME FROM USER_TABLES")
    res = cur.fetchall()
    tables = [table_name for (table_name,) in res]
    cur.close()
    return tables

def getRefContraints(con, table_name):
    cur = con.cursor()
    cur.execute(f"""
        SELECT DISTINCT
            a.constraint_name 
        FROM all_cons_columns a 
            JOIN all_constraints c 
            ON a.owner = c.owner AND a.constraint_name = c.constraint_name 
            JOIN all_cons_columns b 
            ON c.owner = b.owner and c.r_constraint_name = b.constraint_name AND a.position = b.position 
        WHERE c.constraint_type = 'R' AND a.table_name = '{table_name}'
    """)

    res = cur.fetchall()
    ref_constraints = [row[0] for row in res]

    cur.close()
    return ref_constraints

def getColumns(cur):
    col_names = [desc[0] for desc in cur.description]
    col_types = [desc[1] for desc in cur.description]
    col_index = {col_name: i for i, col_name in enumerate(col_names)}
    return (col_names, col_types, col_index)

def getColNames(con, table_name):
    cur = con.cursor()
    cur.execute(f"""
        SELECT COLUMN_NAME
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = '{table_name}'
    """)

    res = cur.fetchall()
    col_names = [col_name for (col_name,) in res]
    return col_names 

def getKeyColumns(con, table_name):
    cur = con.cursor()
    cur.execute(f"""
        SELECT COLUMN_NAME FROM USER_CONS_COLUMNS CL 
        JOIN USER_CONSTRAINTS CT 
        ON CL.CONSTRAINT_NAME = CT.CONSTRAINT_NAME 
        WHERE CL.TABLE_NAME = '{table_name}' AND
            CT.CONSTRAINT_TYPE = 'P'
    """)
    res = cur.fetchall()
    keys = [col_name for (col_name,) in res]
    cur.close()
    return keys

def getNotKeys(keys, col_names):
    not_keys = []
    for col in col_names:
        if col not in keys:
            not_keys.append(col)
    return not_keys

def writeMQL(mql):
    with open('mongo.mql', 'w') as arquivo:
        arquivo.write(mql)

    print("\nMQL escrito em arquivo 'mongo.mql' no diretório corrente.\n")

def formatField(col, col_index, col_types, value):
    if value is None:
        return 'null'
    if col_types[col_index[col]] == cx_Oracle.BLOB:
        return '"<<BLOB>>"'
    if col_types[col_index[col]] == cx_Oracle.DB_TYPE_DATE:
        formatted_date = datetime.strftime(value, "%Y-%m-%dT%H:%M:%S.000Z")
        return f'ISODate("{formatted_date}")'
    if col_types[col_index[col]] == cx_Oracle.STRING:
        return f'"{value}"'
    return f'{value}'

def formatFieldSingleQuote(col, col_index, col_types, value):
    if value is None:
        return 'null'
    if col_types[col_index[col]] == cx_Oracle.BLOB:
        return "'<<BLOB>>'"
    if col_types[col_index[col]] in [cx_Oracle.STRING, cx_Oracle.DB_TYPE_DATE]:
        return f"'{value}'"
    return f'{value}'
    
def condComma(i, res):
    mql = ""
    if i < len(res) - 1:
        mql += ","
    mql += "\n"
    return mql

def readMultilineString():
    print("[[Em uma linha separada, pressione Ctrl+D (Unix) ou Ctrl+Z (Windows) para finalizar]]\n")

    multiline_string = ''
    while True:
        try:
            line = input()
            multiline_string += line + '\n'
        except EOFError:
            break

    return multiline_string

def convertStringToList(input_string):
    input_string = input_string.strip('()')

    values = input_string.split(',')
    values = [value.strip() for value in values]

    return values

def replaceMultipleSpaces(input_string):
    output_string = re.sub(r'\s+', ' ', input_string)
    return output_string

def removePrefix(target_string, search_string):
    if target_string.startswith(search_string):
        return target_string[len(search_string):]
    else:
        return target_string

def noItemInTargetList(items, targetList):
    for item in items:
        if item is None:
            return False
        if item.upper() in targetList:
            return False
    return True

def everyItemInTargetList(items, targetList):
    for item in items:
        if item is None:
            return False
        if item.upper() not in targetList:
            return False
    return True

def allNone(items):
    for item in items:
        if item is not None:
            return False
    return True