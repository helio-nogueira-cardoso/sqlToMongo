from utilities import getTables
from func1a import mqlTable
from func1b import mqlTableEmbedded

def mqlAllTables(con):
    tables = getTables(con)
    mql = ""
    for table in tables:
        try:
            mql += mqlTable(con, table)
            mql += "\n"
        except Exception as e:
            print(str(e))
    return mql

def mqlAllTablesEmbedded(con):
    tables = getTables(con)
    mql = ""
    for table in tables:
        try:
            print(f"Iniciando tabela {table}.")
            mql += mqlTableEmbedded(con, table)
            print(f"Tabela {table} finalizada.")
            mql += "\n"
        except Exception as e:
            print(str(e))
    return mql