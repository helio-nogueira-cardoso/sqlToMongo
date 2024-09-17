import sys
sys.path.append("functionalities")
sys.path.append("assets")
from func1a import mqlTable
from func1b import mqlTableEmbedded
from func2 import mqlAllTables, mqlAllTablesEmbedded
from func3 import mqlQuery
from func4 import mqlFutebol
from func5 import mqlIndexes
from func6 import mqlIndexesFutebol
from func7 import mqlSelect
from utilities import init, connect, writeMQL, readMultilineString
from control import selectOption, selectTable, hold

def main():
    seq = [1]
    init()

    print("Conecte em uma conta")
    con = connect()

    options = [
        # 0:
        "Trocar de conta.",
        # 1:
        "Gerar MQL para inserção de uma tabela. (questão 1a)", 
        # 2:
        "Gerar MQL para inserção de todas as tabelas. (questão 1b)",
        # 3:
        "Gerar MQL para inserção do resultado de uma consulta. (questão 1c)", 
        # 4:
        "Gerar MQL para inserção considerando a base de Futebol. (questão 1d)",
        # 5:
        "Gerar índices para chaves primárias e secundárias genérica. (questão 2a)",
        # 6:
        "Gerar índices apenas para chaves secundárias considerando base de Futebol. (questão 2b)",
        # 7:
        "Gerar consulta mongo a partir de consulta sql. (questões 4, 5 e 6)", 
        # 8:
        "Finalizar programa." 
    ]

    while True:
        command = selectOption(options, "-- Opções --", "O que deseja? ")

        if command == 0:
            con.close()
            con = connect()
        elif command == 1:
            try:
                table_name = selectTable(con)
                emb_options = [
                    "Gerar documentos apenas com linking",
                    "Gerar documentos com embeddings recursivamente (demora um pouco mais)"
                ]
                emb = selectOption(emb_options, "\n-- Escolha --", "Como deseja que seja feito? ")
                
                try:
                    mql = ""
                    if emb == 0:
                        mql += mqlTable(con, table_name)
                    elif emb == 1:
                        mql += mqlTableEmbedded(con, table_name)
                    writeMQL(mql)
                except Exception as e:
                    print("\n" + str(e))
            except Exception as e:
                print("\n" + str(e))
        elif command == 2:
            emb_options = [
                "Gerar documentos apenas com linking",
                "Gerar documentos com embeddings recursivamente (demora)"
            ]
            emb = selectOption(emb_options, "\n-- Escolha --", "Como deseja que seja feito? ")
            try:
                mql = ""
                if emb == 0:
                    mql += mqlAllTables(con)
                elif emb == 1:
                    mql += mqlAllTablesEmbedded(con)
                writeMQL(mql)
            except Exception as e:
                print("\n" + str(e))
        elif command == 3:
            print("\nDigite a consulta abaixo:")
            query = readMultilineString()
            try:
                mql = mqlQuery(con, query, seq)
                writeMQL(mql)
            except Exception as e:
                print("\n" + str(e))
        elif command == 4:
            try:
                mql = mqlFutebol(con)
                writeMQL(mql)
            except Exception as e:
                print("\n" + str(e))
        elif command == 5:
            try:
                mql = mqlIndexes(con)
                writeMQL(mql)
            except Exception as e:
                print("\n" + str(e))
        elif command == 6:
            try:
                mql = mqlIndexesFutebol(con)
                writeMQL(mql)
            except Exception as e:
                print("\n" + str(e))
        elif command == 7:
            print("\nDigite a consulta abaixo:")
            query = readMultilineString().strip()
            try:
                mql = mqlSelect(con, query)
                print("\nConvertido:\n")
                print(mql)
            except Exception as e:
                print("\n" + str(e))
        elif command == 8:
            break
        hold()

    con.close()

if __name__ == "__main__":
    main()