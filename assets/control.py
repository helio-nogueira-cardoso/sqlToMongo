from utilities import getTables
import os

def clearTerminal():
    if os.name == 'posix':  # Linux and macOS
        os.system('clear')
    elif os.name == 'nt':  # Windows
        os.system('cls')
    else:
        print('\n' * 100)

def hold():
    input("\n\nEnter para continuar.")
    clearTerminal()

def selectOption(options, header, prompt):
    selected = -1

    while True:
        print(header)

        for i, option in enumerate(options):
            print(i, "-", option)
        print("")

        selected = input(prompt).strip()

        try:
            if int(selected) in range(len(options)):
                return int(selected)
            else:
                raise Exception
        except Exception:
            print("Escolha dentre as opções\n")
        hold()

def selectTable(con):
    tables = getTables(con)
    if len(tables) == 0:
        raise Exception('\nNão há tabelas para se selecionar nesta base.')
    selected = selectOption(tables, "\n-- Tabelas da Base --", "Qual tabela deseja? ")
    return tables[selected]