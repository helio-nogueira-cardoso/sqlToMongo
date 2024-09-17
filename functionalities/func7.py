from utilities import getKeyColumns, getColNames, removePrefix, condComma
from utilities import replaceMultipleSpaces, allNone, everyItemInTargetList, noItemInTargetList, convertStringToList
import re

def check_sql_pattern(sql_query):
    pattern1 = re.compile(r'^SELECT .* FROM \w+ JOIN \w+ ON .+ WHERE .+$', re.IGNORECASE)
    pattern2 = re.compile(r'^SELECT .* FROM \w+ JOIN \w+ ON .+$', re.IGNORECASE)
    pattern3 = re.compile(r'^SELECT .* FROM \w+ WHERE .+$', re.IGNORECASE)
    pattern4 = re.compile(r'^SELECT .* FROM \w+$', re.IGNORECASE)

    if pattern1.match(sql_query):
        return 1
    elif pattern2.match(sql_query):
        return 2
    elif pattern3.match(sql_query):
        return 3
    elif pattern4.match(sql_query):
        return 4
    else:
        return 0

def mqlSelect(con, sql_query):
    sql_query = sql_query.replace('\n', ' ')
    sql_query = sql_query.replace('\r', ' ')
    sql_query = sql_query.replace('\t', ' ')
    sql_query = sql_query.strip()
    sql_query = replaceMultipleSpaces(sql_query)

    query_type = check_sql_pattern(sql_query)

    if query_type == 0:
        raise Exception("Formato incorreto de consulta.")
    elif query_type == 1:
        return mqlAggregateWithWhere(con, sql_query)
    elif query_type == 2:
        return mqlAggregateWithoutWhere(con, sql_query)
    elif query_type == 3:
        return mqlFindOrSimpleAggregate(con, sql_query, True)
    elif query_type == 4:
        return mqlFindOrSimpleAggregate(con, sql_query, False)

def mqlFindOrSimpleAggregate(con, sql_query, has_conditions):
    is_aggregate = False
    mql = ""

    if has_conditions:
        attributes, table_name, conditions, and_or = parseSQLPattern3(sql_query)
    else:
        attributes, table_name = parseSQLPattern4(sql_query)

    if len(attributes) == 0:
        raise Exception('Erro ao interpretar a consulta')
    
    attributes, functions = extractAttributesAndFunctions(attributes)

    if allNone(functions):
        is_aggregate = False
    elif everyItemInTargetList(functions, ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'MEDIAN']):
        is_aggregate = True
    else:
        raise Exception('Consulta incorreta ou não suportada.')
    
    keys = getKeyColumns(con, table_name)
    
    if is_aggregate:
        mql = f'db.{table_name}' + '.aggregate([\n'
        if has_conditions:
            mql += '{\n'
            mql += '"$match": {\n'
            mql += f'${and_or}: [\n'
            for i, condition in enumerate(conditions):
                mql += makeConditionPattern3(condition, keys)
                mql += condComma(i, conditions)
            mql += ']'
            mql += '}\n'
            mql += '},\n'
        mql += makeAggregatePatterns3and4(attributes, functions, keys)
        mql += '\n])'
        return mql
    
    mql += f'db.{table_name}' + '.find({\n'
    if has_conditions:
        mql += f'${and_or}: [\n'
        for i, condition in enumerate(conditions):
            mql += makeConditionPattern3(condition, keys)
            mql += condComma(i, conditions)
        mql += ']'
    mql += '}'

    if attributes[0] == '*':
        mql += ')'
        return mql
    mql += ',\n'
    mql += makeProjectionPatterns3and4(attributes, keys)
    mql += ')'

    return mql
    
def mqlAggregateWithoutWhere(con, sql_query):
    mql = ""
    attributes, local_table_name, foreign_table_name, local_foreign = parseSQLPattern2(sql_query)

    if len(attributes) == 0:
        raise Exception('Erro ao interpretar a consulta')
    elif len(local_foreign) == 0:
        raise Exception('Erro ao interpretar JOIN na consulta')
    elif len(local_foreign) > 2:
        raise Exception('Erro: JOIN com predicado de junção contendo mais de um atributo (chaves referenciadas compostas)')
    
    attributes, functions = extractAttributesAndFunctions(attributes)

    if allNone(functions):
        is_aggregate = False
    elif everyItemInTargetList(functions, ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'MEDIAN']):
        is_aggregate = True
    else:
        raise Exception('Consulta incorreta ou não suportada.')

    for i in range(len(attributes)):
        attributes[i] = removePrefix(attributes[i], local_table_name + '.')
        attributes[i] = removePrefix(attributes[i], foreign_table_name + '.')

    local_table_keys = getKeyColumns(con, local_table_name)
    foreign_table_keys = getKeyColumns(con, foreign_table_name)
    local_cols = getColNames(con, local_table_name)
    foreign_cols = getColNames(con, foreign_table_name)

    mql += f'db.{local_table_name}' + '.aggregate([{\n'
    mql += "$lookup:\n{\n"
    mql += f'from: "{foreign_table_name}", \n'

    if local_foreign[0] in local_table_keys:
        local_foreign[0] = f'_id.{local_foreign[0]}'

    mql += f'localField: "{local_foreign[0]}", \n'

    if local_foreign[1] in foreign_table_keys:
        local_foreign[1] = f'_id.{local_foreign[1]}'

    mql += f'foreignField: "{local_foreign[1]}", \n'
    mql += f'as: "{foreign_table_name}"\n'
    mql += "}\n"
    mql += '},\n'
    mql += '{\n$unwind: {\npath: ' + f'"${foreign_table_name}"' + '\n}\n}'
    if is_aggregate:
        mql += ',\n'
        mql += makeAggregatePatterns1and2(attributes, functions, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)
        mql += '\n])'
        return mql

    if attributes[0] == '*':
        mql += '\n])'
        return mql
    mql += ',\n'

    local_cols = getColNames(con, local_table_name)
    foreign_cols = getColNames(con, foreign_table_name)
    
    mql += '{\n'
    mql += '$project: '
    mql += makeProjectionPatterns1and2(attributes, local_table_keys, foreign_table_keys, local_cols, foreign_cols)
    mql += '\n}])'
    return mql
    
def mqlAggregateWithWhere(con, sql_query):
    mql = ""
    attributes, local_table_name, foreign_table_name, local_foreign, condition_list, and_or = parseSQLPattern1(sql_query)

    if len(attributes) == 0:
        raise Exception('Erro ao interpretar a consulta')
    elif len(local_foreign) == 0:
        raise Exception('Erro ao interpretar JOIN na consulta')
    elif len(local_foreign) > 2:
        raise Exception('Erro: JOIN com predicado de junção contendo mais de um atributo (chaves referenciadas compostas)')
    
    attributes, functions = extractAttributesAndFunctions(attributes)

    if allNone(functions):
        is_aggregate = False
    elif everyItemInTargetList(functions, ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'MEDIAN']):
        is_aggregate = True
    else:
        raise Exception('Consulta incorreta ou não suportada.')

    for i in range(len(attributes)):
        attributes[i] = removePrefix(attributes[i], local_table_name + '.')
        attributes[i] = removePrefix(attributes[i], foreign_table_name + '.')
    
    local_table_keys = getKeyColumns(con, local_table_name)
    foreign_table_keys = getKeyColumns(con, foreign_table_name)
    local_cols = getColNames(con, local_table_name)
    foreign_cols = getColNames(con, foreign_table_name)

    mql += f'db.{local_table_name}' + '.aggregate([{\n'
    mql += "$lookup:\n{\n"
    mql += f'from: "{foreign_table_name}", \n'

    if local_foreign[0] in local_table_keys:
        local_foreign[0] = f'_id.{local_foreign[0]}'

    mql += f'localField: "{local_foreign[0]}", \n'

    if local_foreign[1] in foreign_table_keys:
        local_foreign[1] = f'_id.{local_foreign[1]}'

    mql += f'foreignField: "{local_foreign[1]}", \n'
    mql += f'as: "{foreign_table_name}"\n'
    mql += '}\n'
    mql += '},\n'

    mql += '{\n$unwind: {\npath: ' + f'"${foreign_table_name}"' + '\n}\n},\n'

    mql += "{\n"
    #---
    
    mql += '$match: {\n'

    mql += f'${and_or}: [\n'
    for i, condition in enumerate(condition_list):
        mql += makeConditionPattern1(condition, local_table_name, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)
        mql += condComma(i, condition_list)
    mql += ']'

    mql += '}\n'
    
    #---
    mql += '}'

    if is_aggregate:
        mql += ',\n'
        mql += makeAggregatePatterns1and2(attributes, functions, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)
        mql += '\n])'
        return mql

    if attributes[0] == '*':
        mql += '\n])'
        return mql
    mql += ',\n'   
    mql += '{\n'
    mql += '$project: '
    mql += makeProjectionPatterns1and2(attributes, local_table_name, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)
    mql += '\n}])'
    return mql
    
def parseSQLPattern1(sql_query):
    select_match = re.search(r'^SELECT (.+?)\s+FROM\s+(.+?)\s+JOIN\s+(.+?)\s+ON\s+(.+?)\s+WHERE\s+(.+?)$', sql_query, re.IGNORECASE)

    if select_match:
        attribute_list = select_match.group(1).strip().split(', ')
        local_table_name = select_match.group(2).strip()
        foreign_table_name = select_match.group(3).strip()
        join_condition = select_match.group(4)
        conditions = select_match.group(5)

        if ' = ' in join_condition:
            local_foreign = join_condition.split(' = ')
        else:
            raise Exception('Condição de junção mal construída.')
            
        local_foreign[0] = removePrefix(local_foreign[0], local_table_name + '.')
        local_foreign[1] = removePrefix(local_foreign[1], foreign_table_name + '.')

        if conditions:
            if ' AND ' in conditions:
                condition_list = conditions.split(' AND ')
                condition_separator = 'and'
            elif ' OR ' in conditions:
                condition_list = conditions.split(' OR ')
                condition_separator = 'or'
            else:
                condition_list = [conditions]
                condition_separator = 'and'
        else:
            condition_list = []
            condition_separator = None

        return attribute_list, local_table_name, foreign_table_name, local_foreign, condition_list, condition_separator
    else:
        raise Exception('Consulta mal construída.')

def parseSQLPattern2(sql_query):
    select_match = re.search(r'SELECT (.+?)\s+FROM\s+(.+?)(?:\s+JOIN\s+(.+?)\s+ON\s+(.*?))?$', sql_query, re.IGNORECASE)

    if select_match:
        attribute_list = select_match.group(1).strip().split(', ')
        local_table_name = select_match.group(2).strip()
        foreign_table_name = select_match.group(3).strip()
        join_condition = select_match.group(4)

        if ' = ' in join_condition:
            local_foreign = join_condition.split(' = ')
        else:
            raise Exception('Condição de junção mal construída.')
            
        local_foreign[0] = removePrefix(local_foreign[0], local_table_name + '.')
        local_foreign[1] = removePrefix(local_foreign[1], foreign_table_name + '.')

        return attribute_list, local_table_name, foreign_table_name, local_foreign
    else:
        raise Exception('Consulta mal construída.')
        
def parseSQLPattern3(sql_query):
    select_match = re.search(r'SELECT (.+?) FROM (.+?)(?: WHERE (.*))?$', sql_query, re.IGNORECASE)
    
    if select_match:
        attribute_list = select_match.group(1).strip().split(', ')
        table_name = select_match.group(2).strip()
        conditions = select_match.group(3)
        
        if conditions:
            if ' AND ' in conditions:
                condition_list = conditions.split(' AND ')
                condition_separator = 'and'
            elif ' OR ' in conditions:
                condition_list = conditions.split(' OR ')
                condition_separator = 'or'
            else:
                condition_list = [conditions]
                condition_separator = 'and'
        else:
            condition_list = []
            condition_separator = None
        
        return attribute_list, table_name, condition_list, condition_separator
    else:
        return [], None, [], None   

def parseSQLPattern4(sql_query):
    select_match = re.search(r'^SELECT (.+?) FROM (\w+)$', sql_query, re.IGNORECASE)
    
    if select_match:
        attribute_list = select_match.group(1).strip().split(', ')
        table_name = select_match.group(2).strip()
        
        return attribute_list, table_name
    else:
        return [], None

def makeProjectionPatterns1and2(attributes, local_table_name, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols):
    mql = '{\n'

    for i in range(len(attributes)):
        attributes[i] = removePrefix(attributes[i], local_table_name + '.')
        attributes[i] = removePrefix(attributes[i], local_table_name + '.')

    for i, attr in enumerate(attributes):
        if attr in local_cols:
            if attr in local_table_keys:
                mql += '"_id.'
            else:
                mql += '"'
        elif attr in foreign_cols:
            if attr in foreign_table_keys:
                mql += f'"{foreign_table_name}._id.'
            else:
                mql += f'"{foreign_table_name}.'
        mql += f'{attr}": 1'
        mql += condComma(i, attributes)
    if noItemInTargetList(attributes, local_table_keys):
        mql += ',\n'
        mql += '"_id": 0\n'
    mql += '}'
    return mql

def makeProjectionPatterns3and4(attributes, keys):
    mql = '{\n'
    for i, attr in enumerate(attributes):
        if attr in keys:
            mql += '"_id.'
        else:
            mql += '"'
        mql += f'{attr}": 1'
        mql += condComma(i, attributes)
    if noItemInTargetList(attributes, keys):
        mql += ',\n'
        mql += '"_id": 0\n'
    mql += '}'
    return mql

def makeConditionPattern1(condition, local_table_name, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols):
    mql = '{'
    attr, value, op = parseCondition(condition) 
    attr = removePrefix(attr, local_table_name + '.')
    attr = removePrefix(attr, foreign_table_name + '.')

    if op in ('in', 'nin'):
        values = convertStringToList(value)
        if attr in local_cols:
            if attr in local_table_keys:
                mql += '"_id.'
            else:
                mql += '"'
        elif attr in foreign_cols:
            if attr in foreign_table_keys:
                mql += f'"{foreign_table_name}._id.'
            else:
                mql += f'"{foreign_table_name}.'

        mql += f'{attr}": ' + '{' + f'${op}: [\n'
        for j, value in enumerate(values):
            mql += value
            mql += condComma(j, values)
        mql += ']}'
    else:
        if attr in local_cols:
            if attr in local_table_keys:
                mql += '"_id.'
            else:
                mql += '"'
        elif attr in foreign_cols:
            if attr in foreign_table_keys:
                mql += f'"{foreign_table_name}._id.'
            else:
                mql += f'"{foreign_table_name}.'
        mql += f'{attr}": ' + '{'+ f'${op}: {value}' + '}'
    mql += '}'
    return mql

def makeConditionPattern3(condition, keys):
    mql = '{'
    attr, value, op = parseCondition(condition) 
    if op in ('in', 'nin'):
        values = convertStringToList(value)
        if attr in keys:
            mql += '"_id.'
        else:
            mql += '"'

        mql += f'{attr}": ' + '{' + f'${op}: [\n'
        for j, value in enumerate(values):
            mql += value
            mql += condComma(j, values)
        mql += ']}'
    else:
        if attr in keys:
            mql += '"_id.'
        else:
            mql += '"'
        mql += f'{attr}": ' + '{'+ f'${op}: {value}' + '}'
    mql += '}'
    return mql

def makeAggregatePatterns1and2(attributes, functions, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols):
    mql = '{\n'
    mql += '"$group": {\n'
    mql += '"_id": null,\n'
    
    for i, function in enumerate(functions):
        if attributes[i] == '*' and function.upper() != 'COUNT':
            raise Exception(f'{function.upper()} não funciona com *')
        elif attributes[i] == '*':
            mql += f'"{function.upper()}": ' + '{'
        else:
            mql += f'"{function.upper()}_{attributes[i]}": ' + '{'

        if function.upper() in ['SUM', 'AVG', 'MIN', 'MAX']:
            mql += f'"${function.lower()}": "${putPrefixIfKeyOrForeignTable(attributes[i], foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)}"' + '}'
        elif function.upper() == 'COUNT' and attributes[i] == '*':
            mql += '"$sum": 1' + '}'
        elif function.upper() == 'COUNT':
            mql += '"$sum": {"$cond": [{"$ne": ['
            mql += f'"${putPrefixIfKeyOrForeignTable(attributes[i], foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)}"'
            mql += ', null]}, 1, 0]}' + '}'
        elif function.upper() == 'MEDIAN':  
            mql += '\n"$median": {\n'
            mql += f'"input": "${putPrefixIfKeyOrForeignTable(attributes[i], foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols)}",\n'
            mql += '"method": "approximate"\n'
            mql += '}\n' + '}' 
        else:
            raise Exception(f'Função {function.upper()} não suportada ou inexistente.')
        mql += condComma(i, functions)
    mql += '}\n'

    mql += '},\n'
    mql += '{"$project": {"_id": 0}}'
    
    return mql

def makeAggregatePatterns3and4(attributes, functions, keys):
    mql = '{\n'
    mql += '"$group": {\n'
    mql += '"_id": null,\n'
    
    for i, function in enumerate(functions):
        if attributes[i] == '*' and function.upper() != 'COUNT':
            raise Exception(f'{function.upper()} não funciona com *')
        elif attributes[i] == '*':
            mql += f'"{function.upper()}": ' + '{'
        else:
            mql += f'"{function.upper()}_{attributes[i]}": ' + '{'

        if function.upper() in ['SUM', 'AVG', 'MIN', 'MAX']:
            mql += f'"${function.lower()}": "${putIdIfKeys(attributes[i], keys)}"' + '}'
        elif function.upper() == 'COUNT' and attributes[i] == '*':
            mql += '"$sum": 1' + '}'
        elif function.upper() == 'COUNT':
            mql += '"$sum": {"$cond": [{"$ne": ['
            mql += f'"${putIdIfKeys(attributes[i], keys)}"'
            mql += ', null]}, 1, 0]}' + '}'
        elif function.upper() == 'MEDIAN':  
            mql += '\n"$median": {\n'
            mql += f'"input": "${putIdIfKeys(attributes[i], keys)}",\n'
            mql += '"method": "approximate"\n'
            mql += '}\n' + '}' 
        else:
            raise Exception(f'Função {function.upper()} não suportada ou inexistente.')
        mql += condComma(i, functions)
    mql += '}\n'

    mql += '},\n'
    mql += '{"$project": {"_id": 0}}'
    return mql

def parseCondition(condition):
    comparators = {
        '=': 'eq',
        '<': 'lt',
        '<=': 'lte',
        '>': 'gt',
        '>=': 'gte',
        '!=': 'ne',
        '<>': 'ne'
    }

    if ' NOT IN ' in condition:
        parts = condition.split(' NOT IN ')
        attribute, values = parts
        return attribute, values, 'nin'
    elif ' IN ' in condition:
        parts = condition.split(' IN ')
        attribute, values = parts
        return attribute, values, 'in'
    else:
        parts = condition.split(' ', 2)
        if len(parts) == 3:
            attribute, operator, value = parts
            operator_str = comparators.get(operator, 'error')
            if operator_str == 'error':
                raise Exception('Condição mal construída.')
            return attribute, value, operator_str
        else:
            raise Exception('Condição mal construída.')
        
def extractAttributesAndFunctions(attributes):
    core_attributes = []
    functions = []

    for attr in attributes:
        match = re.match(r'(?i)^(\w+)\((\w+)\)$', attr)
        if match:
            func, core_attr = match.groups()
            core_attributes.append(core_attr)
            functions.append(func)
        else:
            match = re.match(r'(?i)^(\w+)\((.*)\)$', attr)
            if match:
                func, core_attr = match.groups()
                core_attributes.append(core_attr)
                functions.append(func)
            else:
                core_attributes.append(attr)
                functions.append(None)

    return core_attributes, functions

def putIdIfKeys(attr, keys):
    if attr in keys:
        attr = '_id.' + attr
    return attr

def putPrefixIfKeyOrForeignTable(attr, foreign_table_name, local_table_keys, foreign_table_keys, local_cols, foreign_cols):
    if attr in local_cols:
        if attr in local_table_keys:
            return f'_id.{attr}'
        else:
            return attr
    elif attr in foreign_cols:
        if attr in foreign_table_keys:
            return f'{foreign_table_name}._id.{attr}'
        else:
            return f'{foreign_table_name}.{attr}'
    return attr