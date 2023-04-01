import argparse
import sqlparse as sp
import psycopg2

# Example command: python3 check_constraint.py input.sql output.sql

def create_new_query(tableName, tableBody):
    s = "DROP TABLE IF EXISTS {};\n\n".format(tableName)
    s+= "CREATE TABLE {}\n".format(tableName)
    s+= "{}".format(tableBody)
    s+= ";"
    return s

def get_columns(statement):
    # https://stackoverflow.com/questions/63247330/python-sql-parser-to-get-the-column-name-and-data-type

    columnNames = []
    # Get all the tokens except whitespaces
    tokens = [t for t in sp.sql.TokenList(statement.tokens) if t.ttype != sp.tokens.Whitespace]
    columnNames = []
    for token in tokens:
        # If it was a create statement and the current token starts with "("
        if token.value.startswith("("):
            txt = token.value
            columns = txt[1:txt.rfind(")")].replace("\n","").split(",")
            for column in columns:
                c = ' '.join(column.split()).split()
                c_name = c[0].replace('\"',"")
                columnNames.append(c_name)
            break
    return columnNames

def get_body_without_checks(bodyTokens):
    modifiedBody = []
    checks = []

    # flatten bodyTokens
    body = ""
    for i in range(1, len(bodyTokens) - 1):
        token = bodyTokens[i]
        body += str(token)
    for line in body.split(","):
        flag1 = line.find("CONSTRAINT")
        flag2 = line.find("CHECK")

        # If there is a check statement
        if flag2 > -1:
            
            # if the check statement is in a constraint line, remove constraint name
            if flag1 == -1:
                if line[:flag2].strip() != "":
                    modifiedBody.append(line[:flag2])

            checkStatement = line[flag2:]
            parameters = checkStatement[checkStatement.find("("):]
            parameters = parameters[1:parameters.rfind(")")]
            checks.append(parameters)
        else:
            modifiedBody.append(line)



    newChecks = []
    for check in checks:
        ss = ""
        for char in check:
            if char == "(":
                ss+= char + " "
            elif char == ")":
                ss+= " " + char
            else:
                ss+=char
        newChecks.append(ss)
    return newChecks, "(" + ",".join(modifiedBody) + ")"

def create_check_function(tableName, checks, columns, idx = ""):
    function_name = "{}_{}".format(tableName,idx)
    s = "\n\nCREATE OR REPLACE FUNCTION {}_check_function()\n".format(function_name)
    s+=("  RETURNS TRIGGER\n")
    s+=("  LANGUAGE PLPGSQL\n")
    s+=("  AS\n")
    s+=("  $$\n")
    s+=("  BEGIN\n")
    s+=("	  IF ")
    conditions = []
    for check in checks:
        ss = ""
        apos_count = 0
        double_apos_count = 0
        for token in check.split(' '):
            apos_count += token.count("'")
            double_apos_count += token.count('"')
            if token in columns and apos_count % 2 == 0 and double_apos_count % 2 == 0: # Ensure identifier not in string
                ss+="NEW." + token + " "
            else:
                ss+= token + " "
        conditions.append("(" + ss + ")")

    s+=" AND ".join(conditions)
    s+=(" THEN\n")
    s+=("		   return NEW;\n")
    s+=("	  END IF;\n")
    s+=("	  RETURN NULL;\n")
    s+=("  END;\n")
    s+=("  $$;\n\n")
    return s
    
def create_trigger(tableName, idx = ""):
    function_name = "{}_{}".format(tableName,idx)
    s = "CREATE TRIGGER {}_check_trigger\n".format(function_name)
    s+=("BEFORE INSERT ON {}\n".format(tableName))
    s+=("  FOR EACH ROW EXECUTE FUNCTION {}_check_function();\n".format(function_name))
    return s

def execute_ddl_query(cursor, filePath):
    with open(filePath, 'r') as file:
        query = file.read()
        cursor.execute(query)

def execute_dml_query(cursor, filePath):
    with open(filePath, 'r') as file:
        query = file.read()
        cursor.execute("BEGIN")
        cursor.execute("EXPLAIN ANALYZE " + query)
        output = cursor.fetchall()
        cursor.execute("ROLLBACK")
        row = output[len(output)-1][0]
        execution_time = row.split(':')[1].strip().split(' ')[0]
        return execution_time

def compare_performance(host, name, user, password, input, output, insert):
    # Connect to the database
    print("Connecting to database...")
    conn = psycopg2.connect(
        host=host,
        dbname=name,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    print("Connected!")

    # Execute the queries in the .sql file
    execute_ddl_query(cursor, input)
    check_constraint_time = execute_dml_query(cursor, insert)
    print("Insert execution time for table with check constraints: {}ms".format(check_constraint_time))

    # Execute the queries in output file
    execute_ddl_query(cursor, output)
    trigger_time = execute_dml_query(cursor, insert)
    print("Insert execution time for table with triggers: {}ms".format(trigger_time))

    # Compare performance
    if check_constraint_time > trigger_time:
        print("Insertion into table with triggers is faster for your data!")
    else:
        print("Insertion into table with check constraints is faster for your data!")

    # Commit the changes to the database and close the connection
    conn.commit()
    conn.close()

parser = argparse.ArgumentParser(description="This program reads an SQL file and converts any check constraints found into triggers.")

subparsers = parser.add_subparsers(dest='command')

transform_parser = subparsers.add_parser('transform', help='convert check constraints in an SQL file to function triggers for each table')
transform_parser.add_argument('input_path', help='path to target DDL SQL file')
transform_parser.add_argument('output_path', help='path to output processed DDL SQL file')
transform_parser.add_argument('-s', action='store_true', help="split function triggers by individual constraints")

analyze_parser = subparsers.add_parser('analyze', help='compare performance between SQL files')
analyze_parser.add_argument('input_path_1', help='path to first DDL SQL file')
analyze_parser.add_argument('input_path_2', help='path to second DDL SQL file')
analyze_parser.add_argument('input_path_3', help='path to DML SQL file to be used on tables created by both DDL SQL files')
analyze_parser.add_argument('--dbhost', type=str, required=True, help='database host')
analyze_parser.add_argument('--dbname', type=str, required=True, help='database name')
analyze_parser.add_argument('--username', type=str, required=True, help='database username')
analyze_parser.add_argument('--password', type=str, required=True, help='database password')

args = parser.parse_args()
command = args.command

if command == 'transform':
    flag_s = args.s
    input_path = args.input_path
    output_path = args.output_path

    if (input_path == output_path):
        raise Exception("Input and output path cannot be the same!")

    input_file = open(input_path, "r")
    output_file = open(output_path, "w")
    raw = input_file.read()
    raw = sp.format(raw, keyword_case="upper", strip_whitespace=True, identifier_case="upper", use_space_around_operators=True)
    statements = sp.parse(raw)

    for s in statements:
        if s.get_type() != 'CREATE':
            continue
        
        tokens = s.tokens
        tableName = tokens[4]

        bodyTokens = tokens[6].tokens
        checks, modifiedBody = get_body_without_checks(bodyTokens)
        columns = get_columns(s)
        
        newSqlQuery = create_new_query(tableName, modifiedBody)
        newSqlQuery = sp.format(newSqlQuery, keyword_case="upper", identifier_case="upper")

        output_file.write(newSqlQuery)

        if flag_s:
            for idx, check in enumerate(checks):
                output_file.write(create_check_function(tableName, [check], columns, str(idx)))
                output_file.write(create_trigger(tableName, str(idx)))
        else:
            output_file.write(create_check_function(tableName, checks, columns))
            output_file.write(create_trigger(tableName))

    input_file.close()
    output_file.close()

elif command == 'analyze':
    compare_performance(args.dbhost, args.dbname, args.username, args.password, args.input_path_1, args.input_path_2, args.input_path_3)