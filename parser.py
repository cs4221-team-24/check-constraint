import sqlparse as sp

sqlFile = "create.sql"

inputFile = open(sqlFile, "r")
outputFile =  open("modified.sql", "w")
raw = inputFile.read()
raw = sp.format(raw, keyword_case="upper", strip_whitespace=True, identifier_case="upper", use_space_around_operators=True)
statements = sp.parse(raw)


def createNewSqlQuery(tableName, tableBody):
    s = "DROP TABLE IF EXISTS {};\n\n".format(tableName)
    s+= "CREATE TABLE {}\n".format(tableName)
    s+= "{}\n\n".format(tableBody)
    return s


def getColumns(statement):
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

def getBodyWithoutChecks(bodyTokens):
    lenn = len(bodyTokens)
    idx = 0
    modifiedBody = ""
    checks = []

    
    while idx < lenn:
        if "CHECK" != str(bodyTokens[idx]):
            modifiedBody+=str(bodyTokens[idx])
            idx+=1
            continue
        # account for whitespace
        checks.append(str(bodyTokens[idx + 2]))
        idx+=3
    checks = list(map(lambda x: x[1:-1].split(' '), checks))
    print(checks)
    return checks, modifiedBody


def createCheckFunction(tableName, checks, columns):
    # checks = [(LHS, operator, RHS)]
    s = "CREATE OR REPLACE FUNCTION {}_check_function()\n".format(tableName)
    s+=("  RETURNS TRIGGER\n")
    s+=("  LANGUAGE PLPGSQL\n")
    s+=("  AS\n")
    s+=("  $$\n")
    s+=("  BEGIN\n")
    for check in checks:
        if check[2] in columns:
            s+=("	  IF NEW.{} {} NEW.{} THEN\n").format(check[0], check[1], check[2])
        else:
            s+=("	  IF NEW.{} {} {} THEN\n").format(check[0], check[1], check[2])
        s+=("		   return NULL\n")
        s+=("	  END IF;\n")
    
    s+=("	  RETURN NEW;\n")
    s+=("  END;\n")
    s+=("  $$\n\n")
    return s
    
def createTrigger(tableName):
    # checks = [(LHS, operator, RHS)]
    s = "CREATE TRIGGER {}_check_trigger\n".format(tableName)
    s+=("BEFORE INSERT ON {}\n".format(tableName))
    s+=("  FOR EACH ROW EXECUTE FUNCTION {}_check_function\n".format(tableName))
    return s
for s in statements:
    if s.get_type() != 'CREATE':
        continue
    
    tokens = s.tokens
    tableName = tokens[4]

    bodyTokens = tokens[6].tokens
    checks, modifiedBody = getBodyWithoutChecks(bodyTokens)
    columns = getColumns(s)

    
    newSqlQuery = createNewSqlQuery(tableName, modifiedBody)
    newSqlQuery = sp.format(newSqlQuery, keyword_case="upper", identifier_case="upper")
    
    outputFile.write(newSqlQuery)
    outputFile.write(createCheckFunction(tableName, checks, columns))
    outputFile.write(createTrigger(tableName))


