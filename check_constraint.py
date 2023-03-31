import sys, argparse
import sqlparse as sp

# Example command: python3 check_constraint.py input.sql output.sql

def create_new_query(tableName, tableBody):
    s = "DROP TABLE IF EXISTS {};\n\n".format(tableName)
    s+= "CREATE TABLE {}\n".format(tableName)
    s+= "{}\n\n".format(tableBody)
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
    return checks, modifiedBody


def create_check_function(tableName, checks, columns):
    # checks = [(LHS, operator, RHS)]
    s = "CREATE OR REPLACE FUNCTION {}_check_function()\n".format(tableName)
    s+=("  RETURNS TRIGGER\n")
    s+=("  LANGUAGE PLPGSQL\n")
    s+=("  AS\n")
    s+=("  $$\n")
    s+=("  BEGIN\n")
    s+=("	  IF ")
    conditions = []
    for check in checks:
        if check[2] in columns:
            conditions.append("NEW.{} {} NEW.{}".format(check[0], check[1], check[2]))
        else:
            conditions.append("NEW.{} {} {}".format(check[0], check[1], check[2]))
    
    s+=" AND ".join(conditions)
    s+=(" THEN\n")
    s+=("		   return NEW\n")
    s+=("	  END IF;\n")
    s+=("	  RETURN NULL;\n")
    s+=("  END;\n")
    s+=("  $$\n\n")
    return s
    
def create_trigger(tableName):
    # checks = [(LHS, operator, RHS)]
    s = "CREATE TRIGGER {}_check_trigger\n".format(tableName)
    s+=("BEFORE INSERT ON {}\n".format(tableName))
    s+=("  FOR EACH ROW EXECUTE FUNCTION {}_check_function\n".format(tableName))
    return s

parser = argparse.ArgumentParser(description="This program reads an SQL file and converts any check constraints found into triggers.")
parser.add_argument('input_path', help='path to target SQL file')
parser.add_argument('output_path', help='path to output processed SQL file')

args = parser.parse_args()

input_path = args.input_path
output_path = args.output_path

if (input_path == output_path):
    raise Exception("Input and output path cannot be the same!")

input_file = open(input_path, "r")
output_file =  open(output_path, "w")
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
    output_file.write(create_check_function(tableName, checks, columns))
    output_file.write(create_trigger(tableName))


