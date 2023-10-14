import os
import ast
import sqlite3

DB_NAME = 'mydatabase.db'
INPUT_DIR = "files_in"

def init_db():
    conn = sqlite3.connect(DB_NAME)

    conn.execute('''CREATE TABLE documents
                (
                doc_id INTEGER PRIMARY KEY,
                doc_type TEXT
                );''')
                
    conn.execute('''CREATE TABLE doc_form_types
                (
                doc_type TEXT PRIMARY KEY,
                OKUD_code TEXT
                );''')

    conn.execute('''CREATE TABLE documents_type_M11
                 (
                 doc_id INTEGER PRIMARY KEY,
                 doc_path TEXT,
                 doc_status TEXT,
                 stat_id INTEGER,
                 bill_num INTEGER,
                 organization_id INTEGER,
                 subdivision_id INTEGER,
                 date DATE,
                 table1_id INTEGER,
                 table2_id INTEGER,
                 sender_id INTEGER,
                 receiver_id INTEGER,
                 mediator_id INTEGER,
                 table_signatures INTEGER
                 );''')
                 
    # TODO - create 76 - type documents table!!!

    conn.execute('''CREATE TABLE organizations
                 (
                 organization_id INTEGER PRIMARY KEY,
                 OKPO_code TEXT,
                 name TEXT
                 );''')

    conn.execute('''CREATE TABLE doc_type_rules
                 (
                 doc_type TEXT PRIMARY KEY,
                 rules_names TEXT,
                 rules_queries TEXT
                 );''')

    conn.execute('''CREATE TABLE queue_preproc
                 (doc_id INTEGER PRIMARY KEY);''')

    conn.execute('''CREATE TABLE queue_inproc
                 (doc_id INTEGER PRIMARY KEY);''')

    conn.execute('''CREATE TABLE table1_sender_receiver
                 (
                 table1_id INTEGER PRIMARY KEY,
                 entry_num INTEGER,
                 organization_id INTEGER,
                 date DATE,
                 operation_code INTEGER,
                 sender_subd_id INTEGER,
                 sender_plant_id INTEGER,
                 receiver_subd_id INTEGER,
                 receiver_plant_id INTEGER,
                 account_id INTEGER,
                 output_normal_unit INTEGER
                 );''')
    
    conn.execute('''CREATE TABLE table2_goods_enumeration
                 (
                 table2_id INTEGER PRIMARY KEY,
                 entry_num INTEGER,
                 organization_id INTEGER,
                 date DATE,
                 account_id INTEGER,
                 material_nom_id INTEGER,
                 characteristic TEXT,
                 factory_num INTEGER,
                 net_num INTEGER,
                 measurement_unit_id INTEGER,
                 amount_demanded FLOAT,
                 amount_granted FLOAT,
                 price FLOAT,
                 sum_without_NDS FLOAT,
                 order_cart_num INTEGER,
                 location TEXT,
                 reg-party_num INTEGER
                 );''')

    conn.execute('''CREATE TABLE table_signatures
                 (tablesign_id INTEGER PRIMARY KEY,
                  organization_id INTEGER,
                  official_id INTEGER,
                  certificate_id INTEGER,
                  signing_date DATE
                 );''')

    conn.execute('''CREATE TABLE material_assets
                 (material_nom_id INTEGER PRIMARY KEY,
                  name TEXT
                 );''')

    conn.execute('''CREATE TABLE account
                 (account_id INTEGER PRIMARY KEY,
                  account INTEGER,
                  analythic_account_id INTEGER
                 );''')

    conn.execute('''CREATE TABLE measurement_units
                 (measurement_unit_id INTEGER PRIMARY KEY,
                  name TEXT
                 );''')

    conn.commit()
    conn.close()
    
def set_doc_type_rules(doc_type: str, rules_names: list[str], rules_queries:list[str]):
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()
    cursor.execute('''DELETE FROM doc_type_rules WHERE doc_type = ?''', (doc_type, ))
    
    cursor.execute('''INSERT INTO doc_type_rules (doc_type, rules_names, rules_queries)
                      VALUES (?, ?, ?)''', (doc_type, "\n".join(rules_names), "\n".join(rules_queries)))
    connection.commit()
    connection.close()

def update_knowledge_with_parsed_document(parsed_document: dict):
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()
    
    _add_organization('00083262', 'ОАО "Никелин"', cursor)
         
    connection.commit()
    connection.close()

def check_parsed_document(parsed_document: dict):
    # create table 1 - now pass
    # create table 2 - now pass
    doc_id = 0 # TODO
    doc_path = "/0.pdf" # TODO - принять автоматически
    doc_okud = "0315006" # TODO - принять автоматически
    doc_type = "М-11" # тут русская раскладка # TODO - принять автоматически
    
    # documents - write type
    
    doc_status = "В очереди на проверку"
    
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()
    
    _add_document_id(doc_id, doc_type, cursor)
    
    reason = ""
    res = True
    
    if doc_type == "М-11":
    
        bill_num = parsed_document["номер накладной"]
        
        # organization
        organization_okpo = parsed_document['код по ОКПО']
        organization_name = parsed_document['организация']
        organization_id = _get_organization_id(organization_okpo, organization_name, cursor) # may be none
        
        rules_mistake_messages, rules_queries = _get_ruleset(doc_type, cursor)
        
        # subdivision
        subdivision_be = parsed_document['код БЕ']
        subdivision_name = parsed_document['структурное подразделение']
        
        date = parsed_document['дата составления']
        
        
        for idx in range(len(rules_mistake_messages)):
            if not eval(rules_queries[idx]): # тут будет добавлена защита от sql-инъекций
                res = False
                reason += rules_mistake_messages[idx]
            
    elif doc_type == "ФМУ-76":
        pass
    else:
        raise ValueError("Неверный тип документа. Это должно детектироваться на стадии предобработки")
         
    connection.commit()
    connection.close()
    
    with open("accept.txt", "w") as file:
        if res:
            file.write("Документ корректен")
        else:
            file.write("Документ некорректен")
            
    with open("log.txt", "w") as file:
        file.write(reason)
        
    with open("statistics.txt", "w") as file:
        file.write("Пока без статистик")

def _get_ruleset(doc_type: str, cursor) -> tuple[list[str], list[str]]:
    cursor.execute('''SELECT rules_names, rules_queries FROM doc_type_rules WHERE doc_type = ?''', (doc_type,))
    result = cursor.fetchone()
    rules_texts = result[0].split('\n')
    rules_queries = result[1].split('\n')
    return rules_texts, rules_queries
                  
def _add_document_id(doc_id: int, doc_type: str, cursor):
    cursor.execute('''INSERT INTO documents (doc_id, doc_type)
                  VALUES (?, ?)''', (doc_id, doc_type))

def _check_organization(organization_okpo: str, organization_name: str, cursor):
    cursor.execute('''SELECT EXISTS(SELECT 1 FROM organizations WHERE OKPO_code = ? AND name = ?)''', (organization_okpo, organization_name))
    result = cursor.fetchone()[0]
    return result is not None

def _get_organization_id(organization_okpo: str, organization_name: str, cursor):
    cursor.execute('''SELECT organization_id FROM organizations WHERE OKPO_code = ? AND name = ?;''', (organization_okpo, organization_name))
    result = cursor.fetchone()[0]
    return result

def _check_subdivision(organization_id: int, subdivision_be: str, subdivision_name: str, cursor):
    cursor.execute('''SELECT EXISTS(SELECT 1 FROM subdivisions_org_''' + str(organization_id) + ''' WHERE be_code = ? AND name = ?)''', (subdivision_be, subdivision_name))
    result = cursor.fetchone()[0]
    return result is not None

### update functionality

def _add_organization(organization_okpo: str, organization_name: str, cursor):
    cursor.execute('''SELECT COUNT(*) FROM organizations''')
    new_id = cursor.fetchone()[0]
    
    cursor.execute('''INSERT INTO organizations (organization_id, OKPO_code, name) VALUES (?, ?, ?)''', (new_id, organization_okpo, organization_name))
    
    organization_id = _get_organization_id(organization_okpo, organization_name, cursor)
    
    # and add subdivisions table
    _init_subdivision_table(organization_id, cursor)
    
def _init_subdivision_table(organization_id: int, cursor):
    cursor.execute('''CREATE TABLE subdivisions_org_''' + str(organization_id) + '''
                 (
                 subdivison_id INTEGER PRIMARY KEY,
                 be_code TEXT,
                 name TEXT
                 );''')

def check_dir() -> None:
    for file in os.listdir(INPUT_DIR):
        with open(os.path.join(INPUT_DIR, file),'r') as f:
            s = f.read()
            s = s.replace("\\xa0", "")
            s = s.replace("'","\\xa0")
            s = s.replace('"',"'")
            s = s.replace("\\xa0", '"')
            data = ast.literal_eval(s)
            check_parsed_document(data)
