import os
import sqlite3
import typing as tp
from collections import defaultdict

DB_NAME = "mydatabase.db"
INPUT_DIR = "../Dicts_in"


def init_db():
    conn = sqlite3.connect(DB_NAME)

    conn.execute(
        """CREATE TABLE documents
                (
                doc_id INTEGER PRIMARY KEY,
                doc_type TEXT
                );"""
    )

    conn.execute(
        """CREATE TABLE doc_form_types
                (
                doc_type TEXT PRIMARY KEY,
                OKUD_code TEXT
                );"""
    )

    conn.execute(
        """CREATE TABLE documents_type_M11
                 (
                 doc_id INTEGER PRIMARY KEY,
                 doc_path TEXT,
                 doc_status TEXT,
                 stat_id INTEGER,
                 bill_num INTEGER,
                 organization_id INTEGER,
                 subdivision_id INTEGER,
                 date TEXT,
                 table1_id INTEGER,
                 table2_id INTEGER,
                 sender_id INTEGER,
                 receiver_id INTEGER,
                 mediator_id INTEGER,
                 table_signatures INTEGER
                 );"""
    )

    # TODO - create 76 - type documents table!!!

    conn.execute(
        """CREATE TABLE organizations
                 (
                 organization_id INTEGER PRIMARY KEY,
                 OKPO_code TEXT,
                 name TEXT
                 );"""
    )

    conn.execute(
        """CREATE TABLE doc_type_rules
                 (
                 doc_type TEXT PRIMARY KEY,
                 rules_names TEXT,
                 rules_queries TEXT
                 );"""
    )

    conn.execute(
        """CREATE TABLE queue_preproc
                 (doc_id INTEGER PRIMARY KEY);"""
    )

    conn.execute(
        """CREATE TABLE queue_inproc
                 (doc_id INTEGER PRIMARY KEY);"""
    )

    conn.execute(
        """CREATE TABLE table1_sender_receiver
                 (
                 table1_id INTEGER PRIMARY KEY,
                 entry_num INTEGER,
                 organization_id INTEGER,
                 date TEXT,
                 operation_code INTEGER,
                 sender_subd_id INTEGER,
                 sender_plant_id INTEGER,
                 receiver_subd_id INTEGER,
                 receiver_plant_id INTEGER,
                 account_id INTEGER,
                 output_normal_unit INTEGER
                 );"""
    )

    conn.execute(
        """CREATE TABLE table2_goods_enumeration
                 (
                 table2_id INTEGER PRIMARY KEY,
                 entry_num INTEGER,
                 organization_id INTEGER,
                 date TEXT,
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
                 reg_party_num INTEGER
                 );"""
    )

    conn.execute(
        """CREATE TABLE table_signatures
                 (tablesign_id INTEGER PRIMARY KEY,
                  organization_id INTEGER,
                  official_id INTEGER,
                  certificate_id INTEGER,
                  signing_date DATE
                 );"""
    )

    conn.execute(
        """CREATE TABLE material_assets
                 (material_nom_id INTEGER PRIMARY KEY,
                  name TEXT
                 );"""
    )

    conn.execute(
        """CREATE TABLE account
                 (account_id INTEGER PRIMARY KEY,
                  account INTEGER,
                  analythic_account_id INTEGER
                 );"""
    )

    conn.execute(
        """CREATE TABLE measurement_units
                 (measurement_unit_id INTEGER PRIMARY KEY,
                  name TEXT
                 );"""
    )

    conn.execute(
        """CREATE TABLE stats
                 (doc_id INTEGER PRIMARY KEY,
                  doc_type TEXT,
                  date TEXT,
                  preproc_status TEXT,
                  log_reject TEXT
                 );"""
    )

    conn.commit()
    conn.close()


def set_doc_type_rules(doc_type: str, rules_names: list[str], rules_queries: list[str]):
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()
    cursor.execute("""DELETE FROM doc_type_rules WHERE doc_type = ?""", (doc_type,))

    cursor.execute(
        """INSERT INTO doc_type_rules (doc_type, rules_names, rules_queries)
                      VALUES (?, ?, ?)""",
        (doc_type, "\n".join(rules_names), "\n".join(rules_queries)),
    )
    connection.commit()
    connection.close()


def _process_subd_plant_data(subd_plant_strs: list[str], cfg: dict[str, tp.Any], table_prefix: str, cursor) -> None:
    subd_strs = []
    plant_strs = []
    table_subd_id_key = f"{table_prefix}_subd_id"
    table_plant_id_key = f"{table_prefix}_plant_id"

    for s_p_s in subd_plant_strs:
        digit_indices = [i for i in range(len(s_p_s)) if s_p_s[i].isdigit()]
        last_digit_index = digit_indices[-1]
        offset = 0

        while digit_indices[-2 - offset] == digit_indices[-1 - offset] - 1:
            offset += 1

        subd_str = s_p_s[:last_digit_index - offset]
        plant_str = s_p_s[last_digit_index - offset:]

        subd_strs.append(subd_str)
        plant_strs.append(plant_str)

        subd_be = subd_str[:4]
        subd_name = subd_str[5:]
        plant_name = plant_str

        if cfg["doc_organization_id"] is not None:
            subd_id = _get_subdivision_id(cfg["doc_organization_id"], subd_be, subd_name, cursor)
            cfg[table_subd_id_key].append(subd_id)
        else:
            cfg[table_subd_id_key].append(None)

        if cfg[table_subd_id_key][-1] is not None:
            plant_id = _get_plant_id(cfg["doc_organization_id"], cfg[table_subd_id_key][-1], plant_name, cursor)
            cfg[table_plant_id_key].append(plant_id)
        else:
            cfg[table_plant_id_key].append(None)


def _get_inner_representation(parsed_document: dict[str, list[str] | str], cursor) -> dict:
    parsed_document = defaultdict(str, parsed_document)
    cfg = defaultdict(str)
    cfg["doc_doc_id"] = _get_doccount(cursor)

    cfg["doc_doc_type"] = parsed_document["Тип формы"][0]

    if cfg["doc_doc_type"] == "М_11":  # русская раскладка
        cfg["doc_bill_num"] = parsed_document["ТРЕБОВАНИЕ-НАКЛАДНАЯ №"][0]
        cfg["doc_doc_status"] = "В очереди на проверку"
        cfg["doc_date"] = parsed_document["дата составления"][0]

        cfg["org_name"] = parsed_document["Организация"][0]
        cfg["org_OKPO_code"] = parsed_document["код по ОКПО"][0]

        cfg["doc_demander_str"] = parsed_document["Затребовал "][0]
        cfg["doc_permitter_str"] = parsed_document["Разрешил"][0]
        cfg["doc_mediator_str"] = parsed_document["Через кого "][0]

        cfg["sub_be_code"] = parsed_document["БЕ"][0]
        cfg["sub_name"] = parsed_document["Структурное подразделение"][0]

        cfg["doc_organization_id"] = _get_organization_id(
            cfg["org_OKPO_code"], cfg["org_name"], cursor
        )
        if cfg["doc_organization_id"] is not None:
            cfg["doc_subdivision_id"] = _get_subdivision_id(
                cfg["doc_organization_id"], cfg["sub_be_code"], cfg["sub_name"], cursor
            )
        else:
            cfg["doc_subdivision_id"] = None

        subd_plant_strs = parsed_document["структурное подразделение отправителя"]
        _process_subd_plant_data(subd_plant_strs, cfg, "table1_sender", cursor)

        subd_plant_strs = parsed_document["структурное подразделение получателя"]
        _process_subd_plant_data(subd_plant_strs, cfg, "table1_receiver", cursor)

        cfg["table1_operation_code"] = parsed_document["код вида операции"]

    return cfg


def update_knowledge_with_parsed_document(parsed_document: dict):
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()

    cfg = _get_inner_representation(parsed_document, cursor)

    # mandatory
    _add_document_id(cfg["doc_doc_id"], cfg["doc_doc_type"], cursor)
    connection.commit()

    if cfg["doc_organization_id"] is None:
        _add_organization(cfg["org_OKPO_code"], cfg["org_name"], cursor)
        cfg["doc_organization_id"] = _get_organization_id(cfg["org_OKPO_code"], cfg["org_name"], cursor)

    connection.commit()

    if cfg["doc_subdivision_id"] is None:
        _add_subdivision(
            cfg["doc_organization_id"], cfg["sub_be_code"], cfg["sub_name"], cursor
        )
        cfg["doc_subdivision_id"] = _get_subdivision_id(
            cfg["doc_organization_id"], cfg["sub_be_code"], cfg["sub_name"], cursor
        )
    connection.commit()

    if cfg["doc_plant_id"] is None:
        _add_plant(
            cfg["doc_organization_id"], cfg["doc_subdivision_id"], "TODO - имя цеха", cursor
        )
        cfg["doc_plant_id"] = _get_plant_id(
            cfg["doc_organization_id"], cfg["doc_subdivision_id"], "TODO - имя цеха", cursor
        )

    connection.commit()

    cursor.close()
    connection.commit()
    connection.close()


def check_parsed_document(parsed_document: dict):
    # create table 1 - now pass
    # create table 2 - now pass
    # doc_path = "/0.pdf" # TODO - принять автоматически
    # doc_okud = "0315006" # TODO - принять автоматически
    # doc_type = "М-11" # тут русская раскладка # TODO - принять автоматически

    # documents - write type

    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()

    cfg = _get_inner_repres(parsed_document, cursor)

    # mandatory
    _add_document_id(cfg["doc_doc_id"], cfg["doc_doc_type"], cursor)
    connection.commit()

    log_reject = ""
    res = True

    rules_mistake_messages, rules_queries = _get_ruleset(cfg["doc_doc_type"], cursor)

    for idx in range(len(rules_mistake_messages)):
        if not eval(rules_queries[idx]):  # тут будет добавлена защита от sql-инъекций
            res = False
            log_reject += rules_mistake_messages[idx]
            log_reject += "\n"

    _dump_log_reject_to_stats(
        cfg["doc_doc_id"],
        cfg["doc_doc_type"],
        cfg["doc_date"],
        "Parsed",
        log_reject,
        cursor,
    )

    connection.commit()
    cursor.close()
    connection.close()

    with open("accept.txt", "a") as file:
        if res:
            file.write("Документ корректен")
        else:
            file.write("Документ некорректен")

    with open("log.txt", "a") as file:
        file.write(log_reject)

    with open("statistics.txt", "a") as file:
        file.write("Пока без статистик")


def _get_ruleset(doc_type: str, cursor) -> tuple[list[str], list[str]]:
    cursor.execute(
        """SELECT rules_names, rules_queries FROM doc_type_rules WHERE doc_type = ?""",
        (doc_type,),
    )
    result = cursor.fetchone()
    rules_texts = result[0].split("\n")
    rules_queries = result[1].split("\n")
    return rules_texts, rules_queries


def _check_organization(organization_okpo: str, organization_name: str, cursor):
    cursor.execute(
        """SELECT EXISTS(SELECT 1 FROM organizations WHERE OKPO_code = ? AND name = ?)""",
        (organization_okpo, organization_name),
    )
    result = cursor.fetchone()[0]
    return result is not None


def _dump_log_reject_to_stats(
    doc_id: int, doc_type: str, date: str, preproc_status: str, log_reject: str, cursor
):
    cursor.execute(
        """INSERT INTO stats (doc_id, doc_type, date, preproc_status, log_reject)
                      VALUES (?, ?, ?, ?, ?)""",
        (doc_id, doc_type, date, preproc_status, log_reject),
    )


### getters


def _get_doccount(cursor):
    cursor.execute("""SELECT COUNT(*) FROM documents""")
    doccount = cursor.fetchone()[0]
    return doccount


def _get_organization_id(organization_okpo: str, organization_name: str, cursor):
    cursor.execute(
        """SELECT organization_id FROM organizations WHERE OKPO_code = ? AND name = ?;""",
        (organization_okpo, organization_name),
    )
    result = cursor.fetchone()
    if result is None:
        return None
    return result[0]


def _get_subdivision_id(
    organization_id: int, subdivision_be: str, subdivision_name: str, cursor
):
    # print("_get_subdivision_id", organization_id, subdivision_be, subdivision_name)
    cursor.execute(
        """SELECT subdivision_id FROM subdivisions_org_"""
        + str(organization_id)
        + """ WHERE be_code = ? AND name = ?;""",
        (subdivision_be, subdivision_name),
    )
    result = cursor.fetchone()
    if result is None:
        return None
    return result[0]


def _get_plant_id(organization_id: int, subdivision_id: int, plant_name: str, cursor):
    cursor.execute(
        """SELECT plant_id FROM plants_org_"""
        + str(organization_id)
        + """_subd_"""
        + str(subdivision_id)
        + """ WHERE name = ?;""",
        (plant_name,),
    )
    result = cursor.fetchone()
    if result is None:
        return None
    return result[0]


### misc


def _parse_person_string(person_string: str) -> tuple[str, str]:
    word_list = person_string.split(" ")
    if len(word_list) == 0:
        return "НЕВАЛИДНАЯ ДОЛЖНОСТЬ", "НЕВАЛИДНОЕ ФИО"
    person_post = ""
    person_name = ""
    fio_words = 0
    if word_list[-1].count(".") == 2:
        # имеем дело с инициалами слипшимися
        fio_words = 2
        person_name = word_list[-2] + " " + word_list[-1]
    else:
        # ФИО как три строки
        fio_words = 3
        person_name = " ".join(word_list[-3:])
    if fio_words == len(word_list):
        # не указана должность
        return "НЕВАЛИДНАЯ ДОЛЖНОСТЬ", "НЕВАЛИДНОЕ ФИО"
    else:
        person_post = " ".join(word_list[:-fio_words])

    return person_post, person_name


def get_statistics(rules_incorrect_msges: list[str]) -> dict:
    statistics = defaultdict(int)
    for rules in rules_incorrect_msges:
        violated_rules = rules.split("\n")
        for rule in violated_rules:
            statistics[rule] += 1
    return statistics


### checkers


def _check_subdivision(
    organization_id: int, subdivision_be: str, subdivision_name: str, cursor
):
    cursor.execute(
        """SELECT EXISTS(SELECT 1 FROM subdivisions_org_"""
        + str(organization_id)
        + """ WHERE be_code = ? AND name = ?)""",
        (subdivision_be, subdivision_name),
    )
    result = cursor.fetchone()[0]
    return result is not None


def _check_person_in_plant(
    organization_id: int, subdivision_id: int, plant_id: int, person_str: str, cursor
):
    return False


### mandatory update


def _add_document_id(doc_id: int, doc_type: str, cursor):
    cursor.execute(
        """INSERT INTO documents (doc_id, doc_type)
                  VALUES (?, ?)""",
        (doc_id, doc_type),
    )


### update functionality


def _add_organization(organization_okpo: str, organization_name: str, cursor):
    cursor.execute("""SELECT COUNT(*) FROM organizations""")
    new_id = cursor.fetchone()[0]

    cursor.execute(
        """INSERT INTO organizations (organization_id, OKPO_code, name) VALUES (?, ?, ?)""",
        (new_id, organization_okpo, organization_name),
    )

    # and add subdivisions table for organization
    _init_subdivision_table(new_id, cursor)


def _add_subdivision(
    organization_id: int, subdivision_be: str, subdivision_name: str, cursor
):
    cursor.execute("""SELECT COUNT(*) FROM subdivisions_org_""" + str(organization_id))
    new_id = cursor.fetchone()[0]

    cursor.execute(
        """INSERT INTO subdivisions_org_"""
        + str(organization_id)
        + """(subdivision_id, be_code, name) VALUES (?, ?, ?)""",
        (new_id, subdivision_be, subdivision_name),
    )

    # and add plants table for subdivision
    _init_plant_table(organization_id, new_id, cursor)


def _add_plant(organization_id: int, subdivision_id: int, plant_name: str, cursor):
    cursor.execute(
        """SELECT COUNT(*) FROM plants_org_"""
        + str(organization_id)
        + """_subd_"""
        + str(subdivision_id)
    )
    new_id = cursor.fetchone()[0]

    cursor.execute(
        """INSERT INTO plants_org_"""
        + str(organization_id)
        + """_subd_"""
        + str(subdivision_id)
        + """(plant_id, name) VALUES (?, ?)""",
        (new_id, plant_name),
    )


def _init_subdivision_table(organization_id: int, cursor):
    # print("_init_subdivision_table", organization_id)
    cursor.execute(
        """CREATE TABLE subdivisions_org_"""
        + str(organization_id)
        + """
                 (
                 subdivision_id INTEGER PRIMARY KEY,
                 be_code TEXT,
                 name TEXT
                 );"""
    )


def _init_plant_table(organization_id: int, subdivision_id: int, cursor):
    # print("_init_plant_table", organization_id, subdivision_id)
    cursor.execute(
        """CREATE TABLE plants_org_"""
        + str(organization_id)
        + """_subd_"""
        + str(subdivision_id)
        + """
                 (
                 plant_id INTEGER PRIMARY KEY,
                 name TEXT
                 );"""
    )


### repr


def repr_table(table_name: str, cursor):
    cursor.execute("SELECT * FROM " + table_name)
    # Fetch all the rows as a list of tuples
    rows = cursor.fetchall()
    # Print the rows in a human-readable format
    for row in rows:
        print(row)


def repr_db():
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    for table in tables:
        print(table[0])
        repr_table(table[0], cursor)

    cursor.close()
    connection.close()


### main


def check_dir():
    try:
        os.remove("accept.txt")
    except FileNotFoundError:
        pass
    try:
        os.remove("log.txt")
    except FileNotFoundError:
        pass
    try:
        os.remove("statistics.txt")
    except FileNotFoundError:
        pass
    for file in os.listdir(INPUT_DIR):
        with open(os.path.join(INPUT_DIR, file), "r") as f:
            s = f.read()
            # s = s.replace("\\xa0", "")
            # s = s.replace("'","\\xa0")
            # s = s.replace('"',"'")
            # s = s.replace("\\xa0", '"')
            s = s.replace("""<class "list">""", "list")
            s = s.replace("""<class 'list'>""", "list")
            if s != "Parsing failed":
                data = eval(s)
                check_parsed_document(data)


def update_dir():
    try:
        os.remove("accept.txt")
    except FileNotFoundError:
        pass
    try:
        os.remove("log.txt")
    except FileNotFoundError:
        pass
    try:
        os.remove("statistics.txt")
    except FileNotFoundError:
        pass
    for file in os.listdir(INPUT_DIR):
        with open(os.path.join(INPUT_DIR, file), "r") as f:
            s = f.read()
            # s = s.replace("\\xa0", "")
            # s = s.replace("'","\\xa0")
            # s = s.replace('"',"'")
            # s = s.replace("\\xa0", '"')
            s = s.replace("""<class "list">""", "list")
            s = s.replace("""<class 'list'>""", "list")
            if s != "Parsing failed":
                # print(s)
                data = eval(s)
                update_knowledge_with_parsed_document(data)
