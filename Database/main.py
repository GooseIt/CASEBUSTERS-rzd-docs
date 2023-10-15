import mydb

import os # debug line
os.remove("mydatabase.db") # debug line

mydb.init_db()

input_example = eval("""{'номер накладной': 'ТРЕБОВАНИЕ-НАКЛАДНАЯ №\xa0 00010354', 'организация': 'ОАО "Никелин"', 'код по ОКПО': '00083262', 'структурное подразделение': 'Северо-Кавказская дирекция инфраструктуры - структурное подразделение', 'код БЕ': '5219', 'дата составления': '29.05.2023', 'код вида операции': '311', 'структурное подразделение отправителя': '2F30 ПЧ-16', 'табельный номер отправителя': '-', 'структурное подразделение получателя': '2F30 ПЧ-16', 'материальные ценности -> наименование': '-', 'характеристика': 'Материальные', 'заводской номер': '11320003', 'инвентарный номер': '60', 'сетевой номер': '-', 'единица измерения->код': '-', 'единица измерения->наименование': '-', 'количество->затребовано': '-', 'отпустил->должность': 'Начальник', 'получил->должность': 'Никифор Даниилович', 'получил->расшифровка подписи': '-', 'организация в подписи': 'ПЧ - 16 Махачкала (I группа) ПЧ -', 'фио подписант': 'Филиппов Сильвестр Гафарович', 'должность подписанта': 'Начальник дистанции пути', 'дата подписи': '\xa0\xa030.05.2023 15:10:22'}""")

M11_rules_text = [
    'Некорректны название или код ОКПО организации',
    'Некорректны название или код БЕ подразделения',
    'Не существует кода подразделения отправителя',
    'Не существует цеха отправителя',
    'Не существует кода подразделения получателя',
    'Не существует цеха получателя',
    'В цехе нет отпустившего работника',
    'В цехе нет затребовавшего работника',
    'В цехе нет работника, через кого прошло списание',
]

M11_rules_queries = [
    '''_check_organization(cfg['org_OKPO_code'], cfg['org_name'], cursor)''',
    '''_check_subdivision(cfg['doc_organization_id'], cfg['sub_be_code'], cfg['sub_name'], cursor)''',
    'True',
    'True',
    'True',
    'True',
    '''_check_person_in_plant(cfg['doc_organization_id'], cfg['doc_subdivision_id'], cfg['doc_plant_id'], cfg['doc_sender_str'], cursor)''',
    '''_check_person_in_plant(cfg['doc_organization_id'], cfg['doc_subdivision_id'], cfg['doc_plant_id'], cfg['doc_receiver_str'], cursor)''',
    '''_check_person_in_plant(cfg['doc_organization_id'], cfg['doc_subdivision_id'], cfg['doc_plant_id'], cfg['doc_mediator_str'], cursor)'''
]

FMU76_rules_text = ['']

FMU76_rules_queries = ['']

mydb.set_doc_type_rules("М_11", M11_rules_text, M11_rules_queries)
mydb.set_doc_type_rules("ФМУ_76", FMU76_rules_text, FMU76_rules_queries)

#mydb.update_knowledge_with_parsed_document({})

#mydb.check_parsed_document(input_example)

mydb.update_dir()
mydb.check_dir()

# mydb.repr_db()
