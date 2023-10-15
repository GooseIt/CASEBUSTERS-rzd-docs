import re
from collections import defaultdict

import fitz


template_line_tables = [
    [[{"text": "Коды"}],
     [{"text": "[\S\s]*", "save": "код по ОКУД"}],
     [{"text": "[\S\s]*", "save": "код по ОКПО"}],
     [{"text": "[\S\s]*", "save": "БЕ"}]],
    [
        [
                {"text": "Дата\nсоста\-\nвления"},
                {"text": "Код вида\nоперации"},
                {"text": "Отправитель"},
                {"text": None},
                {"text": "Получатель"},
                {"text": None},
                {"text": "Корреспондирующий счет"},
                {"text": None},
                {"text": "Учетная\nединица\nвыпуска\nпродукции\n\(работ,\nуслуг\)"}
        ],
        [
                {"text": None},
                {"text": None},
                {"text": "структурное\nподразделение"},
                {"text": "табельный\nномер\nМОЛ \(ЛОС\)"},
                {"text": "структурное\nподразделение"},
                {"text": "табельный\nномер\nМОЛ \(ЛОС\)"},
                {"text": "счет, субсчет"},
                {"text": "код аналити\-\nческого учета"},
                {"text": None}
        ],
        [
                {"text": "[\S\s]*",
                        "save": "дата составления"},
                {"text": "[\S\s]*",
                        "save": "код вида операции"},
                {"text": "[\S\s]*",
                        "save": "структурное подразделение отправителя"},
                {"text": "[\S\s]*",
                        "save": "табельный номер отправителя"},
                {"text": "[\S\s]*",
                        "save": "структурное подразделение получателя"},
                {"text": "[\S\s]*",
                        "save": "табельный номер получателя"},
                {"text": "[\S\s]*",
                        "save": "корреспондирующий счет -> счет, субсчет"},
                {"text": "[\S\s]*",
                        "save": "корреспондирующий счет -> код аналитического учета"},
                {"text": "[\S\s]*",
                        "save": "Учетная единица выпуска продукции (работ, услуг)"}
        ],
    ],
    [
        [
                {"text": "Коррес\-\nпонди\-\nрующий\nсчет"},
                {"text": "Материальные\nценности"},
                {"text": None},
                {"text": "Харак\-\nтеристи\-\nка"},
                {"text": "Завод\-\nской\nномер"},
                {"text": "Инвен\-\nтарный\nномер"},
                {"text": "Сетевой\nномер"},
                {"text": "Единица\nизмерения"},
                {"text": None},
                {"text": "Количество"},
                {"text": None},
                {"text": "Цена,\nруб\.\nкоп\."},
                {"text": "Сумма\nбез\nучета\nНДС,\nруб\. коп\."},
                {"text": "Порядко\-\nвый\nномер по\nсклад\-\nской\nкартотеке"},
                {"text": "Место\-\nнахож\-\nдение"},
                {"text": "Регистра\-\nционный\nномер\nпартии\nтовара,\nподлежа\-\nщего\nпрослежи\-\nваемости"}
        ],
        [
                {"text": None},
                {"text": "наиме\-\nнование"},
                {"text": "номенк\-\nлатурный\nномер"},
                {"text": None},
                {"text": None},
                {"text": None},
                {"text": None},
                {"text": "код"},
                {"text": "наиме\-\nнова\-\nние"},
                {"text": "затре\-\nбова\-\nно"},
                {"text": "отпу\-\nщено"},
                {"text": None},
                {"text": None},
                {"text": None},
                {"text": None},
                {"text": None}
        ],
        [
                {"text": "1"},
                {"text": "2"},
                {"text": "3"},
                {"text": "4"},
                {"text": "5"},
                {"text": "6"},
                {"text": "7"},
                {"text": "8"},
                {"text": "9"},
                {"text": "10"},
                {"text": "11"},
                {"text": "12"},
                {"text": "13"},
                {"text": "14"},
                {"text": "15"},
                {"text": "16"}
        ],
        [
                {"text": "[\S\s]*",
                 "save": "корреспондирующий счет (вторая таблица)"},
                {"text": "[\S\s]*",
                "save": "материальные ценности -> наименование"},
                {"text": "[\S\s]*"},
                {"text": "[\S\s]*",
                        "save": "материальные ценности -> номенклатурный номер"},
                {"text": "[\S\s]*",
                        "save": "характеристика"},
                {"text": "[\S\s]*",
                        "save": "заводской номер"},
                {"text": "[\S\s]*",
                        "save": "инвентарный номер"},
                {"text": "[\S\s]*",
                        "save": "сетевой номер"},
                {"text": "[\S\s]*",
                        "save": "единица измерения -> код"},
                {"text": "[\S\s]*",
                        "save": "единица измерения -> наименование"},
                {"text": "[\S\s]*",
                        "save": "количество -> затребовано"},
                {"text": "[\S\s]*",
                        "save": "количество -> отпущено"},
                {"text": "[\S\s]*",
                        "save": "цена, руб. коп."},
                {"text": "[\S\s]*",
                        "save": "сумма без учета НДС, руб. коп"},
                {"text": "[\S\s]*",
                        "save": "порядковый номер по складской картотеке"},
                {"text": "[\S\s]*",
                        "save": "местонахождение"},
                {"text": "[\S\s]*",
                        "save": "Регистрационный номер партии товара, подлежащего прослеживаемости"}
        ],
    ],
]


def parse_M11(path: str) -> dict[str, list[str] | str]:
    my_dict: dict[str, list[str] | str] = defaultdict(list)
    text_array: list[str] = []
    with fitz.open(path) as doc:
        for table_index, (table, template_table) in enumerate(zip(doc[0].find_tables(), template_line_tables)):
            for line_index, line in enumerate(table.extract()):
                template_line = template_table[line_index] if line_index < len(template_table) else template_table[-1]
                for cell, template_cell in zip(line, template_line):
                    if (template_cell['text'] is None and cell is not None) or \
                        (template_cell['text'] is not None and cell is None) or \
                        (template_cell['text'] is not None and not re.fullmatch(template_cell['text'], cell)):
                        raise Exception('    incorrect cell text! "%s" doesn\'t match to "%s"', cell, template_cell['text'])
                    elif 'save' in template_cell:
                        if template_cell["save"] not in my_dict:
                            my_dict[template_cell["save"]] = [cell]
                        else:
                            my_dict[template_cell["save"]].append(cell)

        for page in doc:
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                for line in block["lines"]:
                    for span in line["spans"]:
                        text_array.append(span["text"])

    def check_string_format1(input_string):
        pattern = r"^[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+$"
        return bool(re.match(pattern, input_string))

    def check_string_format2(input_string):
        pattern = r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}|\d{2}\.\d{2}\.\d{4}\xa0\d{2}:\d{2}:\d{2}"
        return bool(re.match(pattern, input_string))

    data = {}
    data["расшифровки комиссии"] = []
    data["подписи комиссии"] = []
    data["должности комиссии"] = []
    data["организации электронной подписи"] = []
    data["подписанты электронной подписи"] = []
    data["сертификаты электронной подписи"] = []
    data["даты электронной подписи"] = []
    start_of_electronic_signatures = False
    for i in range(len(text_array)):
        text = text_array[i]
        if (
            text
            == "Сведения об электронных подписях, соответствующих файлу электронного документа"
        ):
            start_of_electronic_signatures = True
        if text == "Документ подписан электронной подписью":
            start_of_electronic_signatures = True
        if "ТРЕБОВАНИЕ-НАКЛАДНАЯ №" in text and (1 not in data.keys()):
            data[1] = text.split()[-1]
        if text == "Организация" and (2 not in data.keys()):
            data[2] = text_array[i + 1]
        if text == "подразделение" and (3 not in data.keys()):
            counter = i + 1
            data[3] = ""
            while not text_array[counter].isdigit():
                data[3] += text_array[counter] + " "
                counter += 1
        if (text == "Через кого " or text == "Через кого") and (13 not in data.keys()):
            counter = i + 1
            data[13] = ""
            while text_array[counter] != "Затребовал ":
                data[13] += text_array[counter] + " "
                counter += 1
        if text == "Затребовал " and (11 not in data.keys()):
            counter = i + 1
            data[11] = ""
            while text_array[counter] != "\xa0\xa0\xa0\xa0Разрешил":
                data[11] += text_array[counter] + " "
                counter += 1
        if text == "\xa0\xa0\xa0\xa0Разрешил" and (12 not in data.keys()):
            counter = i + 1
            data[12] = ""
            while text_array[counter] != "Материальные":
                data[12] += text_array[counter] + " "
                counter += 1
        if text == "Отпустил" and (25 not in data.keys()):
            counter = i + 1
            data[25] = ""
            while text_array[counter] != "(должность)":
                data[25] += text_array[counter] + " "
                counter += 1
        if text == "(должность)" and (23 not in data.keys()):
            counter = i + 1
            data[23] = ""
            while text_array[counter] != "(подпись)":
                data[23] += text_array[counter] + " "
                counter += 1
        if text == "(подпись)" and (26 not in data.keys()):
            counter = i + 1
            data[26] = ""
            while text_array[counter] != "(расшифровка подписи)":
                data[26] += text_array[counter] + " "
                counter += 1

        if start_of_electronic_signatures and (
            text == "Дата подписи" or text == "\xa0\xa0Дата подписи"
        ):
            counter = i + 1
            temp = ""
            while counter < len(text_array) and not check_string_format1(
                text_array[counter]
            ):
                temp += text_array[counter] + " "
                counter += 1
            if counter < len(text_array):
                data["организации электронной подписи"].append(temp)
                if check_string_format1(text_array[counter]):
                    data["подписанты электронной подписи"] = text_array[counter]

        if start_of_electronic_signatures and check_string_format2(text):
            counter = i + 1
            temp = ""
            while counter < len(text_array) and not check_string_format1(
                text_array[counter]
            ):
                temp += text_array[counter] + " "
                counter += 1
            if counter < len(text_array):
                data["организации электронной подписи"].append(temp)
                if check_string_format1(text_array[counter]):
                    data["подписанты электронной подписи"] = text_array[counter]

        if start_of_electronic_signatures and check_string_format2(text):
            data["даты электронной подписи"].append(text)

    my_dict["ТРЕБОВАНИЕ-НАКЛАДНАЯ №"].append(data[1])
    my_dict["Организация"].append(data[2])
    my_dict["Структурное подразделение"].append(data[3])
    my_dict["Через кого "].append(data[13])
    my_dict["Затребовал "].append(data[11])
    my_dict["Разрешил"].append(data[12])
    my_dict["отпустил -> должность"].append(data[25])
    my_dict["отпустил -> подпись"].append(data[23])
    my_dict["отпустил -> расшифровка подписи"].append(data[26])
    my_dict["даты электронной подписи"] = data["даты электронной подписи"]
    my_dict["организации электронной подписи"] = data["организации электронной подписи"]

    for key, value in my_dict.items():
        my_dict[key] = [x.replace("\n", "") for x in value]

    my_dict["Тип формы"] = ["М_11"] # русская раскладка
    return my_dict
