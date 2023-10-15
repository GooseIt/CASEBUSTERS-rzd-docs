import re
from collections import defaultdict

import fitz
import pdfplumber


def parse_FMU76(path: str):
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()
    my_dict = defaultdict(list)
    my_dict["код по ОКУД"] = tables[0][1]
    my_dict["код по ОКПО"] = tables[0][2]
    my_dict["БЕ"] = tables[0][3]
    my_dict["номер"].append(tables[1][1][0])
    my_dict["дата"].append(tables[1][1][1])

    for i in range(2, len(tables[2])):
        my_dict["Структурное подразделение (цех, участок и др.)"].append(
            tables[2][i][0]
        )
        my_dict["Код операции"].append(tables[2][i][1])
        my_dict["Корреспондирующий счет -> Счет, субсчет"].append(tables[2][i][2])
        my_dict["Корреспондирующий счет -> Статья расходов/носитель затрат"].append(
            tables[2][i][3]
        )
    for i in range(3, len(tables[3])):
        my_dict["Технический счет 32 'Затраты'"].append(tables[3][i][0])
        my_dict["Производстенный заказ"].append(tables[3][i][1])
        my_dict["Корреспондирующий счет -> счет,субсчет"].append(tables[3][i][2])
        my_dict["Корреспондирующий счет -> код аналитического учета"].append(
            tables[3][i][3]
        )
        my_dict["Материальные ценности -> наименование, сорт,размер, марка"].append(
            tables[3][i][4]
        )
        my_dict["Материальные ценности -> номенклатурный номер"].append(tables[3][i][5])
        my_dict["Заводской номер детали"].append(tables[3][i][6])
        my_dict["единица измерения -> код"].append(tables[3][i][7])
        my_dict["единица измерения -> наименование"].append(tables[3][i][8])
        my_dict["Нормативное количество"].append(tables[3][i][9])
        my_dict["Фактически израсходованно -> Количество"].append(tables[3][i][10])
        my_dict["Фактически израсходованно -> Цена, руб. коп"].append(tables[3][i][11])
        my_dict["Фактически израсходованно -> Сумма, руб. коп."].append(
            tables[3][i][12]
        )
        my_dict[
            "Отклонение фактического расхода от нормы ('-' экономия, '+' перерасход)"
        ].append(tables[3][i][13])
        my_dict["Вид работ или ремонта, содержание хозяйственной операции"].append(
            tables[3][i][14]
        )
        my_dict[
            "Срок полезного использования использования, причина отклонения в расходе и другое"
        ].append(tables[3][i][15])
        my_dict["Регистрационный номер партии товара, подлежащего прослежива"].append(
            tables[3][i][16]
        )
    for v in my_dict.values():
        for i in range(len(v)):
            v[i] = v[i].replace("\n", "")
    text_array = []

    doc = fitz.open("ФМУ-76/good/4.pdf")

    for page_num in range(doc.page_count):
        page = doc[page_num]
        blocks = page.get_text("dict")["blocks"]

        for block in blocks:
            for line in block["lines"]:
                for span in line["spans"]:
                    text = span["text"]
                    text_array.append(text)
    data = {}
    data["расшифровки комиссии"] = []
    data["подписи комиссии"] = []
    data["должности комиссии"] = []
    end_of_comission = False
    start_of_comission = False
    data["организации электронной подписи"] = []
    data["подписанты электронной подписи"] = []
    data["сертификаты электронной подписи"] = []
    data["даты электронной подписи"] = []

    def check_string_format1(input_string):
        pattern = r"^[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+$"
        return bool(re.match(pattern, input_string))

    def check_string_format2(input_string):
        pattern = r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}|\d{2}\.\d{2}\.\d{4}\xa0\d{2}:\d{2}:\d{2}"
        return bool(re.match(pattern, input_string))

    for i in range(len(text_array)):
        text = text_array[i]
        if (
            text == "Материально ответственное лицо"
            and "подпись председателя" in data.keys()
        ):
            end_of_comission = True

        if text == "Подписи лиц, составивших акт:":
            start_of_comission = True

        if "Сведения об электронных подписях, соответствующих файлу электронного документа":
            start_of_electronic_signatures = True

        if (
            text == "(подпись)" and ("расшифровка председателя" in data.keys())
        ) and not end_of_comission:
            counter = i + 1
            temp = ""
            while text_array[counter] != "(расшифровка подписи)":
                temp += text_array[counter] + " "
                counter += 1
            data["расшифровки комиссии"].append(temp)

        if (
            text == "(должность)" and ("подпись председателя" in data.keys())
        ) and not end_of_comission:
            counter = i + 1
            temp = ""
            while text_array[counter] != "(подпись)":
                temp += text_array[counter] + " "
                counter += 1
            data["подписи комиссии"].append(temp)

        if (
            (text == "(расшифровка подписи)" or text == "Члены комиссии:")
            and ("должность председателя" in data.keys())
        ) and not end_of_comission:
            counter = i + 1
            if (
                text_array[counter] != "Члены комиссии:"
                and text_array[counter] != "Материально ответственное лицо"
            ):
                temp = ""
                while text_array[counter] != "(должность)":
                    temp += text_array[counter] + " "
                    counter += 1
                data["должности комиссии"].append(temp)

        if text == "0315835" and (3 not in data.keys()):
            data[3] = text_array[i + 1]

        if text == "организация" and (4 not in data.keys()):
            counter = i + 1
            data[4] = ""
            while text_array[counter] != "БЕ\xa0":
                data[4] += text_array[counter] + " "
                counter += 1

        if text == "ответственное лицо" and (7 not in data.keys()):
            counter = i + 1
            data[7] = ""
            while text_array[counter] != "Направление расхода":
                data[7] += text_array[counter] + " "
                counter += 1

        if text == "Направление расхода" and (8 not in data.keys()):
            counter = i + 1
            data[8] = ""
            while (
                text_array[counter]
                != "Инвентарный номер ремонтируемого основного средства"
            ):
                data[8] += text_array[counter] + " "
                counter += 1

        if text == "Инвентарный номер ремонтируемого основного средства" and (
            9 not in data.keys()
        ):
            counter = i + 1
            data[9] = ""
            while text_array[counter] != "Комиссия в составе:":
                data[9] += text_array[counter] + " "
                counter += 1

        if text == "Комиссия в составе:" and (10 not in data.keys()):
            counter = i + 1
            data[10] = ""
            while (
                text_array[counter]
                != "составила настоящий акт о том, что при производстве работ (услуг) израсходованы материалы (введены в эксплуатацию)"
            ):
                data[10] += text_array[counter] + " "
                counter += 1

        if (
            text == "Председатель комиссии"
            and ("должность председателя" not in data.keys())
        ) and start_of_comission:
            counter = i + 1
            data["должность председателя"] = ""
            while text_array[counter] != "(должность)":
                data["должность председателя"] += text_array[counter] + " "
                counter += 1

        if (
            text == "(должность)" and ("подпись председателя" not in data.keys())
        ) and start_of_comission:
            counter = i + 1
            data["подпись председателя"] = ""
            while text_array[counter] != "(подпись)":
                data["подпись председателя"] += text_array[counter] + " "
                counter += 1

        if (
            text == "(подпись)" and ("расшифровка председателя" not in data.keys())
        ) and start_of_comission:
            counter = i + 1
            data["расшифровка председателя"] = ""
            while text_array[counter] != "(расшифровка подписи)":
                data["расшифровка председателя"] += text_array[counter] + " "
                counter += 1

        if text == "(подпись)" and end_of_comission:
            counter = i + 1
            data["расшифровка материально ответственного"] = ""
            while text_array[counter] != "(расшифровка подписи)":
                data["расшифровка материально ответственного"] += (
                    text_array[counter] + " "
                )
                counter += 1

        if text == "(должность)" and end_of_comission:
            counter = i + 1
            data["подпись материально ответственного"] = ""
            while text_array[counter] != "(подпись)":
                data["подпись материально ответственного"] += text_array[counter] + " "
                counter += 1

        if text == "Материально ответственное лицо" and end_of_comission:
            counter = i + 1
            data["должность материально ответственного"] = ""
            while text_array[counter] != "(должность)":
                data["должность материально ответственного"] += (
                    text_array[counter] + " "
                )
                counter += 1

        if start_of_electronic_signatures and text == "Дата подписи":
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

    my_dict["расшифровки комиссии"] = data["расшифровки комиссии"]
    my_dict["подписи комиссии"] = data["подписи комиссии"]
    my_dict["должности комиссии"] = data["должности комиссии"]
    my_dict["организации электронной подписи"] = data["организации электронной подписи"]
    my_dict["даты электронной подписи"] = data["даты электронной подписи"]
    my_dict["организация"] = data[3]
    my_dict["структурное подразделение"] = data[4]
    my_dict["материальное ответственное лицо"] = data[7]
    my_dict["направление расхода"] = data[8]
    my_dict["Инвентарный номер ремонтируемого основного средства"] = data[9]
    my_dict["Комиссия в составе"] = data[10]

    for key, value in my_dict.items():
        my_dict[key] = [x.replace("\n", "") for x in value]

    my_dict["Тип формы"] = ["ФМУ_76"] # русская раскладка
    return my_dict
