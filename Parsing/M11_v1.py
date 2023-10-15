import pdfplumber
import pandas as pd
import numpy as np
import fitz 
from collections import defaultdict

def parse_M11(path):
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()
    my_dict = defaultdict(list)
    my_dict["код по ОКУД"] = tables[0][1]
    my_dict["код по ОКПО"] = tables[0][2]
    my_dict["БЕ"] = tables[0][3]
    for i in range(2, len(tables[1])):
        my_dict["дата составления"].append(tables[1][i][0])
        my_dict["код вида операции"].append(tables[1][i][1])
        my_dict["структурное подразделение отправителя"].append(tables[1][i][2])
        my_dict["табельный номер отправителя"].append(tables[1][i][3])
        my_dict["структурное подразделение получателя"].append(tables[1][i][4])
        my_dict["табельный номер получателя"].append(tables[1][i][5])
        my_dict["корреспондирующий счет -> счет, субсчет"].append(tables[1][i][6])
        my_dict["корреспондирующий счет -> код аналитического учета"].append(tables[1][i][7])
        my_dict["Учетная единица выпуска продукции (работ, услуг)"].append(tables[1][i][8])
    for i in range(3, len(tables[2])):
        my_dict["корреспондирующий счет"].append(tables[2][i][0])
        my_dict["материальные ценности -> наименование"].append(tables[2][i][1])
        my_dict["материальные ценности -> номенклатурный номер"].append(tables[2][i][2])
        my_dict["характеристика"].append(tables[2][i][3])
        my_dict["заводской номер"].append(tables[2][i][4])
        my_dict["инвертарный номер"].append(tables[2][i][5])
        my_dict["сетевой номер"].append(tables[2][i][6])
        my_dict["единица измерения -> код"].append(tables[2][i][7])
        my_dict["единица измерения -> наименование"].append(tables[2][i][8])
        my_dict["количество -> затребовано"].append(tables[2][i][9])
        my_dict["количество -> отпущено"].append(tables[2][i][10])
        my_dict["цена, руб. коп."].append(tables[2][i][11])
        my_dict["сумма без учета НДС, руб. коп"].append(tables[2][i][12])
        my_dict["порядковый номер по складской картотеке"].append(tables[2][i][13])
        my_dict["местонахождение"].append(tables[2][i][14])
        my_dict["Регистрационный номер партии товара, подлежащего прослеживаемости"].append(tables[2][i][15])
    pdf_document = fitz.open(path)
    text_array = []
    for page in pdf_document:
        blocks = page.get_text('dict')['blocks']
        for block_index, block in enumerate(blocks):
            for line in block['lines']:
                for span in line['spans']:
                    text_array.append(span['text'])
    data = {}
    for i in range(len(text_array)):
        text = text_array[i]
        if ("ТРЕБОВАНИЕ-НАКЛАДНАЯ №" in text and (1 not in data.keys())):
            data[1] = text.split()[-1]
        if (text == 'Организация' and (2 not in data.keys())):
            data[2] = text_array[i+1]
        if (text == 'подразделение' and (3 not in data.keys())):
            counter = i+1
            data[3] = ""
            while not text_array[counter].isdigit():
                data[3] += text_array[counter] + ' '
                counter+=1
        if (text == 'Через кого ' and (13 not in data.keys())):
            counter = i+1
            data[13] = ""
            while (text_array[counter] != 'Затребовал '):
                data[13] += text_array[counter] + ' '
                counter+=1
        if (text == 'Затребовал ' and (11 not in data.keys())):
            counter = i+1
            data[11] = ""
            while(text_array[counter] != '\xa0\xa0\xa0\xa0Разрешил'):
                data[11] += text_array[counter] + ' '
                counter+=1
        if (text == '\xa0\xa0\xa0\xa0Разрешил' and (12 not in data.keys())):
            counter = i+1
            data[12] = ""
            while(text_array[counter] != 'Материальные'):
                data[12] += text_array[counter] + ' '
                counter+=1
        if (text == 'Отпустил' and (25 not in data.keys())):
            counter = i+1
            data[25] = ""
            while(text_array[counter] != '(должность)'):
                data[25] += text_array[counter] + ' '
                counter+=1
        if (text == '(должность)' and (23 not in data.keys())):
            counter = i+1
            data[23] = ""
            while(text_array[counter] != '(подпись)'):
                data[23] += text_array[counter] + ' '
                counter+=1
        if (text == '(подпись)' and (26 not in data.keys())):
            counter = i+1
            data[26] = ""
            while(text_array[counter] != '(расшифровка подписи)'):
                data[26] += text_array[counter] + ' '
                counter+=1
    my_dict['ТРЕБОВАНИЕ-НАКЛАДНАЯ №'].append(data[1])
    my_dict['Организация'].append(data[2])
    my_dict['Структурное подразделение'].append(data[3])
    my_dict['Через кого '].append(data[13])
    my_dict['Затребовал '].append(data[11])
    my_dict['Разрешил'].append(data[12])
    my_dict['отпустил -> должность'].append(data[25])
    my_dict['отпустил -> подпись'].append(data[23])
    my_dict['отпустил -> расшифровка подписи'].append(data[26])
    
    for key, value in my_dict.items():
        if type(value) == str:
            my_dict[key] = value.replace("\n", "")
        else:
            my_dict[key] = [x.replace("\n", "") for x in value]
            
    my_dict['Тип формы'] = "М-11"
    return my_dict

