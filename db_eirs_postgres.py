import glob
import pandas as pd
from psycopg2 import Error
from sqlalchemy import create_engine
import datetime
import warnings

warnings.simplefilter("ignore")

dict_for_operator = \
        {
            'Общество с ограниченной ответственностью «Скартел»': 'Скартел',
            'Общество с ограниченной ответственностью \"Скартел\"': 'Скартел',

            'Общество с ограниченной ответственностью \"Т2 Мобайл\"': 'Т2 Мобайл',
            'Общество с ограниченной ответственностью «Т2 Мобайл»': 'Т2 Мобайл',

            'Публичное акционерное общество «Мобильные ТелеСистемы»': 'МТС',
            'Публичное акционерное общество \"Мобильные ТелеСистемы\"': 'МТС',

            'Публичное акционерное общество \"МегаФон\"': 'МегаФон',
            'Публичное акционерное общество «МегаФон»': 'МегаФон',

            'Публичное акционерное общество \"Ростелеком\"': 'Ростелеком',
            'Публичное акционерное общество «Ростелеком»': 'Ростелеком',
            'Публичное акционерное общество междугородной и международной электрической связи \"Ростелеком\"': 'Ростелеком',

            'Публичное акционерное общество «Вымпел-Коммуникации»': 'ВымпелКом',
            'Публичное акционерное общество \"Вымпел-Коммуникации\"': 'ВымпелКом'
        }

dict_ETC = \
        {
            '18.1.1.3.': 'GSM',
            '18.1.1.8.': 'GSM',
            '18.1.1.5.': 'UMTS',
            '18.1.1.6.': 'UMTS',
            '18.7.1.': 'LTE',
            '18.7.4.': 'LTE',
            '18.7.5.': '5G NR',
            '19.2.': 'РРС'
        }

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time}")
        return result

    return wrapper

@measure_time
def to_sql(file):

    df = pd.read_excel(file).loc[:,
               ['Наименование РЭС', 'Адрес', '№ вида ЕТС', 'Владелец', 'Широта', 'Долгота', 'Частоты',
                'Дополнительные параметры', 'Классы излучения', 'Серия последнего действующего РЗ/СоР',
                'Номер последнего действующего РЗ/СоР']]

    df['№ вида ЕТС'] = [dict_ETC[x.strip()] for x in df['№ вида ЕТС']]
    df['Владелец'] = [dict_for_operator[x.strip()] for x in df['Владелец']]

    df['Серия_Номер_последнего_действующего_РЗ_СоР'] = df['Серия последнего действующего РЗ/СоР'].astype(str) + ' ' + df['Номер последнего действующего РЗ/СоР'].astype(str)

    df = df.drop(['Серия последнего действующего РЗ/СоР'], axis=1)
    df = df.drop(['Номер последнего действующего РЗ/СоР'], axis=1)

    print('data_reading_complite')

    try:
        engine = create_engine('postgresql://postgres:1234@localhost:5432/eirs')
        df.to_sql('cellular', engine, index=False)
        print("Table created successfully")

    except (Exception, Error) as error:
        print("Error while creating the table", error)


file = glob.glob('source_folder/*.xlsx')[0]
to_sql(file)
