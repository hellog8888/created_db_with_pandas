import glob
import pandas as pd
from psycopg2 import Error
from sqlalchemy import create_engine
import datetime
import warnings

warnings.simplefilter("ignore")


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

    df = pd.read_excel(file)
    print('data_reading_done')

    try:
        engine = create_engine('postgresql://postgres:1234@localhost:5432/eirs')
        df.to_sql('cellular', engine, index=False)
        print("Table created successfully")

    except (Exception, Error) as error:
        print("Error while creating the table", error)


file = glob.glob('source_folder/*.xlsx')[0]
to_sql(file)
