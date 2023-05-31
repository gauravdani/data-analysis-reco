import sys
import pandas as pd
"""
pip install "snowflake-connector-python"
pip install --upgrade snowflake-sqlalchemy
pip install "snowflake-connector-python[secure-local-storage,pandas]"
"""
print ('hello world')

print (sys.version_info)
print (sys.version)
print (sys.executable)


from sqlalchemy import create_engine
engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user='GOOGLE_DATA_STUDIO',
        password='',
        account_identifier='maxdome.eu-west-1',
    )
)
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()
    
lv_dataset = pd.read_sql_query("select date(load_time), count(distinct asset_id) from recsys.main.mlt_assets \
                where date(load_time)>date('2023-01-01') \
                group by date(load_time)  \
                order by 1 desc", engine)
lv_dataset.head(5)
    
    

