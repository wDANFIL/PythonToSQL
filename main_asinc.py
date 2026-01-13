import pandas as pd
import cx_Oracle
import asyncio
from concurrent.futures import ThreadPoolExecutor
import config as cfg
import datetime

cx_Oracle.init_oracle_client(lib_dir="C:\\specsoft\\instantclient_11_2")

file = r'C:\\pyImport\\DDS-212991.xlsx'
sql_table = "INSERT INTO tables (col1, col2, col3, col4) VALUES (:1, :2, :3, :4)"

def sync_ins_table(server):
    try:
        print(f'Step: Start {server} {datetime.datetime.now()}')   
        connection = cx_Oracle.connect("host", "password", server)
        cursor = connection.cursor()    
        
        df = pd.read_excel(file)    
        df_list = df.fillna('').values.tolist() 

        for row in df_list:
            cursor.executemany(sql_table, [row])    

        cursor.close()
        connection.commit()
        connection.close()
        
        print(f'END {server} {datetime.datetime.now()}')
        
        return f"Успех: {server}"
        
    except Exception as e:
        print(f"Ошибка на сервере {server}: {e}")
        return f"Ошибка: {server} - {e}"

async def main():
    print("Запуск параллельной загрузки...")
    
    with ThreadPoolExecutor(max_workers=len(cfg.SERVER_LIST)) as executor:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(executor, sync_ins_table, server)
            for server in cfg.SERVER_LIST
        ]
        
        results = await asyncio.gather(*tasks)
        
    print("Все задачи завершены!")
    for result in results:
        print(f" - {result}")

asyncio.run(main())
print('Done')