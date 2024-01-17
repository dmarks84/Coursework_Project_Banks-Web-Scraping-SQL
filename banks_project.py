from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open('code_log.txt',"a") as lf: 
        lf.write(timestamp + ' : ' + message + '\n') 

def extract(url, attribs):
    ''' The purpose of this function is to extract the required information
    from the website and save it to a dataframe. The function returns the 
    dataframe for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            name = str(col[1].find_all('a')[1].contents[0])
            mc = float(str(col[2].contents[0]).strip('\n'))
            data_dict = {attribs[0]: name,
                        attribs[1]: mc}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    exchange_dict = exchange_df.set_index('Currency').to_dict()['Rate']
    df['MC_EUR_Billion'] = [np.round(x*exchange_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_dict['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# def run_query(query_statement, sql_connection):
#     ''' This function runs the query on the database table and
#     prints the output on the terminal. Function returns nothing. '''
# ''' Here, you define the required entities and call the relevant
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''

data_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_name = 'Largest_banks'
table_ext_attribs = ["Name", "MC_USD_Billion"]
table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
csv_exchange = "./exchange_rate.csv"
csv_out_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
log_progress("Preliminaries complete. Initiating ETL process")

data_frame = extract(data_url, table_ext_attribs)
# print(data_frame)
log_progress("Data extraction complete. Initiating Transformation process")

data_frame = transform(data_frame,csv_exchange)
# print(data_frame)
# print(data_frame['Name'][4])
# print(data_frame['MC_EUR_Billion'][4])
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(data_frame,csv_out_path)
log_progress("Data saved to CSV file")

# Initiate SQLite3 Connection
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(data_frame, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Run fist query to select the entire table
run_query(f'SELECT * FROM {table_name}',conn)
log_progress("Process Complete")
# Run second query to print the average market cap for all banks in Billion USD
run_query(f'SELECT AVG("MC_GBP_Billion") FROM {table_name}',conn)
log_progress("Process Complete")
# Run third quere to print the names of the top five banks
run_query(f'SELECT {table_attribs[0]} from {table_name} LIMIT 5',conn)
log_progress("Process Complete")

#Close SQLite3 connection
conn.close()
log_progress("Server Connection closed")