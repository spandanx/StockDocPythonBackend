import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import json
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split

from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dropout
from keras.layers import Dense
from tensorflow.keras import optimizers
from keras.models import model_from_json

import time
import math

import requests
import paramiko
import pickle

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

sftp_host = Variable.get("sftp_host")
sftp_username = Variable.get("sftp_username")
sftp_password = Variable.get("sftp_password")

stock_name = "RVNL"
end_date = datetime.today().strftime('%Y-%m-%d')

scalers_pickled_file_sftp_path = '/home/gamma/stockdoc/{}/scalers/'.format(stock_name)
model_schema_file_sftp_path = '/home/gamma/stockdoc/{}/model_schemas/'.format(stock_name)
model_weights_file_sftp_path = '/home/gamma/stockdoc/{}/model_weights/'.format(stock_name)
model_predictions_file_sftp_path = '/home/gamma/stockdoc/{}/predictions/{}_mlmodel_prediction.csv'.format(stock_name, end_date)

params_ = {
    "stock_name": stock_name,
    "stock_data_end_date": end_date,
    "input_sequence_length": 100,
    "output_sequence_length": 5,
    "fetch_start_date_diff": 180,
    "random_state": 42,
    "epochs": 100,
    "batch_size": 32,
    "test_size": 0.2,
    "dropout_layer_size": 0.2,
    "lstm_units": 50,
    "dense_units": 20,
    "learning_rate": 0.001,
    # "optimizer": "adam",
    "loss_function": "mean_squared_error",
    "scalers_pickled_file_sftp_path": scalers_pickled_file_sftp_path,
    "model_schema_file_sftp_path": model_schema_file_sftp_path,
    "model_weights_file_sftp_path": model_weights_file_sftp_path,
    "model_predictions_file_sftp_path": model_predictions_file_sftp_path,
    "stock_mapping": {stock_name: "NSE_EQ|INE415G01027", "NIFTY50": "NSE_INDEX|Nifty 50", "SENSEX": "BSE_INDEX|SENSEX"}
}

start_date = (datetime.today() - timedelta(days=params_["fetch_start_date_diff"])).strftime('%Y-%m-%d')
params_["stock_data_start_date"] = start_date


def create_directory_if_not_created_sftp(sftp_folder_path):
    folder_to_create = '/'.join(sftp_folder_path.split("/")[:-1]) + '/'
    print("folder_to_create", folder_to_create)
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)
        sftp = ssh.open_sftp()

        try:
            sftp.chdir(folder_to_create)  # Test if remote_path exists
            print("Folder Present")
        except IOError:
            print("Folder Not Present, creating")
            sftp.mkdir(folder_to_create)  # Create remote_path

def get_files_in_folder(folder_path, file_extention):
    files = []
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        filesInSFTP = sftp.listdir_attr(folder_path)
        filesInSFTP.sort(key=lambda f: -f.st_mtime)
        for file_ in filesInSFTP:
            file_permissions = str(file_).split(' ')[0]
            if 'd' not in file_permissions and file_.filename.endswith(file_extention):
                files.append(folder_path + '/' + file_.filename)
                # t = datetime.fromtimestamp(file_.st_mtime).strftime('%Y-%m-%dT%H:%M:%S')
                # print(file_.filename, file_.st_size, t, str(file_).split(' ')[0])
        # print(filesInSFTP)
        # print(files)
    return files


def push_csv_to_sftp(df, sftp_location):
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(sftp_location, "w") as f:
            f.write(df.to_csv(index=False))


def read_csv_to_sftp(sftp_location):
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()

        with sftp.open(sftp_location, "r") as f:
            df_read_from_sftp = pd.read_csv(f)
        return df_read_from_sftp


def write_minmaxscalers_to_sftp(scalers_pickled_file_path, scalers_pickled_object):
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(scalers_pickled_file_path, "w") as f:
            f.write(scalers_pickled_object)


def read_minmaxscalers_from_sftp(scalers_pickled_file_path):
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)
        sftp = ssh.open_sftp()

        with sftp.open(scalers_pickled_file_path, "r") as f:
            scalers = pickle.loads(f.read())
        print("Read Scalers file")
        print(scalers)
        return scalers


def write_mlmodel_to_sftp(model, model_schema_file_sftp_path, model_weights_file_sftp_path):
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(model_schema_file_sftp_path, "w") as f:
            f.write(model.to_json())
        with sftp.open(model_weights_file_sftp_path, "w") as f:
            f.write(pickle.dumps(model.get_weights()))


def write_predictions_to_sftp(df, model_predictions_file_sftp_path):
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(model_predictions_file_sftp_path, "w") as f:
            f.write(df.to_csv(index=False))


def load_mlmodel_from_sftp(model_schema_file_sftp_path, model_weights_file_sftp_path):
    model_json_files_ = get_files_in_folder(model_schema_file_sftp_path, '.json')
    print("model_json_files_", model_json_files_)
    model_weights_files_ = get_files_in_folder(model_weights_file_sftp_path, '.h5')
    print("model_weights_files_", model_weights_files_)
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(model_json_files_[0], "r") as f:
            loaded_model_json = f.read()
            loaded_model = model_from_json(loaded_model_json)

        with sftp.open(model_weights_files_[0], "r") as f:
            x = pickle.loads(f.read())
            loaded_model.set_weights(x)
        return loaded_model

def fetch_stock_data_upstocks_and_convert_to_dataframe(url):
    print(url)
    res = requests.get(url)
    d = json.loads(res.text)
    rows = d['data']['candles']
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'dummy']
    df = pd.DataFrame(rows, columns=cols)
    return df


default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

with DAG(dag_id='ml_predict_{}_dag_1day_sensex_nifty50_combined_1'.format(stock_name),
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:

    @task(multiple_outputs=True)
    def read_data(params_):

        stock_mapping = params_["stock_mapping"]
        # generic_url = 'https://kite.zerodha.com/oms/instruments/historical/{}/day?user_id={}&oi=1&from={}&to={}'
        generic_url = 'https://api.upstox.com/v2/historical-candle/{}/day/{}/{}'

        # generic_file_name = "/home/gamma/data/{}_{}_to_{}.csv"
        start_date = params_["stock_data_start_date"]
        end_date = params_["stock_data_end_date"]

        stock_name = params_["stock_name"]
        url = generic_url.format(stock_mapping[stock_name], end_date, start_date)
        stock_df = fetch_stock_data_upstocks_and_convert_to_dataframe(url)[::-1]
        stock_df.reset_index(inplace=True)

        stock_name = "NIFTY50"
        url = generic_url.format(stock_mapping[stock_name], end_date, start_date)
        nifty50_df = fetch_stock_data_upstocks_and_convert_to_dataframe(url)[::-1]
        nifty50_df.reset_index(inplace=True)

        stock_name = "SENSEX"
        url = generic_url.format(stock_mapping[stock_name], end_date, start_date)
        sensex_df = fetch_stock_data_upstocks_and_convert_to_dataframe(url)[::-1]
        sensex_df.reset_index(inplace=True)

        return {"stock_df": stock_df, "nifty50_df": nifty50_df, "sensex_df": sensex_df, "params_": params_}

    @task()
    def data_merge_and_enrichment(stock_df, nifty50_df, sensex_df):
        stock_df["stock_open"] = stock_df["open"]
        stock_df["nifty50_open"] = 0
        stock_df["sensex_open"] = 0

        for idx, row in stock_df.iterrows():
            # print(row.timestamp)
            if len(nifty50_df[nifty50_df['timestamp'] == row.timestamp]) == 1:
                stock_df.at[idx, "nifty50_open"] = nifty50_df.loc[nifty50_df['timestamp'] == row.timestamp].open.values
            if len(sensex_df[sensex_df['timestamp'] == row.timestamp]) == 1:
                stock_df.at[idx, "sensex_open"] = sensex_df.loc[sensex_df['timestamp'] == row.timestamp].open.values

        stock_df["weekday_number"] = stock_df["timestamp"].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z').weekday())

        for idx, row in stock_df.iterrows():
            if idx>=1:
                stock_df.at[idx, "daily_stock_change"] = ((stock_df.iloc[idx-1]['stock_open'] - stock_df.iloc[idx-2]['stock_open'])/stock_df.iloc[idx-1]['stock_open'])*100 # for the prev day
                stock_df.at[idx, "daily_nifty50_open_change"] = ((stock_df.iloc[idx - 1]['nifty50_open'] - stock_df.iloc[idx - 2][
                    'nifty50_open']) / stock_df.iloc[idx - 1]['nifty50_open']) * 100  # for the prev day
                stock_df.at[idx, "daily_sensex_open_change"] = ((stock_df.iloc[idx - 1]['sensex_open'] -
                                                                  stock_df.iloc[idx - 2]['sensex_open']) / stock_df.iloc[idx - 1][
                    'sensex_open']) * 100  # for the prev day

        stock_df = stock_df[1:]
        stock_df = stock_df[stock_df.weekday_number.isin([0, 1, 2, 3, 4])]
        stock_df.reset_index(inplace=True)
        print(stock_df.head())
        return stock_df


    @task()
    def get_latest_stock_price(test_df_full, stock_price_column): #To be used for prediction
        # last_max = test_df_full.iloc[len(test_df_full)-1]["stock_open"]
        last_max = test_df_full.iloc[len(test_df_full) - 1][stock_price_column]
        return last_max

    @task()
    def generate_onehot_encoding(columns, df_input):
        df_output = df_input
        for col_ in columns:
            # one_hot_df = pd.get_dummies(df_output[col_], dtype = int, drop_first = True, prefix = col_)
            one_hot_df = pd.get_dummies(df_output[col_], dtype = int, drop_first = True, prefix = col_)
            df_output.drop([col_], axis=1, inplace = True)
            df_output = pd.concat([df_output, one_hot_df], axis=1)
        return df_output


    @task()
    def data_scale(df, scalers_pickled_file_sftp_path):
        print("Called data_scale()")
        files_ = get_files_in_folder(scalers_pickled_file_sftp_path, '.json')
        print("files_", files_)

        scalers = read_minmaxscalers_from_sftp(files_[0])
        print(scalers)
        df[['daily_stock_change_scaled']] = scalers["daily_stock_change"].transform(df[['daily_stock_change']])
        df[['daily_nifty50_open_change_scaled']] = scalers["daily_nifty50_open_change"].transform(df[['daily_nifty50_open_change']])
        df[['daily_sensex_open_change_scaled']] = scalers["daily_sensex_open_change"].transform(df[['daily_sensex_open_change']])
        return df

    @task(multiple_outputs=True)
    def preprocess_prediction_data(train_df, params_):
        X_train_columns = ['weekday_number_1',
                           'weekday_number_2', 'weekday_number_3', 'weekday_number_4', 'daily_nifty50_open_change_scaled',
                           'daily_sensex_open_change_scaled', 'daily_stock_change_scaled']
        y_train_columns = ['daily_stock_change_scaled']

        params_["X_train_columns"] = X_train_columns
        params_["y_train_columns"] = y_train_columns
        X_arr = []
        for i in range(params_["input_sequence_length"], len(train_df) - params_["output_sequence_length"], 1):
            X_arr.append(train_df.loc[i - params_["input_sequence_length"]:i - 1, X_train_columns])
        X_arr = X_arr[-1:]
        X_arr = np.array(X_arr)
        print(X_arr.shape)
        return {"to_predict": X_arr.tolist()}

    @task()
    def get_prediction(model_schema_file_sftp_path, model_weights_file_sftp_path, X_test):
        files_ = get_files_in_folder(scalers_pickled_file_sftp_path, '.json')
        print("scaler files_", files_)
        ## Prediction
        scalers = read_minmaxscalers_from_sftp(files_[0])
        model = load_mlmodel_from_sftp(model_schema_file_sftp_path, model_weights_file_sftp_path)
        pred_ = model.predict(X_test)
        print(pred_[:4])
        y_pred = scalers["daily_stock_change"].inverse_transform(pred_)
        return y_pred.tolist()

    @task()
    def publish_prediction_to_sftp(y_pred_list, model_predictions_file_sftp_path, last_price):
        y_pred = np.asarray(y_pred_list)

        pred_arr = []
        idx = 1
        date_offset = 1
        latest_price = last_price
        for pred_val in y_pred[0]:
            future_date = (datetime.today() + timedelta(days=idx))
            #.strftime('%Y-%m-%d')
            while (date_offset<7):
                future_date = (datetime.today() + timedelta(days=idx+date_offset))
                if (future_date.weekday() in [0, 1, 2, 3, 4]):
                    break
                date_offset+=1
            new_price = round(latest_price + (latest_price*round(pred_val, 2)/100), 2)
            pred_arr.append([idx, new_price, round(pred_val, 2), future_date.strftime('%Y-%m-%d')])
            latest_price = new_price
            idx += 1
        print(pred_arr[:10])

        df_pred = pd.DataFrame(pred_arr, columns=["test_number", "predicted", "predicted_percentage", "date"])
        print(df_pred.head(10))
        create_directory_if_not_created_sftp(model_predictions_file_sftp_path)
        write_predictions_to_sftp(df_pred, model_predictions_file_sftp_path)

    read_info = read_data(params_)
    stock_df = read_info["stock_df"]
    nifty50_df = read_info["nifty50_df"]
    sensex_df = read_info["sensex_df"]
    params_ = read_info["params_"]

    test_df_full = data_merge_and_enrichment(stock_df, nifty50_df, sensex_df)
    last_max = get_latest_stock_price(test_df_full, "stock_open")
    test_df = generate_onehot_encoding(['weekday_number'], test_df_full)
    scaled_data = data_scale(test_df, scalers_pickled_file_sftp_path)

    to_predict_data = preprocess_prediction_data(scaled_data, params_)
    X_pred = to_predict_data["to_predict"]
    predicted_data_list = get_prediction(model_schema_file_sftp_path, model_weights_file_sftp_path, X_pred)
    publish_prediction_to_sftp(predicted_data_list, model_predictions_file_sftp_path, last_max)