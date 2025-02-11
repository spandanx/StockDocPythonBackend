import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import json
from datetime import datetime
from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta

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
import mlflow
import os
from mlflow.models import infer_signature

# from pickle import dump, load

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable


sftp_host = Variable.get("sftp_host")
sftp_username = Variable.get("sftp_username")
sftp_password = Variable.get("sftp_password")

stock_name = "RVNL"
years_of_data = 4

end_date = datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days = years_of_data * 365)).strftime('%Y-%m-%d')

scalers_pickled_file_sftp_path = '/home/gamma/stockdoc/{}/scalers/{}_scalers.json'.format(stock_name, end_date)
model_schema_file_sftp_path = '/home/gamma/stockdoc/{}/model_schemas/{}_mlmodel_schema.json'.format(stock_name, end_date)
model_weights_file_sftp_path = '/home/gamma/stockdoc/{}/model_weights/{}_mlmodel_weights.h5'.format(stock_name, end_date)

params_ = {
    "stock_name": stock_name,
    "stock_data_start_date": start_date,
    "stock_data_end_date": end_date,
    "input_sequence_length": 100,
    "output_sequence_length": 5,
    "years_of_data": years_of_data,
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
    "stock_mapping" : {stock_name: "NSE_EQ|INE415G01027", "NIFTY50": "NSE_INDEX|Nifty 50", "SENSEX": "BSE_INDEX|SENSEX"}
}


scalers = dict()
scalers["stock_percent_scaler_daily"] = MinMaxScaler(feature_range=(0, 1))
scalers["stock_percent_scaler_weekly"] = MinMaxScaler(feature_range=(0, 1))
scalers["week_value_scaler"] = MinMaxScaler(feature_range=(0, 1))
scalers["stock_price_scaler"] = MinMaxScaler(feature_range=(0, 1))

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
    create_directory_if_not_created_sftp(sftp_location)
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
    create_directory_if_not_created_sftp(scalers_pickled_file_path)
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
    create_directory_if_not_created_sftp(model_schema_file_sftp_path)
    create_directory_if_not_created_sftp(model_weights_file_sftp_path)
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
    create_directory_if_not_created_sftp(model_predictions_file_sftp_path)
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(model_predictions_file_sftp_path, "w") as f:
            f.write(df.to_csv(index=False))


def load_mlmodel_from_sftp(model_schema_file_sftp_path, model_weights_file_sftp_path):
    with paramiko.SSHClient() as ssh:
        # ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_username, password=sftp_password)

        sftp = ssh.open_sftp()
        with sftp.open(model_schema_file_sftp_path, "r") as f:
            loaded_model_json = f.read()
            loaded_model = model_from_json(loaded_model_json)

        with sftp.open(model_weights_file_sftp_path, "r") as f:
            x = pickle.loads(f.read())
            loaded_model.set_weights(x)
        return loaded_model

def fetch_stock_data_upstocks_and_convert_to_dataframe(url):
    print(url)
    res = requests.get(url)
    d = json.loads(res.text)
    rows = d['data']['candles']
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'dummy']
    df = pd.DataFrame(rows, columns = cols)
    return df

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

with DAG(dag_id='ml_train_{}_dag_1day_sensex_nifty50_combined_1'.format(stock_name),
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:

    os.environ['MLFLOW_TRACKING_USERNAME'] = Variable.get("MLFLOW_TRACKING_USERNAME")
    os.environ['MLFLOW_TRACKING_PASSWORD'] = Variable.get("MLFLOW_TRACKING_PASSWORD")

    ## FETCH data from upstocks
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
        # stock_df["weekday"] = stock_df["timestamp"].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime('%a'))
        # stock_df["weekly_change"] = 0
        # stock_df["daily_stock_change"] = 0
        for idx, row in stock_df.iterrows():
            if idx>=1:
                stock_df.at[idx, "daily_stock_change"] = ((stock_df.iloc[idx-1]['stock_open'] - stock_df.iloc[idx-2]['stock_open'])/stock_df.iloc[idx-1]['stock_open'])*100 # for the prev day
                stock_df.at[idx, "daily_nifty50_open_change"] = ((stock_df.iloc[idx - 1]['nifty50_open'] - stock_df.iloc[idx - 2][
                    'nifty50_open']) / stock_df.iloc[idx - 1]['nifty50_open']) * 100  # for the prev day
                stock_df.at[idx, "daily_sensex_open_change"] = ((stock_df.iloc[idx - 1]['sensex_open'] -
                                                                  stock_df.iloc[idx - 2]['sensex_open']) / stock_df.iloc[idx - 1][
                    'sensex_open']) * 100  # for the prev day
        # for idx, row in stock_df.iterrows():
        #     if idx>=6:
        #         stock_df.at[idx, "weekly_change"] = ((stock_df.iloc[idx-1]['open'] - stock_df.iloc[idx-6]['open'])/stock_df.iloc[idx-1]['open'])*100 # for the prev day

        stock_df = stock_df[1:]
        # stock_df.reset_index(inplace = True)
        stock_df = stock_df[stock_df.weekday_number.isin([0, 1, 2, 3, 4])]
        stock_df.reset_index(inplace=True)
        return stock_df

    @task()
    def generate_onehot_encoding(columns, df_input):
        df_output = df_input
        for col_ in columns:
            # one_hot_df = pd.get_dummies(df_output[col_], dtype = int, drop_first = True, prefix = col_)
            one_hot_df = pd.get_dummies(df_output[col_], dtype=int, drop_first=True, prefix=col_)
            df_output.drop([col_], axis=1, inplace=True)
            df_output = pd.concat([df_output, one_hot_df], axis=1)
        return df_output

    @task()
    def data_scale(df, scalers_pickled_file_sftp_path):
        scalers = dict()
        scalers["daily_stock_change"] = MinMaxScaler(feature_range=(0, 1))
        scalers["daily_nifty50_open_change"] = MinMaxScaler(feature_range=(0, 1))
        scalers["daily_sensex_open_change"] = MinMaxScaler(feature_range=(0, 1))

        df[['daily_stock_change_scaled']] = scalers["daily_stock_change"].fit_transform(df[['daily_stock_change']])
        df[['daily_nifty50_open_change_scaled']] = scalers["daily_nifty50_open_change"].fit_transform(df[['daily_nifty50_open_change']])
        df[['daily_sensex_open_change_scaled']] = scalers["daily_sensex_open_change"].fit_transform(df[['daily_sensex_open_change']])

        scalers_pickled = pickle.dumps(scalers)
        write_minmaxscalers_to_sftp(scalers_pickled_file_sftp_path, scalers_pickled)
        return df


    @task(multiple_outputs=True)
    def train_test_split_generator(train_df, params_):
        X_train_columns = ['weekday_number_1',
                           'weekday_number_2', 'weekday_number_3', 'weekday_number_4', 'daily_nifty50_open_change_scaled',
                           'daily_sensex_open_change_scaled', 'daily_stock_change_scaled']
        y_train_columns = ['daily_stock_change_scaled']

        params_["X_train_columns"] = X_train_columns
        params_["y_train_columns"] = y_train_columns

        ############
        X_arr = []
        y_arr = []
        # print(len(train_df))
        # print("#################")
        for i in range(params_["input_sequence_length"], len(train_df) - params_["output_sequence_length"], 1):
            # print(i-10, i)
            X_arr.append(train_df.loc[i - params_["input_sequence_length"]:i - 1, X_train_columns])
            y_arr.append(train_df.loc[i:i + params_["output_sequence_length"] - 1, y_train_columns])
        X_arr, y_arr = np.array(X_arr), np.array(y_arr)
        y_arr = np.reshape(y_arr, (y_arr.shape[0], y_arr.shape[1]))
        print(X_arr.shape)
        print(y_arr.shape)

        ################
        X_train, X_test, y_train, y_test = train_test_split(X_arr, y_arr, test_size=params_["test_size"],
                                                            random_state=params_["random_state"])
        params_["X_train.shape"] = X_train.shape
        params_["y_train.shape"] = y_train.shape

        return {"X_train": X_train.tolist(), "X_test": X_test.tolist(), "y_train": y_train.tolist(),
                "y_test": y_test.tolist()}


    @task()
    def model_creation_and_fit_model(params_, X_train_list, y_train_list):
        X_train = np.asarray(X_train_list)
        y_train = np.asarray(y_train_list)
        model = Sequential()
        model.add(LSTM(units=params_["lstm_units"], activation='tanh', input_shape=(X_train.shape[1], X_train.shape[2]),
                       return_sequences=False))
        model.add(Dense(units=params_["dense_units"], activation='relu'))
        model.add(Dense(units=params_["output_sequence_length"], activation='linear'))
        adam = optimizers.Adam(lr=params_["learning_rate"])

        a_time = time.time()
        model.compile(optimizer=adam, loss=params_["loss_function"],
                      metrics=['accuracy'])  # mean_squared_error
        model.fit(X_train, y_train, epochs=params_["epochs"], batch_size=params_["batch_size"])

        b_time = time.time()
        time_diff = b_time - a_time

        # print("Diff", time_diff, "Seconds")
        params_["model_fit_time"] = str(math.ceil((time_diff) / 60)) + " minutes and " + str(
            round((time_diff) % 60, 2)) + " seconds"

        write_mlmodel_to_sftp(model, model_schema_file_sftp_path, model_weights_file_sftp_path)
        return model_schema_file_sftp_path ## Dummy Wait

    @task()
    def get_prediction(model_schema_file_sftp_path, model_weights_file_sftp_path, X_test, dummy_wait):
        # create_directory_if_not_created_sftp(model_schema_file_sftp_path)
        # create_directory_if_not_created_sftp(model_weights_file_sftp_path)
        model = load_mlmodel_from_sftp(model_schema_file_sftp_path, model_weights_file_sftp_path)
        y_pred = model.predict(X_test)
        return y_pred.tolist()

    @task()
    def calculate_prediction_error(y_test_list, y_pred_list):
        y_test = np.asarray(y_test_list)
        y_pred = np.asarray(y_pred_list)
        ############# Load from SFTP
        scalers = read_minmaxscalers_from_sftp(scalers_pickled_file_sftp_path)

        ############
        y_test_upscaled = scalers["daily_stock_change"].inverse_transform(y_test)
        y_pred_upscaled = scalers["daily_stock_change"].inverse_transform(y_pred)
        ###########
        pred_arr = []
        for idx in range(len(y_pred_upscaled)):
            for pred_val, actual_val in zip(y_pred_upscaled[idx], y_test_upscaled[idx]):
                pred_arr.append([idx, pred_val, actual_val])
            # break
        print(pred_arr[:10])

        df_pred = pd.DataFrame(pred_arr, columns=["test_number", "predicted", "actual"])
        # df_pred["actual"] = stock_percent_scaler_daily.inverse_transform(df_pred["actual"])
        df_pred.head(10)

        ############
        # prediction difference
        df_pred["difference"] = abs(df_pred["predicted"] - df_pred["actual"])
        ###########
        average_error = sum(df_pred["difference"] / len(df_pred))

        print(average_error)
        return average_error

    @task()
    def ml_flow_log(params_, average_error):
        mlflow.set_tracking_uri(uri="http://180.188.226.161:8081")
        mlflow.set_experiment("{} LSTM Training 1 day interval".format(stock_name))

        with mlflow.start_run():
            # Log the hyperparameters
            mlflow.log_params(params_)

            # Log the loss metric
            mlflow.log_metric("average_error", average_error)

            # Set a tag that we can use to remind ourselves what this run was for
            mlflow.set_tag("Training Info", "LSTM")

            # Infer the model signature
            # signature = infer_signature(X_train, y_pred)
            # Log the model
            # model_info = mlflow.keras.log_model(
            #     model,
            #     artifact_path="lstm_stock_models",
            #     code_paths = ['StockPredictionRVNLDraft-Many-to-many-30mins-interval.ipynb']
            #     signature=signature,
            #     registered_model_name="lstm_many_to_many_30_mins_"+ params_["stock_name"] + "_seq_length_" + str(params_["input_sequence_length"]) ,
            # )


    ## DAG Worflow- ETL Pipeline
    read_info = read_data(params_)
    stock_df = read_info["stock_df"]
    nifty50_df = read_info["nifty50_df"]
    sensex_df = read_info["sensex_df"]
    params_ = read_info["params_"]

    train_df_full = data_merge_and_enrichment(stock_df, nifty50_df, sensex_df)
    train_df = generate_onehot_encoding(['weekday_number'], train_df_full)
    train_df = data_scale(train_df, scalers_pickled_file_sftp_path)
    train_test_splitted_data = train_test_split_generator(train_df, params_)

    X_train, X_test, y_train, y_test = train_test_splitted_data["X_train"], train_test_splitted_data["X_test"], \
    train_test_splitted_data["y_train"], train_test_splitted_data["y_test"]
    dummy_wait = model_creation_and_fit_model(params_, X_train, y_train)
    y_pred = get_prediction(model_schema_file_sftp_path, model_weights_file_sftp_path, X_test, dummy_wait)
    average_error = calculate_prediction_error(y_test, y_pred)
    ml_flow_log(params_, average_error)