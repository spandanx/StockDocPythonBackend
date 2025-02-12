# StockDoc Python Application

This is a web application that can display and predict stock price predition using LSTM.

## Features
 - 5 day prediction using last 100 days of data
 - Schedule Model prediction data daily using hosted Apache Airflow Dag and SFTP server.
 - Log model metrics to hosted MLFlow

## Application Screen Shots
<p>Register</p>
<img src="https://github.com/user-attachments/assets/08f68014-c867-4011-b0bb-8ed7f7de3568" width=50% height=50%>
<p>Login</p>
<img src="https://github.com/user-attachments/assets/e44ff482-12bb-48af-b02f-788bc431367f" width=50% height=50%>
<p>Holdings</p>
<img src="https://github.com/user-attachments/assets/68f96f78-854c-4911-a796-000f41949a61" width=50% height=50%>
<p>Stock Chart Page</p>
<img src="https://github.com/user-attachments/assets/c45c9e16-76a0-4aac-aaf0-7b644dc5c923" width=50% height=50%>
<p>Prediction</p>
<img src="https://github.com/user-attachments/assets/5b4dbc2c-e61b-4a5b-84b5-ad2293a1b070" width=50% height=50%>
<p>Set Daily Stock Token</p>
<img src="https://github.com/user-attachments/assets/598286e8-782c-4245-af21-134411b4e441" width=50% height=50%>
<p>Aiflow Dags</p>
<img src="https://github.com/user-attachments/assets/f2649596-6745-4641-936b-7bbd25dbdfff" width=50% height=50%>
<p>AirFlow Detailed tasks</p>
<img src="https://github.com/user-attachments/assets/eb4a7f64-9df4-4484-ae6d-99bebf6009bf" width=50% height=50%>
<p>MLFlow Tracker</p>
<img src="https://github.com/user-attachments/assets/6273685e-adb2-4612-9dde-ea7a6c3b09cd" width=50% height=50%>


## Instructions to use the application
1. Register with the application with the referral code to be given by the admin (me), provide the stock userid (Kite-Zerodha) while registering.
2. Login with the username and password
3. Add the daily stock token from zerodha. (You can get this from kite web app, inspect any api (ex - holding, orders etc) and get the authentication token (Starts with enctoken <>))
4. Click at the arrow mark (top right) of any stock to view the price chart.
5. Click on Load Predictions button, select the prediction day if available and click on Show Prediction button
6. View the prediction
 
## Technologies used

Frontend - `React.js`

Backend - `FastAPI`, `OAuth2 Security`, `Apache Airflow`, `MLFlow`, `Docker`, `Python`, `LSTM`

## Steps to run locally


### Step 1. Clone the repositories
-------------------------
#### Backend: https://github.com/spandanx/StockDocPythonBackend
#### Frontend: https://github.com/spandanx/StockDocReactJs


### Step 2. Install required softwares
-------------------------

`Node.js`
`Miniconda`

#### Install required packages
```
python -m pip install -r requirements.txt
```

### Step 3. Prepare backend
-------------------------

#### Create new environment
<p>Open miniconda console. Run the below commands </p>

```
conda create -n env-name python=3.10
conda activate env-name
```
#### Prepare Airflow - Custom Docker
##### Download docker-compose.yaml
-----------------------
'https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml'

##### Add Dockerfile and add the below lines
-----------------------
```
FROM apache/airflow:2.10.4-python3.10
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
```
##### Create requirements.txt file add the below dependecies
```
pandas==2.2.2
numpy==1.26.4
scikit-learn==1.5.0
matplotlib==3.9.0
keras==2.10.0
tensorflow-estimator==2.10.0
tensorflow-gpu==2.10.0
requests==2.32.3
mlflow==2.19.0
paramiko==3.5.0
```

##### Run the below command
```docker build . --tag extending_airflow_python_310:latest```

##### Change the image name in docker-compose.yaml with 
```extending_airflow_python_310:latest```



### Step 4. Run the python application
-------------------------
```
python -m uvicorn main:app --env-file path-to-env-file/custom_env_data.env
```

