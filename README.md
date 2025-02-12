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

	
## Technologies used

Frontend - `React.js`

Backend - `FastAPI`, `OAuth2 Security`, `Apache Airflow`, `MLFlow`, `Docker`, `Python`, `LSTM`

## Steps to run locally



### Step 1. Clone the repositories
#### Backend: https://github.com/spandanx/StockDocPythonBackend
#### Frontend: https://github.com/spandanx/StockDocReactJs


### Step 2. Install required softwares

`Node.js`
`Miniconda`

### Step 3. Prepare backend

#### Create new environment
<p>Open miniconda console. Run the below commands </p>

```
conda create -n env-name python=3.10
conda activate env-name
```


