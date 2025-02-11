# StockDoc Python Application

This is a web application that can display and predict stock price predition using LSTM.

## Features
 - 5 day prediction using last 100 days of data
 - Schedule Model prediction data daily using hosted Apache Airflow Dag and SFTP server.
 - Log model metrics to hosted MLFlow

<details>
   

</details>
	
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


