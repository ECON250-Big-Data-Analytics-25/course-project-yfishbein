Welcome to your new dbt project!

# Installation

The following tutorial assumes you're already familiar with git and command line usage.

## Getting the code to your local machine
1. Fork this github repository into your local account

2. Copy it to your local machine: `git clone https://github.com/your_account_name/econ250_2025.git`



## gcloud authentication

To run queries from your command line, you'll first need to install `gcloud` utility.

Follow the instructions here: https://cloud.google.com/sdk/docs/install. After installation you should have `gcloud` command available for running in the terminal.

Now, try to authenticate with your **kse email** using the following command: 

```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/bigquery,\
https://www.googleapis.com/auth/drive.readonly,\
https://www.googleapis.com/auth/iam.test,\
https://www.googleapis.com/auth/cloud-platform
```

Now, when you run the following commands something similar should be response: 

```bash
$ gcloud auth list

     Credentialed Accounts
ACTIVE  ACCOUNT
*       o_omelchenko@kse.org.ua

```
To set the active project, run the following: 

```bash
gcloud config set project econ250-2025
```


## venv and libraries
Prerequisites: having Python installed on your machine. 
Following instructions are for Linux or WSL; if you'd like to run Windows - please refer to the documentation below.

```bash

# change directory to the one you just copied from github
cd econ250_2025 

# create and activate venv
python3 -m venv env 
source env/bin/activate

pip install -r requirements.txt

```

If everything is installed correctly, you should run the following commands successfully: 


```
$ dbt --version

Core:
  - installed: 1.9.3
  - latest:    1.9.3 - Up to date!

Plugins:
  - bigquery: 1.9.1 - Up to date!
```


For more detailed reference, refer to the official documentation here: 
- https://docs.getdbt.com/docs/core/pip-install
- https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup#local-oauth-gcloud-setup

## Adjusting the configuration

You'll need to specify your own dataset to save your models to. To do so, navigate to the `profiles.yml` in the root directory of the project, and replace `o_omelchenko` with your bigquery dataset name with which you have been working previously.




## Final check

Try running the following command:
- dbt run

If everything is set up well, you will see similar output: 

```log
❯ dbt run
01:18:56  Running with dbt=1.9.3
01:18:57  Registered adapter: bigquery=1.9.1
01:18:57  Found 2 models, 4 data tests, 491 macros
01:18:57  
01:18:57  Concurrency: 2 threads (target='dev')
01:18:57  
01:19:00  1 of 2 START sql table model o_omelchenko.my_first_dbt_model ................... [RUN]
01:19:04  1 of 2 OK created sql table model o_omelchenko.my_first_dbt_model .............. [CREATE TABLE (2.0 rows, 0 processed) in 4.44s]
01:19:04  2 of 2 START sql view model o_omelchenko.my_second_dbt_model ................... [RUN]
01:19:06  2 of 2 OK created sql view model o_omelchenko.my_second_dbt_model .............. [CREATE VIEW (0 processed) in 2.13s]
01:19:06  
01:19:06  Finished running 1 table model, 1 view model in 0 hours 0 minutes and 9.64 seconds (9.64s).
```

If you have any troubles with installation, please contact the course instructor (Oleh Omelchenko) in slack for assist.

