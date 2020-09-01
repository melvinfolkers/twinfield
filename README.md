<img src="https://static.wixstatic.com/media/a9ca5e_825bd4d39e7d468faf735b801fa3dea4~mv2.png/v1/fill/w_1458,h_246,al_c,usm_0.66_1.00_0.01/a9ca5e_825bd4d39e7d468faf735b801fa3dea4~mv2.png" width="200">


# twinfield

> Repository for downloading twinfield data to a azure database


#### Environments variables
some database credentials are pulled from environment variables. add these first before running the script <br>
add the following credentials to your environment variables `(~/.bash_profile` on mac and `~/etc/environment` on linux : 
```
    uid = os.environ.get('SQL_USER_1')
    password = os.environ.get('SQL_PW_1')
    server = os.environ.get('SQL_SERVER_1')
```

#### Yaml files

```
yml
|-- custom
|   |-- blob.yml
    |-- run_settings.yml
|   `-- twinfield_settings.yml
`-- templates
    |-- blob_template.yml
    |-- run_settings_template.yml
    `-- twinfield_settings_template.yml


```

in order to get the API running, the templates from `yml/default` should be copied to the folder `yml/custom` and remove the '_template suffix'

#### Install ODBC driver

The connection with the azure database is made with pyodbc. this library requires ODBC drivers `'ODBC Driver 17 for SQL Server'`<br>
download link: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15

