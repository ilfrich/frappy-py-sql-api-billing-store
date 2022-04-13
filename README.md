# Frappy Python Mongo Store for API Billing

Python SQLAlchemy Store Implementation for Tracking [API Billing](https://github.com/ilfrich/frappy-api-billing) Usage.

## Usage

```python
from frappysqlapibilling import UsageStore
from frappyapibilling import ApiBilling
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# create flask app
app = Flask(__name__)

# create SQL DB connection
sql_host, sql_port, sql_user, sql_pass, sql_db = ...
sql_connect_str = f"{sql_user}:{sql_pass}@{sql_host}:{sql_port}/{sql_db}"
# example for postgres
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql+psycopg2://{sql_connect_str}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# create db
sql_db = SQLAlchemy(app)

# create sql store
store = UsageStore(sql_db=sql_db)

# pass the store instance to the api billing constructor
api_billing = ApiBilling(usage_store=store)
```

See [API Billing Usage](https://github.com/ilfrich/frappy-api-billing#usage) for details on how to use the module.
