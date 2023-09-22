import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, ForeignKey, DateTime, Table, MetaData
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, mapper
from sqlalchemy.dialects.postgresql import JSON
import json

# ----- Global Config -----
DEBUG_TICKER = 1  # Set to 0 to turn off debugging output, 1 to turn it on

# Debug function to print out messages if DEBUG_TICKER is set to 1
def debug_print(msg):
    if DEBUG_TICKER:
        print(f"DEBUG: {msg}")

# ----- Fetching Data from SmartSuite API -----
debug_print("Fetching data from SmartSuite API...")

BASE_URL = "https://app.smartsuite.com/api/v1/applications"
HEADERS = {
    "Authorization": "Token 58a900e8fbcbe3c8cc6ffc8e360f8db7d5066a37",
    "Account-Id": "sj8qvf6p",
    "Content-Type": "application/json"
}
app_ids = [
    "64e5561b21f2d0402ae66cfd",
    "64e55236fe94933e2e380e60",
    "64fcb50b94e0f64a6a73147c",
    "64ee591bd392bd8ec9c62d73"
]
dataframes = {}
for app_id in app_ids:
    response = requests.post(f"{BASE_URL}/{app_id}/records/list/", headers=HEADERS, json={"hydrated": True})
    response_data = response.json()
    df = pd.DataFrame(response_data['items'])
    dataframes[app_id] = df

# ----- Database Setup -----
debug_print("Setting up database...")

Base = declarative_base()
DATABASE_URL = "postgresql://artie:1ytFzB#yF4ImRpqb$W98@trails.cv69d99rcbc7.us-east-1.rds.amazonaws.com:5432/trails"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# ----- Model Creation -----
debug_print("Creating dynamic models...")

metadata = MetaData()

def transform_data(df):
    for column in df.columns:
        # Check if the column contains dictionaries or lists
        if any(isinstance(i, (dict, list)) for i in df[column]):
            # Convert dictionaries or lists to JSON strings
            df[column] = df[column].apply(json.dumps)
    return df
    
    
def create_model_from_dataframe(df, base, app_id):
    table_name = f"records_{app_id}"
    columns = [Column('id', String, primary_key=True)]

    for column, dtype in df.dtypes.items():
        if column == 'id':
            continue
        if dtype == 'int64':
            columns.append(Column(column, Integer))
        elif dtype == 'float64':
            columns.append(Column(column, Float))
        elif dtype == 'O' and df[column].apply(isinstance, args=(dict,)).all():
            columns.append(Column(column, JSON))
        else:
            columns.append(Column(column, String))

    table = Table(table_name, metadata, *columns)
    model_class = type(f"Model_{app_id}", (base,), {'__table__': table})

    return model_class

# ----- Data Insertion -----
debug_print("Inserting data...")

def insert_data(df, model, session):
    for index, row in df.iterrows():
        record_data = row.to_dict()

        # Check if the record already exists
        existing_record = session.query(model).filter_by(id=record_data["id"]).first()
        
        if existing_record:
            debug_print(f"Record with ID {record_data['id']} already exists. Updating.")
            
            for col, value in record_data.items():
                if isinstance(value, dict):
                    setattr(existing_record, col, json.dumps(value))
                else:
                    setattr(existing_record, col, value)
        else:
            for col, value in record_data.items():
                if isinstance(value, dict):
                    record_data[col] = json.dumps(value)
            
            record = model(**record_data)
            session.add(record)
            
    session.commit()


# Main Execution
with SessionLocal() as session:
    for app_id, df in dataframes.items():
        DynamicModel = create_model_from_dataframe(df, Base, app_id)
        DynamicModel.__table__.create(bind=engine, checkfirst=True)
        df = transform_data(df)
        insert_data(df, DynamicModel, session)

debug_print("Data insertion completed!")
