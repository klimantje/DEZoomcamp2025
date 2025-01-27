import argparse
import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)
logger.setLevel('INFO')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name =params.table_name
    url = params.url 

    ny_data = pd.read_parquet(url)
    print(f"ingested {len(ny_data)} rows")
    ny_data.lpep_dropoff_datetime = pd.to_datetime(ny_data.lpep_dropoff_datetime)
    ny_data.lpep_pickup_datetime = pd.to_datetime(ny_data.lpep_pickup_datetime)
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()
    ny_data.to_sql(name=table_name,con=engine, if_exists='replace')

    zone_data =  pd.read_csv("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
    zone_data.to_sql(name="zones", con=engine, if_exists="replace")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='data pipeline',
                    description='De zoomcamp pipeline')

    parser.add_argument("--url", required=False, default="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet")
    parser.add_argument("--user", required=False, default="root")
    parser.add_argument("--password", required=False, default = "root")
    parser.add_argument("--host", required=False, default="localhost")
    parser.add_argument("--port", required=False, default=5432)
    parser.add_argument("--db", required=False, default="ny_taxi")
    parser.add_argument("--table_name", required=False, default = "green_taxi_data")

    args = parser.parse_args()
    main(args)
