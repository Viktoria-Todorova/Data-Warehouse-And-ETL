import pandas as pd
from sqlalchemy import create_engine


def load_to_postgresql(df: pd.DataFrame, table_name: str, db_url: str) :
    engine = create_engine(db_url)
    df.to_sql(name=table_name, con= engine, if_exists='replace', index=False)
