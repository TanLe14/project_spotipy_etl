from sqlalchemy import create_engine
from airflow.models.variable import Variable
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

user=Variable.get('user')
password=Variable.get('password')
host=Variable.get('host')
database=Variable.get('database')

base=declarative_base()
engine=create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=user,password=password,host=host,database=database))
Session=sessionmaker(bind=engine)
session=Session()

