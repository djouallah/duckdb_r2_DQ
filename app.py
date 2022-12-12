import streamlit as st
import duckdb
con=duckdb.connect()
#make sure you don't include http into the endpoint
con.execute(f'''
install httpfs;
LOAD httpfs;
set s3_region = 'auto';
set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
set s3_endpoint = '{st.secrets["endpoint_url_secret"]}'  ;
SET s3_url_style='path';
create view lineitem IF NOT EXISTS as select  *  from parquet_scan('s3://delta/lineitem/*/*.parquet' , HIVE_PARTITIONING = 1,filename= 1)
''')

SQL = st.text_input('Write a SQL Query', 'describe  lineitem')

df  =con.execute(SQL).df()
st.write(df)
