import streamlit as st
import duckdb
con=duckdb.connect()
#make sure you don't include http into the endpoint
@st.experimental_singleton
def define_view():
    con.execute(f'''
    install httpfs;
    LOAD httpfs;
    set s3_region = 'auto';
    set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
    set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
    set s3_endpoint = '{st.secrets["endpoint_url_secret"]}'  ;
    SET s3_url_style='path';
    create or replace view lineitem as select  *  from parquet_scan('s3://delta/lineitem/*/*.parquet' , HIVE_PARTITIONING = 1,filename= 1)
    ''')
    return con
con=define_view()
SQL = st.text_input('Write a SQL Query, Streamlit Cache the results of existing Queries', 'select * from lineitem limit 5')
@st.experimental_memo
def get_data(SQL):
  return con.execute(SQL).df()
df = get_data(SQL)  
st.write(df)
