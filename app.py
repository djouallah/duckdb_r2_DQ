import streamlit as st
import duckdb
from timeit import default_timer as timer
st.set_page_config(
    page_title="Example of using DuckDB with Cloudflare R2",
    page_icon="✅",
    layout="wide",
                  )
col1, col2 = st.columns([3, 1])
################################
#make sure you don't include http into the endpoint
@st.experimental_singleton
def define_view():
    con=duckdb.connect()
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
###############################
SQL = st.text_input('Write a SQL Query, Streamlit Cache the results of existing Queries', 'select * from lineitem limit 5')
@st.experimental_memo (persist="disk")
def get_data(SQL):
  return con.execute(SQL).df()
try :
  start = timer()
  df = get_data(SQL) 
  end = timer()
  st.write("Duration in Second")
  st.write(round(end - start),2)
  st.write(df)
except Exception as er:
 st.write(er)

################################################################################
def convert_df(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return df.to_csv().encode('utf-8')

csv = convert_df(df)
col2.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='large_df.csv',
            mime='text/csv',
        )
