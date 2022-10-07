import streamlit as st
import duckdb
import s3fs
import pyarrow.dataset as ds
base = "delta/lineitem/"
fs1 = s3fs.S3FileSystem(
      key=  st.secrets["aws_access_key_id_secret"],
      secret= st.secrets["aws_secret_access_key_secret"] ,
      client_kwargs={
         'endpoint_url': st.secrets["endpoint_url_secret"] 
      }
   )
con=duckdb.connect()
xxx = ds.dataset(base, filesystem=fs1,format="parquet",partitioning="hive")
st.write(xxx.schema)
con.register('lineitem',xxx)

SQL = st.text_input('Write a SQL Query', 'select  *  from lineitem where year = 2000 limit 1')

df = con.execute(SQL).df()
st.write(df)
