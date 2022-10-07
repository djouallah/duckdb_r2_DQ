import streamlit as st
import duckdb
import s3fs
import pyarrow.dataset as ds


fs1 = s3fs.S3FileSystem(
      key=  st.secrets["aws_access_key_id_secret"],
      secret= st.secrets["aws_secret_access_key_secret"] ,
      client_kwargs={
         'endpoint_url': st.secrets["endpoint_url_secret"] 
      }
   )

lineitem = ds.dataset("delta/lineitem/", filesystem=fs1,partitioning="hive")
st.write(lineitem.schema)
SQL = st.text_input('Write a SQL Query', 'select  *  from lineitem where year = 2000 limit 1')

con=duckdb.connect()
df = con.execute(SQL).df()
st.write(df)




