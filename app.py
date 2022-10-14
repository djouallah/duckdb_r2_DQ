import s3fs 
import streamlit as st
import duckdb
import pyarrow.dataset as ds
SQL = st.text_input('Write a SQL Query', 'describe  lineitem')
s3 = s3fs.S3FileSystem(
      key=  st.secrets["aws_access_key_id_secret"],
      secret= st.secrets["aws_secret_access_key_secret"] ,
      client_kwargs={
         'endpoint_url': st.secrets["endpoint_url_secret"] 
      }
   )
lineitem = ds.dataset("delta/lineitem/", filesystem=s3,format="parquet", partitioning="hive")
con=duckdb.connect()
con.register("lineitem",lineitem)
df  =con.execute(SQL).df()
st.write(df)
