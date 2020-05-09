#!/usr/bin/env python
# coding: utf-8

# # Exercise 03 - Columnar Vs Row Storage

# - The columnar storage extension used here: 
#     - cstore_fdw by citus_data [https://github.com/citusdata/cstore_fdw](https://github.com/citusdata/cstore_fdw)
# - The data tables are the ones used by citus_data to show the storage extension
# 

# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')


# ## STEP 0 : Connect to the local database where Pagila is loaded
# 
# ### Create the database

# In[ ]:





# In[2]:


get_ipython().system("sudo -u postgres psql -c 'CREATE DATABASE reviews;'")

get_ipython().system('wget http://examples.citusdata.com/customer_reviews_1998.csv.gz')
get_ipython().system('wget http://examples.citusdata.com/customer_reviews_1999.csv.gz')

get_ipython().system('gzip -d customer_reviews_1998.csv.gz ')
get_ipython().system('gzip -d customer_reviews_1999.csv.gz ')

get_ipython().system('mv customer_reviews_1998.csv /tmp/customer_reviews_1998.csv')
get_ipython().system('mv customer_reviews_1999.csv /tmp/customer_reviews_1999.csv')


# ### Connect to the database

# In[3]:


DB_ENDPOINT = "127.0.0.1"
DB = 'reviews'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)


# In[4]:


get_ipython().run_line_magic('sql', '$conn_string')


# ## STEP 1 :  Create a table with a normal  (Row) storage & load data
# 
# **TODO:** Create a table called customer_reviews_row with the column names contained in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files.

# In[5]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS customer_reviews_row;\nCREATE TABLE customer_reviews_row \n( customer_id text,\n  review_date date,\n  review_rating integer,\n  review_votes integer,\n  review_helpful_votes integer,\n  product_id char(10),\n  product_title text,\n  product_sales_rank bigint,\n  product_group text,\n  product_category text,\n  product_subcategory text,\n  similar_product_ids char(10)[]\n)')


# In[ ]:





# **TODO:** Use the [COPY statement](https://www.postgresql.org/docs/9.2/sql-copy.html) to populate the tables with the data in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files. You can access the files in the `/tmp/` folder.

# In[6]:


get_ipython().run_cell_magic('sql', '', "COPY customer_reviews_row FROM '/tmp/customer_reviews_1998.csv' WITH CSV;\nCOPY customer_reviews_row FROM '/tmp/customer_reviews_1999.csv' WITH CSV;")


# ## STEP 2 :  Create a table with columnar storage & load data
# 
# First, load the extension to use columnar storage in Postgres.

# In[7]:


get_ipython().run_cell_magic('sql', '', '\n-- load extension first time after install\nCREATE EXTENSION cstore_fdw;\n\n-- create server object\nCREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;')


# **TODO:** Create a `FOREIGN TABLE` called `customer_reviews_col` with the column names contained in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files.

# In[8]:


get_ipython().run_cell_magic('sql', '', "-- create foreign table\nDROP FOREIGN TABLE IF EXISTS customer_reviews_col;\n\n-------------\nCREATE FOREIGN TABLE customer_reviews_col\n( customer_id text,\n  review_date date,\n  review_rating integer,\n  review_votes integer,\n  review_helpful_votes integer,\n  product_id char(10),\n  product_title text,\n  product_sales_rank bigint,\n  product_group text,\n  product_category text,\n  product_subcategory text,\n  similar_product_ids char(10)[]\n)\n\n-------------\n-- leave code below as is\nSERVER cstore_server\nOPTIONS(compression 'pglz');")


# **TODO:** Use the [COPY statement](https://www.postgresql.org/docs/9.2/sql-copy.html) to populate the tables with the data in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files. You can access the files in the `/tmp/` folder.

# In[9]:


get_ipython().run_cell_magic('sql', '', "COPY customer_reviews_col FROM '/tmp/customer_reviews_1998.csv' WITH CSV;\nCOPY customer_reviews_col FROM '/tmp/customer_reviews_1999.csv' WITH CSV;")


# ## Step 3: Compare perfromamce
# 
# Now run the same query on the two tables and compare the run time. Which form of storage is more performant?
# 
# **TODO**: Write a query that calculates the average `review_rating` by `product_title` for all reviews in 1995. Sort the data by `review_rating` in descending order. Limit the results to 20.
# 
# First run the query on `customer_reviews_row`:

# In[12]:


get_ipython().run_cell_magic('time', '', "%%sql\n\nSELECT product_title, AVG(review_rating) AS avg_rating\nFROM customer_reviews_row\nWHERE review_date >= '1995-01-01' \nAND review_date <= '1995-12-31'\nGROUP BY product_title\nORDER BY avg_rating DESC\nLIMIT 20;")


#  Then on `customer_reviews_col`:

# In[13]:


get_ipython().run_cell_magic('time', '', "%%sql\n\nSELECT product_title, AVG(review_rating) AS avg_rating\nFROM customer_reviews_col\nWHERE review_date >= '1995-01-01' \nAND review_date <= '1995-12-31'\nGROUP BY product_title\nORDER BY avg_rating DESC\nLIMIT 20;")


# ## Conclusion: We can see that the columnar storage is faster!
