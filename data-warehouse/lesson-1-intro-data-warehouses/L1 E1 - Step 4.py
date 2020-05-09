#!/usr/bin/env python
# coding: utf-8

# # STEP4 : Creating Facts & Dimensions
# Start by connecting to the database by running the cells below. If you are coming back to this exercise, then uncomment and run the first cell to recreate the database. If you recently completed steps 1 and 2, then skip to the second cell.

# In[1]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql')


# In[2]:


get_ipython().run_line_magic('load_ext', 'sql')

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# ### Star Schema - Entity Relationship Diagram

# <img src="pagila-star.png" width="50%"/>

# #### Create the first dimension table
# TODO: Create the dimDate dimension table with the fields and data types shown in the ERD above.

# In[3]:


get_ipython().run_cell_magic('sql', '', 'CREATE TABLE dimDate\n(\n    date_key   int NOT NULL PRIMARY KEY,\n    date       date NOT NULL, \n    year       smallint NOT NULL, \n    quarter    smallint NOT NULL,\n    month      smallint NOT NULL,\n    day        smallint NOT NULL,\n    week       smallint NOT NULL,\n    is_weekend bool \n);')


# To check your work, run the following query to see a table with the field names and data types. The output should match the table below.

# In[4]:


get_ipython().run_cell_magic('sql', '', "SELECT column_name, data_type\nFROM information_schema.columns\nWHERE table_name   = 'dimdate'")


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>column_name</th>
#         <th>data_type</th>
#     </tr>
#     <tr>
#         <td>date_key</td>
#         <td>integer</td>
#     </tr>
#     <tr>
#         <td>date</td>
#         <td>date</td>
#     </tr>
#     <tr>
#         <td>year</td>
#         <td>smallint</td>
#     </tr>
#     <tr>
#         <td>quarter</td>
#         <td>smallint</td>
#     </tr>
#     <tr>
#         <td>month</td>
#         <td>smallint</td>
#     </tr>
#     <tr>
#         <td>day</td>
#         <td>smallint</td>
#     </tr>
#     <tr>
#         <td>week</td>
#         <td>smallint</td>
#     </tr>
#     <tr>
#         <td>is_weekend</td>
#         <td>boolean</td>
#     </tr>
# </tbody></table></div>

# Run the cell below to create the rest of the dimension tables.

# In[5]:


get_ipython().run_cell_magic('sql', '', 'CREATE TABLE dimCustomer\n(\n  customer_key SERIAL PRIMARY KEY,\n  customer_id  smallint NOT NULL,\n  first_name   varchar(45) NOT NULL,\n  last_name    varchar(45) NOT NULL,\n  email        varchar(50),\n  address      varchar(50) NOT NULL,\n  address2     varchar(50),\n  district     varchar(20) NOT NULL,\n  city         varchar(50) NOT NULL,\n  country      varchar(50) NOT NULL,\n  postal_code  varchar(10),\n  phone        varchar(20) NOT NULL,\n  active       smallint NOT NULL,\n  create_date  timestamp NOT NULL,\n  start_date   date NOT NULL,\n  end_date     date NOT NULL\n);\n\nCREATE TABLE dimMovie\n(\n  movie_key          SERIAL PRIMARY KEY,\n  film_id            smallint NOT NULL,\n  title              varchar(255) NOT NULL,\n  description        text,\n  release_year       year,\n  language           varchar(20) NOT NULL,\n  original_language  varchar(20),\n  rental_duration    smallint NOT NULL,\n  length             smallint NOT NULL,\n  rating             varchar(5) NOT NULL,\n  special_features   varchar(60) NOT NULL\n);\nCREATE TABLE dimStore\n(\n  store_key           SERIAL PRIMARY KEY,\n  store_id            smallint NOT NULL,\n  address             varchar(50) NOT NULL,\n  address2            varchar(50),\n  district            varchar(20) NOT NULL,\n  city                varchar(50) NOT NULL,\n  country             varchar(50) NOT NULL,\n  postal_code         varchar(10),\n  manager_first_name  varchar(45) NOT NULL,\n  manager_last_name   varchar(45) NOT NULL,\n  start_date          date NOT NULL,\n  end_date            date NOT NULL\n);')


# #### Create the fact table
# TODO: Create the factSales table with the fields and data types shown in the ERD above. 
# 
# **Note on REFERENCES constraints:**<br> 
# The demo video does not cover the REFERENCES constraint. When building a fact table, you use the REFERENCES constrain to identify which table and column a foreign key is connected to. This ensures that the fact table does not refer to items that do not appear in the respective dimension tables. You can read more [here](https://www.postgresql.org/docs/9.2/ddl-constraints.html). Here's an example of the syntax on a different schema:
# 
# ```
# CREATE TABLE orders (
#     order_id integer PRIMARY KEY,
#     product_no integer REFERENCES products (product_no),
#     quantity integer
# );
# ```
# 

# In[6]:


# define a foreign key constraint in the factSales table that references that table


# In[7]:


get_ipython().run_cell_magic('sql', '', 'CREATE TABLE factSales\n(\n    sales_key    int NOT NULL PRIMARY KEY,\n    date_key     int REFERENCES dimDate (date_key) ,\n    customer_ke  int REFERENCES dimCustomer (customer_key),\n    movie_key    int REFERENCES dimMovie (movie_key) ,\n    store_key    int REFERENCES dimStore (store_key)  ,\n    sales_amount numeric NOT NULL \n);\n')


# To check your work, run the following query to see a table with the field names and data types. The output should match the table below.

# In[8]:


get_ipython().run_cell_magic('sql', '', "SELECT column_name, data_type\nFROM information_schema.columns\nWHERE table_name   = 'factsales'")


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>column_name</th>
#         <th>data_type</th>
#     </tr>
#     <tr>
#         <td>sales_key</td>
#         <td>integer</td>
#     </tr>
#     <tr>
#         <td>date_key</td>
#         <td>integer</td>
#     </tr>
#     <tr>
#         <td>customer_key</td>
#         <td>integer</td>
#     </tr>
#     <tr>
#         <td>movie_key</td>
#         <td>integer</td>
#     </tr>
#     <tr>
#         <td>store_key</td>
#         <td>integer</td>
#     </tr>
#     <tr>
#         <td>sales_amount</td>
#         <td>numeric</td>
#     </tr>
# </tbody></table></div>

# In[ ]:





# If you need to delete the table and start over, use the DROP TABLE command: `DROP TABLE <table_name>`
# 

# In[9]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE factSales')


# In[10]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE dimDate ')


# In[11]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE dimCustomer ')


# In[12]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE dimMovie ')


# In[13]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE dimStore')


# In[ ]:




