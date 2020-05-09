#!/usr/bin/env python
# coding: utf-8

# # STEP 6: Repeat the computation from the facts & dimension table
# 
# Note: You will not have to write any code in this notebook. It's purely to illustrate the performance difference between Star and 3NF schemas.
# 
# Start by running the code in the cell below to connect to the database.

# In[ ]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-data.sql')


# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila_star'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# ## 6.1 Facts Table has all the needed dimensions, no need for deep joins

# In[2]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT movie_key, date_key, customer_key, sales_amount\nFROM factSales \nlimit 5;')


# ## 6.2 Join fact table with dimensions to replace keys with attributes
# 
# As you run each cell, pay attention to the time that is printed. Which schema do you think will run faster?
# 
# ##### Star Schema

# In[3]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dimMovie.title, dimDate.month, dimCustomer.city, sum(sales_amount) as revenue\nFROM factSales \nJOIN dimMovie    on (dimMovie.movie_key      = factSales.movie_key)\nJOIN dimDate     on (dimDate.date_key         = factSales.date_key)\nJOIN dimCustomer on (dimCustomer.customer_key = factSales.customer_key)\ngroup by (dimMovie.title, dimDate.month, dimCustomer.city)\norder by dimMovie.title, dimDate.month, dimCustomer.city, revenue desc;')


# ##### 3NF Schema

# In[4]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT f.title, EXTRACT(month FROM p.payment_date) as month, ci.city, sum(p.amount) as revenue\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\ngroup by (f.title, month, ci.city)\norder by f.title, month, ci.city, revenue desc;')


# # Conclusion

# We were able to show that:
# * The star schema is easier to understand and write queries against.
# * Queries with a star schema are more performant.
