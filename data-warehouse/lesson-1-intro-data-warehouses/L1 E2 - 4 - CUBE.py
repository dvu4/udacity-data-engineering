#!/usr/bin/env python
# coding: utf-8

# # Exercise 02 -  OLAP Cubes - CUBE

# All the databases table in this demo are based on public database samples and transformations
# - `Sakila` is a sample database created by `MySql` [Link](https://dev.mysql.com/doc/sakila/en/sakila-structure.html)
# - The postgresql version of it is called `Pagila` [Link](https://github.com/devrimgunduz/pagila)
# - The facts and dimension tables design is based on O'Reilly's public dimensional modelling tutorial schema [Link](http://archive.oreilly.com/oreillyschool/courses/dba3/index.html)
# 
# Start by connecting to the database by running the cells below. If you are coming back to this exercise, then uncomment and run the first cell to recreate the database. If you recently completed the slicing and dicing exercise, then skip to the second cell.

# In[ ]:


# !PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star
# !PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-star.sql


# ### Connect to the local database where Pagila is loaded

# In[1]:


import sql
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


# ### Star Schema

# <img src="pagila-star.png" width="50%"/>

# # CUBE 
# - Group by CUBE (dim1, dim2, ..) , produces all combinations of different lenghts in one go.
# - This view could be materialized in a view and queried which would save lots repetitive aggregations

# TODO: Write a query that calculates the various levels of aggregation done in the grouping sets exercise (total, by month, by country, by month & country) using the CUBE function. Your output should match the table below.
# 

# In[2]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dd.month, ds.country, SUM(fs.sales_amount) AS revenue\nFROM factsales fs\nJOIN dimdate  dd ON fs.date_key     = dd.date_key\nJOIN dimstore ds ON fs.store_key    = ds.store_key\nGROUP BY cube (dd.month, ds.country);')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>month</th>
#         <th>country</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>Australia</td>
#         <td>2364.19</td>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>Canada</td>
#         <td>2460.24</td>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>None</td>
#         <td>4824.43</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>Australia</td>
#         <td>4895.10</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>Canada</td>
#         <td>4736.78</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>None</td>
#         <td>9631.88</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>Australia</td>
#         <td>12060.33</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>Canada</td>
#         <td>11826.23</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>None</td>
#         <td>23886.56</td>
#     </tr>
#     <tr>
#         <td>4</td>
#         <td>Australia</td>
#         <td>14136.07</td>
#     </tr>
#     <tr>
#         <td>4</td>
#         <td>Canada</td>
#         <td>14423.39</td>
#     </tr>
#     <tr>
#         <td>4</td>
#         <td>None</td>
#         <td>28559.46</td>
#     </tr>
#     <tr>
#         <td>5</td>
#         <td>Australia</td>
#         <td>271.08</td>
#     </tr>
#     <tr>
#         <td>5</td>
#         <td>Canada</td>
#         <td>243.10</td>
#     </tr>
#     <tr>
#         <td>5</td>
#         <td>None</td>
#         <td>514.18</td>
#     </tr>
#     <tr>
#         <td>None</td>
#         <td>None</td>
#         <td>67416.51</td>
#     </tr>
#     <tr>
#         <td>None</td>
#         <td>Australia</td>
#         <td>33726.77</td>
#     </tr>
#     <tr>
#         <td>None</td>
#         <td>Canada</td>
#         <td>33689.74</td>
#     </tr>
# </tbody></table></div>

# ## Revenue Total, by Month, by Country, by Month & Country All in one shot, NAIVE way
# The naive way to create the same table as above is to write several queries and UNION them together. Grouping sets and cubes produce queries that are shorter to write, easier to read, and more performant. Run the naive query below and compare the time it takes to run to the time it takes the cube query to run.

# In[3]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT  NULL as month, NULL as country, sum(sales_amount) as revenue\nFROM factSales\n    UNION all \nSELECT NULL, dimStore.country,sum(sales_amount) as revenue\nFROM factSales\nJOIN dimStore on (dimStore.store_key = factSales.store_key)\nGROUP by  dimStore.country\n    UNION all \nSELECT cast(dimDate.month as text) , NULL, sum(sales_amount) as revenue\nFROM factSales\nJOIN dimDate on (dimDate.date_key = factSales.date_key)\nGROUP by dimDate.month\n    UNION all\nSELECT cast(dimDate.month as text),dimStore.country,sum(sales_amount) as revenue\nFROM factSales\nJOIN dimDate     on (dimDate.date_key         = factSales.date_key)\nJOIN dimStore on (dimStore.store_key = factSales.store_key)\nGROUP by (dimDate.month, dimStore.country)')


# In[ ]:




