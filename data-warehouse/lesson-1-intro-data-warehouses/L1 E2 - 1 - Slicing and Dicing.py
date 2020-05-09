#!/usr/bin/env python
# coding: utf-8

# # Exercise 02 -  OLAP Cubes - Slicing and Dicing

# All the databases table in this demo are based on public database samples and transformations
# - `Sakila` is a sample database created by `MySql` [Link](https://dev.mysql.com/doc/sakila/en/sakila-structure.html)
# - The postgresql version of it is called `Pagila` [Link](https://github.com/devrimgunduz/pagila)
# - The facts and dimension tables design is based on O'Reilly's public dimensional modelling tutorial schema [Link](http://archive.oreilly.com/oreillyschool/courses/dba3/index.html)
# 
# Start by creating and connecting to the database by running the cells below.

# In[1]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-star.sql')


# ### Connect to the local database where Pagila is loaded

# In[2]:


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

# # Start with a simple cube
# TODO: Write a query that calculates the revenue (sales_amount) by day, rating, and city. Remember to join with the appropriate dimension tables to replace the keys with the dimension labels. Sort by revenue in descending order and limit to the first 20 rows. The first few rows of your output should match the table below.

# In[4]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dd.day, dm.rating, dc.city, SUM(fs.sales_amount) AS revenue\nFROM factsales fs\nJOIN dimdate dd ON fs.date_key = dd.date_key\nJOIN dimcustomer dc ON fs.customer_key = dc.customer_key\nJOIN dimmovie dm ON fs.movie_key = dm.movie_key\nGROUP BY (dd.day, dm.rating, dc.city)\nORDER BY revenue DESC\nLIMIT 20;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>day</th>
#         <th>rating</th>
#         <th>city</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>G</td>
#         <td>San Bernardino</td>
#         <td>24.97</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>NC-17</td>
#         <td>Apeldoorn</td>
#         <td>23.95</td>
#     </tr>
#     <tr>
#         <td>21</td>
#         <td>NC-17</td>
#         <td>Belm</td>
#         <td>22.97</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>Zanzibar</td>
#         <td>21.97</td>
#     </tr>
#     <tr>
#         <td>28</td>
#         <td>R</td>
#         <td>Mwanza</td>
#         <td>21.97</td>
#     </tr>
# </tbody></table></div>

# ## Slicing
# 
# Slicing is the reduction of the dimensionality of a cube by 1 e.g. 3 dimensions to 2, fixing one of the dimensions to a single value. In the example above, we have a 3-dimensional cube on day, rating, and country.
# 
# TODO: Write a query that reduces the dimensionality of the above example by limiting the results to only include movies with a `rating` of "PG-13". Again, sort by revenue in descending order and limit to the first 20 rows. The first few rows of your output should match the table below. 

# In[5]:


get_ipython().run_cell_magic('time', '', "%%sql\nSELECT dd.day, dm.rating, dc.city, SUM(fs.sales_amount) AS revenue\nFROM factsales fs\nJOIN dimdate dd ON fs.date_key = dd.date_key\nJOIN dimcustomer dc ON fs.customer_key = dc.customer_key\nJOIN dimmovie dm ON fs.movie_key = dm.movie_key\nWHERE dm.rating = 'PG-13'\nGROUP BY (dd.day, dm.rating, dc.city)\nORDER BY revenue DESC\nLIMIT 20;")


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>day</th>
#         <th>rating</th>
#         <th>city</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>Zanzibar</td>
#         <td>21.97</td>
#     </tr>
#     <tr>
#         <td>28</td>
#         <td>PG-13</td>
#         <td>Dhaka</td>
#         <td>19.97</td>
#     </tr>
#     <tr>
#         <td>29</td>
#         <td>PG-13</td>
#         <td>Shimoga</td>
#         <td>18.97</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>Osmaniye</td>
#         <td>18.97</td>
#     </tr>
#     <tr>
#         <td>21</td>
#         <td>PG-13</td>
#         <td>Asuncin</td>
#         <td>18.95</td>
#     </tr>
# </tbody></table></div>

# ## Dicing
# Dicing is creating a subcube with the same dimensionality but fewer values for  two or more dimensions. 
# 
# TODO: Write a query to create a subcube of the initial cube that includes moves with:
# * ratings of PG or PG-13
# * in the city of Bellevue or Lancaster
# * day equal to 1, 15, or 30
# 
# The first few rows of your output should match the table below. 

# In[6]:


get_ipython().run_cell_magic('time', '', "%%sql\nSELECT dd.day, dm.rating, dc.city, SUM(fs.sales_amount) AS revenue\nFROM factsales fs\nJOIN dimdate dd ON fs.date_key = dd.date_key\nJOIN dimcustomer dc ON fs.customer_key = dc.customer_key\nJOIN dimmovie dm ON fs.movie_key = dm.movie_key\nWHERE dm.rating in ('PG-13', 'PG')\nAND dc.city in ('Bellevue', 'Lancaster')\nAND dd.day in (1, 15, 30)\nGROUP BY (dd.day, dm.rating, dc.city)\nORDER BY revenue DESC\nLIMIT 20;")


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>day</th>
#         <th>rating</th>
#         <th>city</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG</td>
#         <td>Lancaster</td>
#         <td>12.98</td>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>PG-13</td>
#         <td>Lancaster</td>
#         <td>5.99</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>Bellevue</td>
#         <td>3.99</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>Lancaster</td>
#         <td>2.99</td>
#     </tr>
#     <tr>
#         <td>15</td>
#         <td>PG-13</td>
#         <td>Bellevue</td>
#         <td>1.98</td>
#     </tr>
# </tbody></table></div>
