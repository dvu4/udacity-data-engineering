#!/usr/bin/env python
# coding: utf-8

# # Exercise 02 -  OLAP Cubes - Roll Up and Drill Down

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

# ## Roll-up
# - Stepping up the level of aggregation to a large grouping
# - e.g.`city` is summed as `country`
# 
# TODO: Write a query that calculates revenue (sales_amount) by day, rating, and country. Sort the data by revenue in descending order, and limit the data to the top 20 results. The first few rows of your output should match the table below.

# In[2]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dd.day, dm.rating, dc.country, SUM(fs.sales_amount) AS revenue\nFROM factsales fs\nJOIN dimdate dd ON fs.date_key = dd.date_key\nJOIN dimcustomer dc ON fs.customer_key = dc.customer_key\nJOIN dimmovie dm ON fs.movie_key = dm.movie_key\nGROUP BY (dd.day, dm.rating, dc.country)\nORDER BY revenue DESC\nLIMIT 20;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>day</th>
#         <th>rating</th>
#         <th>country</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>G</td>
#         <td>China</td>
#         <td>169.67</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG</td>
#         <td>India</td>
#         <td>156.67</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>NC-17</td>
#         <td>India</td>
#         <td>153.64</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>China</td>
#         <td>146.67</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>R</td>
#         <td>China</td>
#         <td>145.66</td>
#     </tr>
# </tbody></table></div>

# ## Drill-down
# - Breaking up one of the dimensions to a lower level.
# - e.g.`city` is broken up into  `districts`
# 
# TODO: Write a query that calculates revenue (sales_amount) by day, rating, and district. Sort the data by revenue in descending order, and limit the data to the top 20 results. The first few rows of your output should match the table below.

# In[4]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT dd.day, dm.rating, dc.district, SUM(fs.sales_amount) AS revenue\nFROM factsales fs\nJOIN dimdate dd ON fs.date_key = dd.date_key\nJOIN dimcustomer dc ON fs.customer_key = dc.customer_key\nJOIN dimmovie dm ON fs.movie_key = dm.movie_key\nGROUP BY (dd.day, dm.rating, dc.district)\nORDER BY revenue DESC\nLIMIT 20;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>day</th>
#         <th>rating</th>
#         <th>district</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>PG-13</td>
#         <td>Southern Tagalog</td>
#         <td>53.88</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>G</td>
#         <td>Inner Mongolia</td>
#         <td>38.93</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>G</td>
#         <td>Shandong</td>
#         <td>36.93</td>
#     </tr>
#     <tr>
#         <td>30</td>
#         <td>NC-17</td>
#         <td>West Bengali</td>
#         <td>36.92</td>
#     </tr>
#     <tr>
#         <td>17</td>
#         <td>PG-13</td>
#         <td>Shandong</td>
#         <td>34.95</td>
#     </tr>
# </tbody></table></div>
