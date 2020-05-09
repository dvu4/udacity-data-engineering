#!/usr/bin/env python
# coding: utf-8

# # Exercise 1 -  Sakila Star Schema & ETL  
# 
# All the database tables in this demo are based on public database samples and transformations
# - `Sakila` is a sample database created by `MySql` [Link](https://dev.mysql.com/doc/sakila/en/sakila-structure.html)
# - The postgresql version of it is called `Pagila` [Link](https://github.com/devrimgunduz/pagila)
# - The facts and dimension tables design is based on O'Reilly's public dimensional modelling tutorial schema [Link](http://archive.oreilly.com/oreillyschool/courses/dba3/index.html)

# # STEP0: Using ipython-sql
# 
# - Load ipython-sql: `%load_ext sql`
# 
# - To execute SQL queries you write one of the following atop of your cell: 
#     - `%sql`
#         - For a one-liner SQL query
#         - You can access a python var using `$`    
#     - `%%sql`
#         - For a multi-line SQL query
#         - You can **NOT** access a python var using `$`
# 
# 
# - Running a connection string like:
# `postgresql://postgres:postgres@db:5432/pagila` connects to the database
# 

# # STEP1 : Connect to the local database where Pagila is loaded

# ##  1.1 Create the pagila db and fill it with data
# - Adding `"!"` at the beginning of a jupyter cell runs a command in a shell, i.e. we are not running python code but we are running the `createdb` and `psql` postgresql commmand-line utilities

# In[1]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql')


# ## 1.2 Connect to the newly created db

# In[2]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[3]:


DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)


# In[4]:


get_ipython().run_line_magic('sql', '$conn_string')


# # STEP2 : Explore the  3NF Schema

# <img src="./pagila-3nf.png" width="50%"/>

# ## 2.1 How much? What data sizes are we looking at?

# In[5]:


nStores = get_ipython().run_line_magic('sql', 'select count(*) from store;')
nFilms = get_ipython().run_line_magic('sql', 'select count(*) from film;')
nCustomers = get_ipython().run_line_magic('sql', 'select count(*) from customer;')
nRentals = get_ipython().run_line_magic('sql', 'select count(*) from rental;')
nPayment = get_ipython().run_line_magic('sql', 'select count(*) from payment;')
nStaff = get_ipython().run_line_magic('sql', 'select count(*) from staff;')
nCity = get_ipython().run_line_magic('sql', 'select count(*) from city;')
nCountry = get_ipython().run_line_magic('sql', 'select count(*) from country;')

print("nFilms\t\t=", nFilms[0][0])
print("nCustomers\t=", nCustomers[0][0])
print("nRentals\t=", nRentals[0][0])
print("nPayment\t=", nPayment[0][0])
print("nStaff\t\t=", nStaff[0][0])
print("nStores\t\t=", nStores[0][0])
print("nCities\t\t=", nCity[0][0])
print("nCountry\t\t=", nCountry[0][0])


# ## 2.2 When? What time period are we talking about?

# In[6]:


get_ipython().run_cell_magic('sql', '', 'select min(payment_date) as start, max(payment_date) as end from payment;')


# ## 2.3 Where? Where do events in this database occur?
# TODO: Write a query that displays the number of addresses by district in the address table. Limit the table to the top 10 districts. Your results should match the table below.

# In[8]:


get_ipython().run_cell_magic('sql', '', 'select district, sum(city_id) as n #write code here\nfrom address #write code here\ngroup by district\norder by n desc\nlimit 10;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>district</th>
#         <th>n</th>
#     </tr>
#     <tr>
#         <td>Buenos Aires</td>
#         <td>10</td>
#     </tr>
#     <tr>
#         <td>California</td>
#         <td>9</td>
#     </tr>
#     <tr>
#         <td>Shandong</td>
#         <td>9</td>
#     </tr>
#     <tr>
#         <td>West Bengali</td>
#         <td>9</td>
#     </tr>
#     <tr>
#         <td>So Paulo</td>
#         <td>8</td>
#     </tr>
#     <tr>
#         <td>Uttar Pradesh</td>
#         <td>8</td>
#     </tr>
#     <tr>
#         <td>Maharashtra</td>
#         <td>7</td>
#     </tr>
#     <tr>
#         <td>England</td>
#         <td>7</td>
#     </tr>
#     <tr>
#         <td>Southern Tagalog</td>
#         <td>6</td>
#     </tr>
#     <tr>
#         <td>Punjab</td>
#         <td>5</td>
#     </tr>
# </tbody></table></div>
