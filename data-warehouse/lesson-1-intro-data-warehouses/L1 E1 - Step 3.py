#!/usr/bin/env python
# coding: utf-8

# # STEP3: Perform some simple data analysis
# 
# Start by connecting to the database by running the cells below. If you are coming back to this exercise, then uncomment and run the first cell to recreate the database. If you recently completed steps 1 and 2, then skip to the second cell.

# In[4]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql')


# In[5]:


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


# ### 3NF - Entity Relationship Diagram
# 
# <img src="./pagila-3nf.png" width="50%"/>
# 
# ## 3.1 Insight 1:   Top Grossing Movies 
# - Payments amounts are in table `payment`
# - Movies are in table `film`
# - They are not directly linked, `payment` refers to a `rental`, `rental` refers to an `inventory` item and `inventory` item refers to a `film`
# - `payment` &rarr; `rental` &rarr; `inventory` &rarr; `film`

# ### 3.1.1 Films

# In[6]:


get_ipython().run_cell_magic('sql', '', 'select film_id, title, release_year, rental_rate, rating  from film limit 5;')


# ### 3.1.2 Payments

# In[7]:


get_ipython().run_cell_magic('sql', '', 'select * from payment limit 5;')


# ### 3.1.3 Inventory

# In[8]:


get_ipython().run_cell_magic('sql', '', 'select * from inventory limit 5;')


# ### 3.1.4 Get the movie of every payment

# In[9]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, p.amount, p.payment_date, p.customer_id                                            \nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nlimit 5;')


# ### 3.1.5 sum movie rental revenue
# TODO: Write a query that displays the amount of revenue from each title. Limit the results to the top 10 grossing titles. Your results should match the table below.

# In[11]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, sum(p.amount) as revenue                                     \nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\ngroup by f.title \norder by revenue DESC\nlimit 10;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>title</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>TELEGRAPH VOYAGE</td>
#         <td>231.73</td>
#     </tr>
#     <tr>
#         <td>WIFE TURN</td>
#         <td>223.69</td>
#     </tr>
#     <tr>
#         <td>ZORRO ARK</td>
#         <td>214.69</td>
#     </tr>
#     <tr>
#         <td>GOODFELLAS SALUTE</td>
#         <td>209.69</td>
#     </tr>
#     <tr>
#         <td>SATURDAY LAMBS</td>
#         <td>204.72</td>
#     </tr>
#     <tr>
#         <td>TITANS JERK</td>
#         <td>201.71</td>
#     </tr>
#     <tr>
#         <td>TORQUE BOUND</td>
#         <td>198.72</td>
#     </tr>
#     <tr>
#         <td>HARRY IDAHO</td>
#         <td>195.70</td>
#     </tr>
#     <tr>
#         <td>INNOCENT USUAL</td>
#         <td>191.74</td>
#     </tr>
#     <tr>
#         <td>HUSTLER PARTY</td>
#         <td>190.78</td>
#     </tr>
# </tbody></table></div>

# ## 3.2 Insight 2:   Top grossing cities 
# - Payments amounts are in table `payment`
# - Cities are in table `cities`
# - `payment` &rarr; `customer` &rarr; `address` &rarr; `city`

# ### 3.2.1 Get the city of each payment

# In[12]:


get_ipython().run_cell_magic('sql', '', 'SELECT p.customer_id, p.rental_id, p.amount, ci.city                            \nFROM payment p\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\norder by p.payment_date\nlimit 10;')


# ### 3.2.2 Top grossing cities
# TODO: Write a query that returns the total amount of revenue by city as measured by the `amount` variable in the `payment` table. Limit the results to the top 10 cities. Your result should match the table below.

# In[15]:


get_ipython().run_cell_magic('sql', '', 'SELECT ci.city  , sum(p.amount) as revenue                          \nFROM payment p\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\ngroup by ci.city\norder by revenue DESC\nlimit 10;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>city</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>Cape Coral</td>
#         <td>221.55</td>
#     </tr>
#     <tr>
#         <td>Saint-Denis</td>
#         <td>216.54</td>
#     </tr>
#     <tr>
#         <td>Aurora</td>
#         <td>198.50</td>
#     </tr>
#     <tr>
#         <td>Molodetno</td>
#         <td>195.58</td>
#     </tr>
#     <tr>
#         <td>Apeldoorn</td>
#         <td>194.61</td>
#     </tr>
#     <tr>
#         <td>Santa Brbara dOeste</td>
#         <td>194.61</td>
#     </tr>
#     <tr>
#         <td>Qomsheh</td>
#         <td>186.62</td>
#     </tr>
#     <tr>
#         <td>London</td>
#         <td>180.52</td>
#     </tr>
#     <tr>
#         <td>Ourense (Orense)</td>
#         <td>177.60</td>
#     </tr>
#     <tr>
#         <td>Bijapur</td>
#         <td>175.61</td>
#     </tr>
# </tbody></table></div>

# ## 3.3 Insight 3 : Revenue of a movie by customer city and by month 

# ### 3.3.1 Total revenue by month

# In[16]:


get_ipython().run_cell_magic('sql', '', 'SELECT sum(p.amount) as revenue, EXTRACT(month FROM p.payment_date) as month\nfrom payment p\ngroup by month\norder by revenue desc\nlimit 10;')


# ### 3.3.2 Each movie by customer city and by month (data cube)

# In[17]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, p.amount, p.customer_id, ci.city, p.payment_date,EXTRACT(month FROM p.payment_date) as month\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\norder by p.payment_date\nlimit 10;')


# ### 3.3.3 Sum of revenue of each movie by customer city and by month
# 
# TODO: Write a query that returns the total amount of revenue for each movie by customer city and by month. Limit the results to the top 10 movies. Your result should match the table below.

# In[25]:


get_ipython().run_cell_magic('sql', '', 'SELECT f.title, ci.city, EXTRACT(month FROM p.payment_date) as month, sum(p.amount) as revenue\nFROM payment p\nJOIN rental r    ON ( p.rental_id = r.rental_id )\nJOIN inventory i ON ( r.inventory_id = i.inventory_id )\nJOIN film f ON ( i.film_id = f.film_id)\nJOIN customer c  ON ( p.customer_id = c.customer_id )\nJOIN address a ON ( c.address_id = a.address_id )\nJOIN city ci ON ( a.city_id = ci.city_id )\ngroup by month, f.title, ci.city\norder by month, revenue DESC\nlimit 10;')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>title</th>
#         <th>city</th>
#         <th>month</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>SHOW LORD</td>
#         <td>Mannheim</td>
#         <td>1.0</td>
#         <td>11.99</td>
#     </tr>
#     <tr>
#         <td>AMERICAN CIRCUS</td>
#         <td>Callao</td>
#         <td>1.0</td>
#         <td>10.99</td>
#     </tr>
#     <tr>
#         <td>CASUALTIES ENCINO</td>
#         <td>Warren</td>
#         <td>1.0</td>
#         <td>10.99</td>
#     </tr>
#     <tr>
#         <td>TELEGRAPH VOYAGE</td>
#         <td>Naala-Porto</td>
#         <td>1.0</td>
#         <td>10.99</td>
#     </tr>
#     <tr>
#         <td>KISSING DOLLS</td>
#         <td>Toulon</td>
#         <td>1.0</td>
#         <td>10.99</td>
#     </tr>
#     <tr>
#         <td>MILLION ACE</td>
#         <td>Bergamo</td>
#         <td>1.0</td>
#         <td>9.99</td>
#     </tr>
#     <tr>
#         <td>TITANS JERK</td>
#         <td>Kimberley</td>
#         <td>1.0</td>
#         <td>9.99</td>
#     </tr>
#     <tr>
#         <td>DARKO DORADO</td>
#         <td>Bhilwara</td>
#         <td>1.0</td>
#         <td>9.99</td>
#     </tr>
#     <tr>
#         <td>SUNRISE LEAGUE</td>
#         <td>Nagareyama</td>
#         <td>1.0</td>
#         <td>9.99</td>
#     </tr>
#     <tr>
#         <td>MILLION ACE</td>
#         <td>Gaziantep</td>
#         <td>1.0</td>
#         <td>9.99</td>
#     </tr>
# </tbody></table></div>
