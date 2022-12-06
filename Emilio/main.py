import psycopg2 # lets me connect to and work with the database 
from tasks import SimpleTask    #lets me use the module tasks
import schedule # lets me implement a scheduler 
import time #lets me implement the scheduler 

#creating a cursor will allow me to establish connection to my dvdrental database 
def create_cursor(): 
    conn = psycopg2.connect(host="localhost",port="5432",dbname="dvdrental",user="postgres",password="Elmopork1@1990#")
    cur = conn.cursor() 
    print("Connection Established")
    return cur, conn

#it runs a given SQL statement, with given curson and connection already established to the dvdrental database
def run_sql(sql,cur,conn):
    cur.execute(sql)
    conn.commit()
    return 



#defining a function that creates the customer Table. It uses cursor and connection object to do it and uses a SQL statement to do it.
def sk_customer(cur,conn):
    sql= "CREATE TABLE IF NOT EXISTS dssa.CUSTOMER as (SELECT customer_id as sk_customer, CONCAT(first_name, ' ', last_name) as name, email from customer)"
    sql2 = "ALTER TABLE dssa.customer ADD Primary Key (sk_customer)"

    run_sql(sql,cur,conn)
    run_sql(sql2,cur,conn)
   
#defining a function that creates the date Table. It uses cursor and connection object to do it and uses a SQL statement to do it.
def sk_date(cur,conn):
    sql = 'CREATE TABLE IF NOT EXISTS dssa.DATE as (SELECT DISTINCT rental_date as sk_date,EXTRACT(YEAR from rental_date)as YEAR,EXTRACT(MONTH from rental_date)as MONTH,EXTRACT(DAY from rental_date)as DAY,EXTRACT(QUARTER FROM rental_date) as QUARTER FROM rental)'
    sql2 = "ALTER TABLE dssa.date ADD Primary Key (sk_date)"
   
    run_sql(sql,cur,conn)
    run_sql(sql2,cur,conn)

#defining a function that creates the film Table. It uses cursor and connection object to do it and uses a SQL statement to do it.
def sk_film(cur,conn):
    sql =  'CREATE TABLE IF NOT EXISTS dssa.film as (SELECT film_id as sk_film, (SELECT rating as RATING_CODE), (SELECT length as FILM_DURATION), (SELECT rental_duration as RENTAL_DURATION), (SELECT language_id  as LANGUAGE), (SELECT release_year as RELEASE_YEAR), (SELECT title as TITLE) FROM film)'
    sql2 = "ALTER TABLE dssa.film ADD Primary Key (sk_film)"
    
    run_sql(sql,cur,conn)
    run_sql(sql2,cur,conn)
    
#defining a function that creates the store Table. It uses cursor and connection object to do it and uses a SQL statement to do it.
def sk_store(cur,conn):
    sql =  "CREATE TABLE IF NOT EXISTS dssa.Store as (SELECT store_id as sk_store, (SELECT CONCAT(first_name, ' ', last_name) as name from staff where store_id = STORE.store_id), (SELECT address from address where address_id = store.address_id), (SELECT city from city where city_id = (SELECT city_id from address where address_id = store.address_id)),(SELECT district as state from address where address_id = store.address_id), (SELECT country from country where country_id = (SELECT country_id from city where city_id = (SELECT city_id from address where address_id = store.address_id))) FROM store)"
    sql2 = "ALTER TABLE dssa.store ADD Primary Key (sk_store)"
    
    run_sql(sql,cur,conn)
    run_sql(sql2,cur,conn)
    
    
#defining a function that creates the staf Table. It uses cursor and connection object to do it and uses a SQL statement to do it.
def sk_staff(cur,conn):
    sql = "CREATE TABLE IF NOT EXISTS dssa.STAFF as (SELECT staff_id as sk_staff, CONCAT(first_name, ' ', last_name) as name, email from staff)"
    sql2 = "ALTER TABLE dssa.staff ADD Primary Key (sk_staff)"

    run_sql(sql,cur,conn)
    run_sql(sql2,cur,conn)

#defining a function that creates the fact_rental Table. Based on the SQL statement that has been used, it needs the previous tables prior to running
def fact_rentals(cur,conn):
    sql = 'CREATE TABLE IF NOT EXISTS dssa.Fact_rental as (SELECT count(rental_id) as fact_rentals,(SELECT customer_id as sk_customer FROM customer where customer_id = rental.customer_id), rental_date as sk_date, (SELECT store_id as sk_store FROM store where store_id = (SELECT store_id from inventory where inventory_id = rental.inventory_id)), (SELECT film_id as sk_film FROM film where film_id = (SELECT film_id from inventory where inventory_id = rental.inventory_id)),(SELECT staff_id as sk_staff FROM staff where staff_id = rental.staff_id)from rental GROUP BY sk_customer, sk_date, sk_store, sk_film, sk_staff)'
    sql2 = 'ALTER TABLE dssa.FACT_RENTAL ADD FOREIGN KEY (sk_customer) REFERENCES dssa.customer(sk_customer), ADD FOREIGN KEY (sk_date) REFERENCES dssa.date(sk_date), ADD FOREIGN KEY (sk_film) REFERENCES dssa.film(sk_film), ADD FOREIGN KEY (sk_store) REFERENCES dssa.store(sk_store), ADD FOREIGN KEY (sk_staff) REFERENCES dssa.staff(sk_staff)'
  
    run_sql(sql,cur,conn)
    run_sql(sql2,cur,conn)
    
 
#defining a function that removes all tables
def demolition(cur,conn):
    sql = 'drop table dssa.fact_rental'
    sql1 = 'drop table dssa.customer'
    sql2 = 'drop table dssa.date'
    sql3 = 'drop table dssa.film'
    sql4 = 'drop table dssa.staff'
    sql5 = 'drop table dssa.store'
    
    run_sql(sql,cur,conn)
    run_sql(sql1,cur,conn)
    run_sql(sql2,cur,conn)
    run_sql(sql3,cur,conn)
    run_sql(sql4,cur,conn)
    run_sql(sql5,cur,conn)
    print("Demolition completed")

#Defines a funcition that closes the connection, once all tables are deleted. 
def disconnect(cur,conn):
    cur.close()
    conn.close()

#creting tasks

connectiontask = SimpleTask(create_cursor)
#connectiontask is a new variable, type SimpleTask, it is instanciated by the function create_cursor

customertask = SimpleTask(sk_customer)
datetask = SimpleTask(sk_date)
filmtask = SimpleTask(sk_film)
storetask = SimpleTask(sk_store)
stafftask = SimpleTask(sk_staff)
factrentalstask = SimpleTask(fact_rentals)
demolitiontask = SimpleTask(demolition)
disconnecttask = SimpleTask(disconnect)


#creating queue
queue = [] #creates an empty queue
queue.append(connectiontask) #
queue.append(customertask)    
queue.append(datetask)
queue.append(filmtask)
queue.append(storetask)
queue.append(stafftask)
queue.append(factrentalstask)
queue.append(demolitiontask)
queue.append(disconnecttask)


#creating a worker, that executes given que. Laboror is a function that takes in the queue        
def laborer(queue):
    counter = 0
    for task in queue: #for each element in the queue which needs to be tasks
        if counter == 0: #start from the beginning
            cur,conn = task.run() #and create a cursor and connection object
            counter = counter + 1 #and then we move the counter up by 1
        else: 
            task.run(cur,conn) #if are not on the first task any more, we execute the current task by using created cursor and connection objects

  
    
#creating scheduller that would run the queue every 15 seconds

schedule.every(15).seconds.do(laborer,queue) #this schedule makes the worker execute a queue every 15 seconds

while True: #
    schedule.run_pending()
    time.sleep(5)
    
    
    

