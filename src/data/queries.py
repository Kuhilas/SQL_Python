import psycopg2
from config import config

def q_person():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def q_pcolumns():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:  # Use context manager for cursor as well
                SQL = 'SELECT * FROM person;'
                cursor.execute(SQL)
                columns = [desc[0] for desc in cursor.description]
                print(columns)

    except psycopg2.Error as error: 
        print(error)

def q_certificates():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:  # Use context manager for cursor as well
                SQL = 'SELECT * FROM certificates;'
                cursor.execute(SQL)
                columns = [desc[0] for desc in cursor.description]
                print(columns)
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
                cursor.close()

    except psycopg2.Error as error: 
        print(error)    

def q_avg_age_person():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:  # Use context manager for cursor as well
                SQL = 'SELECT avg(age) FROM person;'
                cursor.execute(SQL)
                rows = cursor.fetchone()[0]
                average_age = float(rows)
                print(average_age)

    except psycopg2.Error as error:  
        print(error)    

def insert_certificate(cert_id, cert_name, person_id):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                 # Define the SQL query for inserting a certificates into the 'certificate' table
                SQL = """
                    INSERT INTO certificates (id, name, person_id)
                    VALUES (%s, %s, %s);
                """                
                # Execute the query with the provided parameters
                cursor.execute(SQL, (cert_id, cert_name, person_id))

    except psycopg2.Error as error:  
        print(error)    

def insert_person(name, age, student):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_insert = """INSERT INTO person (name, age, student)
                VALUES (%s, %s, %s);"""
                cursor.execute(SQL_insert, (name, age, student))
                con.commit()
                
                print("Person inserted successfully.")
    
    except psycopg2.Error as error:
        print(error) 

def update_person(name, age, student, id):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_update = """ UPDATE person
                SET name = %s,
                    age = %s,
                    student = %s
                WHERE id = %s"""
                cursor.execute(SQL_update, (name, age, student, id))
                con.commit()
                
                print("Person updated successfully.")
    
    except psycopg2.Error as error:
        print(error) 

def update_certificate_name(name, id):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_update = """ UPDATE certificates
                SET name = %s
                WHERE id = %s"""
                cursor.execute(SQL_update, (name, id))
                con.commit()
                
                print("Certificate updated successfully.")
    
    except psycopg2.Error as error:
        print(error) 

def delete_person(id):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_delete = """DELETE FROM person WHERE id = %s;"""
                cursor.execute(SQL_delete, (id,))
                con.commit()
                
                print(f"Person with id {id} deleted successfully.")
    
    except psycopg2.Error as error:
        print(error)

def delete_certificate(id):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_delete = """DELETE FROM certificates WHERE id = %s;"""
                cursor.execute(SQL_delete, (id,))
                con.commit()
                
                print(f"Certificate with id {id} deleted successfully.")
    
    except psycopg2.Error as error:
        print(error)

if __name__ == '__main__':
    # Uncomment function calls to execute them
    
    # Query functions
    q_person()                 # Queries all people in the 'person' table
    q_pcolumns()               # Queries and prints column names in the 'person' table
    q_certificates()           # Queries and prints all certificates
    q_avg_age_person()         # Queries and prints the average age of people

    # Insert functions
    insert_certificate(6, 'Python', 3)  # Inserts a certificate with id=6, name='Python', person_id=3
    insert_person('Jannu', 25, False)   # Inserts a person named 'Jannu', age=25, student=False

    # Update functions
    update_person('Kuhilas', 22, True, 4)  # Updates person with id=4 to have name='Kuhilas', age=22, student=True
    update_certificate_name('AWS', 6)      # Updates certificate with id=6 to have name='AWS'

    # Delete functions
    delete_person(4)    # Deletes person with id=4
    delete_certificate(2)  # Deletes certificate with id=2

    