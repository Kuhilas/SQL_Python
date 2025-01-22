import psycopg2
from config import config

# Elektroniikka alan yritys

def test():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor: 
                SQL = 'SELECT * FROM products;'
                cursor.execute(SQL)
                columns = [desc[0] for desc in cursor.description]
                print(columns)

    except psycopg2.Error as error: 
        print(error)

if __name__ == '__main__':
    test()