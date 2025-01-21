import psycopg2
from config import config

# Additional exercises. Creating a table and making multiple transactions

def create_accounts():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        # SQL statement to create the 'accounts' table with a foreign key
        SQL = """
        DROP TABLE IF EXISTS accounts;
        CREATE TABLE accounts (
            account_id SERIAL PRIMARY KEY,
            person_id INT,
            account_number VARCHAR(20),
            account_balance INT,
            FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE
        );
        """
        cursor.execute(SQL)
        con.commit()
        print("New table created successfully")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insert_account(account_number, person_id, account_balance):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL = """
                    INSERT INTO accounts (account_number, person_id, account_balance)
                    VALUES (%s, %s, %s);
                """                

                cursor.execute(SQL, (account_number, person_id, account_balance))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def transfer_money(debit_account, credit_account, amount):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_1 = """
                    UPDATE accounts
                    SET account_balance = account_balance - %s
                    WHERE account_number = %s AND account_balance >= %s;
                """
                cursor.execute(SQL_1, (amount, debit_account, amount))

                # Check if the debit operation was successful
                if cursor.rowcount == 0:
                    print("Insufficient funds in the debit account.")
                    con.rollback()  # Rollback the transaction if there's an issue
                    return

                # SQL query to add money to the credit account
                SQL_2 = """
                    UPDATE accounts
                    SET account_balance = account_balance + %s
                    WHERE account_number = %s;
                """
                cursor.execute(SQL_2, (amount, credit_account))

                con.commit()
                print("Transfer succesfull")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
if __name__ == '__main__':
    # Uncomment function calls to execute them

    create_accounts()
    insert_account('400001-4679876', 2, 10000)
    transfer_money('400001-4679876', '400001-467934', 100)