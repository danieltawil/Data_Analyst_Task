import sqlite3
import unicodecsv
import schedule
import time

# Create Database
con = sqlite3.connect("bank_mock_database.db")

cur = con.cursor()

# Create Tables
cur.execute("CREATE TABLE IF NOT EXISTS clients (customer_id integer auto_increment PRIMARY KEY,"
            "join_date date NOT NULL,"
            "age integer NOT NULL,"
            "gender varchar(20) NOT NULL,"
            "income decimal NOT NULL,"
            "credit_score integer NOT NULL,"
            "education varchar(30) NOT NULL);")

cur.execute("CREATE TABLE IF NOT EXISTS financials (customer_id integer NOT NULL,"
            "date_id date NOT NULL,"
            "balance_amount decimal NOT NULL,"
            "deposits_amount decimal,"
            "withdrawal_amount decimal,"
            "credit_card_spending decimal,"
            "debit_card_spending decimal,"
            "cash_spending decimal,"
            "FOREIGN KEY (customer_id) REFERENCES clients (customer_id));")

cur.execute("CREATE TABLE IF NOT EXISTS ledger (customer_id integer NOT NULL,"
            "loan_size decimal NOT NULL,"
            "loan_start_date date NOT NULL,"
            "FOREIGN KEY (customer_id) REFERENCES clients (customer_id));")

# Populate the tables
cur.execute("""
    INSERT INTO clients
    VALUES
    (1, '2016/03/21', 26, 'Female', 22000, 732, 'Bachelor Degree'),
    (2, '2018/08/16', 19, 'Male', 10000, 300, 'Middle School'),
    (3, '2017/10/20', 40, 'Female', 50000, 800, 'Masters Degree'),
    (4, '2020/07/03', 25, 'Male', 12000, 450, 'Bachelor Degree'),
    (5, '2019/01/01', 21, 'Male', 10000, 500, 'High School');
""")

with open('clients_financials.txt', 'rb') as input_file:
    reader = unicodecsv.reader(input_file, delimiter="\t")
    data = [row for row in reader]
cur.executemany("INSERT INTO financials VALUES (?, ?, ?, ?, ?, ?, ?, ?);", data)

with open('ledger.txt', 'rb') as input_file:
    reader = unicodecsv.reader(input_file, delimiter="\t")
    data = [row for row in reader]
cur.executemany("INSERT INTO ledger VALUES (?, ?, ?);", data)
con.commit()

# Create a view of Ledger <> Financials
cur.execute("""
    CREATE VIEW financial_status_of_all_clients_with_loans AS
    SELECT l.customer_id, l.loan_size, l.loan_start_date, f.balance_amount, f.deposits_amount, f.withdrawal_amount,
    f.credit_card_spending, f.debit_card_spending, f.cash_spending 
    FROM ledger l LEFT JOIN financials f ON l.customer_id = f.customer_id 
    WHERE l.loan_start_date = f.date_id;
""")


# Update the Financials table every day
def update_financials_table():
    with open('clients_financials.txt', 'rb') as daily_input_file:
        read = unicodecsv.reader(daily_input_file, delimiter="\t")
        new_data = [row for row in read]
    cur.executemany("INSERT INTO financials VALUES (?, ?, ?, ?, ?, ?, ?, ?);", new_data)
    con.commit()


schedule.every().day.at("00:00").do(update_financials_table)

# To prevent Python from using 100% of CPU core
while True:
    schedule.run_pending()
    time.sleep(1)
