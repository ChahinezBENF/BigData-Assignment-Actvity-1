import pandas as pd
import sqlite3

def process_data():

    # Loading the transactions data from the CSV file into a pandas DataFrame
    file_path = r"src/data/transactions.csv" 
    df = pd.read_csv(file_path, encoding="utf-8")
    
    # Removing any rows with missing values in the DataFrame (Use dropna or another method)
    df.dropna(inplace=True)  # You can change this to other methods if required

    # Converting the 'TransactionDate' column to a datetime format using pandas
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    # Setting up a connection to SQLite database and create a table if it doesn't exist
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product TEXT,
        amount REAL,
        TransactionDate TEXT,
        PaymentMethod TEXT,
        City TEXT,
        Category TEXT
    )
    """)
    
    # 1- TO DO: Insert data into the database
    # Your task: Insert the cleaned DataFrame into the SQLite database. Ensure to replace the table if it already exists.
    df.to_sql("transactions", conn,if_exists="replace", index=False )

    # Example Queries - Write SQL queries based on the instructions below

    # 2- TO DO: Query for Top 5 Most Sold Products
    print("------Top 5 Most Sold Products------")
    # Your task: Write an SQL query to find the top 5 most sold products based on transaction count.
    cursor.execute(""" SELECT COUNT(Product) AS sels_count
                       FROM transactions
                       GROUP BY Product
                       ORDER BY sels_count DESC
                       LIMIT 5;  """)


    # 3- TO DO:  Query for Monthly Revenue Trend
    print("------Monthly Revenue Trend------")
    # Your task: Write an SQL query to find the total revenue per month.
    cursor.execute("""  SELECT strftime('%Y-%M', TransactionDate) AS month
                               SUM(Amount) AS total_rev
                        FROM transactions
                        GROUP BY month
                        ORDER BY month """)

    # TO DO:  Query for Payment Method Popularity
    
    # Your task: Write an SQL query to find the popularity of each payment method used in transactions.
    cursor.execute("""  Enter your query  """)


    # TO DO:  Query for Top 5 Cities with Most Transactions
    # Your task: Write an SQL query to find the top 5 cities with the most transactions.
    cursor.execute("""  Enter your query  """)


    # TO DO:  Query for Top 5 High-Spending Customers
    # Your task: Write an SQL query to find the top 5 customers who spent the most in total.
    cursor.execute("""  Enter your query  """)


    # TO DO:  Query for Hadoop vs Spark Related Product Sales
    # Your task: Write an SQL query to categorize products related to Hadoop and Spark and find their sales.
    cursor.execute("""  Enter your query  """)


    # TO DO:  Query for Top Spending Customers in Each City
    # Your task: Write an SQL query to find the top spending customer in each city using subqueries.
    cursor.execute("""  Enter your query  """)


    # Step 8: Close the connection
    # Your task: After all queries, make sure to commit any changes and close the connection
    conn.commit()
    conn.close()
    print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()
