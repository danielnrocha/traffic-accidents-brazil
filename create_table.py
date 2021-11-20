import mysql.connector as mysql

db = mysql.connect(
    host = "us-cdbr-east-04.cleardb.com",
    user = "bd7bbf83ab7643",
    password = "dfce7117",
    database='heroku_aba902d59bebc6b')
    # schema='heroku_aba902d59bebc6b')
## creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'
cursor = db.cursor()

## creating a databse called 'datacamp'
## 'execute()' method is used to compile a 'SQL' statement
## below statement is used to create tha 'datacamp' database
cursor.execute("CREATE TABLE traffic_accidents_br (id INT(50))")
