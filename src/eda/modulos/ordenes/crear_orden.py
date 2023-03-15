import mysql.connector

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='ordenes',
                                         user='root',
                                         password='adminadmin')

    mySql_insert_query = """INSERT INTO ordenes (id, nombre, producto, cantidad, email, direccion, fecha_creacion) 
                           VALUES 
                           (2, 'Juan', 'Computador', 2, 'a@a.com', 'Calle', '2023-03-15') """

    cursor = connection.cursor()
    cursor.execute(mySql_insert_query)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into ordenes table")
    cursor.close()

except mysql.connector.Error as error:
    print("Failed to insert record into ordenes table {}".format(error))

finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")