import mysql.connector
from mysql.connector import Error
import os

# Database connection details
db_config = {
    'host': 'database-1.cnic0gams8c1.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'admin123',
    'database': 'music'
}

# Function to create a connection to the MySQL database
def create_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to create a sample table
def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT AUTO_INCREMENT PRIMARY KEY,
        fname VARCHAR(255) NOT NULL,
        lname VARCHAR(255) NOT NULL,
        email VARCHAR(100) NOT NULL,
        phone_num VARCHAR(10)
    );
    """
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    print("Table 'users' created successfully")

# Function to create the wav_files table
def create_wav_files_table(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wav_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            file_path VARCHAR(255) NOT NULL,
            file_size INT,
            file_type VARCHAR(50)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'wav_files' created successfully")
    except Error as e:
        print(f"Error: {e}")

# Function to insert metadata of WAV file into the database
def insert_wav_metadata(connection, filename, file_path, file_size, file_type):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO wav_files (filename, file_path, file_size, file_type)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (filename, file_path, file_size, file_type))
        connection.commit()
        print("WAV file metadata inserted successfully")
    except Error as e:
        print(f"Error: {e}")

# Function to insert a record into the table
def insert_user(connection, fname, lname, email, phone_num):
    insert_query = """
    INSERT INTO users (fname, lname, email, phone_num)
    VALUES (%s, %s, %s, %s);
    """
    cursor = connection.cursor()
    cursor.execute(insert_query, (fname, lname, email, phone_num))
    connection.commit()
    print("User inserted successfully")

# Function to update a record in the table
def update_user(connection, user_id, fname=None, lname=None, email=None, phone_num=None):
    update_query = "UPDATE users SET "
    update_values = []
    
    if fname:
        update_query += "fname = %s, "
        update_values.append(fname)
    if lname:
        update_query += "lname = %s, "
        update_values.append(lname)
    if email:
        update_query += "email = %s, "
        update_values.append(email)
    if phone_num:
        update_query += "phone_num = %s, "
        update_values.append(phone_num)
    
    update_query = update_query.rstrip(', ')
    update_query += " WHERE id = %s"
    update_values.append(song_id)
    
    cursor = connection.cursor()
    cursor.execute(update_query, tuple(update_values))
    connection.commit()
    print("Record updated successfully")

# Function to delete a record from the table
def delete_user(connection, user_id):
    delete_query = "DELETE FROM users WHERE id = %s"
    cursor = connection.cursor()
    cursor.execute(delete_query, (user_id,))
    connection.commit()
    print("User deleted successfully")

# Function to select records from the table
def select_users(connection):
    select_query = "SELECT * FROM users"
    cursor = connection.cursor()
    cursor.execute(select_query)
    users = cursor.fetchall()
    print("Selected Users:")
    for row in users:
        print(row)

# Function to fetch tables in the database
def fetch_tables(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table[0])
    except Error as e:
        print(f"Error: {e}")


# Function to insert file metadata into the 'wav_files' table, checking for duplicates
def insert_file_metadata(connection, folder_path):
    try:
        cursor = connection.cursor()
        
        # Retrieve existing filenames from the database
        cursor.execute("SELECT filename FROM wav_files")
        existing_filenames = {row[0] for row in cursor.fetchall()}  # Set comprehension
        
        # Iterate over WAV files in the specified folder
        for filename in os.listdir(folder_path):
            if filename.endswith(".wav"):
                file_path = os.path.join(folder_path, filename)
                file_size = os.path.getsize(file_path)
                file_type = "audio/wav"
                
                # Check if filename already exists in database
                if filename not in existing_filenames:
                    insert_query = """
                    INSERT INTO wav_files (filename, file_path, file_size, file_type)
                    VALUES (%s, %s, %s, %s);
                    """
                    cursor.execute(insert_query, (filename, file_path, file_size, file_type))
        
        connection.commit()
        print("File metadata inserted successfully")
    except Error as e:
        print(f"Error inserting file metadata: {e}")

def fetch_metadata(connection):
    try:
        select_query = "SELECT * FROM wav_files"
        cursor = connection.cursor()
        cursor.execute(select_query)
        metadata = cursor.fetchall()
        print("Selected metadata:")
        for row in metadata:
            print(row)
    except Error as e:
        print(f"Error: {e}")

# Main function to demonstrate the operations
def main():
    # Connect to the database
    connection = create_connection()

    # folder_path for beats
    folder_path = r'C:\Users\Kanin\Desktop\Music\audio files'

    if connection:
        # Create the table
        create_table(connection)
        
        select_users(connection)

        create_wav_files_table(connection)

        fetch_metadata(connection)


        # Close the connection
        connection.close()

if __name__ == "__main__":
    main()
