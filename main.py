import csv
import sqlite3
import os

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    load_and_clean_users(os.path.join(curr_dir, '../../resources/users.csv'))
    load_and_clean_call_logs(os.path.join(curr_dir,'../../resources/callLogs.csv'))
    write_user_analytics(os.path.join(curr_dir,'../../resources/userAnalytics.csv'))
    write_ordered_calls(os.path.join(curr_dir,'../../resources/orderedCalls.csv'))

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        user_id = 1
        for row in reader:
            if row['firstName'] and row['lastName'] and len(row) == 2 and row['firstName'].strip() and row['lastName'].strip():
                cursor.execute('''
                    INSERT INTO users (userId, firstName, lastName)
                    VALUES (?, ?, ?)
                ''', (user_id, row['firstName'].strip(), row['lastName'].strip()))
                user_id += 1
        conn.commit()            
    print("User data loaded successfully.")


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        call_id = 1
        for row in reader:
            print(row)
            if row['phoneNumber'] and row['startTime'] and row['endTime'] and row['direction'] and row['userId']:
                try:
                    cursor.execute('''
                        INSERT INTO callLogs (callId, phoneNumber, startTime, endTime, direction, userId)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (call_id, row['phoneNumber'], int(row['startTime']), int(row['endTime']), row['direction'], int(row['userId'])))
                    call_id += 1
                except ValueError:
                    print("Invalida data in row:", row)
                    continue
        conn.commit()
    print("Call log data loaded successfully.")


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    cursor.execute('''
        SELECT userId, 
               AVG(endTime - startTime) AS avgDuration, 
               COUNT(callId) AS numCalls
        FROM callLogs
        GROUP BY userId
    ''')
    
    analytics_data = cursor.fetchall()
    
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])  # Write header
        writer.writerows(analytics_data)  # Write data rows
    
    print("User analytics data written successfully.")


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cursor.execute('''
        SELECT * 
        FROM callLogs
        ORDER BY userId, startTime
    ''')
    
    ordered_calls = cursor.fetchall()
    
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])  # Write header
        writer.writerows(ordered_calls)  # Write data rows
    
    print("Ordered call logs written successfully.")



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
