import csv
import logging
import mariadb
import sys

def main():
    # create a logger Log with name 'question8_log'
    Log = logging.getLogger('css-Exporter')
    # create file handler which logs even ERROR messages or higher
    Log.setLevel(logging.INFO)
    fh = logging.FileHandler('css-Exporter.log')
    fh.setLevel(logging.INFO)
    # create formatter and add it to the file handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # add the file handler to the (root) logger
    Log.addHandler(fh)

    Log.info("------------------------------------------")
    Log.info("Program Ran")
    Log.info("------------------------------------------")
    # Database format
    # Date, date | duration (seconds), int | Quest Name, string | Completed(1=True,0=false,2=Not mission), int | profit, int | Fuel Cost, int | Other Cost, int 
    try:
        Log.info('Attempting to connect to MariaDB')
        conn = mariadb.connect(
            user="EliteDU",
            password="EliteDP",
            host="192.168.0.217",
            port=6936,
            database="ElitDD"

        )
        Log.info('Connected to MariaDB')

    except mariadb.Error as e:
        Log.warning(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    cur = conn.cursor()

    file = open('EliteD.csv')
    for row in file:
        try:
            CurrentRow = row.split(",") # Split the current row into a table
            Date = CurrentRow[0] # Define Date as being the 1st item in row
            Date = Date.split("/") # Split the Date string into a list seperated by /
            Date = str(Date[2] + "-" + Date[0] + "-" + Date[1]) # Reorganize date to be accepted by the database(maria)
            Duration = CurrentRow[3] # Define Date as being the 3rd item in row
            Duration = Duration.split(":") # splits Duration into a list so we can add them all up to get total seconds
            sec = 0 # Defines the sec variable as 0
            sec += ((int(Duration[0]) * 60)*60) # Makes the first item in the Duration list (hour) muiltiply down to seconds
            sec += (int(Duration[1]) * 60) # Makes the second item in the Duration list (minute) muiltiply down to seconds
            sec += int(Duration[2]) # Makes the 3rd item (seconds) just get added to the total
            QuestName = CurrentRow[4] # Define Date as being the 4th item in row
            if "[completed]" in QuestName.lower(): # Checks for the tag [completed] in which case it defines completed to 1 and removes tag from string
                Completed = 1
                QuestName = QuestName.lower().replace("[completed]","")
            elif "[failed]" in QuestName.lower(): # Checks for the tag [failed] in which case it defines completed to 0 and removes tag from string
                Completed = 0
                QuestName = QuestName.lower().replace("[failed]","")
            else: # all else fails define it to 2
                Completed = 2
            QuestName = QuestName[0].upper() + QuestName[1:]
            Profit = CurrentRow[5] # Define Date as being the 5th item in row
            FuelC = CurrentRow[6] # Define Date as being the 6th item in row
            OtherCost = CurrentRow[7]# Define Date as being the 7th item in row
            Log.info("Adding Item")
            Log.info("Date: "+str(Date))
            Log.info("Duration: "+str(sec))
            Log.info("Quest Name: "+str(QuestName))
            Log.info("Completed: "+str(Completed))
            Log.info("Profit: "+str(Profit))
            Log.info("Fuel Cost: "+str(FuelC))
            Log.info("Other Cost: "+str(OtherCost))
            Log.info("Finished Adding Item")
            cur.execute(
            "INSERT INTO entries (Date,Duration,Quest_Name,Completed,profit,Fuel_Cost,Other_Cost) VALUES (?, ?, ?, ?, ?, ?, ?);", 
            (Date, sec, QuestName, Completed, Profit, FuelC, OtherCost))
        except Exception as e:
            Log.error('Unkown error!')
            Log.error(e)
    conn.commit()

if __name__ == "__main__":
    main()