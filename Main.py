import csv
import logging
import mariadb
import sys
import time
from datetime import date
# create a logger Log with name 'question8_log'
Log = logging.getLogger('Main')
# create file handler which logs even ERROR messages or higher
Log.setLevel(logging.INFO)
fh = logging.FileHandler('log.log')
fh.setLevel(logging.INFO)
# create formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the file handler to the (root) logger
Log.addHandler(fh)

Log.info("------------------------------------------")
Log.info("Program Ran")
Log.info("------------------------------------------")


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

def getbal():
    cur.execute("select sum(profit)-sum(Fuel_Cost)-sum(Other_Cost) from entries")
    rows = cur.fetchone()
    return str(rows[0])
def pushintodb(sec,QuestName,Completed,Profit,FuelC,OtherCost):
    Date = date.today()
    cur.execute("INSERT INTO entries (Date,Duration,Quest_Name,Completed,profit,Fuel_Cost,Other_Cost) VALUES (?, ?, ?, ?, ?, ?, ?);", (Date, sec, QuestName, Completed, Profit, FuelC, OtherCost))
    conn.commit()
def gettransactionid(QuestName, sec, profit, fuel, other):
    cur.execute("select Transaction_ID from entries where Quest_Name=? and Duration=? and profit=? and Fuel_Cost=? and Other_Cost=?;",(QuestName,sec,profit,fuel,other))
    rows = cur.fetchone()
    return str(rows[0])
def printcomp(com):
    if com == 1:
        return 'Completed'
    elif com == 0:
        return 'Failed'
    else:
        return 'Not Job'
def printstatus(startTime,quest,comp,profit,fuel,other):
    print("Bal in DB:", getbal())
    print("Projected Bal:", int(int(getbal())-fuel-other+profit))
    print("Date:", date.today())
    print("Elapsed Time:", round(time.time()-startTime),"sec")
    print("Quest Name:",quest)
    print("Completed: ",end="")
    print(printcomp(comp))
    print("Profit:",profit)
    print("Fuel Cost:",fuel)
    print("Other Cost:",other)
def printoptions():
    print("1) Set Mission Title")
    print("2) Set Complete Status")
    print("3) Add to profit")
    print("4) Add to fuel cost")
    print("5) Add to other cost")
    print("9) Finish")
def main():
    run = True
    while run:
        tmp = input("Press enter to start a new job or enter Q to quit: ")
        if tmp.lower() == 'q':
            run2 = False
            run = False
        else:
            run2 = True
            startTime = time.time()
            quest = "Unkown"
            comp = 2
            profit = 0
            fuel = 0
            other = 0
        while run2:
            printstatus(startTime,quest,comp,profit,fuel,other)
            printoptions()
            sel = input(": ")
            if sel == "1":
                quest = input("Please enter Mission Title: ")
            elif sel == "2":
                comp = str(input("0] Failed\n1] Completed\n2] Not a Mission\nPlease enter Completion Status: "))
                if comp == "0" or comp == "1" or comp == "2":
                    comp = int(comp)
            elif sel == "3":
                try:
                    profit += int(input("Enter additional profit: "))
                except:
                    pass
            elif sel == "4":
                try:
                    fuel += int(input("Enter additional fuel cost: "))
                except:
                    pass
            elif sel == "5":
                try:
                    other += int(input("Enter additional other cost: "))
                except:
                    pass
            elif sel == "9":
                ui = input(str("You are about to push data!\nThis is irreversible!\nCurrent balance in game should be " + str(int(int(getbal())-fuel-other+profit)) + "\nIf this is correct enter [y] and enter\nOtherwise just press enter.\n"))
                if ui.lower() == "y":
                    dur = round(time.time()-startTime)
                    pushintodb(dur, quest, comp, profit, fuel, other)
                    print("Transaction ID: " + str(gettransactionid(quest, dur, profit, fuel, other)))
                    run2 = False


if __name__ == "__main__":
    main()