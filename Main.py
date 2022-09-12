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

import json
  
# Opening JSON file
f = open('DB.json')
  
# returns JSON object as 
# a dictionary
datafile = json.load(f)
# Iterating through the json
# list


try:
    Log.info('Attempting to connect to MariaDB')
    conn = mariadb.connect(
        user=datafile['user'],
        password=datafile['password'],
        host=datafile['host'],
        port=datafile['port'],
        database=datafile['database']

    )
    Log.info('Connected to MariaDB')

except mariadb.Error as e:
    Log.warning(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
cur = conn.cursor()
def printpretty(item1,item2):
    spacing = 20
    spacing = spacing - len(str(item1))
    print(item1 + str(" " * spacing) + item2)
def intWithCommas(x):
    if type(x) not in [type(0), type(0)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)

def getbal():
    cur.execute("select sum(profit)-sum(Fuel_Cost)-sum(Other_Cost) from entries")
    rows = cur.fetchone()
    bal = rows[0]
    Log.info("fetched bal: " + str(bal))
    return str(bal)
def getaverage(col):
    cur.execute("select avg("+str(col)+") from entries;")
    rows = cur.fetchone()
    Log.info("fetched average " +str(col) +": " + str(rows[0]))
    return str(round(int(rows[0])))
def pushintodb(sec,QuestName,Completed,Profit,FuelC,OtherCost):
    Date = date.today()
    Log.info("Begining Push")
    cur.execute("INSERT INTO entries (Date,Duration,Quest_Name,Completed,profit,Fuel_Cost,Other_Cost) VALUES (?, ?, ?, ?, ?, ?, ?);", (Date, sec, QuestName, Completed, Profit, FuelC, OtherCost))
    Log.info("Push finished with no errors")
    conn.commit()
def gettransactionid(QuestName, sec, profit, fuel, other):
    Log.info("Attempting to get Transaction ID")
    cur.execute("select Transaction_ID from entries where Quest_Name=? and Duration=? and profit=? and Fuel_Cost=? and Other_Cost=?;",(QuestName,sec,profit,fuel,other))
    rows = cur.fetchone()
    Log.info("fetched transaction ID: " + str(rows[0]))
    return str(rows[0])
def printcomp(com):
    if com == 1:
        return 'Completed'
    elif com == 0:
        return 'Failed'
    else:
        return 'Not Job'
def getcompletionperc():
    cur.execute("select count(Completed) from entries where Completed=0;")
    rows = cur.fetchone()
    Log.info("fetched Completed=0: " + str(rows[0]))
    failed = int(rows[0])

    cur.execute("select count(Completed) from entries where Completed=1;")
    rows = cur.fetchone()
    Log.info("fetched Completed=1: " + str(rows[0]))
    comple = int(rows[0])

    return int(comple*100/(comple+failed))
    
def printstatus(startTime,quest,comp,profit,fuel,other):
    printpretty("Bal in DB:", str(intWithCommas(int(getbal()))))
    printpretty("Projected Bal:", str(intWithCommas(int(int(getbal())-fuel-other+profit))))
    printpretty("Date:", str(date.today()))
    printpretty("Elapsed Time:", str(str(round(time.time()-startTime)) + " sec"))
    printpretty("Quest Name:",str(quest))
    printpretty("Completed:", str(printcomp(comp)))
    printpretty("Completion rate:", str(str(getcompletionperc()) + "%"))
    printpretty("Profit:", str(intWithCommas(int(profit))))
    printpretty("Average Profit:",  str(intWithCommas(int(getaverage('profit')))))
    printpretty("Fuel Cost:", str(str(intWithCommas(int(fuel)))))
    printpretty("Average Fuel Cost:", str(intWithCommas(int(getaverage('Fuel_Cost')))))
    printpretty("Other Cost:", str(str(intWithCommas(int(other)))))
    printpretty("Average Other Cost:", str(intWithCommas(int(getaverage('Other_Cost')))))
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
            Log.info("Set all default variables")
        while run2:
            printstatus(startTime,quest,comp,profit,fuel,other)
            printoptions()
            sel = input(": ")
            if sel == "1":
                quest = input("Please enter Mission Title: ")
                Log.info("Set title to: " + str(quest))
            elif sel == "2":
                comp = str(input("0] Failed\n1] Completed\n2] Not a Mission\nPlease enter Completion Status: "))
                if comp == "0" or comp == "1" or comp == "2":
                    comp = int(comp)
                    Log.info("Set completion to: " + str(comp))
            elif sel == "3":
                try:
                    profit += int(input("Enter additional profit: "))
                    Log.info("Set profit to: " + str(profit))
                except:
                    pass
            elif sel == "4":
                try:
                    fuel += int(input("Enter additional fuel cost: "))
                    Log.info("Set fuel to: " + str(fuel))
                except:
                    pass
            elif sel == "5":
                try:
                    other += int(input("Enter additional other cost: "))
                    Log.info("Set other to: " + str(other))
                except:
                    pass
            elif sel == "9":
                ui = input(str("You are about to push data!\nThis is irreversible!\nCurrent balance in game should be " + str(int(int(getbal())-fuel-other+profit)) + "\nIf this is correct enter [y] and enter\nOtherwise just press enter.\n"))
                if ui.lower() == "y":
                    Log.info("Begining push")
                    dur = round(time.time()-startTime)
                    Log.info("Set duration to: " + str(dur))
                    pushintodb(dur, quest, comp, profit, fuel, other)
                    print("Transaction ID: " + str(gettransactionid(quest, dur, profit, fuel, other)))
                    run2 = False


if __name__ == "__main__":
    main()