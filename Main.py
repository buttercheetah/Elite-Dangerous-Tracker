import csv
import logging
import mariadb
import sys
import time
from datetime import date
import json
import re
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

  
# Opening JSON file
f = open('DB.json')
  
# returns JSON object as 
# a dictionary
datafile = json.load(f)

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

def recon():
    global conn,cur
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
def getaverage(col,Date=False,iscom=True):
    Log.info(str("Getaverage called with data "+str(col)+" | "+str(Date)+" | "+str(iscom)))
    if iscom == False:
        if col == 'Duration':
            Log.info("Running " + "select avg("+str(col)+") from entries where Duration > 60 and MONTH(Date) = "+str(Date[1])+" AND YEAR(Date) = "+str(Date[0])+";")
            cur.execute("select avg("+str(col)+") from entries where Duration > 60 and MONTH(Date) = "+str(Date[1])+" AND YEAR(Date) = "+str(Date[0])+";")
        else:
            Log.info("Running " + "select avg("+str(col)+") from entries where MONTH(Date) = "+str(Date[1])+" AND YEAR(Date) = "+str(Date[0])+";")
            cur.execute("select avg("+str(col)+") from entries where MONTH(Date) = "+str(Date[1])+" AND YEAR(Date) = "+str(Date[0])+";")
        rows = cur.fetchone()
        Log.info("fetched average " +str(col) +": " + str(rows[0]))
        return str(round(int(rows[0])))
    if Date != False:
        Date = str(Date).split("-")
        if col == 'Duration':
            #Log.info("Running " + "select avg("+str(col)+") from entries where Duration > 60 and MONTH(Date) = "+str(Date[1])+" AND DAY(Date) = "+str(Date[2])+" AND YEAR(Date) = "+str(Date[0])+";")
            cur.execute("select avg("+str(col)+") from entries where Duration > 60 and MONTH(Date) = "+str(Date[1])+" AND DAY(Date) = "+str(Date[2])+" AND YEAR(Date) = "+str(Date[0])+";")
        else:
            cur.execute("select avg("+str(col)+") from entries where MONTH(Date) = "+str(Date[1])+" AND DAY(Date) = "+str(Date[2])+" AND YEAR(Date) = "+str(Date[0])+";")
        rows = cur.fetchone()
        Log.info("fetched average " +str(col) +": " + str(rows[0]))
        rv = (rows[0])
        if rv == None:
            rv = 0
        return str(round(int(rv)))
    if col == 'Duration':
        cur.execute("select avg("+str(col)+") from entries where Duration > 60;")
    else:
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
def makeprettytable(dalist,spacing):
    for x in dalist:
        tspacing = spacing - len(str(x))
        print(str(x) + str(" " * tspacing),end="")
    print()
def printstatus(startTime,quest,comp,profit,fuel,other):
    bal = int(getbal())
    printpretty("Bal:", str(intWithCommas(bal-fuel-other+profit)))
    printpretty("Date:", str(date.today()))
    if time.time()-startTime > int(getaverage('Duration')):
        tmp = "v"
    else:
        tmp = "^"
    printpretty("Elapsed Time:", str(str(round(time.time()-startTime)) + " sec " + str(tmp)))
    printpretty("Quest Name:",str(quest))
    printpretty("Completed:", str(printcomp(comp)))
    if comp == 0:
        tmp = "v"
    elif comp == 1:
        tmp = "^"
    printpretty("Completion rate:", str(str(getcompletionperc()) + "% " + str(tmp)))
    if profit < int(getaverage('profit')):
        tmp = "v"
    else:
        tmp = "^"
    printpretty("Profit:", str(intWithCommas(int(profit)) + " " + str(tmp)))
    if fuel < int(getaverage('Fuel_Cost')):
        tmp = "v"
    else:
        tmp = "^"
    printpretty("Fuel Cost:", str(intWithCommas(int(fuel)) + " " + str(tmp)))
    if fuel < int(getaverage('Other_Cost')):
        tmp = "v"
    else:
        tmp = "^"
    printpretty("Other Cost:", str(intWithCommas(int(other)) + " " + str(tmp)))
def printaverages():
    printpretty("Average Profit:",  str(intWithCommas(int(getaverage('profit')))))
    printpretty("Average Fuel Cost:", str(intWithCommas(int(getaverage('Fuel_Cost')))))
    printpretty("Average Other Cost:", str(intWithCommas(int(getaverage('Other_Cost')))))
    printpretty("Average Time spent:", str(intWithCommas(int(getaverage('Duration'))) + " seconds"))
def printoptions():
    print("1) Set Mission Title")
    print("2) Set Complete Status")
    print("3) Add to profit")
    print("4) Add to fuel cost")
    print("5) Add to other cost")
    print("9) Finish")
def getUniqeDates():
    cur.execute("select distinct Date from entries;")
    rows = cur.fetchall()
    rows = str(rows).replace("(datetime.date", "").replace(",),", "").replace("[", "").replace("]", "")
    rows = str(rows).replace(",)", "")
    rows = str(rows).replace(", ", "-")
    rows = str(rows).replace(")", "|").replace("(", "")
    rows = rows.split("|")
    #rows = rows.split(",")
    Log.info("fetched all unuqie dates " + str(rows))
    return rows
def getlastNmissions(N=10):
    cur.execute("select * from entries order by Transaction_ID desc limit "+str(N)+";")
    rows = cur.fetchall()
    Log.info("fetched all unuqie dates " + str(rows))
    return rows
def nospaceinstring(item):
    return str(item).replace(" ", "")
def getUniqeMonths():
    UniqeDates = getUniqeDates()
    nlist = []
    for i in range(len(UniqeDates)-1):
        mon = str(UniqeDates[i]).split("-")
        if (nospaceinstring(mon[0]),nospaceinstring(mon[1])) in nlist:
            pass
        else:
            nlist.append((nospaceinstring(mon[0]),nospaceinstring(mon[1])))
    return nlist
def gettradecount():
    cur.execute("SELECT count(Transaction_ID) FROM entries WHERE Quest_Name like '%Trad%';")
    rows = cur.fetchone()
    Log.info("fetched average Trade count: " + str(rows[0]))
    return (int(rows[0]))
def getmissioncount():
    cur.execute("SELECT count(Transaction_ID) FROM entries WHERE Completed=0 or Completed=1;")
    rows = cur.fetchone()
    Log.info("fetched average Trade count: " + str(rows[0]))
    return (int(rows[0]))
def getsum(col):
    cur.execute("SELECT sum("+str(col)+") FROM entries;")
    rows = cur.fetchone()
    Log.info("fetched sum of "+str(col)+": " + str(rows[0]))
    return (int(rows[0]))
def main():
    run = True
    while run:
        tmp = input("Press A to start a new job, T to start trading, S for statistics, or enter Q to quit: ")
        if tmp.lower() == 'q':
            run2 = False
            run = False
        elif tmp.lower() == 'a':
            run2 = 1
            startTime = time.time()
            quest = "Unkown"
            comp = 2
            profit = 0
            fuel = 0
            other = 0
            Log.info("Set all default variables for a job")
        elif tmp.lower() == 't':
            run2 = 2
            startTime = time.time()
            quest = "Unkown"
            comp = 2
            profit = 0
            fuel = 0
            other = 0
            Log.info("Set all default variables for trading")
        elif tmp.lower() == 's':
            run2 = 3
            Log.info("Set required variables for statistics to load")
        while run2 == 1:
            recon()
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
                ui = input(str("You are about to push data!\nThis is irreversible!\nCurrent balance in game should be " + str(intWithCommas(int(int(getbal())-fuel-other+profit))) + "\nIf this is correct enter [y] and enter\nOtherwise just press enter.\n"))
                if ui.lower() == "y":
                    Log.info("Begining push")
                    dur = round(time.time()-startTime)
                    Log.info("Set duration to: " + str(dur))
                    pushintodb(dur, quest, comp, profit, fuel, other)
                    print("Transaction ID: " + str(gettransactionid(quest, dur, profit, fuel, other)))
                    run2 = False
        if run2 == 2:
            recon()
            printpretty("Your Bal:", str(intWithCommas(int(int(getbal())-fuel-other+profit))))
            tmp = True
            while tmp:
                tradeItem = str(input("Enter item you are trading: "))
                tradeCount = int(input("Enter item count you are trading: "))
                tradeCost = int(input("Enter per item cost you are trading: "))
                verification = str(input("Total cost should be " + str(intWithCommas(tradeCount*tradeCost)) + " Y/n"))
                if verification.lower() != 'n':
                    other = tradeCount*tradeCost
                    tmp = False
            quest = "Trading " + str(tradeCount) + "x " + tradeItem + " at " + str(tradeCost)
            Log.info("Set title to: " + str(quest))
            comp = int(2)
            Log.info("Set completion to: " + str(comp))
            tmp = True
            while tmp:
                tfuel = str(input("Enter fuel cost\nLeave empty when done: "))
                if tfuel == "":
                    tmp = False
                else:
                    fuel += int(tfuel)
                    Log.info("Set fuel to: " + str(fuel))
            
            eother = str(input("Enter additional other cost: "))
            if eother == "":
                pass
            else:
                other += int(eother)
            Log.info("Set other to: " + str(other))
            profit += int(input("Enter profit: "))
            Log.info("Set profit to: " + str(profit))
            tmp = True
            recon()
            while tmp:
                printstatus(startTime,quest,comp,profit,fuel,other)
                sel = input("1) Additional profit\n2) Additional other costs\n9)finish\n: ")
                if sel == "1":
                    try:
                        profit += int(input("Enter additional profit: "))
                        Log.info("Set profit to: " + str(profit))
                    except:
                        pass
                elif sel == "2":
                    try:
                        other += int(input("Enter additional other cost: "))
                        Log.info("Set other to: " + str(other))
                    except:
                        pass
                elif sel == "9":
                    Log.info("Begining push")
                    dur = round(time.time()-startTime)
                    Log.info("Set duration to: " + str(dur))
                    pushintodb(dur, quest, comp, profit, fuel, other)
                    print("Transaction ID: " + str(gettransactionid(quest, dur, profit, fuel, other)))
                    run2 = False
                    tmp = False
        while run2 == 3:
            printaverages()
            print("Total time spent: " + str(round(float((int(getsum('Duration')) /60)/60),2)) + str(" hours"))
            print("Trade count: " + str(gettradecount()))
            print("Mission count: " + str(getmissioncount()))
            print("1) Breakdown by day")
            print("2) Breakdown by month")
            print("3) Mission log")
            print("9) Quit")
            sel = str(input(": "))
            if sel == "1":
                UniqeDates = getUniqeDates()
                dalist = ['#','Year','Month','Day','Average profit','Average Fuel Cost','Average Other Cost','Average Duration (sec)']
                makeprettytable(dalist,20)
                for i in range(len(UniqeDates)-1):
                    mon = str(UniqeDates[i]).split("-")
                    dalist = [str(str(i)+"."),mon[0],mon[1],mon[2],getaverage('profit',UniqeDates[i]),getaverage('Fuel_Cost',UniqeDates[i]),getaverage('Other_Cost',UniqeDates[i]),getaverage('Duration',UniqeDates[i])]
                    for x in range(len(dalist)):
                        dalist[x] = str(dalist[x]).replace(" ", "")
                    makeprettytable(dalist,20)
                input("Press enter to go back")
            elif sel == "2":
                uniquemonths = getUniqeMonths()
                dalist = ['#','Year','Month','Average profit','Average Fuel Cost','Average Other Cost','Average Duration (sec)']
                makeprettytable(dalist,20)
                for i in range(len(uniquemonths)):
                    mon = uniquemonths[i]
                    #mon = str(mon).replace("(","").replace(")","").replace(" ","").replace("'","")
                    #mon = mon.split(',')
                    dalist = [str(str(i)+"."),mon[0],mon[1],getaverage('profit',mon,False),getaverage('Fuel_Cost',mon,False),getaverage('Other_Cost',mon,False),getaverage('Duration',mon,False)]
                    for x in range(len(dalist)):
                        dalist[x] = str(dalist[x]).replace(" ", "")
                    makeprettytable(dalist,20)
                input("Press enter to go back")
            elif sel == "3":
                log = getlastNmissions(10)
                spacing = 0
                for i in log:
                    for x in i:
                        if len(str(x)) > spacing: spacing = len(str(x))+2
                dalist = ['ID','Title','profit','Fuel Cost','Other Cost','Duration']
                makeprettytable(dalist,spacing)
                for i in range(len(log)):
                    mon = log[i]
                    #mon = str(mon).replace("(","").replace(")","").replace(" ","").replace("'","")
                    #mon = mon.split(',')
                    dalist = [mon[0],mon[3],mon[5],mon[6],mon[7],mon[2]]
                    makeprettytable(dalist,spacing)
            elif sel == "9":
                run2=False
            
            
if __name__ == "__main__":
    main()