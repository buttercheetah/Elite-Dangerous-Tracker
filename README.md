# Elite-Dangerous-Tracker
The worst possible way to keep track of elite dangerous play sessions.

This was mostly made as a way to teach myself sql in python with real world data.

There are likely thousands of better ways to collect the wanted data than the way I have it in the program, but that was not the purpose of this program.

This program does not automatically create the table, so here is what the table should look like for the program to fuction properly.
+----------------+-----------+------+-----+---------+----------------+
| Field          | Type      | Null | Key | Default | Extra          |
+----------------+-----------+------+-----+---------+----------------+
| Transaction_ID | int(11)   | NO   | PRI | NULL    | auto_increment |
| Date           | date      | YES  |     | NULL    |                |
| Duration       | int(11)   | YES  |     | NULL    |                |
| Quest_Name     | char(100) | YES  |     | NULL    |                |
| Completed      | int(11)   | YES  |     | NULL    |                |
| profit         | int(11)   | YES  |     | NULL    |                |
| Fuel_Cost      | int(11)   | YES  |     | NULL    |                |
| Other_Cost     | int(11)   | YES  |     | NULL    |                |
+----------------+-----------+------+-----+---------+----------------+