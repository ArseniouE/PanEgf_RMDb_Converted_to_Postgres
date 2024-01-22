------------------------------------------------------------
-----------------------PANEGF/RM----------------------------
------------------------------------------------------------
1) Connect to olap db with olap user in Postgres and run the following scripts:
	1. Create_Table_PanEgf_RMDb_Report.sql
	2. Function_PanEgf_Report.sql
	3. Function_RMDB_Report.sql 
	4. View_PanEgf_RMDB_Report.sql

------------------------------------------------------------
------------------------PANEGF------------------------------
------------------------------------------------------------
------------------------START-------------------------------
Instructions to Run PanEgf Report:
1)Copy PanEgfViewToCSV.zip to your app  server and unzip the folder in C:// 
2)After the unzip, open PanegfViewToCSV.dll.config with notepad.
	Line 9. Change the value attribute and fill in the connection string of the olap database in the desired environment.
	Line 12. Change the value attribute and fill in the path where the exported csv file will be saved.
	Line 15. Change the value attribute and fill in the path where the log file of the executable will be produced.
	Save the file with above changes and close it.
3)Run PanEgfViewToCSV.exe by passing a date parameter (YYYY-MM-DD) to get the Report.
-------------------------END--------------------------------

 
------------------------------------------------------------
-------------------------RMDB-------------------------------
------------------------------------------------------------
------------------------START-------------------------------
Instructions to Run RM Report:
1)Copy RMViewToCSV.zip to your app server and unzip the folders in C:// 
2)After the unzip, open RMViewToCSV.dll.config with notepad.
	Line 9. Change the value attribute and fill in the connection string of the olap database in the desired environment.
	Line 12. Change the value attribute and fill in the path where the exported csv file will be saved.
	Line 15. Change the value attribute and fill in the path where the log file of the executable will be produced.
	Save the file with above changes and close it.
3)Run RMViewToCSV.exe by passing a date parameter (YYYY-MM-DD) to get the Report. 
-------------------------END--------------------------------


