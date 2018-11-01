# dbIO is a lightweight, straightforward MySQL interface written in Go.  
This script is still (somewhat) under developement, but is more or less ready for use.  

Copyright 2018 by Shawn Rupp

1. [Dependencies](#dependencies)  
2. [Usage](#usage)  
3. [Uploading](#uploading-to-a-database)  
4. [Extracting](#extracting-from-a-database)  
5. [Update/Delete](#updating-and-deleting)

## Dependencies:  

### Prompter  
dbIO used Prompter to query the user's MySQL password.  

	go get github.com/Songmu/prompter  

### Golang Mysql driver
Required for using go's sql package with MySQL.  

	go get github.com/go-sql-driver/mysql  

### iotools
dbIO uses iotools to read in database schema template file.  

	go get github.com/icwells/go-tools/iotools  

## Installation  

	go get github.com/icwells/dbIO  

## Usage  
dbIO stores relevant connection in a DBIO struct which is returned the Connect function.  

### Connect and the DBIO struct  
	dbIO.Connect(database, user string) *DBIO  

Attempts to connect to given sql database with given user name. It will prompt for a password from 
the user before storing the start time (for recording program run time).  

Returns a DBIO instance containing:  

```
DB        *sql.DB  
Database  string  
User      string  
password  string  
Starttime time.Time  
Columns   map[string]string  
```

DB is the database connection, while database stores the database name. User is the user name, and Starttime 
is the time point after the password is given (while the password is stored, it is not exported). Lastly, columns 
stores a map with a comma-seperated string of column name for each table. This map is currently read in from a 
text file (for initializing a new database), but it may be updated to read from the database in the future.  

### Uploading to a database 

#### DBIO.NewTables(infile string)  
This command will read a text file of tables, columns, and types and initialize new tables if they do not already exist.  

The input file should be in the following format:  
```
\# TableName  
ID INT PRIMARY KEY  
Name TEXT
```  

Table names should be preceded with a pound sign. Column names should be the first element of the line and must be 
followed by the column type. Any valid MySQL key words for column creation (UNIQUE, PRIMARY KEY, ...) may follow the type.  

#### DBIO.ReadColumns(infile string, types bool)  
Reads in tables and columns from input file (see above) and stores in DBIO.Columns. If types is true, the column types and 
any additional column descriptors will be stored (for creating tables). Otherwise, only the column names are stored.  

#### Formatting data for upload  
```
dbIO.FormatMap(data map[string][]string) (string, int)  
dbIO.FormatSlice(data [][]string) (string, int)  
```

These functions will format a map or slice of string slices into a comma/parentheses seperated string for upload to a database:  
```
[][]string{{"5, "Apple"}, {"3", "Orange"},}  
```
becomes 
```
"(5,Apple),(3,Orange)"  
```
They both return a string of the data and an ionteger of the number rows that were formatted. Both are stand-alone functions and do not use a DBIO object.  
The input data should contian the same number of columns as the table is to be uploaded to. (Map keys are not included in the upload.)  

#### DBIO.UpdateDB(table, values string, l int) int  

UpdateDB uploads a formatted string of data (see previous functions) to a given table. It will print the number of rows uploaded (given with l).  
It returns an integer (rather than a boolean) so multiple results can be tallied if needed.  

### Extracting from a database  

#### DBIO.GetTable(table string) [][]string  
Returns contents of a given table as a slice of string slices.  

#### DBIO.GetTableMap(table string) map[string][]string  
Returns contents of a given table as a map with the first column as the key.  

#### DBIO.GetNumOccurances(table, column string) map[string]int  
Returns a map with number of unique entries in the given column of the table.  

#### DBIO.GetColumns(table string, columns []string) [][]string  
Returns slice of string slices of all entries in given columns.  

#### DBIO.GetColumnText(table, column string) []string  
Returns a string slice of all entries in column.  

#### DBIO.GetColumnInt(table, column string) []int  
Returns an integer slice of all entries in column.  

#### DBIO.GetRows(table, column, key, target string) [][]string  
Returns rows of target columns with key in column. Use "*" for target to select entire row.  

#### DBIOEvaluateRows(table, column, op, key, target string) [][]string  
Returns rows of target columns where key relates to column via given operator (>=/=/...).  

#### DBIO.GetRowsMin(table, column, target string, min int) [][]string  
Returns rows of target columns where column is greater than or equal to key.  

#### DBIO.GetMax(table, column string) int  
Returns maximum number froma  given column.  

#### DBIO.GetCount(table string) int  
Returns number of rows from a table.  

### Updating and deleting  

#### DBIO.UpdateRow(table, target, key string, values [][]string) int 
Updates rows where target == key with given values (automatically matched to columns). 
Returns number of rows updated.  

#### DBIO.DeleteRow(table, column, value string)  
Deletes row(s) from database where column name == given value.  

#### DBIO.TruncateTable(table string)  
Clears all table contents (for re-creating summary tables....).  
