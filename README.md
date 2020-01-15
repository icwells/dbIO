[![Build Status](https://travis-ci.com/icwells/dbIO.svg?branch=master)](https://travis-ci.com/icwells/dbIO)
[![GoDoc](https://godoc.org/github.com/icwells/dbIO?status.svg)](https://godoc.org/github.com/icwells/dbIO)

# dbIO is a lightweight, straightforward MySQL interface written in Go.  
These scripts are still under developement, but are available for use.  

Copyright 2018 by Shawn Rupp

1. [Dependencies](#dependencies)  
2. [Usage](#usage)  
3. [Uploading](#uploading-to-a-database)  
4. [Extracting](#extracting-from-a-database)  

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
dbIO stores relevant connection in a DBIO struct which is returned by the Connect function. Below are some examples of usage. 
See the GoDocs page for a comprehensive list.  

### Connect and the DBIO struct  
	dbIO.Connect(host, database, user, password string) *DBIO  

Attempts to connect to given sql database with given user name. If the password is left blank, it will prompt for a password from 
the user before storing the start time (for recording program run time).  

Returns a DBIO instance containing:  
```
DB        *sql.DB  
Host	  string
Database  string  
User      string  
Password  string  
Starttime time.Time  
Columns   map[string]string  
```

#### Creating/Replacing Databases  
CreateDatabase can be used to initializes a database with a given name (although NewTables must be called to initialize the tables within the databse).  
Similarly, ReplaceDatabase will drop an existing database (if it exists) and re-initialize it (for testing).  
```
dbio.CreateDatabase(host, database, user string)  
dbio.ReplaceDatabase(host, database, user, password string)  
```

Additionally, the Ping function can be used to test credentials:  

	dbio.Ping(host, database, user, password string)  

It will return true if a connection was successfully established, or false if it was not.  

### Uploading to a database 

#### DBIO.NewTables(infile string)  
This command will read a text file of tables, columns, and types and initialize new tables if they do not already exist.  

The input file should be in the following format:  

	# TableName  
	ID INT PRIMARY KEY  
	Name TEXT 

Table names should be preceded with a pound sign. Column names should be the first element of the line and must be 
followed by the column type. Any valid MySQL key words for column creation (UNIQUE, PRIMARY KEY, ...) may follow the type.  

#### DBIO.ReadColumns(infile string)  
Reads in tables and columns from input file (see above) and stores in DBIO.Columns. The column types and 
any additional column descriptors will be stored for creating tables.  

#### DBIO.GetTableColumns()  
Retrieves names tables and their columns from an existing database and stores in Columns map.  

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
"('5','Apple'),('3','Orange')"  
```
They both return a string of the data and an integer of the number rows that were formatted. Both are stand-alone functions and do not use a DBIO struct.  
The input data should contian the same number of columns as the table is to be uploaded to. (Map keys are not included in the upload.)  

#### DBIO.UpdateDB(table, values string, l int) int  

UpdateDB uploads a formatted string of data (see previous functions) to a given table. It will print the number of rows uploaded (given with l).  
It returns an integer (rather than a boolean) so multiple results can be tallied if needed.  

### Extracting from a database  

#### DBIO.GetRows(table, column, key, target string) [][]string  
Returns rows of target columns with key in column. Use "*" for target to select entire row or a comma seperated string of column names for multiple columns.  

#### DBIOEvaluateRows(table, column, op, key, target string) [][]string  
Returns rows of target column(s) same as GetRows, except it compares key to the column value using the given operator (>=/=/...; ie. column >= 7).  

