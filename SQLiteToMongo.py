import sqlite3
import pymongo
import json
from pymongo import MongoClient
import sys
import os.path

###########################################################################
# SQLite to Mongo 
# Version: 0.01
# Author: Frank Hernandez (Code Minion)
#
# Script to transfer a SQLite database to a MongoDB database. 
#
# Usage: python SQLiteToMongo.py PathToSQLiteDB MongoDBName 
#				MongoDBHost(optional) MongoDBPort(optional)
#
###########################################################################


###########################################################################
# Returns a list containing the names for all the tables inside the 
# SQLite database. 
#
# @return - List of names of tables inside the database. 
###########################################################################
def getTableNamesFromDB(dbCursor):
	# Get all the tables the sqlite database.
	# These will become the documents on MongoDB
	tablesList = []
	dbCursor.execute("SELECT name FROM sqlite_master where type='table' ORDER BY name")
	for tableRow in dbCursor:
		tablesList.append(tableRow[0])

	return tablesList

###########################################################################
# Returns a list containing all tuple with (columnName, dataType).
# 
# @returns - List of table column info.
###########################################################################
def getTableColumnNamesFromDB(tableName, dbCursor):
	# Get the columns of each tables. These
	# will become the fields in the MongoDB record.
	columnsList = []
	dbCursor.execute("PRAGMA table_info("+tableName+")")
	for tableColRow in dbCursor:
		# Print Data Type
		#print tableColRow[2]
		columnsList.append((tableColRow[1],tableColRow[2]))
	
	return columnsList
	
	
###########################################################################
# Returns a dictionary with information about each table in the database. 
# It is index on the table name.
#
# @returns - Dictionary containing information about the structure of each 
#			 table. 
###########################################################################
def getDatabaseStructureInfoMap(dbCursor):
	tableInfoMap = {}
	columnsList = []
	tablesList = getTableNamesFromDB(dbCursor)
	for table in tablesList:
		columnsList = getTableColumnNamesFromDB(table, dbCursor)
		tableInfoMap[table] = columnsList
		
	return tableInfoMap

###########################################################################
# Helper function to convert the incoming data from the SQLite database.
#
# @param valueIn - incoming value to be added.
# @param sqlite3Type - Type of the column where value is coming from.
#
# @returns - JSON friendly representation of valueIn
###########################################################################
def buildJSONType(valueIn, sqlite3Type):
	sqlite3Type = sqlite3Type.upper()
	
	# If for some reason there is no value in the field
	if not valueIn:
		valueIn = 0.0
		
	valueOut = json.dumps(valueIn)
	
	if sqlite3Type == "TEXT" or not sqlite3Type or not isNumber(valueIn):
		valueOut = str(valueOut).strip().replace("\n", "")
		
	return valueOut

###########################################################################
# Helper function to test if value is a number.
#
# @param valueIn - value to check if it is a number.
#
# @returns - True if value in is a number, False otherwise.  
###########################################################################
def isNumber(valueIn):
	try:
		float(valueIn)
		return True
	except ValueError:
		return False
		

###########################################################################
# Function to transfer a SQLite database into files of JSON objects.
# It saves every table as a separate file with the name <tableName>.tbl
# under the outFiles directory.
#
# @param dbCursor - Cursor to the SQLDatabase to copy data from.
# @param tableInfoMap - Map with information about the structure of 
#						the SQLite database.
###########################################################################
def writeSQLiteDataToJSONFile(dbCursor, tableInfoMap):
	# For every table, generate a JSON file with 
	# each field in that table. 
	for table in tableInfoMap:
		# Open a file for each table to store
		# the JSON in it.
		tableFile = open("./outFiles/"+table+".tbl", 'w')
		# Query SQLite Database for this table.
		dbCursor.execute("SELECT * from "+table)
		# For each row create a json object and
		# write it to the file.
		for tableRow in dbCursor:
			jsonObjStr = generateJSONObjectFromRow(table, tableRow, tableInfoMap)
			tableFile.write(jsonObjStr)
			tableFile.write("\n\n")
		tableFile.close()

###########################################################################
# Helper function to turn a row in an SQLite table into a JSON object.
#
# @param table - Name of the table the row belongs to.
# @param tableRow - Row from the table to turn into JSON
# @param tableInfoMap - Information about the structure of the SQLite Database
#
# @returns - JSON representation of the row as a string. 
###########################################################################
def generateJSONObjectFromRow(table, tableRow, tableInfoMap):
	jsonObjStr = ""
	jsonObjList = []
	jsonObjList.append("{");
	for rowKey in tableInfoMap[table]:
		
		if tableInfoMap[table].index(rowKey) != 0:
			jsonObjList.append(",")
		
		jsonObjList.append("\n")
		propertyName = '"'+rowKey[0] +'"'
		propertyValue = buildJSONType(tableRow[str(rowKey[0])], rowKey[1])
			
		jsonObjList.append("\t"+propertyName+ ": "+ str(propertyValue))
			
	jsonObjList.append("\n}")
	
	jsonObjStr = ''.join(jsonObjList)
	return jsonObjStr

###########################################################################
# Function to transfer a SQLiteDatabase to a MongoDB.
#
# @param @sqliteDBCursor - Cursor to the sqlite database.
# @param @tableInfoMap - Structure information about the SQLite database.
# @param @dbMongo - Reference to MongoDB database.
###########################################################################
def writeSQLiteDataToMongoDB(sqliteDBCursord, tableInfoMap, dbMongo):
	for table in tableInfoMap:
		mongoBulk = [] 
		# Reference mongo collection.
		mongoTable = dbMongo[table]
		# Query SQLite Database for this table.
		sqliteDBCursord.execute("SELECT * from "+table)
		# For each row create a json object and
		# write it to the file.
		for tableRow in sqliteDBCursord:
			jsonObjStr = generateJSONObjectFromRow(table, tableRow, tableInfoMap)
			jsonObj = json.loads(jsonObjStr)
			mongoBulk.append(jsonObj)
		mongoTable.insert(mongoBulk)
	
def main():
	argv = sys.argv
	
	sqliteDBPath = argv[1]
	
	if not os.path.isfile(sqliteDBPath):
		print "Database File not Found."
		return 

	if len(argv) < 3:
		print "**************************************************************************"
		print "Script to transfer a SQLite Database to a MongoDB. "
		print "Author: Frank Hernandez"
		print "\n"
		print "Usage: python " + argv[0] + "PathToSQLiteDB MongoDBName MongoDBHost(optional) MongoDBPort(optional)"
		print "\n"
		print "**************************************************************************"
		return; 
		
	#mongoDatabaseName = "mtg_doctor"
	
	mongoHost = "localhost"
	mongoPort = 27017
	
	mongoDatabaseName = argv[2]
	
	if len(argv)  > 3:
		mongoHost = str(argv[3])

	if len(argv) > 4:
		mongoPort = int(argv[4])
	
	# Connect to the SQLDatabase
	conn = sqlite3.connect(sqliteDBPath)
	conn.row_factory = sqlite3.Row
	dbCursor = conn.cursor()
	
	#print sqlite3.version
	#print sqlite3.sqlite_version_info 
	
	# Get the database information map with all 
	# the info in each table structure.
	tableInfoMap = {}
	tableInfoMap = getDatabaseStructureInfoMap(dbCursor)
	
	
	# Open MongoDB for population
	mongoClient = MongoClient(mongoHost, mongoPort)
	# Empty Database if Any
	mongoClient.drop_database(mongoDatabaseName)
	# Get database
	dbMongo = mongoClient[mongoDatabaseName]
	
	
	# For every table, generate a JSON file with 
	# each field in that table. 
	print "Generating JSON Table Files..."
	writeSQLiteDataToJSONFile(dbCursor, tableInfoMap)
	print "DONE!"
	
	# For every table, get each row.
	# For every row, insert a record in the MongoDB
	print "Transferring SQLite db to MongoDB..."
	writeSQLiteDataToMongoDB(dbCursor, tableInfoMap, dbMongo)
	print "DONE!"
	
	# Close the SQLite DB
	conn.close()
	
if __name__ == '__main__':
	main()