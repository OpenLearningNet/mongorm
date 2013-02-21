from pymongo import Connection

connection = None
database = None

connectionSettings = None

def connect( databaseName, **kwargs ):
	global database, connection, connectionSettings
	
	connectionSettings = {}
	connectionSettings.update( kwargs )
	connectionSettings.update( {
		'database': databaseName
	} )

	if 'safe' not in connectionSettings and 'w' not in connectionSettings:
		connectionSettings['w'] = 1

	# Reset database & connection
	connection = None
	database = None

def getConnection( ):
	global database, connection, connectionSettings
	
	assert connectionSettings is not None, "No database specified: call mongorm.connect() before use"
	
	if connection is None:
		connectionArgs = {}
		
		for key in ('host', 'port', 'replicaSet', 'safe', 'w'):
			if key in connectionSettings:
				connectionArgs[key] = connectionSettings[key]
		
		connection = Connection( **connectionArgs )
	
	return connection

def getDatabase( ):
	global database, connectionSettings
	
	if database is None:
		connection = getConnection( )
		databaseName = connectionSettings['database']
		database = connection[databaseName]
		
		if 'username' in connectionSettings and \
			'password' in connectionSettings:
			database.authenticate( connectionSettings['username'], connectionSettings['password'] )
	
	return database
