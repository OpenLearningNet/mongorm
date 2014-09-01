from pymongo import MongoClient, MongoReplicaSetClient

connection = None
database = None

connectionSettings = None

stagedIndexes = []
registeredIndexes = []

def connect( databaseName, **kwargs ):
	global database, connection, connectionSettings
	
	connectionSettings = {}
	connectionSettings.update( kwargs )
	connectionSettings.update( {
		'database': databaseName
	} )

	# Reset database & connection
	connection = None
	database = None

def getConnection( ):
	global database, connection, connectionSettings
	
	assert connectionSettings is not None, "No database specified: call mongorm.connect() before use"
	
	if connection is None:
		connectionArgs = {}
		
		for key in ['host', 'port', 'replicaSet', 'read_preference']:
			if key in connectionSettings:
				connectionArgs[key] = connectionSettings[key]

		client = MongoReplicaSetClient if 'replicaSet' in connectionArgs else MongoClient
		
		connection = client( **connectionArgs )
	
	return connection

def ensureIndexes( ):
	global stagedIndexes, registeredIndexes

	error = None

	# Ensure indexes on the documents
	try:
		for collection, key_or_list, kwargs in stagedIndexes:
			database[collection].ensure_index(key_or_list, **kwargs)
	except Exception as err:
		raise error
	else:
		registeredIndexes += stagedIndexes
		stagedIndexes = []		

def getDatabase( ):
	global database, connectionSettings
	
	if database is None:
		connection = getConnection( )
		databaseName = connectionSettings['database']
		database = connection[databaseName]
		
		if 'username' in connectionSettings and \
			'password' in connectionSettings:
			database.authenticate( connectionSettings['username'], connectionSettings['password'] )

		ensureIndexes()
	
	return database
