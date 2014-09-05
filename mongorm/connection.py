from pymongo import MongoClient, MongoReplicaSetClient

connection = None
database = None

connectionSettings = None

stagedIndexes = []
registeredIndexes = []
droppedIndexes = []
indexInfoList = []

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
	global stagedIndexes, registeredIndexes, droppedIndexes, indexInfoList

	assert database is not None, "Must be connected to database before ensuring indexes"

	# Ensure indexes on the documents
	try:
		indexInfo = database[collection].index_information()

		for collection, key_or_list, kwargs in stagedIndexes:
			if isinstance(key_or_list, basestring):
				key = key_or_list

				# if args on the index have changed, drop the index
				keyIndexInfo = indexInfo.get(key + '_1', indexInfo.get(key + '_-1'))
				
				if keyIndexInfo is not None:
					indexInfoList.append(keyIndexInfo)
					hasChanged = False
					if kwargs.get('unique', False) or keyIndexInfo.get('unique', False):
						if kwargs.get('unique', False) != keyIndexInfo.get('unique', False):
							hasChanged = True
						if kwargs.get('dropDups', False) != keyIndexInfo.get('dropDups', False):
							hasChanged = True

					if hasChanged:
						database[collection].drop_index(key)
						droppedIndexes.append(key)

			database[collection].ensure_index(key_or_list, **kwargs)
	except Exception as err:
		raise err
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
