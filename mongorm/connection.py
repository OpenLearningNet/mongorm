from pymongo import MongoClient, MongoReplicaSetClient, IndexModel
from pymongo.collection import Collection
connection = None
database = None

connectionSettings = None

pymongoWrapper = None

stagedIndexes = []
droppedIndexes = []

def connect( databaseName, autoEnsure=False, wrapPymongo=None, **kwargs ):
	global database, connection, connectionSettings, autoEnsureIndexes, pymongoWrapper

	pymongoWrapper = wrapPymongo

	autoEnsureIndexes = autoEnsure

	connectionSettings = {}
	connectionSettings.update( kwargs )
	connectionSettings.update( {
		'database': databaseName
	} )

	# Reset database & connection
	connection = None
	database = None

	# Initialise connection and ensure indexes if configured
	getDatabase()

def getConnection( ):
	global database, connection, connectionSettings

	assert connectionSettings is not None, "No database specified: call mongorm.connect() before use"

	if connection is None:
		connectionArgs = {}

		# read_preference Not supported in pymongo 3.0+.
		# it should be an option on get_database, get_collection, with_options
		for key in ['host', 'port', 'replicaSet', 'username', 'password']:
			if key in connectionSettings:
				connectionArgs[key] = connectionSettings[key]

		client = MongoReplicaSetClient if 'replicaSet' in connectionArgs else MongoClient

		connection = client( **connectionArgs )

	return connection

def index_name_from_index_fields(index_fields):
	return '_'.join([
		field + '_1' if direction is 1 else '_-1'
		for (field, direction) in index_fields
	])

def ensureIndexes( ):
	global stagedIndexes, droppedIndexes

	if not autoEnsureIndexes:
		return

	assert database is not None, "Must be connected to database before ensuring indexes"

	# Ensure indexes on the documents
	indexes_by_collection = {}
	for collection, key_or_list, kwargs in stagedIndexes:
		if collection not in indexes_by_collection:
			indexes_by_collection[collection] = []

		indexes_by_collection[collection].append((key_or_list, kwargs))

	for collection in indexes_by_collection:
		# Create the collection if necessary
		if collection not in database.collection_names():
			Collection(database, collection, create=True)

		indexInfo = database[collection].index_information()
		dropped_indexes = []

		for index_fields, kwargs in indexes_by_collection[collection]:
			expected_index_name = index_name_from_index_fields(index_fields)

			existing_index = indexInfo.get(expected_index_name)
			if existing_index is not None:
				hasChanged = False
				if kwargs.get('unique', False) or existing_index.get('unique', False):
					if kwargs.get('unique', False) != existing_index.get('unique', False):
						hasChanged = True

				if hasChanged:
					database[collection].drop_index(expected_index_name)
					dropped_indexes.append(expected_index_name)

		database[collection].create_indexes([
			IndexModel(index_fields, **kwargs)
			for index_fields, kwargs in indexes_by_collection[collection]
			if (
				index_name_from_index_fields(index_fields) not in indexes_by_collection[collection] or
				index_name_from_index_fields(index_fields) in dropped_indexes)
		])

	stagedIndexes = []

def getDatabase( ):
	global database, connectionSettings

	if database is None:
		connection = getConnection( )
		databaseName = connectionSettings['database']
		database = connection[databaseName]

		ensureIndexes()

	return database
