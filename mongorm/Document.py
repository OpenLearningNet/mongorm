import pymongo
import warnings 
from bson import ObjectId
from mongorm.BaseDocument import BaseDocument
from mongorm.connection import getDatabase

from mongorm.errors import OperationError

class Document(BaseDocument):
	__internal__ = True
	__needs_primary_key__ = True
	__shard_key__ = None
	
	def __eq__(self, other):
		if isinstance(other, self.__class__) and hasattr(other, self._primaryKeyField):
			assert self._primaryKeyField == other._primaryKeyField
			if getattr(self, self._primaryKeyField) == getattr(other, other._primaryKeyField):
				return True
		return False
	
	def __ne__(self, other):
		return not (self == other)
	
	def __init__( self, **kwargs ):
		super(Document, self).__init__( **kwargs )
	
	def save( self, forceInsert=False, **kwargs ):
		database = getDatabase( )
		collection = database[self._collection]
		
		self._resyncFromPython( )
		
		if '_id' in self._data and self._data['_id'] is None:
			del self._data['_id']

		# safe not supported in pymongo 3.0+, use w for write concern instead
		if 'safe' in kwargs:
			kwargs['w'] = 1 if kwargs['safe'] else 0
			del kwargs['safe']
			warnings.warn('{} safe not supported in pymongo 3.0+, use w for write concern instead'.format(collection.full_name), DeprecationWarning)

		try:
			if forceInsert:
				newId = collection.insert( self._data, **kwargs )
			else:
				if self.__shard_key__:
					# This is specific to cosmosdb because it does not support collection.save when the collection is sharded.
					if self.__shard_key__ != '_id' and self.__shard_key__ not in self._data:
						raise OperationError('Could not find shard key in document data to save')
					
					if '_id' not in self._data:
						self._data['_id'] = ObjectId()

					_filter = {
						self.__shard_key__: self._data[self.__shard_key__],
						'_id': self._data['_id']
					}
					kwargs['upsert'] = True
					collection.update(_filter, self._data, **kwargs)
					newId = self._data['_id']
				else:
					newId = collection.save( self._data, **kwargs )
		except pymongo.errors.OperationFailure, err:
			message = 'Could not save document (%s)'
			if u'duplicate key' in unicode(err):
				message = u'Tried to save duplicate unique keys (%s)'
			raise OperationError( message % unicode(err) )
		if newId is not None:
			setattr(self, self._primaryKeyField, newId)
		
		return self

	def __repr__( self ):
		return '<%s id=%s>' % (self.__class__.__name__, getattr(self, self._primaryKeyField))