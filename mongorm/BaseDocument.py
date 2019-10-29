from builtins import object
from mongorm import connection
from mongorm.DocumentMetaclass import DocumentMetaclass

from mongorm.blackMagic import serialiseTypesForDocumentType
from future.utils import with_metaclass

class BaseDocument(with_metaclass(DocumentMetaclass, object)):
	__internal__ = True
	
	class DoesNotExist(Exception):
		pass
	
	def __init__( self, **kwargs ):
		self._is_lazy = False
		self._data = {}
		self._values = {}
		
		self._data['_types'] = serialiseTypesForDocumentType( self.__class__ )
		
		for name,value in kwargs.items( ):
			setattr(self, name, value)
	
	def _fromMongo( self, data, overwrite=True ):
		self._is_lazy = True
		
		for (name,field) in self._fields.items( ):
			dbField = field.dbField
			if dbField in data and ( overwrite or not name in self._values ):
				pythonValue = field.toPython( data[dbField] )
				setattr(self, name, pythonValue)
		
		return self

	# The following three methods are used for pickling/unpickling.
	# If it weren't for the fact that  __getattr__ returns None
	# for non-existing attributes (rather than raising an AttributeError),
	# we would not need to define them.

	def __getstate__( self ):
		return self.__dict__

	def __setstate__( self, state ):
		self.__dict__.update( state )

	def __getnewargs__( self ):
		return ( )

	def __setattr__( self, name, value ):
		assert name[0] == '_' or (name in self._fields), \
			"Field '%s' does not exist in document '%s'" \
			% (name, self.__class__.__name__)
		
		if name in self._fields:
			field = self._fields[name]
			mongoValue = field.fromPython( value )
			self._data[field.dbField] = mongoValue
			pythonValue = None
			if mongoValue is not None:
				pythonValue = field.toPython( mongoValue )
			self._values[name] = pythonValue
		else:
			assert name.startswith( '_' ), 'Only internal variables should ever be set as an attribute'
			super(BaseDocument, self).__setattr__( name, value )
	
	def __getattr__( self, name ):
		if name not in self._values and self._is_lazy and \
			'_id' in self._data and self._data['_id'] is not None:

			if name not in self._fields:
				raise AttributeError

			# field is being accessed and the object is currently in lazy mode
			# may need to retrieve rest of document
			field = self._fields[name]
			if field.dbField not in self._data:
				# field not retrieved from database! load whole document. weeee
				result = connection.getDatabase( )[self._collection].find_one( { '_id': self._data['_id'] } )
				if result is None:
					raise self.DoesNotExist
				self._fromMongo( result, overwrite=False )
				
				self._is_lazy = False
		
		field = self._fields.get( name, None )

		if not name in self._values:
			default = None
			if field is not None:
				default = field.getDefault( )

			self._values[name] = default
		
		value = self._values.get( name )
		
		return value
	
	def _resyncFromPython( self ):
		# before we go any further, re-sync from python values where needed
		for (name,field) in self._fields.items( ):
			requiresDefaultCall = (name not in self._values and callable(field.default))
			if field._resyncAtSave or requiresDefaultCall:
				dbField = field.dbField
				pythonValue = getattr(self, name)
				self._data[dbField] = field.fromPython( pythonValue )
				#print 'resyncing', dbField, 'to', self._data[dbField]
		
