from bson import objectid, dbref
import bson.errors

from mongorm.fields.BaseField import BaseField
from mongorm.DocumentRegistry import DocumentRegistry

from mongorm.blackMagic import serialiseTypesForDocumentType

class ReferenceField(BaseField):
	def __init__( self, documentClass, *args, **kwargs ):
		super(ReferenceField, self).__init__( *args, **kwargs )
		self._use_ref_id = kwargs.get('use_ref_id', False)
		self.inputDocumentClass = documentClass
	
	def _getClassInfo( self ):
		if hasattr(self, 'documentName'): return
		
		documentClass = self.inputDocumentClass
		
		if isinstance(documentClass, basestring):
			if documentClass == 'self':
				self.documentName = self.ownerDocument.__name__
				self.documentClass = self.ownerDocument
			else:
				self.documentName = documentClass
				self.documentClass = DocumentRegistry.getDocument( self.documentName )
		else:
			self.documentClass = documentClass
			self.documentName = documentClass.__name__
	
	def fromPython( self, pythonValue, dereferences=[], modifier=None ):
		self._getClassInfo( )
		
		if pythonValue is None:
			return None
		
		if isinstance(pythonValue, dbref.DBRef):
			return {
				'_ref': pythonValue
			}
		elif not isinstance(pythonValue, self.documentClass):
			# try mapping to an objectid
			try:
				objectId = objectid.ObjectId( str( pythonValue ) )
			except bson.errors.InvalidId:
				pass # if it's not a valid ObjectId, then pass through and allow the assert to fail
			else:
				return {
					'_ref': dbref.DBRef( self.documentClass._collection, objectId ),
				}
		
		assert isinstance(pythonValue, self.documentClass), \
				"Referenced value must be a document of type %s" % (self.documentName,)
		assert pythonValue.id is not None, "Referenced Document must be saved before being assigned"
		
		data = {
			'_types': serialiseTypesForDocumentType(pythonValue.__class__),
			'_ref': dbref.DBRef( pythonValue.__class__._collection, pythonValue.id ),
		}
		
		return data
	
	def toQuery( self, pythonValue, dereferences=[] ):
		if pythonValue is None:
			return None
		# Note: this is only specific for cosmosdb which doesn't support dbref
		if self._use_ref_id:
			return {
				'_ref.$id': self.fromPython( pythonValue )['_ref'].id
			}
		else:
			return {
				'_ref': self.fromPython( pythonValue )['_ref']
			}
	
	def toPython( self, bsonValue ):
		self._getClassInfo( )
		
		if bsonValue is None:
			return None
		
		documentClass = None

		if isinstance(bsonValue, dbref.DBRef):
			# old style (mongoengine)
			dbRef = bsonValue
			documentClass = self.documentClass
			documentName = self.documentName
			initialData = {
				'_id': bsonValue.id,
			}
		else:
			# new style (dict with extra info)
			dbRef = bsonValue['_ref']
			if '_cls' in bsonValue:
				# mongoengine GenericReferenceField compatibility
				documentName = bsonValue['_cls']
			elif '_types' in bsonValue: 
				documentName = bsonValue['_types'][0]
			else:
				return dbRef

			documentClass = DocumentRegistry.getDocument( documentName )
			
			initialData = {
				'_id': dbRef.id,
			}
			initialData.update( bsonValue.get( '_cache', {} ) )
		
		return documentClass( )._fromMongo( initialData )
	
	def optimalIndex( self ):
		return self.dbField + '._ref'
