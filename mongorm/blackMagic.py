from future.types.newobject import newobject

def serialiseTypesForDocumentType( documentType ):
	return [ cls.__name__ for cls in documentType.mro() if cls not in [object, newobject] \
			 and cls.__name__ not in ['Document', 'BaseDocument', 'EmbeddedDocument'] ]
