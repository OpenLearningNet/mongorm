from builtins import str

from mongorm.fields.BaseField import BaseField


class StringField(BaseField):
	def fromPython( self, pythonValue, dereferences=[], modifier=None ):
		if pythonValue is not None:
			pythonValue = str(pythonValue)
		return pythonValue

	def toPython( self, bsonValue ):
		if bsonValue is not None:
			bsonValue = str(bsonValue)
		return bsonValue
