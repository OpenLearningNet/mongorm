from mongorm.fields.BaseField import BaseField

from datetime import datetime

try:
	from pytz import utc
except ImportError:
	PYTZ = False
else:
	PYTZ = True

class DateTimeField(BaseField):
	def fromPython( self, pythonValue, dereferences=[], modifier=None ):
		if pythonValue is not None and not isinstance(pythonValue, datetime):
			raise ValueError, "Value must be a datetime object not %r" % (pythonValue,)

		if PYTZ and pythonValue is not None and pythonValue.tzinfo is not None:
			pythonValue = pythonValue.astimezone( utc )
			pythonValue = pythonValue.replace( tzinfo=None )

		return pythonValue
	
	def toPython( self, bsonValue ):
		if PYTZ and bsonValue is not None:
			bsonValue = utc.localize( bsonValue )
		return bsonValue
