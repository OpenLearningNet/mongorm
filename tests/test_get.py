from mongorm import *
try:
	from pymongo.dbref import DBRef
	from pymongo.objectid import ObjectId
except ImportError:
	from bson.dbref import DBRef
	from bson.objectid import ObjectId

from pytest import raises

def teardown_module(module):
	DocumentRegistry.clear( )

def test_equality( ):
	"""Tests to make sure comparisons work. Equality compares database
	identity, not value similarity."""
	connect( 'test_mongorm' )
	
	class TestDocument(Document):
		s = StringField( )
	
	TestDocument.objects.delete()
	
	with raises(TestDocument.DoesNotExist):
		TestDocument.objects.get(s="hello")
	
	item = TestDocument(s="hello")
	item.save()
	
	assert item == TestDocument.objects.get(s="hello")
	
	item2 = TestDocument(s="hello")
	item2.save()
	
	with raises(TestDocument.MultipleObjectsReturned):
		TestDocument.objects.get(s="hello")

def test_non_existing_document( ):
	"""Tests to make sure non-existing documents raise the correct error."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	TestDocument.objects.delete( )

	item = TestDocument( )
	item._is_lazy = True
	item._data['_id'] = 123

	with raises(TestDocument.DoesNotExist):
		item.s

def test_non_existing_attribute( ):
	"""Tests to make sure non-existing attributes raise the correct error."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	item = TestDocument( )

	with raises(AttributeError):
		item.t

def test_non_existing_private_attribute( ):
	"""Tests to make sure non-existing private attributes raise the correct error."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	item = TestDocument( )

	with raises(AttributeError):
		item.__getstate__

def test_non_existing_attribute_lazy( ):
	"""Tests to make sure non-existing attributes in lazy mode raise the correct error."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	item = TestDocument( )
	item.s = 'spam'
	item.save( )

	with raises(AttributeError):
		TestDocument.objects.get( pk=item.id ).t

def test_non_existing_attribute_init( ):
	"""Tests to make sure non-existing attributes on init raise the correct error."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	with raises(AttributeError):
		TestDocument( t='spam' )

def test_unset_attribute_none( ):
	"""Tests to make sure unset (but existing) attributes return None."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	assert TestDocument( ).s is None
