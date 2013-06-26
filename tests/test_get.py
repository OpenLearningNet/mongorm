from mongorm import *
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
