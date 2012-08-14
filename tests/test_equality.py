from mongorm import *
from pymongo.dbref import DBRef

def teardown_module(module):
	DocumentRegistry.clear( )

def test_equality( ):
	"""Tests to make sure comparisons work. Equality compares database
	identity, not value similarity."""
	connect( 'test_mongorm' )
	
	class TestDocument(Document):
		s = StringField( )
	
	a = TestDocument( s="Hello" )
	a.save( )
	
	b = TestDocument( s="Hello" )
	b.save( )

	assert not (a == b)
	assert a != b
	
	c = TestDocument.objects.get(pk=a.id)

	assert c == a
	assert not (c != a)

def test_equality_with_none( ):
	"""Tests to make sure comparisons with None work."""

	class TestDocument(Document):
		s = StringField( )

	a = TestDocument( )
	a.save( )

	assert a.s is None

	a.s = ""
	a.save( )

	assert a.s == ""

	a.s = None
	a.save( )

	assert a.s is None

def test_equality_with_unicode( ):
	"""Tests to make sure comparisons with None work."""

	class TestDocument(Document):
		s = StringField( )

	a = TestDocument( s=u"déjà vu" )
	a.save( )

	assert a.s == u"déjà vu"
	assert a.s != "déjà vu"
	assert a.s != "deja vu"
