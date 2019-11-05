from future import standard_library
standard_library.install_aliases()
from mongorm import Document, DocumentRegistry, StringField, connect

try:
	import cPickle as pickle
except ImportError:
	import pickle

class TestPickledDocument(Document):
	s = StringField( )

def setup_module( module ):
	DocumentRegistry.registerDocument( "TestPickledDocument", TestPickledDocument )

def teardown_module( module ):
	TestPickledDocument.objects.delete( )
	DocumentRegistry.clear( )

def test_pickle( ):
	"""Tests to make sure pickling works."""
	connect( 'test_mongorm' )

	assert DocumentRegistry.hasDocument( "TestPickledDocument" )

	cucumber = TestPickledDocument( s="spam" )
	cucumber.save( )

	assert cucumber == TestPickledDocument.objects.get( s="spam" )

	gherkin = pickle.dumps( cucumber )

	assert pickle.loads( gherkin ) == cucumber

	assert pickle.loads( gherkin ) == TestPickledDocument.objects.get( s="spam" )

def test_binary_pickle( ):
	"""Tests to make sure binary pickling works."""
	connect( 'test_mongorm' )

	assert DocumentRegistry.hasDocument( "TestPickledDocument" )

	cucumber = TestPickledDocument( s="eggs" )
	cucumber.save( )

	assert cucumber == TestPickledDocument.objects.get( s="eggs" )

	gherkin = pickle.dumps( cucumber, pickle.HIGHEST_PROTOCOL )

	assert pickle.loads( gherkin ) == cucumber

	assert pickle.loads( gherkin ) == TestPickledDocument.objects.get( s="eggs" )

def test_deleted_pickle( ):
	"""Tests to make sure deleted objects can be unpickled."""
	connect( 'test_mongorm' )

	assert DocumentRegistry.hasDocument( "TestPickledDocument" )

	cucumber = TestPickledDocument( s="onions" )
	cucumber.save( )

	assert cucumber == TestPickledDocument.objects.get( s="onions" )

	gherkin = pickle.dumps( cucumber, pickle.HIGHEST_PROTOCOL )

	TestPickledDocument.objects.filter( pk=cucumber.id ).delete( )
	assert TestPickledDocument.objects.filter( pk=cucumber.id ).count( ) == 0

	assert pickle.loads( gherkin ).s == "onions"
	assert pickle.loads( gherkin ) == cucumber

def test_modified_pickle( ):
	"""Tests to make sure pickled objects are updated."""
	connect( 'test_mongorm' )

	assert DocumentRegistry.hasDocument( "TestPickledDocument" )

	cucumber = TestPickledDocument( s="cabbage" )
	cucumber.save( )

	assert cucumber == TestPickledDocument.objects.get( s="cabbage" )

	gherkin = pickle.dumps( cucumber, pickle.HIGHEST_PROTOCOL )

	cucumber.s = "kimchi"
	cucumber.save( )

	assert pickle.loads( gherkin ) == cucumber

	assert TestPickledDocument.objects.filter( s="cabbage" ).count( ) == 0
	assert pickle.loads( gherkin ) == TestPickledDocument.objects.get( s="kimchi" )
