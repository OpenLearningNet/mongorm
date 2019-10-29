# -*- coding: utf8 -*-

from builtins import str
from mongorm import *
from bson.dbref import DBRef

try:
	from pytz import timezone, UTC
except ImportError:
	PYTZ = False
else:
	PYTZ = True

try:
	from iso8601 import parse_date
except ImportError:
	ISO8601 = False
else:
	ISO8601 = True

from datetime import datetime

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
	connect( 'test_mongorm' )

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
	"""Tests to make sure comparisons with unicode work."""
	connect( 'test_mongorm' )

	class TestDocument(Document):
		s = StringField( )

	a = TestDocument( s=u"déjà vu" )
	a.save( )

	assert a.s == str(u"déjà vu")

def test_equality_with_datetime( ):
	"""Tests to make sure comparisons with datetime objects work."""
	connect( 'test_mongorm' )

	class TestDateTime(Document):
		timestamp = DateTimeField( )

	# Get current UTC time to the nearest millisecond
	now = datetime.utcnow( )
	now = now.replace( microsecond=(now.microsecond//1000)*1000 )

	if PYTZ:
		now = UTC.localize( now )
		tz = timezone( "Australia/Sydney" )
		now = now.astimezone( tz )
		now = tz.normalize( now )

	t = TestDateTime( timestamp=now )
	t.save( )

	assert t.timestamp == now

	if PYTZ:
		now = now.astimezone( UTC )

	t = TestDateTime.objects.get( pk=t.id )

	assert t.timestamp == now

	try:
		t.timestamp = now.isoformat( )
		t.save( )
	except ValueError:
		assert not ISO8601
	else:
		assert ISO8601

	if ISO8601:
		t = TestDateTime.objects.get( pk=t.id )

		assert t.timestamp == now

def test_equality_with_old_datetime( ):
	"""Tests to make sure comparisons with datetime objects
	before the epoch work."""
	connect( 'test_mongorm' )

	class TestDateTime(Document):
		timestamp = DateTimeField( )

	bestDaysOfMyLife = datetime( 1969, 1, 1, 12, 0, 0, 9000 )

	if PYTZ:
		tz = timezone( "Australia/Sydney" )
		bestDaysOfMyLife = tz.localize( bestDaysOfMyLife )
		bestDaysOfMyLife = tz.normalize( bestDaysOfMyLife )

	t = TestDateTime( timestamp=bestDaysOfMyLife )
	t.save( )

	assert t.timestamp == bestDaysOfMyLife

	t = TestDateTime.objects.get( pk=t.id )

	assert t.timestamp == bestDaysOfMyLife

	try:
		t.timestamp = bestDaysOfMyLife.isoformat( )
		t.save( )
	except ValueError:
		assert not ISO8601
	else:
		assert ISO8601

	if ISO8601:
		t = TestDateTime.objects.get( pk=t.id )

		assert t.timestamp == bestDaysOfMyLife
