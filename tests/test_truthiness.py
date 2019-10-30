from mongorm import *

def test_document_truth():
    assert bool(Document())