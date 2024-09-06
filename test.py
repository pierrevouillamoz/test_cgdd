import utils
import pandas as pd
from numpy import nan
import requests_mock

    
def test_france_correction1():
    x="Farce"
    assert utils.france_correction(x)=="FRANCE"


def test_verification_france1():
    
    start_country="France"
    end_country="FRANCE"
    assert utils.verification_france(start_country,end_country)=="Journey in France"

def test_verification_france2():
    
    start_country="Allemagne"
    end_country="Belgique"
    assert utils.verification_france(start_country,end_country)=="Not France at all"

def test_verification_france3():
    
    start_country="Allemagne"
    end_country=314
    assert utils.verification_france(start_country,end_country)=="Not France at all"

def test_remove_units1():
    a=10
    assert utils.remove_units(a)==a

def test_remove_units2():
    a="10km"
    assert utils.remove_units(a)==10000

def test_remove_units3():
    a="10hours"
    assert utils.remove_units(a)==600

def test_zero_to_one1():
    x=-9
    assert utils.zero_to_one(x)==9

def test_zero_to_one2():
    x="O"
    assert utils.zero_to_one(x)==None

def test_zero_to_one3():
    x="1.3"
    assert utils.zero_to_one(x)==1.3

