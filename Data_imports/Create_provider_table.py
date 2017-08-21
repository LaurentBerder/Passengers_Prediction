from __future__ import print_function
import sys
sys.path.append('../')
from optidb.model import *

Model.init_db(def_w=True)

class Provider(Model):
    __collection__ = 'provider'

new_providers = \
[{'provider' : 'Mexico',
    'index' : {
        'confidence' : 22,
        'ym_start' : '2002-01'},
    'import_process' : True},

{'provider' : 'India - intl',
    'index' : {
        'confidence' : 15,
        'ym_start' : '2015-04'},
    'import_process' : True},

{'provider' : 'India - domestic',
    'index' : {
        'confidence' : 20,
        'ym_start' : '2015-01'},
    'import_process' : True},

{'provider' : 'Ireland',
    'index' : {
        'confidence' : 20,
        'ym_start' : '2006-01'},
    'import_process' : False},

{'provider' : 'Australia - intl',
    'index' : {
        'confidence' : 17,
        'ym_start' : '2009-01'},
    'import_process' : False},

{'provider' : 'Australia - domestic',
    'index' : {
        'confidence' : 38,
        'ym_start' : '2009-01'},
    'import_process' : False},

{'provider' : 'Chile',
    'index' : {
        'confidence' : 24,
        'ym_start' : '2010-01'},
    'import_process' : True},

{'provider' : 'UK',
    'index' : {
        'confidence' : 25,
        'ym_start' : '1983-01'},
    'import_process' : False},

{'provider' : 'USA',
    'index' : {
        'confidence' : 40,
        'ym_start' : '1990-01'},
    'import_process' : True},

{'provider' : 'Brazil',
    'index' : {
        'confidence' : 45,
        'ym_start' : '2000-01'},
    'import_process' : True},

{'provider' : 'Colombia',
    'index' : {
        'confidence' : 51,
        'ym_start' : '1992-01'},
    'import_process' : True},

{'provider' : 'Eurostat-bg',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-cz',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-dk',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-de',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-ee',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-ie',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-el',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-es',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-fr',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-hr',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-it',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-cy',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-lv',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-lt',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-lu',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-hu',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-mt',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-nl',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-at',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-pl',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-pt',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-ro',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-si',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-sk',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-fi',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-se',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-uk',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-is',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-no',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False},

{'provider' : 'Eurostat-ch',
    'index' : {
        'confidence' : 43,
        'ym_start' : '2003-01'},
    'import_process' : False}
 ]

for prov in new_providers:
    Provider.save(prov)
