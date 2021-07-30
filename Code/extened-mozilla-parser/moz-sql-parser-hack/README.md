# Moz SQL Parser

Let's make a SQL parser so we can provide a familiar interface to non-sql datastores!


|Branch      |Status   |
|------------|---------|
|master      | [![Build Status](https://travis-ci.org/mozilla/moz-sql-parser.svg?branch=master)](https://travis-ci.org/mozilla/moz-sql-parser) |
|dev         | [![Build Status](https://travis-ci.org/mozilla/moz-sql-parser.svg?branch=dev)](https://travis-ci.org/mozilla/moz-sql-parser)    |


## Problem Statement

SQL is a familiar language used to access databases. Although, each database vendor has its quirky implementation, the average developer does not know enough SQL to be concerned with those quirks. This familiar core SQL (lowest common denominator, if you will) is useful enough to explore data in primitive ways. It is hoped that, once programmers have reviewed a datastore with basic SQL queries, and they see the value of that data, they will be motivated to use the datastore's native query format.

## Objectives

The primary objective of this library is to convert some subset of [SQL-92](https://en.wikipedia.org/wiki/SQL-92) queries to JSON-izable parse trees. A big enough subset to provide superficial data access via SQL, but not so much as we must deal with the document-relational impedance mismatch.

## Non-Objectives 

* No plans to provide update statements, like `update` or `insert`
* No plans to expand the language to all of SQL:2011
* No plans to provide data access tools 


## Project Status

There are [over 400 tests](https://github.com/mozilla/moz-sql-parser/tree/dev/tests). This parser is good enough for basic usage, including inner queries.

You can see the parser in action at [https://sql.telemetry.mozilla.org/](https://sql.telemetry.mozilla.org/) while using the ActiveData datasource

## Install

    pip install moz-sql-parser

## Parsing SQL

    >>> from moz_sql_parser import parse
    >>> import json
    >>> json.dumps(parse("select count(1) from jobs"))
    '{"select": {"value": {"count": 1}}, "from": "jobs"}'
    
Each SQL query is parsed to an object: Each clause is assigned to an object property of the same name. 

    >>> json.dumps(parse("select a as hello, b as world from jobs"))
    '{"select": [{"value": "a", "name": "hello"}, {"value": "b", "name": "world"}], "from": "jobs"}'

The `SELECT` clause is an array of objects containing `name` and `value` properties. 

### Recursion Limit 

**WARNING!** There is a recursion limit of `1500`. This prevents parsing of complex expressions or deeply nested nested queries. You can increase the recursion limit *after* you have imported `moz_sql_parser`, and before you `parse`:

    >>> from moz_sql_parser import parse
    >>> sys.setrecursionlimit(3000)
    >>> parse(complicated_sql)


## Generating SQL

You may also generate SQL from the a given JSON document. This is done by the formatter, which is still incomplete (Jan2020).

    >>> from moz_sql_parser import format
    >>> format({"from":"test", "select":["a.b", "c"]})
    'SELECT a.b, c FROM test'


## Contributing

In the event that the parser is not working for you, you can help make this better but simply pasting your sql (or JSON) into a new issue. Extra points if you describe the problem. Even more points if you submit a PR with a test.  If you also submit a fix, then you also have my gratitude. 


## Run Tests

See [the tests directory](https://github.com/mozilla/moz-sql-parser/tree/dev/tests) for instructions running tests, or writing new ones.

## More about implementation

SQL queries are translated to JSON objects: Each clause is assigned to an object property of the same name.

    
    # SELECT * FROM dual WHERE a>b ORDER BY a+b
    {
        "select": "*", 
        "from": "dual", 
        "where": {"gt": ["a", "b"]}, 
        "orderby": {"value": {"add": ["a", "b"]}}
    }
        
Expressions are also objects, but with only one property: The name of the operation, and the value holding (an array of) parameters for that operation. 

    {op: parameters}

and you can see this pattern in the previous example:

    {"gt": ["a","b"]}


### Notes

* Uses the glorious `pyparsing` library (see https://github.com/pyparsing/pyparsing) to define the grammar, and define the shape of the tokens it generates. 
* [sqlparse](https://pypi.python.org/pypi/sqlparse) does not provide a tree, rather a list of tokens. 
