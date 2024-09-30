# MIODB


V2.0 Featuring MongoDB support

- alias() has been deprecated in favour of tableAlias because it is used only for `select ... from table AS alias`
- join() have changed the behaviour because it was inconsistent (some tests passed some others no), so it may break code. Check out the examples in TestQueries.swift. Old behaviour, including non-passing tests, in TestQueries.v1.swift
- Added support to define alias for fields in select clauses. 



