# SemanticDataDictionary
Tools and specifications for Semantic Data Dictionaries

# SDD Specification
The SDD may contain the following columns

| DM Column | Related Property | Description |
|------------ | -------------| -------------|
| Attribute    | _rdf:type_ | Class of attribute entry |
| attributeOf | _sio:isAttributeOf_ | Entity having the attribute |
| Column |  | Entry column header in dataset |
| Comment | _rdfs:comment_ | Comment for the entry |
| Definition | _skos:definition_ | Entry text definition|
| Entity | _rdf:type_ | Class of entity entry |
| Format | | Specifies the structure of the Unit value|
| inRelationTo | _sio:inRelationTo_ | Entity that the role is linked to |
| Label | _rdfs:label_ | Label for the entry |
| Relation | | Custom relation that replaces inRelationTo |
| Role | _sio:hasRole_ | Type of the role of the entry |
| Time | _sio:existsAt_ | Time point of measurement |
| Unit | _sio:hasUnit_ | Unit of Measure for entry |
| wasDerivedFrom | _prov:wasDerivedFrom_ | Entity from which the entry was derived |
| wasGeneratedBy | _prov:wasGeneratedBy_ | Activity from which the entry was produced |

# sdd2owl
store required prefixes in prefixes.txt file

Usage: python sdd2rdf.py <config_file> 

