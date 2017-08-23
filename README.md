# SemanticDataDictionary
Tools and specifications for Semantic Data Dictionaries

# SDD Specification
The SDD may contain the following columns

Column - Description - Related Property

Column - Column header from dataset - 
Label (OPTIONAL) - Label for the column - rdfs:label
Comment (OPTIONAL) - Comment for the column - rdfs:comment
Definition (OPTIONAL) - Column definition - skos:definition
Attribute - URI of the base attribute type - sio:hasAttribute
attributeOf - Entity having the attribute - sio:isAttributeOf
Unit - Unit of measure for the attribute - sio:hasUnit
Time - Time point attribute was measured - sio:existsAt
Entity - URI of the entity type - rdf:type
Role - Role that the entity plays - sio:hasRole
Relation - alternate relation linked to inRelationTo column - 
inRelationTo - entity related to the Role or described by the Relation - sio:inRelationTo

# sdd2owl
store required prefixes in prefixes.txt file

Usage: python sdd2owl.py <SDD_file> [ <data_file> ] [ <codebook_file> ] [ <output_file> ]


