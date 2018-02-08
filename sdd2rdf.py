import urllib2
import csv
import sys
import re
from datetime import datetime
import time
import pandas as pd
import configparser
import hashlib

kb=":"
out_fn = "out.ttl"
prefix_fn="prefixes.txt"

studyRef = None

# Need to implement input flags rather than ordering
if (len(sys.argv) < 2) :
    #print "Usage: python sdd2rdf.py <SDD_file> [<data_file>] [<codebook_file>] [<output_file>] [kb_prefix]\nOptional arguments can be skipped by entering '!'"
    print "Usage: python sdd2rdf.py <configuration_file>"
    sys.exit(1)

#file setup and configuration
config = configparser.ConfigParser()
try:
    config.read(sys.argv[1])
except:
    print "[ERROR] Unable to open configuration file."
    sys.exit(1)

#unspecified parameters in the config file should set the corresponding read string to ""
prefix_fn = config['Prefixes']['prefixes']
kb = config['Prefixes']['base_uri'] + ":"

sdd_fn = config['Source Files']['dictionary']
cb_fn = config['Source Files']['codebook']
timeline_fn = config['Source Files']['timeline']
cmap_fn = config['Source Files']['code_mappings']
data_fn = config['Source Files']['data_file']

out_fn = config['Output Files']['out_file']

if out_fn == "" :
    out_fn = "out.ttl"

output_file = open(out_fn,"w")
prefix_file = open(prefix_fn,"r")
prefixes = prefix_file.readlines()

for prefix in prefixes :
    output_file.write(prefix)
output_file.write("\n")

# K: parameterize this, too?
#code_mappings_url = 'https://raw.githubusercontent.com/tetherless-world/chear-ontology/master/code_mappings.csv'
#code_mappings_response = urllib2.urlopen(code_mappings_url)
#code_mappings_reader = csv.reader(code_mappings_response)
code_mappings_reader = pd.read_csv(cmap_fn)

unit_code_list = []
unit_uri_list = []
unit_label_list = []

actual_list = []
virtual_list = []

actual_tuples = []
virtual_tuples = []
cb_tuple = {}
timeline_tuple = {}

try :
    sdd_file = pd.read_csv(sdd_fn)
except:
    print "Error: The specified SDD file does not exist."
    sys.exit(1)

try: 
    # Set virtual and actual columns
    for row in sdd_file.itertuples() :
        if (pd.isnull(row.Column)) :
            print "Error: The SDD must have a column named 'Column'"
            sys.exit(1)
        if row.Column.startswith("??") :
            virtual_list.append(row)
        else :
            actual_list.append(row)
except : 
    print "Something went wrong when trying to read the SDD"
    sys.exit(1)

#Using itertuples on a data frame makes the column heads case-sensitive
for code_row in code_mappings_reader.itertuples() :
    if pd.notnull(code_row.code):
        unit_code_list.append(code_row.code)
    if pd.notnull(code_row.uri):
        unit_uri_list.append(code_row.uri)
    if pd.notnull(code_row.label):
        unit_label_list.append(code_row.label)

def parseString(input_string, delim) :
    my_list = input_string.split(delim)
    for i in range(0,len(my_list)) :
        my_list[i] = my_list[i].strip()
    return my_list

def codeMapper(input_word) :
    unitVal = input_word
    for unit_label in unit_label_list :
        if (unit_label == input_word) :
            unit_index = unit_label_list.index(unit_label)
            unitVal = unit_uri_list[unit_index]
    for unit_code in unit_code_list :
        if (unit_code == input_word) :
            unit_index = unit_code_list.index(unit_code)
            unitVal = unit_uri_list[unit_index]
    return unitVal    

def convertVirtualToKGEntry(*args) :
    if (args[0][:2] == "??") :
        if (studyRef is not None ) :
            if (args[0]==studyRef) :
                return kb + args[0][2:]
        if (len(args) == 2) :
            return kb + args[0][2:] + "-" + args[1]
        else : 
            return kb + args[0][2:]
    elif (':' not in args[0]) :
        # Check for entry in column list
        for item in actual_list :
            if args[0] == item.Column :
                if (len(args) == 2) :
                    return kb + args[0] + "-" + args[1]
                else :
                    return kb + args[0]
        return '"' + args[0] + "\"^^xsd:string"
    else :
        return args[0]

def checkVirtual(input_word) :
    try:
        if (input_word[:2] == "??") :
            return True
        else :
            return False
    except :
        print "Something went wrong in checkVirtual()"
        sys.exit(1)

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def writeVirtualRDF(virtual_list, virtual_tuples, output_file) :
    assertionString = ''
    provenanceString = ''
    output_file.write(kb + "head-virtual_entry { ")
    output_file.write("\n\t" + kb + "nanoPub-virtual_entry\trdf:type np:Nanopublication")
    output_file.write(" ;\n\t\tnp:hasAssertion " + kb + "assertion-virtual_entry")
    output_file.write(" ;\n\t\tnp:hasProvenance " + kb + "provenance-virtual_entry")
    output_file.write(" ;\n\t\tnp:hasPublicationInfo " + kb + "pubInfo-virtual_entry")
    output_file.write(" .\n}\n\n")
    for item in virtual_list :
        virtual_tuple = {}
        assertionString += "\n\t" + kb + item.Column[2:] + "\trdf:type\towl:Class"
        assertionString += " ;\n\t\trdfs:label \"" + item.Column[2:] + "\""
        # Set the rdf:type of the virtual row to either the Attribute or Entity value (or else owl:Individual)
        if (pd.notnull(item.Entity)) and (pd.isnull(item.Attribute)) :
            if ',' in item.Entity :
                entities = parseString(item.Entity,',')
                for entity in entities :
                    assertionString += " ;\n\t\trdfs:subClassOf " + codeMapper(entity)
            else :
                assertionString += " ;\n\t\trdfs:subClassOf " + codeMapper(item.Entity)
            virtual_tuple["Column"]=item.Column
            virtual_tuple["Entity"]=codeMapper(item.Entity)
            if (virtual_tuple["Entity"] == "hasco:Study") :
                global studyRef
                studyRef = item.Column
                virtual_tuple["Study"] = item.Column
        elif (pd.isnull(item.Entity)) and (pd.notnull(item.Attribute)) :
            if ',' in item.Attribute :
                attributes = parseString(item.Attribute,',')
                for attribute in attributes :
                    assertionString += " ;\n\t\trdfs:subClassOf " + codeMapper(attribute)
            else :
                assertionString += " ;\n\t\trdfs:subClassOf " + codeMapper(item.Attribute)
            assertionString += " ;\n\t\trdfs:subClassOf " + codeMapper(item.Attribute)
            virtual_tuple["Column"]=item.Column
            virtual_tuple["Attribute"]=codeMapper(item.Attribute)
        else :
            print "Warning: Virtual column not assigned an Entity or Attribute value, or was assigned both."
            virtual_tuple["Column"]=item.Column
        
        # If there is a value in the inRelationTo column ...
        if (pd.notnull(item.inRelationTo)) :
            virtual_tuple["inRelationTo"]=item.inRelationTo
            # If there is a value in the Relation column but not the Role column ...
            if (pd.notnull(item.Relation)) and (pd.isnull(item.Role)) :
                assertionString += " ;\n\t\t" + item.Relation + " " + convertVirtualToKGEntry(item.inRelationTo) 
                virtual_tuple["Relation"]=item.Relation
            # If there is a value in the Role column but not the Relation column ...
            elif (pd.isnull(item.Relation)) and (pd.notnull(item.Role)) :
                assertionString += " ;\n\t\tsio:hasRole [ rdf:type\t" + item.Role + " ;\n\t\t\tsio:inRelationTo " + convertVirtualToKGEntry(item.inRelationTo) + " ]"
                virtual_tuple["Role"]=item.Role
            # If there is a value in the Role and Relation columns ...
            elif (pd.notnull(item.Relation)) and (pd.notnull(item.Role)) :
                virtual_tuple["Relation"]=item.Relation
                virtual_tuple["Role"]=item.Role
                assertionString += " ;\n\t\tsio:inRelationTo " + convertVirtualToKGEntry(item.inRelationTo) 
        assertionString += " .\n"
        provenanceString += "\n\t" + kb + item.Column[2:] 
        provenanceString +="\n\t\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
        if pd.notnull(item.wasDerivedFrom) :
            provenanceString += " ;\n\t\tprov:wasDerivedFrom " + convertVirtualToKGEntry(item.wasDerivedFrom)
            virtual_tuple["wasDerivedFrom"]=item.wasDerivedFrom
        if pd.notnull(item.wasGeneratedBy) :
            provenanceString += " ;\n\t\tprov:wasGeneratedBy " + convertVirtualToKGEntry(item.wasGeneratedBy)
            virtual_tuple["wasGeneratedBy"]=item.wasGeneratedBy
        provenanceString += " .\n"
        virtual_tuples.append(virtual_tuple)
    
    if timeline_fn is not None :
        for key in timeline_tuple :
            assertionString += "\n\t" + convertVirtualToKGEntry(key)
            for timeEntry in timeline_tuple[key] :
                if 'Type' in timeEntry :
                    assertionString += " ;\n\t\trdf:subClassOf\t" + timeEntry['Type']
                if 'Label' in timeEntry :
                    assertionString += " ;\n\t\trdfs:label\t\"" + timeEntry['Label'] + "\"^^xsd:string"
                if 'Start' in timeEntry and 'End' in timeEntry and timeEntry['Start'] == timeEntry['End']:
                    assertionString += " ;\n\t\tsio:hasValue " + str(timeEntry['Start'])
                if 'Start' in timeEntry :
                    assertionString += " ;\n\t\tsio:hasStartTime [ sio:hasValue " + str(timeEntry['Start']) + " ]"
                if 'End' in timeEntry :
                    assertionString += " ;\n\t\tsio:hasEndTime [ sio:hasValue " + str(timeEntry['End']) + " ]"
                if 'Unit' in timeEntry :
                    assertionString += " ;\n\t\tsio:hasUnit\t" + timeEntry['Unit']
                if 'inRelationTo' in timeEntry :
                    assertionString += " ;\n\t\tsio:inRelationTo\t" + convertVirtualToKGEntry(timeEntry['inRelationTo'], index)
                assertionString += " .\n"
            provenanceString += "\n\t" + convertVirtualToKGEntry(key) + "\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n"
    output_file.write(kb + "assertion-virtual_entry {")
    output_file.write(assertionString + "\n}\n\n")
    output_file.write(kb + "provenance-virtual_entry {")
    provenanceString = "\n\t" + kb + "assertion-virtual_entry\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString
    output_file.write(provenanceString + "\n}\n\n")
    output_file.write(kb + "pubInfo-virtual_entry {\n\t" + kb + "nanoPub-virtual_entry\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n}\n\n")

def writeActualRDF(actual_list, actual_tuples, output_file) :
    assertionString = ''
    provenanceString = ''
    publicationInfoString = ''
    output_file.write(kb + "head-actual_entry { ")
    output_file.write("\n\t" + kb + "nanoPub-actual_entry\trdf:type np:Nanopublication")
    output_file.write(" ;\n\t\tnp:hasAssertion " + kb + "assertion-actual_entry")
    output_file.write(" ;\n\t\tnp:hasProvenance " + kb + "provenance-actual_entry")
    output_file.write(" ;\n\t\tnp:hasPublicationInfo " + kb + "pubInfo-actual_entry")
    output_file.write(" .\n}\n\n")
    for item in actual_list :
        actual_tuple = {}
        assertionString += "\n\t" + kb + item.Column.replace(" ","_").replace(",","").replace("(","").replace(")","") + "\trdf:type\towl:Class"
        if (pd.notnull(item.Attribute)) :
            if ',' in item.Attribute :
                attributes = parseString(item.Attribute,',')
                for attribute in attributes :
                    assertionString += " ;\n\t\trdfs:subClassOf " + convertVirtualToKGEntry(codeMapper(attribute))
            else :
                assertionString += " ;\n\t\trdfs:subClassOf " + convertVirtualToKGEntry(codeMapper(item.Attribute))
            actual_tuple["Column"]=item.Column
            actual_tuple["Attribute"]=codeMapper(item.Attribute)
        else :
            assertionString += " ;\n\t\trdfs:subClassOf\tsio:Attribute"
            actual_tuple["Column"]=item.Column
            actual_tuple["Attribute"]=codeMapper("sio:Attribute")
            print "Warning: Actual column not assigned an Attribute value."
        if (pd.notnull(item.attributeOf)) :
            assertionString += " ;\n\t\tsio:isAttributeOf " + convertVirtualToKGEntry(item.attributeOf)
            actual_tuple["isAttributeOf"]=item.attributeOf
        else :
            print "WARN: Actual column not assigned an isAttributeOf value. Skipping...."
        if (pd.notnull(item.Unit)) :
            assertionString += " ;\n\t\tsio:hasUnit " + codeMapper(item.Unit)
            actual_tuple["Unit"] = codeMapper(item.Unit)
        if (pd.notnull(item.Time)) :
            assertionString += " ;\n\t\tsio:existsAt " + convertVirtualToKGEntry(item.Time)
            actual_tuple["Time"]=item.Time
        if (pd.notnull(item.Relation) and pd.notnull(item.inRelationTo)) :
            assertionString += " ;\n\t\t" + item.Relation + " " + convertVirtualToKGEntry(item.inRelationTo)
            actual_tuple["Relation"]=item.Relation
        elif (pd.notnull(item.inRelationTo)) :
            assertionString += " ;\n\t\tsio:inRelationTo " + convertVirtualToKGEntry(item.inRelationTo)
            actual_tuple["inRelationTo"]=item.inRelationTo
        if (pd.notnull(item.Label)) :
            assertionString += " ;\n\t\trdfs:label \"" + item.Label + "\"^^xsd:string" 
            actual_tuple["Label"]=item.Label
        if (pd.notnull(item.Comment)) :
            assertionString += " ;\n\t\trdfs:comment \"" + item.Comment + "\"^^xsd:string"
            actual_tuple["Comment"]=item.Comment
        assertionString += " .\n" 
        
        provenanceString += "\n\t" + kb + item.Column.replace(" ","_").replace(",","").replace("(","").replace(")","")
        provenanceString += "\n\t\tprov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
        if (pd.notnull(item.wasDerivedFrom)) :
            if ',' in item.wasDerivedFrom :
                derivedFromTerms = parseString(item.wasDerivedFrom,',')
                for derivedFromTerm in derivedFromTerms :
                    provenanceString += " ;\n\t\tprov:wasDerivedFrom " + convertVirtualToKGEntry(derivedFromTerm)
            else :
                provenanceString += " ;\n\t\tprov:wasDerivedFrom " + convertVirtualToKGEntry(item.wasDerivedFrom)
            actual_tuple["wasDerivedFrom"]=item.wasDerivedFrom
        if (pd.notnull(item.wasGeneratedBy)) :
            if ',' in item.wasGeneratedBy :
                generatedByTerms = parseString(item.wasGeneratedBy,',')
                for generatedByTerm in generatedByTerms :
                    provenanceString += " ;\n\t\tprov:wasGeneratedBy " + convertVirtualToKGEntry(generatedByTerm)
            else :
                provenanceString += " ;\n\t\tprov:wasGeneratedBy " + convertVirtualToKGEntry(item.wasGeneratedBy)
            actual_tuple["wasGeneratedBy"]=item.wasGeneratedBy
        provenanceString += " .\n"
        if (pd.notnull(item.hasPosition)) :
            publicationInfoString += "\n\t" + kb + item.Column.replace(" ","_").replace(",","").replace("(","").replace(")","") + "\thasco:hasPosition\t\"" + str(item.hasPosition) + "\"^^xsd:integer ."
            actual_tuple["hasPosition"]=item.hasPosition
        actual_tuples.append(actual_tuple)
    output_file.write(kb + "assertion-actual_entry {")
    output_file.write(assertionString + "\n}\n\n")
    output_file.write(kb + "provenance-actual_entry {")
    provenanceString = "\n\t" + kb + "assertion-actual_entry\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString
    output_file.write(provenanceString + "\n}\n\n")
    output_file.write(kb + "pubInfo-actual_entry {\n\t" + kb + "nanoPub-actual_entry\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .")
    output_file.write(publicationInfoString + "\n}\n\n")

def writeVirtualEntry(assertionString, provenanceString,publicationInfoString, vref_list, v_column, index) : 
    try :
        if timeline_fn is not None :
            if v_column in timeline_tuple :
                assertionString += "\n\t" + convertVirtualToKGEntry(v_column, index) + "\trdf:type\t" + convertVirtualToKGEntry(v_column)
                for timeEntry in timeline_tuple[v_column] :
                    if 'Type' in timeEntry :
                        assertionString += " ;\n\t\trdf:type\t" + timeEntry['Type']
                    if 'Label' in timeEntry :
                        assertionString += " ;\n\t\trdfs:label\t\"" + timeEntry['Label'] + "\"^^xsd:string"
                    if 'Start' in timeEntry and 'End' in timeEntry and timeEntry['Start'] == timeEntry['End']:
                        assertionString += " ;\n\t\tsio:hasValue " + str(timeEntry['Start'])
                    if 'Start' in timeEntry :
                        assertionString += " ;\n\t\tsio:hasStartTime [ sio:hasValue " + str(timeEntry['Start']) + " ]"
                    if 'End' in timeEntry :
                        assertionString += " ;\n\t\tsio:hasEndTime [ sio:hasValue " + str(timeEntry['End']) + " ]"
                    if 'Unit' in timeEntry :
                        assertionString += " ;\n\t\tsio:hasUnit\t" + timeEntry['Unit']
                    if 'inRelationTo' in timeEntry :
                        assertionString += " ;\n\t\tsio:inRelationTo\t" + convertVirtualToKGEntry(timeEntry['inRelationTo'], index)
                assertionString += " .\n"
        for v_tuple in virtual_tuples :
            if (v_tuple["Column"] == v_column) :
                if "Study" in v_tuple :
                    continue
                else :
                    assertionString += "\n\t" + kb + v_tuple["Column"][2:] + "-" + index + "\trdf:type\t" + kb + v_tuple["Column"][2:]
                    if "Entity" in v_tuple :
                        if ',' in v_tuple["Entity"] :
                            entities = parseString(v_tuple["Entity"],',')
                            for entity in entities :
                                assertionString += ";\n\t\trdf:type\t" + entity
                        else :
                            assertionString += ";\n\t\trdf:type\t" + v_tuple["Entity"]
                    if "Attribute" in v_tuple :
                        if ',' in v_tuple["Attribute"] :
                            attributes = parseString(v_tuple["Attribute"],',')
                            for attribute in attributes :
                                assertionString += ";\n\t\trdf:type\t" + attribute
                        else :
                            assertionString += ";\n\t\trdf:type\t" + v_tuple["Attribute"]
                    if "Subject" in v_tuple :
                        assertionString += ";\n\t\tsio:hasIdentifier " + kb + v_tuple["Subject"] + "-" + index
                    if "inRelationTo" in v_tuple :
                        if ("Role" in v_tuple) and ("Relation" not in v_tuple) :
                            assertionString += " ;\n\t\tsio:hasRole [ rdf:type\t" + v_tuple["Role"] + " ;\n\t\t\tsio:inRelationTo " + convertVirtualToKGEntry(v_tuple["inRelationTo"], index) + " ]"
                        elif ("Role" not in v_tuple) and ("Relation" in v_tuple) :
                            assertionString += " ;\n\t\t" + v_tuple["Relation"] + " " + convertVirtualToKGEntry(v_tuple["inRelationTo"],index)
                        elif ("Role" not in v_tuple) and ("Relation" not in v_tuple) :
                            assertionString += " ;\n\t\tsio:inRelationTo " + convertVirtualToKGEntry(v_tuple["inRelationTo"],index)
                    assertionString += " .\n"
                    #if  "wasGeneratedBy" in v_tuple or "wasDerivedFrom" in v_tuple  :
                    provenanceString += "\n\t" + kb + v_tuple["Column"][2:] + "-" + index + "\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
                    if "wasGeneratedBy" in v_tuple : 
                        if ',' in v_tuple["wasGeneratedBy"] :
                            generatedByTerms = parseString(v_tuple["wasGeneratedBy"],',')
                            for generatedByTerm in generatedByTerms :
                                provenanceString += " ;\n\t\tprov:wasGeneratedBy " + convertVirtualToKGEntry(generatedByTerm,index)
                                if checkVirtual(generatedByTerm) and generatedByTerm not in vref_list :
                                    vref_list.append(generatedByTerm)
                        else :
                            provenanceString += " ;\n\t\tprov:wasGeneratedBy " + convertVirtualToKGEntry(v_tuple["wasGeneratedBy"],index)
                            if checkVirtual(v_tuple["wasGeneratedBy"]) and v_tuple["wasGeneratedBy"] not in vref_list :
                                vref_list.append(v_tuple["wasGeneratedBy"]);
                    if "wasDerivedFrom" in v_tuple : 
                        if ',' in v_tuple["wasDerivedFrom"] :
                            derivedFromTerms = parseString(v_tuple["wasDerivedFrom"],',')
                            for derivedFromTerm in derivedFromTerms :
                                provenanceString += " ;\n\t\tprov:wasDerivedFrom " + convertVirtualToKGEntry(derivedFromTerm,index)
                                if checkVirtual(derivedFromTerm) and derivedFromTerm not in vref_list :
                                    vref_list.append(derivedFromTerm);
                        else :
                            provenanceString += " ;\n\t\tprov:wasDerivedFrom " + convertVirtualToKGEntry(v_tuple["wasDerivedFrom"],index)
                            if checkVirtual(v_tuple["wasDerivedFrom"]) and v_tuple["wasDerivedFrom"] not in vref_list :
                                vref_list.append(v_tuple["wasDerivedFrom"]);
                    #if  "wasGeneratedBy" in v_tuple or "wasDerivedFrom" in v_tuple  :
                    provenanceString += " .\n"
        return [assertionString,provenanceString,publicationInfoString,vref_list]
    except :
        print "Warning: Unable to create virtual entry."

if cb_fn is not None :
    try :
        cb_file = pd.read_csv(cb_fn)
    except :
        print "Error: The specified Codebook file does not exist."
        sys.exit(1)
    try :
        inner_tuple_list = []
        row_num=0
        for row in cb_file.itertuples():
            if (pd.notnull(row.Column) and row.Column not in cb_tuple) :
                inner_tuple_list=[]
            inner_tuple = {}
            inner_tuple["Code"]=row.Code
            if(pd.notnull(row.Label)):
                inner_tuple["Label"]=row.Label
            if(pd.notnull(row.Class)) :
                inner_tuple["Class"]=row.Class
            inner_tuple_list.append(inner_tuple)
            cb_tuple[row.Column]=inner_tuple_list
            row_num += 1
    except :
        print "Warning: Unable to process Codebook file"

if timeline_fn is not None :
    try :
        timeline_file = pd.read_csv(timeline_fn)
    except :
        print "Error: The specified Codebook file does not exist."
        sys.exit(1)
    try :
        inner_tuple_list = []
        row_num=0
        for row in timeline_file.itertuples():
            if (pd.notnull(row.Name) and row.Name not in timeline_tuple) :
                inner_tuple_list=[]
            inner_tuple = {}
            inner_tuple["Type"]=row.Type
            if(pd.notnull(row.Label)):
                inner_tuple["Label"]=row.Label
            if(pd.notnull(row.Start)) :
                inner_tuple["Start"]=row.Start
            if(pd.notnull(row.End)) :
                inner_tuple["End"]=row.End
            if(pd.notnull(row.Unit)) :
                inner_tuple["Unit"]=row.Unit
            inner_tuple_list.append(inner_tuple)
            timeline_tuple[row.Name]=inner_tuple_list
            row_num += 1
    except :
        print "Warning: Unable to process Timeline file"

writeActualRDF(actual_list, actual_tuples, output_file)
writeVirtualRDF(virtual_list, virtual_tuples, output_file)

if data_fn != "" :
    try :
        data_file = pd.read_csv(data_fn)
    except :
        print "Error: The specified Data file does not exist."
        sys.exit(1)
    try :
        # ensure that there is a column annotated as the sio:Identifier or hasco:originalID in the data file:
        # TODO make sure this is getting the first available ID property for the _subject_ (and not anything else)
        col_headers=list(data_file.columns.values)
        #id_index=None
        try :
            for a_tuple in actual_tuples :
                if ((a_tuple["Attribute"] == "hasco:originalID") or (a_tuple["Attribute"] == "sio:Identifier")) :
                    if(a_tuple["Column"] in col_headers) :
                        #print a_tuple["Column"]
                        #id_index = col_headers.index(a_tuple["Column"]) + 1
                        #print id_index
                        for v_tuple in virtual_tuples :
                            if (a_tuple["isAttributeOf"] == v_tuple["Column"]) :
                                v_tuple["Subject"]=a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","")
        except: 
            print "Error processing column headers"
        for row in data_file.itertuples() :
            assertionString = ''
            provenanceString = ''
            publicationInfoString = ''
            id_string=''
            for term in row :
                id_string+=str(term)
            identifierString = hashlib.md5(id_string).hexdigest()
            '''if (id_index is None) :
                id_string=''
                for term in row :
                    id_string+=str(term)
                #print id_string
                identifierString = hashlib.md5(id_string).hexdigest()
            else :
                identifierString = str(row[id_index])'''
            try:
                output_file.write(kb + "head-" + identifierString + " {")
                output_file.write("\n\t" + kb + "nanoPub-" + identifierString)
                output_file.write("\n\t\trdf:type np:Nanopublication")
                output_file.write(" ;\n\t\tnp:hasAssertion " + kb + "assertion-" + identifierString)
                output_file.write(" ;\n\t\tnp:hasProvenance " + kb + "provenance-" + identifierString)
                output_file.write(" ;\n\t\tnp:hasPublicationInfo " + kb + "pubInfo-" + identifierString)
                output_file.write(" .\n}\n\n")# Nanopublication head
            #except : 
            #    print "Warning: Something went wrong when creating Nanopublicatipon head."
            #try :
                vref_list = []
                for a_tuple in actual_tuples :
                    #print a_tuple
                    if (a_tuple["Column"] in col_headers ) :
                        try :
                            try :
                                assertionString += "\n\t" + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","") + "-" + identifierString + "\trdf:type\t" + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","")
                                if ',' in a_tuple["Attribute"] :
                                    attributes = parseString(a_tuple["Attribute"],',')
                                    for attribute in attributes :
                                         assertionString + " ;\n\trdf:type\t" + attribute
                                else :
                                    " ;\n\trdf:type\t" + a_tuple["Attribute"]
                                assertionString += " ;\n\t\tsio:isAttributeOf " + convertVirtualToKGEntry(a_tuple["isAttributeOf"],identifierString)
                                if checkVirtual(a_tuple["isAttributeOf"]) :
                                    if a_tuple["isAttributeOf"] not in vref_list :
                                        vref_list.append(a_tuple["isAttributeOf"])
                                if "Unit" in a_tuple :
                                    assertionString += " ;\n\t\tsio:hasUnit " + a_tuple["Unit"]
                                if "Time" in a_tuple :
                                    assertionString += " ;\n\t\tsio:existsAt " + convertVirtualToKGEntry(a_tuple["Time"], identifierString)
                                    if checkVirtual(a_tuple["Time"]) :
                                        if a_tuple["Time"] not in vref_list :
                                            vref_list.append(a_tuple["Time"])
                                if "Label" in a_tuple :
                                    assertionString += " ;\n\t\trdfs:label \"" + a_tuple["Label"] + "\"^^xsd:string"
                                if "Comment" in a_tuple :
                                    assertionString += " ;\n\t\trdfs:comment \"" + a_tuple["Comment"] + "\"^^xsd:string"
                            except :
                                print "Error writing initial assertion elements"
                            try :
                                if row[col_headers.index(a_tuple["Column"])+1] != "" :
                                    #print row[col_headers.index(a_tuple["Column"])]
                                    if cb_fn is not None :
                                        if a_tuple["Column"] in cb_tuple :
                                            for tuple_row in cb_tuple[a_tuple["Column"]] :
                                                if ("Code" in tuple_row) and tuple_row['Code'] == str(row[col_headers.index(a_tuple["Column"])+1]) :
                                                    if ("Class" in tuple_row) and (tuple_row['Class'] is not "") :
                                                        if ',' in tuple_row['Class'] :
                                                            classTerms = parseString(tuple_row['Class'],',')
                                                            for classTerm in classTerms :
                                                                assertionString += " ;\n\t\trdf:type\t" + classTerm
                                                        else :
                                                            assertionString += " ;\n\t\trdf:type\t" + tuple_row['Class']
                                                    if ("Label" in tuple_row) and (tuple_row['Label'] is not "") :
                                                        assertionString += " ;\n\t\trdfs:label\t\"" + tuple_row['Label'] + "\"^^xsd:string"
                                    #print str(row[col_headers.index(a_tuple["Column"])])
                                    if str(row[col_headers.index(a_tuple["Column"])+1]) == "nan" :
                                        pass
                                    elif str(row[col_headers.index(a_tuple["Column"])+1]).isdigit() :
                                        assertionString += " ;\n\t\tsio:hasValue\t\"" + str(row[col_headers.index(a_tuple["Column"])+1]) + "\"^^xsd:integer"
                                    elif isfloat(str(row[col_headers.index(a_tuple["Column"])+1])) :
                                        assertionString += " ;\n\t\tsio:hasValue\t\"" + str(row[col_headers.index(a_tuple["Column"])+1]) + "\"^^xsd:float"
                                    else :
                                        assertionString += " ;\n\t\tsio:hasValue\t\"" + str(row[col_headers.index(a_tuple["Column"])+1]) + "\"^^xsd:string"
                                assertionString += " .\n"
                            except :
                                print "Error writing data value to assertion string:", row[col_headers.index(a_tuple["Column"])+1]
                            try :
                                provenanceString += "\n\t" + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","") + "-" + identifierString + "\tprov:generatedAtTime\t\"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
                                if "wasDerivedFrom" in a_tuple :
                                    if ',' in a_tuple["wasDerivedFrom"] :
                                        derivedFromTerms = parseString(a_tuple["wasDerivedFrom"],',')
                                        for derivedFromTerm in derivedFromTerms :
                                            provenanceString += " ;\n\t\tprov:wasDerivedFrom\t" + convertVirtualToKGEntry(derivedFromTerm, identifierString)
                                            if checkVirtual(derivedFromTerm) :
                                                if derivedFromTerm not in vref_list :
                                                    vref_list.append(derivedFromTerm)
                                    else :
                                        provenanceString += " ;\n\t\tprov:wasDerivedFrom\t" + convertVirtualToKGEntry(a_tuple["wasDerivedFrom"], identifierString)
                                    if checkVirtual(a_tuple["wasDerivedFrom"]) :
                                        if a_tuple["wasDerivedFrom"] not in vref_list :
                                            vref_list.append(a_tuple["wasDerivedFrom"])
                                if "wasGeneratedBy" in a_tuple :
                                    if ',' in a_tuple["wasGeneratedBy"] :
                                        generatedByTerms = parseString(a_tuple["wasGeneratedBy"],',')
                                        for generatedByTerm in generatedByTerms :
                                            provenanceString += " ;\n\t\tprov:wasGeneratedBy\t" + convertVirtualToKGEntry(generatedByTerm, identifierString)
                                            if checkVirtual(generatedByTerm) :
                                                if generatedByTerm not in vref_list :
                                                    vref_list.append(generatedByTerm)
                                    else :
                                        provenanceString += " ;\n\t\tprov:wasGeneratedBy\t" + convertVirtualToKGEntry(a_tuple["wasGeneratedBy"], identifierString)
                                    if checkVirtual(a_tuple["wasGeneratedBy"]) :
                                        if a_tuple["wasGeneratedBy"] not in vref_list :
                                            vref_list.append(a_tuple["wasGeneratedBy"])
                                if "inRelationTo" in a_tuple :
                                    if checkVirtual(a_tuple["inRelationTo"]) :
                                        if a_tuple["inRelationTo"] not in vref_list :
                                            vref_list.append(a_tuple["inRelationTo"])
                                    if "Relation" in a_tuple :
                                        provenanceString += " ;\n\t\t" + a_tuple["Relation"] + "\t" + convertVirtualToKGEntry(a_tuple["inRelationTo"], identifierString)
                                    else :
                                        provenanceString += " ;\n\t\tsio:inRelationTo\t" + convertVirtualToKGEntry(a_tuple["inRelationTo"], identifierString)
                                provenanceString += " .\n"
                                publicationInfoString += "\n\t" + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","") + "-" + identifierString
                                publicationInfoString += "\n\t\tprov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
                                if "hasPosition" in a_tuple :
                                    publicationInfoString += ";\n\t\thasco:hasPosition\t\"" + str(a_tuple["hasPosition"]) + "\"^^xsd:integer"
                                publicationInfoString += " .\n"
                            except :
                                print "Error writing provenance or publication info"
                        except :
                            print "Unable to process tuple" + a_tuple.__str__()
                try: 
                    for vref in vref_list : 
                        [assertionString,provenanceString,publicationInfoString,vref_list] = writeVirtualEntry(assertionString,provenanceString,publicationInfoString, vref_list, vref, identifierString)
                except :
                    print "Warning: Something went writing vref entries."
            except:
                print "Error: Something went wrong when processing actual tuples."
                sys.exit(1)
            output_file.write(kb + "assertion-" + identifierString + " {")
            output_file.write(assertionString + "\n}\n\n")
            output_file.write(kb + "provenance-" + identifierString + " {")
            provenanceString = "\n\t" + kb + "assertion-" + identifierString + " prov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString
            output_file.write(provenanceString + "\n}\n\n")
            output_file.write(kb + "pubInfo-" + identifierString + " {")
            publicationInfoString = "\n\t" + kb + "nanoPub-" + identifierString + " prov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + publicationInfoString
            output_file.write(publicationInfoString + "\n}\n\n")
    except :
        print "Warning: Unable to process Data file"

output_file.close()
