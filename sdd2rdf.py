import urllib2
import csv
import sys
import re
from datetime import datetime
import time
import pandas as pd
import configparser
import hashlib
import os
reload(sys)
sys.setdefaultencoding('utf8')

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
        for item in explicit_entry_list :
            if args[0] == item.Column :
                if (len(args) == 2) :
                    return kb + args[0].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-") + "-" + args[1]
                else :
                    return kb + args[0].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-")
        return '"' + args[0] + "\"^^xsd:string"
    else :
        return args[0]

def checkVirtual(input_word) :
    try:
        if (input_word[:2] == "??") :
            return True
        else :
            return False
    except Exception as e:
        print "Something went wrong in checkVirtual()" + str(e)
        sys.exit(1)

def isfloat(term):
    try:
        float(term)
        return True
    except ValueError:
        return False

def isURI(term):
    try:
        if any(c in term for c in ("http://","https://")) :
            return True
        else:
            return False
    except ValueError:
        return False

def isSchemaVar(term) :
    for entry in explicit_entry_list :
        if term == entry[1] :
            return True
    return False

def writeClassAttributeOrEntity(item, term, input_tuple, assertionString, whereString, swrlString) :
    if (pd.notnull(item.Entity)) and (pd.isnull(item.Attribute)) :
        if ',' in item.Entity :
            entities = parseString(item.Entity,',')
            for entity in entities :
                assertionString += " ;\n        rdfs:subClassOf    " + codeMapper(entity)
                whereString += codeMapper(entity) + " "
                swrlString += codeMapper(entity) + "(" + term + ") ^ "
                if entities.index(entity) + 1 != len(entities) :
                    whereString += ", "
        else :
            assertionString += " ;\n        rdfs:subClassOf    " + codeMapper(item.Entity)
            whereString += codeMapper(item.Entity) + " "
            swrlString += codeMapper(item.Entity) + "(" + term + ") ^ "
        input_tuple["Entity"]=codeMapper(item.Entity)
        if (input_tuple["Entity"] == "hasco:Study") :
            global studyRef
            studyRef = item.Column
            input_tuple["Study"] = item.Column
    elif (pd.isnull(item.Entity)) and (pd.notnull(item.Attribute)) :
        if ',' in item.Attribute :
            attributes = parseString(item.Attribute,',')
            for attribute in attributes :
                assertionString += " ;\n        rdfs:subClassOf    " + codeMapper(attribute)
                whereString += codeMapper(attribute) + " "
                swrlString += codeMapper(attribute) + "(" + term + ") ^ "
                if attributes.index(attribute) + 1 != len(attributes) :
                    whereString += ", "
        else :
            assertionString += " ;\n        rdfs:subClassOf    " + codeMapper(item.Attribute)
            whereString += codeMapper(item.Attribute) + " "
            swrlString += codeMapper(item.Attribute) + "(" + term + ") ^ "
        input_tuple["Attribute"]=codeMapper(item.Attribute)
    else :
        print "Warning: Entry not assigned an Entity or Attribute value, or was assigned both."
        input_tuple["Attribute"]=codeMapper("sio:Attribute")
        assertionString += " ;\n        rdfs:subClassOf    sio:Attribute"
        whereString += "sio:Attribute "
        swrlString += "sio:Attribute(" + term + ") ^ "
    return [input_tuple, assertionString, whereString, swrlString]

def writeClassAttributeOf(item, term, input_tuple, assertionString, whereString, swrlString) :
    if (pd.notnull(item.attributeOf)) :
        assertionString += " ;\n        " + properties_tuple["attributeOf"] + "    " + convertVirtualToKGEntry(item.attributeOf)
        whereString += ";\n    " + properties_tuple["attributeOf"] + "    " +  [item.attributeOf + " ",item.attributeOf[1:] + "_V "][checkVirtual(item.attributeOf)]
        swrlString += properties_tuple["attributeOf"] + "(" + term + " , " + [item.attributeOf,item.attributeOf[1:] + "_V"][checkVirtual(item.attributeOf)] + ") ^ "
        input_tuple["isAttributeOf"]=item.attributeOf
    return [input_tuple, assertionString, whereString, swrlString]

def writeClassUnit(item, term, input_tuple, assertionString, whereString, swrlString) :
    if (pd.notnull(item.Unit)) :
        assertionString += " ;\n        " + properties_tuple["Unit"] + "    " + str(codeMapper(item.Unit))
        whereString += " ;\n    " + properties_tuple["Unit"] + "    " + str(codeMapper(item.Unit))
        swrlString += properties_tuple["Unit"] + "(" + term + " , " + str(codeMapper(item.Unit)) + ") ^ "
        input_tuple["Unit"] = codeMapper(item.Unit)
    # Incorporate item.Format here
    return [input_tuple, assertionString, whereString, swrlString]

def writeClassTime(item, term, input_tuple, assertionString, whereString, swrlString) :
    if (pd.notnull(item.Time)) :
        assertionString += " ;\n        " + properties_tuple["Time"] + "    " + convertVirtualToKGEntry(item.Time)
        whereString += " ;\n    " + properties_tuple["Time"] + "     " + [item.Time + " ",item.Time[1:] + "_V "][checkVirtual(item.Time)]
        swrlString += properties_tuple["Time"] + "(" + term + " , " + [item.Time + " ",item.Time[1:] + "_V "][checkVirtual(item.Time)] + ") ^ "
        input_tuple["Time"]=item.Time
    return [input_tuple, assertionString, whereString, swrlString]

def writeClassRelation(item, term, input_tuple, assertionString, whereString, swrlString) :
    if (pd.notnull(item.inRelationTo)) :
        input_tuple["inRelationTo"]=item.inRelationTo
        # If there is a value in the Relation column but not the Role column ...
        if (pd.notnull(item.Relation)) and (pd.isnull(item.Role)) :
            assertionString += " ;\n        " + item.Relation + " " + convertVirtualToKGEntry(item.inRelationTo)
            if(isSchemaVar(item.inRelationTo)):
                whereString += ";\n    " + item.Relation + " ?" + item.inRelationTo.lower() + "_E "
                swrlString += item.Relation + "(" + term + " , " + "?" + item.inRelationTo.lower() + "_E) ^ "
            else :
                whereString += ";\n    " + item.Relation + " " + [item.inRelationTo + " ",item.inRelationTo[1:] + "_V "][checkVirtual(item.inRelationTo)]
                swrlString += item.Relation + "(" + term + " , " + [item.inRelationTo,item.inRelationTo[1:] + "_V"][checkVirtual(item.inRelationTo)] + ") ^ "
            input_tuple["Relation"]=item.Relation
        # If there is a value in the Role column but not the Relation column ...
        elif (pd.isnull(item.Relation)) and (pd.notnull(item.Role)) :
            assertionString += " ;\n        " + properties_tuple["Role"] + "    [ rdf:type    " + item.Role + " ;\n            " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(item.inRelationTo) + " ]"
            whereString += ";\n    " + properties_tuple["Role"] + "    [ rdf:type " + item.Role + " ;\n      " + properties_tuple["inRelationTo"] + "    " + [item.inRelationTo + " ",item.inRelationTo[1:] + "_V "][checkVirtual(item.inRelationTo)] + " ]"
            swrlString += "" # add appropriate swrl term
            input_tuple["Role"]=item.Role
        # If there is a value in the Role and Relation columns ...
        elif (pd.notnull(item.Relation)) and (pd.notnull(item.Role)) :
            input_tuple["Relation"]=item.Relation
            input_tuple["Role"]=item.Role
            assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(item.inRelationTo)
            if(isSchemaVar(item.inRelationTo)):
                whereString += ";\n    " + properties_tuple["inRelationTo"] + "    ?" + item.inRelationTo.lower() + "_E "
                swrlString += "" # add appropriate swrl term
            else :
                whereString += ";\n    " + properties_tuple["inRelationTo"] + "    " + [item.inRelationTo + " ",item.inRelationTo[1:] + "_V "][checkVirtual(item.inRelationTo)]
                swrlString += "" # add appropriate swrl term
        elif (pd.isnull(item.Relation)) and (pd.isnull(item.Role)) :
            assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(item.inRelationTo)
            if(isSchemaVar(item.inRelationTo)):
                whereString += ";\n    " + properties_tuple["inRelationTo"] + "    ?" + item.inRelationTo.lower() + "_E "
                swrlString += properties_tuple["inRelationTo"] + "(" + term + " , " + "?" + item.inRelationTo.lower() + "_E) ^ "
            else :
                whereString += ";\n    " + properties_tuple["inRelationTo"] + "    " + [item.inRelationTo + " ",item.inRelationTo[1:] + "_V "][checkVirtual(item.inRelationTo)] 
                swrlString += properties_tuple["inRelationTo"] + "(" + term + " , " + [item.inRelationTo,item.inRelationTo[1:] + "_V"][checkVirtual(item.inRelationTo)] + ") ^ "
    return [input_tuple, assertionString, whereString, swrlString]

def writeClassWasDerivedFrom(item, term, input_tuple, provenanceString, whereString, swrlString) :
    if pd.notnull(item.wasDerivedFrom) :
        provenanceString += " ;\n        " + properties_tuple["wasDerivedFrom"] + "    " + convertVirtualToKGEntry(item.wasDerivedFrom)
        input_tuple["wasDerivedFrom"]=item.wasDerivedFrom
        if(isSchemaVar(item.wasDerivedFrom)):
            whereString += ";\n    " + properties_tuple["wasDerivedFrom"] + "    ?" + item.wasDerivedFrom.lower() + "_E "
            swrlString += properties_tuple["wasDerivedFrom"] + "(" + term + " , " + "?" + item.wasDerivedFrom.lower() + "_E) ^ " 
        else :
            whereString += ";\n    " + properties_tuple["wasDerivedFrom"] + "    " + [item.wasDerivedFrom + " ",item.wasDerivedFrom[1:] + "_V "][checkVirtual(item.wasDerivedFrom)]
            swrlString += properties_tuple["wasDerivedFrom"] + "(" + term + " , " + [item.wasDerivedFrom,item.wasDerivedFrom[1:] + "_V"][checkVirtual(item.wasDerivedFrom)] + ") ^ " 
    return [input_tuple, provenanceString, whereString, swrlString]

def writeClassWasGeneratedBy(item, term, input_tuple, provenanceString, whereString, swrlString) :
    if pd.notnull(item.wasGeneratedBy) :
        provenanceString += " ;\n        " + properties_tuple["wasGeneratedBy"] + "    " + convertVirtualToKGEntry(item.wasGeneratedBy)
        input_tuple["wasGeneratedBy"]=item.wasGeneratedBy
        if(isSchemaVar(item.wasDerivedFrom)):
            whereString += ";\n    " + properties_tuple["wasGeneratedBy"] + "    ?" + item.wasGeneratedBy.lower() + "_E "
            swrlString += properties_tuple["wasGeneratedBy"] + "(" + term + " , " + "?" + item.wasGeneratedBy.lower() + "_E) ^ " 
        else :
            whereString += ";\n    " + properties_tuple["wasGeneratedBy"] + "    " + [item.wasGeneratedBy + " ",item.wasGeneratedBy[1:] + "_V "][checkVirtual(item.wasGeneratedBy)]
            swrlString += properties_tuple["wasGeneratedBy"] + "(" + term + " , " + [item.wasGeneratedBy,item.wasGeneratedBy[1:] + "_V"][checkVirtual(item.wasGeneratedBy)] + ") ^ " 
    return [input_tuple, provenanceString, whereString, swrlString]

def writeVirtualEntryTrig(virtual_entry_list, virtual_entry_tuples, output_file, query_file, swrl_file) :
    assertionString = ''
    provenanceString = ''
    whereString = '\n'
    swrlString = ''
    output_file.write(kb + "head-virtual_entry { ")
    output_file.write("\n    " + kb + "nanoPub-virtual_entry    rdf:type np:Nanopublication")
    output_file.write(" ;\n        np:hasAssertion " + kb + "assertion-virtual_entry")
    output_file.write(" ;\n        np:hasProvenance " + kb + "provenance-virtual_entry")
    output_file.write(" ;\n        np:hasPublicationInfo " + kb + "pubInfo-virtual_entry")
    output_file.write(" .\n}\n\n")
    for item in virtual_entry_list :
        virtual_tuple = {}
        assertionString += "\n    " + kb + item.Column[2:] + "    rdf:type    owl:Class ;\n        " + properties_tuple["Label"] + "    \"" + item.Column[2:] + "\""
        term_virt = item.Column[1:] + "_V"
        whereString += "  " + term_virt + " rdf:type " 
        virtual_tuple["Column"]=item.Column       
        [virtual_tuple, assertionString, whereString, swrlString] = writeClassAttributeOrEntity(item, term_virt, virtual_tuple, assertionString, whereString, swrlString)
        [virtual_tuple, assertionString, whereString, swrlString] = writeClassAttributeOf(item, term_virt, virtual_tuple, assertionString, whereString, swrlString)
        [virtual_tuple, assertionString, whereString, swrlString] = writeClassUnit(item, term_virt, virtual_tuple, assertionString, whereString, swrlString)
        [virtual_tuple, assertionString, whereString, swrlString] = writeClassTime(item, term_virt, virtual_tuple, assertionString, whereString, swrlString)
        [virtual_tuple, assertionString, whereString, swrlString] = writeClassRelation(item, term_virt, virtual_tuple, assertionString, whereString, swrlString)
        assertionString += " .\n"
        provenanceString += "\n    " + kb + item.Column[2:] 
        provenanceString +="\n        prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
        [virtual_tuple, provenanceString, whereString, swrlString] = writeClassWasGeneratedBy(item, term_virt, virtual_tuple, provenanceString, whereString, swrlString)
        [virtual_tuple, provenanceString, whereString, swrlString] = writeClassWasDerivedFrom(item, term_virt, virtual_tuple, provenanceString, whereString, swrlString)
        provenanceString += " .\n"
        whereString += ".\n\n"
        virtual_entry_tuples.append(virtual_tuple)

    if timeline_fn is not None :
        for key in timeline_tuple :
            assertionString += "\n    " + convertVirtualToKGEntry(key) + "    rdf:type    owl:Class "
            for timeEntry in timeline_tuple[key] :
                if 'Type' in timeEntry :
                    assertionString += " ;\n        rdf:subClassOf    " + timeEntry['Type']
                if 'Label' in timeEntry :
                    assertionString += " ;\n        " + properties_tuple["Label"] + "    \"" + timeEntry['Label'] + "\"^^xsd:string"
                if 'Start' in timeEntry and 'End' in timeEntry and timeEntry['Start'] == timeEntry['End']:
                    assertionString += " ;\n        sio:hasValue " + str(timeEntry['Start'])
                if 'Start' in timeEntry :
                    assertionString += " ;\n        sio:hasStartTime [ sio:hasValue " + str(timeEntry['Start']) + " ]"
                if 'End' in timeEntry :
                    assertionString += " ;\n        sio:hasEndTime [ sio:hasValue " + str(timeEntry['End']) + " ]"
                if 'Unit' in timeEntry :
                    assertionString += " ;\n        " + properties_tuple["Unit"] + "    " + timeEntry['Unit']
                if 'inRelationTo' in timeEntry :
                    assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(timeEntry['inRelationTo'])
                assertionString += " .\n"
            provenanceString += "\n    " + convertVirtualToKGEntry(key) + "    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n"
    output_file.write(kb + "assertion-virtual_entry {")
    output_file.write(assertionString + "\n}\n\n")
    output_file.write(kb + "provenance-virtual_entry {")
    provenanceString = "\n    " + kb + "assertion-virtual_entry    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString
    output_file.write(provenanceString + "\n}\n\n")
    output_file.write(kb + "pubInfo-virtual_entry {\n    " + kb + "nanoPub-virtual_entry    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n}\n\n")
    whereString += "}"
    #print whereString
    query_file.write(whereString.replace('-','_'))
    swrl_file.write(swrlString[:-2])

def writeExplicitEntryTrig(explicit_entry_list, explicit_entry_tuples, output_file, query_file, swrl_file) :
    assertionString = ''
    provenanceString = ''
    publicationInfoString = ''
    selectString = "SELECT "
    whereString = "WHERE {\n"
    swrlString = ""
    output_file.write(kb + "head-explicit_entry { ")
    output_file.write("\n    " + kb + "nanoPub-explicit_entry    rdf:type np:Nanopublication")
    output_file.write(" ;\n        np:hasAssertion " + kb + "assertion-explicit_entry")
    output_file.write(" ;\n        np:hasProvenance " + kb + "provenance-explicit_entry")
    output_file.write(" ;\n        np:hasPublicationInfo " + kb + "pubInfo-explicit_entry")
    output_file.write(" .\n}\n\n")
    for item in explicit_entry_list :
        explicit_entry_tuple = {}
        term = item.Column.replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-")
        assertionString += "\n    " + kb + term + "    rdf:type    owl:Class"
        selectString += "?" + term.lower() + " "
        whereString += "  ?" + term.lower() + "_E rdf:type "
        term_expl = "?" + term.lower() + "_E"
        explicit_entry_tuple["Column"]=item.Column
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassAttributeOrEntity(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassAttributeOf(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassUnit(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassTime(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassRelation(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        if ("Label" in item and pd.notnull(item.Label)) :
            assertionString += " ;\n        " + properties_tuple["Label"] + "    \"" + item.Label + "\"^^xsd:string" 
            explicit_entry_tuple["Label"]=item.Label
        if ("Comment" in item and pd.notnull(item.Comment)) :
            assertionString += " ;\n        " + properties_tuple["Comment"] + "    \"" + item.Comment + "\"^^xsd:string"
            explicit_entry_tuple["Comment"]=item.Comment
        assertionString += " .\n" 
        
        provenanceString += "\n    " + kb + term
        provenanceString += "\n        prov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
        [explicit_entry_tuple, provenanceString, whereString, swrlString] = writeClassWasGeneratedBy(item, term_expl, explicit_entry_tuple, provenanceString, whereString, swrlString)
        [explicit_entry_tuple, provenanceString, whereString, swrlString] = writeClassWasDerivedFrom(item, term_expl, explicit_entry_tuple, provenanceString, whereString, swrlString)
        provenanceString += " .\n"
        whereString += ";\n    sio:hasValue ?" + term.lower() + " .\n\n"
        if ("hasPosition" in item and pd.notnull(item.hasPosition)) :
            publicationInfoString += "\n    " + kb + term + "    hasco:hasPosition    \"" + str(item.hasPosition) + "\"^^xsd:integer ."
            explicit_entry_tuple["hasPosition"]=item.hasPosition
        explicit_entry_tuples.append(explicit_entry_tuple)
    output_file.write(kb + "assertion-explicit_entry {")
    output_file.write(assertionString + "\n}\n\n")
    output_file.write(kb + "provenance-explicit_entry {")
    provenanceString = "\n    " + kb + "assertion-explicit_entry    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString
    output_file.write(provenanceString + "\n}\n\n")
    output_file.write(kb + "pubInfo-explicit_entry {\n    " + kb + "nanoPub-explicit_entry    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .")
    output_file.write(publicationInfoString + "\n}\n\n")
    #print selectString
    #print whereString
    query_file.write(selectString)
    query_file.write(whereString.replace('-','_'))
    swrl_file.write(swrlString)

def writeVirtualEntry(assertionString, provenanceString,publicationInfoString, vref_list, v_column, index) : 
    try :
        if timeline_fn is not None :
            if v_column in timeline_tuple :
                assertionString += "\n    " + convertVirtualToKGEntry(v_column, index) + "    rdf:type    " + convertVirtualToKGEntry(v_column)
                for timeEntry in timeline_tuple[v_column] :
                    if 'Type' in timeEntry :
                        assertionString += " ;\n        rdf:type    " + timeEntry['Type']
                    if 'Label' in timeEntry :
                        assertionString += " ;\n        " + properties_tuple["Label"] + "    \"" + timeEntry['Label'] + "\"^^xsd:string"
                    if 'Start' in timeEntry and 'End' in timeEntry and timeEntry['Start'] == timeEntry['End']:
                        assertionString += " ;\n        sio:hasValue " + str(timeEntry['Start'])
                    if 'Start' in timeEntry :
                        assertionString += " ;\n        sio:hasStartTime [ sio:hasValue " + str(timeEntry['Start']) + " ]"
                    if 'End' in timeEntry :
                        assertionString += " ;\n        sio:hasEndTime [ sio:hasValue " + str(timeEntry['End']) + " ]"
                    if 'Unit' in timeEntry :
                        assertionString += " ;\n        " + properties_tuple["Unit"] + "    " + timeEntry['Unit']
                    if 'inRelationTo' in timeEntry :
                        assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(timeEntry['inRelationTo'], index)
                        if checkVirtual(timeEntry['inRelationTo']) and timeEntry['inRelationTo'] not in vref_list :
                            vref_list.append(timeEntry['inRelationTo'])
                assertionString += " .\n"
        for v_tuple in virtual_entry_tuples :
            if (v_tuple["Column"] == v_column) :
                if "Study" in v_tuple :
                    continue
                else :
                    assertionString += "\n    " + kb + v_tuple["Column"][2:] + "-" + index + "    rdf:type    " + kb + v_tuple["Column"][2:]
                    if "Entity" in v_tuple :
                        if ',' in v_tuple["Entity"] :
                            entities = parseString(v_tuple["Entity"],',')
                            for entity in entities :
                                assertionString += ";\n        rdf:type    " + entity
                        else :
                            assertionString += ";\n        rdf:type    " + v_tuple["Entity"]
                    if "Attribute" in v_tuple :
                        if ',' in v_tuple["Attribute"] :
                            attributes = parseString(v_tuple["Attribute"],',')
                            for attribute in attributes :
                                assertionString += ";\n        rdf:type    " + attribute
                        else :
                            assertionString += ";\n        rdf:type    " + v_tuple["Attribute"]
                    if "Subject" in v_tuple :
                        assertionString += ";\n        sio:hasIdentifier " + kb + v_tuple["Subject"] + "-" + index
                    if "inRelationTo" in v_tuple :
                        if ("Role" in v_tuple) and ("Relation" not in v_tuple) :
                            assertionString += " ;\n        " + properties_tuple["Role"] + "    [ rdf:type    " + v_tuple["Role"] + " ;\n            " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(v_tuple["inRelationTo"], index) + " ]"
                        elif ("Role" not in v_tuple) and ("Relation" in v_tuple) :
                            assertionString += " ;\n        " + v_tuple["Relation"] + " " + convertVirtualToKGEntry(v_tuple["inRelationTo"],index)
                        elif ("Role" not in v_tuple) and ("Relation" not in v_tuple) :
                            assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(v_tuple["inRelationTo"],index)
                    assertionString += " .\n"
                    #if  "wasGeneratedBy" in v_tuple or "wasDerivedFrom" in v_tuple  :
                    provenanceString += "\n    " + kb + v_tuple["Column"][2:] + "-" + index + "    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
                    if "wasGeneratedBy" in v_tuple : 
                        if ',' in v_tuple["wasGeneratedBy"] :
                            generatedByTerms = parseString(v_tuple["wasGeneratedBy"],',')
                            for generatedByTerm in generatedByTerms :
                                provenanceString += " ;\n        " + properties_tuple["wasGeneratedBy"] + "    " + convertVirtualToKGEntry(generatedByTerm,index)
                                if checkVirtual(generatedByTerm) and generatedByTerm not in vref_list :
                                    vref_list.append(generatedByTerm)
                        else :
                            provenanceString += " ;\n        " + properties_tuple["wasGeneratedBy"] + "    " + convertVirtualToKGEntry(v_tuple["wasGeneratedBy"],index)
                            if checkVirtual(v_tuple["wasGeneratedBy"]) and v_tuple["wasGeneratedBy"] not in vref_list :
                                vref_list.append(v_tuple["wasGeneratedBy"]);
                    if "wasDerivedFrom" in v_tuple : 
                        if ',' in v_tuple["wasDerivedFrom"] :
                            derivedFromTerms = parseString(v_tuple["wasDerivedFrom"],',')
                            for derivedFromTerm in derivedFromTerms :
                                provenanceString += " ;\n        " + properties_tuple["wasDerivedFrom"] + "    " + convertVirtualToKGEntry(derivedFromTerm,index)
                                if checkVirtual(derivedFromTerm) and derivedFromTerm not in vref_list :
                                    vref_list.append(derivedFromTerm);
                        else :
                            provenanceString += " ;\n        " + properties_tuple["wasDerivedFrom"] + "    " + convertVirtualToKGEntry(v_tuple["wasDerivedFrom"],index)
                            if checkVirtual(v_tuple["wasDerivedFrom"]) and v_tuple["wasDerivedFrom"] not in vref_list :
                                vref_list.append(v_tuple["wasDerivedFrom"]);
                    #if  "wasGeneratedBy" in v_tuple or "wasDerivedFrom" in v_tuple  :
                    provenanceString += " .\n"
        return [assertionString,provenanceString,publicationInfoString,vref_list]
    except Exception as e :
        print "Warning: Unable to create virtual entry: " + str(e)

kb=":"
out_fn = "out.ttl"
prefix_fn="prefixes.txt"

studyRef = None

# Need to implement input flags rather than ordering
if (len(sys.argv) < 2) :
    #print "Usage: python sdd2rdf.py <DM_file> [<data_file>] [<codebook_file>] [<output_file>] [kb_prefix]\nOptional arguments can be skipped by entering '!'"
    print "Usage: python sdd2rdf.py <configuration_file>"
    sys.exit(1)

#file setup and configuration
config = configparser.ConfigParser()
try:
    config.read(sys.argv[1])
except Exception as e :
    print "[ERROR] Unable to open configuration file:" + str(e)
    sys.exit(1)

#unspecified parameters in the config file should set the corresponding read string to ""
prefix_fn = config['Prefixes']['prefixes']
kb = config['Prefixes']['base_uri'] + ":"

dm_fn = config['Source Files']['dictionary']

if 'codebook' in config['Source Files'] :
    cb_fn = config['Source Files']['codebook']
else :
    cb_fn = None

if 'timeline' in config['Source Files'] :
    timeline_fn = config['Source Files']['timeline']
else :
    timeline_fn = None

if 'infosheet' in config['Source Files'] :
    infosheet_fn = config['Source Files']['infosheet']
else :
    infosheet_fn = None


if 'properties' in config['Source Files'] :
    properties_fn = config['Source Files']['properties']
else :
    properties_fn = None


cmap_fn = config['Source Files']['code_mappings']
data_fn = config['Source Files']['data_file']

out_fn = config['Output Files']['out_file']

if 'query_file' in config['Output Files'] :
    query_fn = config['Output Files']['query_file']
else :    
    query_fn = "queryQ"

if 'swrl_file' in config['Output Files'] :
    swrl_fn = config['Output Files']['swrl_file']
else :    
    swrl_fn = "swrlModel"

if out_fn == "" :
    out_fn = "out.ttl"

output_file = open(out_fn,"w")
query_file = open(query_fn,"w")
swrl_file = open(swrl_fn,"w")
prefix_file = open(prefix_fn,"r")
prefixes = prefix_file.readlines()

for prefix in prefixes :
    #print prefix.find(">")
    output_file.write(prefix)
    query_file.write(prefix[1:prefix.find(">")+1])
    query_file.write("\n")
output_file.write("\n")

unit_code_list = []
unit_uri_list = []
unit_label_list = []

explicit_entry_list = []
virtual_entry_list = []

explicit_entry_tuples = []
virtual_entry_tuples = []
cb_tuple = {}
timeline_tuple = {}
infosheet_tuple = {}
properties_tuple = {'Comment': 'rdfs:comment', 'attributeOf': 'sio:isAttributeOf', 'Attribute': 'rdf:type', 'wasDerivedFrom': 'prov:wasDerivedFrom', 'Label': 'rdfs:label', 'inRelationTo': 'sio:inRelationTo', 'Role': 'sio:hasRole', 'Time': 'sio:existsAt', 'Entity': 'rdf:type', 'Unit': 'sio:hasUnit', 'wasGeneratedBy': 'prov:wasGeneratedBy'}


if infosheet_fn is not None :
    try :
        infosheet_file = pd.read_csv(infosheet_fn, dtype=object)
    except Exception as e :
        print "Error: The specified Infosheet file does not exist or is unreadable: " + str(e)
        sys.exit(1)
    for row in infosheet_file.itertuples() :
        if(pd.notnull(row.Value)):
            infosheet_tuple[row.Attribute]=row.Value    
    output_file.write(kb + "head-dataset_metadata { ")
    output_file.write("\n    " + kb + "nanoPub-dataset_metadata    rdf:type np:Nanopublication")
    output_file.write(" ;\n        np:hasAssertion    " + kb + "assertion-dataset_metadata")
    output_file.write(" ;\n        np:hasProvenance    " + kb + "provenance-dataset_metadata")
    output_file.write(" ;\n        np:hasPublicationInfo    " + kb + "pubInfo-dataset_metadata")
    output_file.write(" .\n}\n\n")
    assertionString = kb + "dataset"
    provenanceString = "    " + kb + "dataset    <http://www.w3.org/ns/prov#generatedAtTime>    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
    if "Type" in infosheet_tuple :
        assertionString += "    rdf:type    <" + infosheet_tuple["Type"]+ ">"
    else :
        print "Error: The Infosheet file is missing the required Type value declaration"
        sys.exit(1)
    if "Title" in infosheet_tuple :
        assertionString += " ;\n        <http://purl.org/dc/terms/title>    \"" + infosheet_tuple["Title"] + "\"^^xsd:string"
    if "Alternative Title" in infosheet_tuple : # should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/alternative>    \"" + infosheet_tuple["Alternative Title"] + "\"^^xsd:string"
    if "Description" in infosheet_tuple :
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/description>    \"" + infosheet_tuple["Description"] + "\"^^xsd:string"
    if "Date Created" in infosheet_tuple :
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/created>    \"" + infosheet_tuple["Date Created"] + "\"^^xsd:date"
    if "Creators" in infosheet_tuple : # currently encoded as string, should also check if IRI, should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/creator>    \"" + infosheet_tuple["Creators"] + "\"^^xsd:string"
    if "Contributors" in infosheet_tuple : # currently encoded as string, should also check if IRI, should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/contributor>    \"" + infosheet_tuple["Contributors"] + "\"^^xsd:string"
    if "Publisher" in infosheet_tuple : # currently encoded as string, should also check if IRI, should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/publisher>    \"" + infosheet_tuple["Publisher"] + "\"^^xsd:string"
    if "Date of Issue" in infosheet_tuple :
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/issued>    \"" + infosheet_tuple["Date of Issue"] + "\"^^xsd:date"
    if "Link" in infosheet_tuple :
        assertionString += " ;\n        <http://xmlns.com/foaf/0.1/page>    <" + infosheet_tuple["Link"] + ">"
    if "Identifier" in infosheet_tuple :
        assertionString += " ;\n        <http://semanticscience.org/resource/hasIdentifier>    \n            [ rdf:type    <http://semanticscience.org/resource/Identifier> ; \n            <http://semanticscience.org/resource/hasValue>    \"" + infosheet_tuple["Identifier"] + "\"^^xsd:string ]"
    if "Keywords" in infosheet_tuple : # should check for multiple values
        assertionString += " ;\n        <http://www.w3.org/ns/dcat#keyword>    \"" + infosheet_tuple["Keywords"] + "\"^^xsd:string"
    if "License" in infosheet_tuple : # should check if IRI
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/license>    \"" + infosheet_tuple["License"] + "\"^^xsd:string"
    if "Rights" in infosheet_tuple : # should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/rights>    \"" + infosheet_tuple["Rights"] + "\"^^xsd:string"
    if "Language" in infosheet_tuple : # should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/language>    \"" + infosheet_tuple["Language"] + "\"^^xsd:string"
    if "Version" in infosheet_tuple :
        provenanceString += " ;\n        <http://purl.org/pav/version>    \"" + infosheet_tuple["Version"] + "\"^^xsd:string"
    if "Source" in infosheet_tuple : # should check for multiple values
        provenanceString += " ;\n        <http://purl.org/dc/terms/dct/source>    \"" + infosheet_tuple["Source"] + "\"^^xsd:string"
    if "File Format" in infosheet_tuple : # should check for multiple values
        assertionString += " ;\n        <http://purl.org/dc/terms/dct/format>    \"" + infosheet_tuple["File Format"] + "\"^^xsd:string"
    if "Documentation" in infosheet_tuple : # currently encoded as string, should check if IRI
        provenanceString += " ;\n        <http://www.w3.org/ns/dcat#landingPage>    \"" + infosheet_tuple["Documentation"] + "\"^^xsd:string"   
    if "Imports" in infosheet_tuple : # should check for multiple values
        assertionString += " ;\n        <http://www.w3.org/2002/07/owl#imports>    \"" + infosheet_tuple["Imports"] + "\"^^xsd:string"
    assertionString += " .\n"
    provenanceString += " .\n"
    output_file.write(kb + "assertion-dataset_metadata {\n    " + assertionString + "\n}\n\n")
    output_file.write(kb + "provenance-dataset_metadata {\n    " + kb + "assertion-dataset_metadata    <http://www.w3.org/ns/prov#generatedAtTime>    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString + "\n}\n\n")
    output_file.write(kb + "pubInfo-dataset_metadata {")
    publicationInfoString = "\n    " + kb + "nanoPub-dataset_metadata    <http://www.w3.org/ns/prov#generatedAtTime>    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n"
    output_file.write(publicationInfoString + "\n}\n\n")
    # If SDD files included in Infosheet, they override the config declarations
    if "Dictionary Mapping" in infosheet_tuple :
        dm_fn = infosheet_tuple["Dictionary Mapping"] 
    if "Codebook" in infosheet_tuple : 
        cb_fn = infosheet_tuple["Codebook"]
    if "Code Mapping" in infosheet_tuple : 
        cmap_fn = infosheet_tuple["Code Mapping"]
    if "Timeline" in infosheet_tuple : 
        timeline_fn = infosheet_tuple["Timeline"]

if properties_fn is not None :
    try :
        properties_file = pd.read_csv(properties_fn, dtype=object)
    except Exception as e :
        print "Error: The specified Properties file does not exist or is unreadable: " + str(e)
        sys.exit(1)
    for row in properties_file.itertuples() :
        if(pd.notnull(row.Property)):
            properties_tuple[row.Column]=row.Property

try :
    dm_file = pd.read_csv(dm_fn, dtype=object)
except Exception, e:
    print "Current directory: " + os.getcwd() + "/ - " + str(os.path.isfile(dm_fn)) 
    print "Error: The processing DM file \"" + dm_fn + "\": " + str(e)
    sys.exit(1)

try: 
    # Set virtual and explicit entries
    for row in dm_file.itertuples() :
        if (pd.isnull(row.Column)) :
            print "Error: The DM must have a column named 'Column'"
            sys.exit(1)
        if row.Column.startswith("??") :
            virtual_entry_list.append(row)
        else :
            explicit_entry_list.append(row)
except Exception as e :
    print "Something went wrong when trying to read the DM: " + str(e)
    sys.exit(1)

code_mappings_reader = pd.read_csv(cmap_fn)
#Using itertuples on a data frame makes the column heads case-sensitive
for code_row in code_mappings_reader.itertuples() :
    if pd.notnull(code_row.code):
        unit_code_list.append(code_row.code)
    if pd.notnull(code_row.uri):
        unit_uri_list.append(code_row.uri)
    if pd.notnull(code_row.label):
        unit_label_list.append(code_row.label)

if cb_fn is not None :
    try :
        cb_file = pd.read_csv(cb_fn, dtype=object)
    except Exception, e:
        print "Error: The processing Codebook file: " + str(e)
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
            #print row
            if ("Resource" in row and pd.notnull(row.Resource)) :
                inner_tuple["Resource"]=row.Resource
            inner_tuple_list.append(inner_tuple)
            cb_tuple[row.Column]=inner_tuple_list
            row_num += 1
    except Exception as e :
        print "Warning: Unable to process Codebook file: " + str(e)

if timeline_fn is not None :
    try :
        timeline_file = pd.read_csv(timeline_fn, dtype=object)
    except Exception as e :
        print "Error: The specified Timeline file does not exist: " + str(e)
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
            if(pd.notnull(row.inRelationTo)) :
                inner_tuple["inRelationTo"]=row.inRelationTo
            inner_tuple_list.append(inner_tuple)
            timeline_tuple[row.Name]=inner_tuple_list
            row_num += 1
    except Exception as e :
        print "Warning: Unable to process Timeline file: " + str(e)

writeExplicitEntryTrig(explicit_entry_list, explicit_entry_tuples, output_file, query_file, swrl_file)
writeVirtualEntryTrig(virtual_entry_list, virtual_entry_tuples, output_file, query_file, swrl_file)

if data_fn != "" :
    try :
        data_file = pd.read_csv(data_fn, dtype=object)
    except Exception as e :
        print "Error: The specified Data file does not exist: " + str(e)
        sys.exit(1)
    try :
        # ensure that there is a column annotated as the sio:Identifier or hasco:originalID in the data file:
        # TODO make sure this is getting the first available ID property for the _subject_ (and not anything else)
        col_headers=list(data_file.columns.values)
        #id_index=None
        try :
            for a_tuple in explicit_entry_tuples :
                if "Attribute" in a_tuple :
                    if ((a_tuple["Attribute"] == "hasco:originalID") or (a_tuple["Attribute"] == "sio:Identifier")) :
                        if(a_tuple["Column"] in col_headers) :
                            #print a_tuple["Column"]
                            #id_index = col_headers.index(a_tuple["Column"])# + 1
                            #print id_index
                            for v_tuple in virtual_entry_tuples :
                                if "isAttributeOf" in a_tuple :
                                    if (a_tuple["isAttributeOf"] == v_tuple["Column"]) :
                                        v_tuple["Subject"]=a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-")
        except Exception as e :
            print "Error processing column headers: " + str(e)
        for row in data_file.itertuples() :
            assertionString = ''
            provenanceString = ''
            publicationInfoString = ''
            #id_string = row[id_index]            
            id_string=''
            for term in row[1:] :
                if term is not None:
                    id_string+=str(term)
            #print row[1:]
            #print id_string
            npubIdentifier = hashlib.md5(id_string).hexdigest()
            '''if (id_index is None) :
                id_string=''
                for term in row :
                    id_string+=str(term)
                #print id_string
                identifierString = hashlib.md5(id_string).hexdigest()
            else :
                identifierString = str(row[id_index])'''
            try:
                output_file.write(kb + "head-" + npubIdentifier + " {")
                output_file.write("\n    " + kb + "nanoPub-" + npubIdentifier)
                output_file.write("\n        rdf:type np:Nanopublication")
                output_file.write(" ;\n        np:hasAssertion " + kb + "assertion-" + npubIdentifier)
                output_file.write(" ;\n        np:hasProvenance " + kb + "provenance-" + npubIdentifier)
                output_file.write(" ;\n        np:hasPublicationInfo " + kb + "pubInfo-" + npubIdentifier)
                output_file.write(" .\n}\n\n")# Nanopublication head
            #except Exception as e : 
            #    print "Warning: Something went wrong when creating Nanopublicatipon head: " + str(e)
            #try :
                vref_list = []
                for a_tuple in explicit_entry_tuples :
                    #print a_tuple
                    if (a_tuple["Column"] in col_headers ) :                     
                        typeString = ""
                        if "Attribute" in a_tuple :
                            typeString += str(a_tuple["Attribute"])
                        if "Entity" in a_tuple :
                            typeString += str(a_tuple["Entity"])
                        if "Label" in a_tuple :
                            typeString += str(a_tuple["Label"])
                        if "Unit" in a_tuple :
                            typeString += str(a_tuple["Unit"])
                        if "Time" in a_tuple :
                            typeString += str(a_tuple["Time"])
                        if "inRelationTo" in a_tuple :
                            typeString += str(a_tuple["inRelationTo"])
                        if "wasGeneratedBy" in a_tuple :
                            typeString += str(a_tuple["wasGeneratedBy"])
                        if "wasDerivedFrom" in a_tuple :
                            typeString += str(a_tuple["wasDerivedFrom"])
                        identifierString = hashlib.md5(str(row[col_headers.index(a_tuple["Column"])+1])+typeString).hexdigest()
                        try :
                            try :
                                assertionString += "\n    " + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-") + "-" + identifierString + "    rdf:type    " + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-")
                                if "Attribute" in a_tuple :
                                    if ',' in a_tuple["Attribute"] :
                                        attributes = parseString(a_tuple["Attribute"],',')
                                        for attribute in attributes :
                                            assertionString += " ;\n        rdf:type    " + attribute
                                    else :
                                        assertionString += " ;\n        rdf:type    " + a_tuple["Attribute"]
                                if "Entity" in a_tuple :
                                    if ',' in a_tuple["Entity"] :
                                        entities = parseString(a_tuple["Entity"],',')
                                        for entity in entities :
                                            assertionString += " ;\n        rdf:type    " + entity
                                    else :
                                        assertionString += " ;\n        rdf:type    " + a_tuple["Entity"]
                                if "isAttributeOf" in a_tuple :
                                    if checkVirtual(a_tuple["isAttributeOf"]) :
                                        assertionString += " ;\n        " + properties_tuple["attributeOf"] + "    " + convertVirtualToKGEntry(a_tuple["isAttributeOf"],npubIdentifier)
                                        if a_tuple["isAttributeOf"] not in vref_list :
                                            vref_list.append(a_tuple["isAttributeOf"])
                                    else:
                                        assertionString += " ;\n        " + properties_tuple["attributeOf"] + "    " + convertVirtualToKGEntry(a_tuple["isAttributeOf"],identifierString)
                                if "Unit" in a_tuple :
                                    assertionString += " ;\n        " + properties_tuple["Unit"] + "    " + a_tuple["Unit"]
                                if "Time" in a_tuple :
                                    if checkVirtual(a_tuple["Time"]) :
                                        assertionString += " ;\n        " + properties_tuple["Time"] + "    " + convertVirtualToKGEntry(a_tuple["Time"], npubIdentifier)
                                        if a_tuple["Time"] not in vref_list :
                                            vref_list.append(a_tuple["Time"])
                                    else :
                                        assertionString += " ;\n        " + properties_tuple["Time"] + "    " + convertVirtualToKGEntry(a_tuple["Time"], identifierString)
                                if "Label" in a_tuple :
                                    assertionString += " ;\n        " + properties_tuple["Label"] + "    \"" + a_tuple["Label"] + "\"^^xsd:string"
                                if "Comment" in a_tuple :
                                    assertionString += " ;\n        " + properties_tuple["Comment"] + "    \"" + a_tuple["Comment"] + "\"^^xsd:string"
                                if "inRelationTo" in a_tuple :
                                    if checkVirtual(a_tuple["inRelationTo"]) :
                                        if a_tuple["inRelationTo"] not in vref_list :
                                            vref_list.append(a_tuple["inRelationTo"])
                                        if "Relation" in a_tuple :
                                            assertionString += " ;\n        " + a_tuple["Relation"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"], npubIdentifier)
                                        elif "Role" in a_tuple :
                                            assertionString += " ;\n        " + properties_tuple["Role"] + "    [ rdf:type    " + a_tuple["Role"] + " ;\n            " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"],npubIdentifier) + " ]"
                                        else :
                                            assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"], npubIdentifier)
                                    else:
                                        if "Relation" in a_tuple :
                                            assertionString += " ;\n        " + a_tuple["Relation"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"], identifierString)
                                        elif "Role" in a_tuple :
                                            assertionString += " ;\n        " + properties_tuple["Role"] + "    [ rdf:type    " + a_tuple["Role"] + " ;\n            " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"],identifierString) + " ]"
                                        else :
                                            assertionString += " ;\n        " + properties_tuple["inRelationTo"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"], identifierString)                       
                            except Exception as e:
                                print "Error writing initial assertion elements: "
                                if hasattr(e, 'message'):
                                    print(e.message)
                                else:
                                    print(e)
                            try :
                                if row[col_headers.index(a_tuple["Column"])+1] != "" :
                                    #print row[col_headers.index(a_tuple["Column"])]
                                    if cb_fn is not None :
                                        if a_tuple["Column"] in cb_tuple :
                                            #print a_tuple["Column"]
                                            for tuple_row in cb_tuple[a_tuple["Column"]] :
                                                #print tuple_row
                                                if ("Code" in tuple_row) and (str(tuple_row['Code']) == str(row[col_headers.index(a_tuple["Column"])+1]) ):
                                                    #print tuple_row['Code']
                                                    if ("Class" in tuple_row) and (tuple_row['Class'] is not "") :
                                                        if ',' in tuple_row['Class'] :
                                                            classTerms = parseString(tuple_row['Class'],',')
                                                            for classTerm in classTerms :
                                                                assertionString += " ;\n        rdf:type    " + classTerm
                                                        else :
                                                            assertionString += " ;\n        rdf:type    " + tuple_row['Class']
                                                    if ("Resource" in tuple_row) and (tuple_row['Resource'] is not "") :
                                                        if ',' in tuple_row['Resource'] :
                                                            classTerms = parseString(tuple_row['Resource'],',')
                                                            for classTerm in classTerms :
                                                                assertionString += " ;\n        rdf:type    " + convertVirtualToKGEntry(codeMapper(classTerm))
                                                        else :
                                                            assertionString += " ;\n        rdf:type    " + convertVirtualToKGEntry(codeMapper(tuple_row['Resource']))
                                                    if ("Label" in tuple_row) and (tuple_row['Label'] is not "") :
                                                        assertionString += " ;\n        " + properties_tuple["Label"] + "    \"" + tuple_row['Label'] + "\"^^xsd:string"
                                    #print str(row[col_headers.index(a_tuple["Column"])])
                                    try :
                                        if str(row[col_headers.index(a_tuple["Column"])+1]) == "nan" :
                                            pass
                                        elif str(row[col_headers.index(a_tuple["Column"])+1]).isdigit() :
                                            assertionString += " ;\n        sio:hasValue    \"" + str(row[col_headers.index(a_tuple["Column"])+1]) + "\"^^xsd:integer"
                                        elif isfloat(str(row[col_headers.index(a_tuple["Column"])+1])) :
                                            assertionString += " ;\n        sio:hasValue    \"" + str(row[col_headers.index(a_tuple["Column"])+1]) + "\"^^xsd:float"
                                        else :
                                            assertionString += " ;\n        sio:hasValue    \"" + str(row[col_headers.index(a_tuple["Column"])+1]) + "\"^^xsd:string"
                                    except Exception as e :
                                        print "Warning: unable to write value to assertion string:", row[col_headers.index(a_tuple["Column"])+1] + ": " + str(e)
                                assertionString += " .\n"
                            except Exception as e:
                                print "Error writing data value to assertion string:", row[col_headers.index(a_tuple["Column"])+1], ": " + str(e)
                            try :
                                provenanceString += "\n    " + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-") + "-" + identifierString + "    prov:generatedAtTime    \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
                                if "wasDerivedFrom" in a_tuple :
                                    if ',' in a_tuple["wasDerivedFrom"] :
                                        derivedFromTerms = parseString(a_tuple["wasDerivedFrom"],',')
                                        for derivedFromTerm in derivedFromTerms :
                                            if checkVirtual(derivedFromTerm) :
                                                provenanceString += " ;\n        " + properties_tuple["wasDerivedFrom"] + "    " + convertVirtualToKGEntry(derivedFromTerm, npubIdentifier)
                                                if derivedFromTerm not in vref_list :
                                                    vref_list.append(derivedFromTerm)
                                            else :
                                                provenanceString += " ;\n        " + properties_tuple["wasDerivedFrom"] + "    " + convertVirtualToKGEntry(derivedFromTerm, identifierString)
                                    else :
                                        provenanceString += " ;\n        " + properties_tuple["wasDerivedFrom"] + "    " + convertVirtualToKGEntry(a_tuple["wasDerivedFrom"], identifierString)
                                    if checkVirtual(a_tuple["wasDerivedFrom"]) :
                                        if a_tuple["wasDerivedFrom"] not in vref_list :
                                            vref_list.append(a_tuple["wasDerivedFrom"])
                                if "wasGeneratedBy" in a_tuple :
                                    if ',' in a_tuple["wasGeneratedBy"] :
                                        generatedByTerms = parseString(a_tuple["wasGeneratedBy"],',')
                                        for generatedByTerm in generatedByTerms :
                                            if checkVirtual(generatedByTerm) :
                                                provenanceString += " ;\n        " + properties_tuple["wasGeneratedBy"] + "    " + convertVirtualToKGEntry(generatedByTerm, npubIdentifier)
                                                if generatedByTerm not in vref_list :
                                                    vref_list.append(generatedByTerm)
                                            else:
                                                provenanceString += " ;\n        " + properties_tuple["wasGeneratedBy"] + "    " + convertVirtualToKGEntry(generatedByTerm, identifierString)
                                    else :
                                        provenanceString += " ;\n        " + properties_tuple["wasGeneratedBy"] + "    " + convertVirtualToKGEntry(a_tuple["wasGeneratedBy"], identifierString)
                                    if checkVirtual(a_tuple["wasGeneratedBy"]) :
                                        if a_tuple["wasGeneratedBy"] not in vref_list :
                                            vref_list.append(a_tuple["wasGeneratedBy"])
#                                if "inRelationTo" in a_tuple :
#                                    if checkVirtual(a_tuple["inRelationTo"]) :
#                                        if a_tuple["inRelationTo"] not in vref_list :
#                                            vref_list.append(a_tuple["inRelationTo"])
#                                    if "Relation" in a_tuple :
#                                        provenanceString += " ;\n        " + a_tuple["Relation"] + "    " + convertVirtualToKGEntry(a_tuple["inRelationTo"], identifierString)
#                                    elif "Role" in a_tuple :
#                                        provenanceString += " ;\n        sio:hasRole [ rdf:type    " + a_tuple["Role"] + " ;\n            sio:inRelationTo " + convertVirtualToKGEntry(a_tuple["inRelationTo"]) + " ]"
#                                    else :
#                                        provenanceString += " ;\n        sio:inRelationTo    " + convertVirtualToKGEntry(a_tuple["inRelationTo"], identifierString)
                                provenanceString += " .\n"
                                publicationInfoString += "\n    " + kb + a_tuple["Column"].replace(" ","_").replace(",","").replace("(","").replace(")","").replace("/","-").replace("\\","-") + "-" + identifierString
                                publicationInfoString += "\n        prov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime "
                                if "hasPosition" in a_tuple :
                                    publicationInfoString += ";\n        hasco:hasPosition    \"" + str(a_tuple["hasPosition"]) + "\"^^xsd:integer"
                                publicationInfoString += " .\n"
                            except Exception as e:
                                print "Error writing provenance or publication info: " + str(e)
                        except Exception as e:
                            print "Unable to process tuple" + a_tuple.__str__() + ": " + str(e)
                try: 
                    for vref in vref_list : 
                        [assertionString,provenanceString,publicationInfoString,vref_list] = writeVirtualEntry(assertionString,provenanceString,publicationInfoString, vref_list, vref, npubIdentifier)
                except Exception as e:
                    print "Warning: Something went writing vref entries: " + str(e)
            except Exception as e:
                print "Error: Something went wrong when processing explicit tuples: " + str(e)
                sys.exit(1)
            output_file.write(kb + "assertion-" + npubIdentifier + " {")
            output_file.write(assertionString + "\n}\n\n")
            output_file.write(kb + "provenance-" + npubIdentifier + " {")
            provenanceString = "\n    " + kb + "assertion-" + npubIdentifier + " prov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + provenanceString
            output_file.write(provenanceString + "\n}\n\n")
            output_file.write(kb + "pubInfo-" + npubIdentifier + " {")
            publicationInfoString = "\n    " + kb + "nanoPub-" + npubIdentifier + " prov:generatedAtTime \"" + "{:4d}-{:02d}-{:02d}".format(datetime.utcnow().year,datetime.utcnow().month,datetime.utcnow().day) + "T" + "{:02d}:{:02d}:{:02d}".format(datetime.utcnow().hour,datetime.utcnow().minute,datetime.utcnow().second) + "Z\"^^xsd:dateTime .\n" + publicationInfoString
            output_file.write(publicationInfoString + "\n}\n\n")
    except Exception as e :
        print "Warning: Unable to process Data file: " + str(e)

output_file.close()
