import urllib2
import csv
import sys
import re
import pandas as pd

kb=":"
out_fn = "out.ttl"
prefix_fn="prefixes.txt"

studyRef = None

# Need to implement input flags rather than ordering
if (len(sys.argv) < 2) :
    print "Usage: python sdd2owl.py <SDD_file> [<data_file>] [<codebook_file>] [<output_file>] [kb_prefix]\nOptional arguments can be skipped by entering '!'"
    sys.exit(1)

if (len(sys.argv) > 1) :
    sdd_fn = sys.argv[1]

    if (len(sys.argv) > 2) :
        data_fn = sys.argv[2]

        if (len(sys.argv) > 3) :
            cb_fn = sys.argv[3]
            if (len(sys.argv) > 4) :
                out_fn = sys.argv[4]
                if (len(sys.argv) > 5) :
                    if not (sys.argv[5] == "!" ):
                        if ":" not in sys.argv[5] :
                            kb = sys.argv[5] + ":"
                        else :                       
                            kb = sys.argv[5]
        else :
            cb_fn = raw_input("If you wish to use a Codebook file, please type the path and press 'Enter'.\n Otherwise, type '!' and press Enter: ")       
    else : 
        data_fn = raw_input("If you wish to use a Data file, please type the path and press 'Enter'.\n Otherwise, type '!' and press Enter: ")
        cb_fn = raw_input("If you wish to use a Codebook file, please type the path and press 'Enter'.\n Otherwise, type '!' and press Enter: ")

if cb_fn == "!" :
    cb_fn = None

if data_fn == "!" :
    data_fn = None

if out_fn == "!" :
    out_fn = "out.ttl"

output_file = open(out_fn,"w")
prefix_file = open(prefix_fn,"r")
prefixes = prefix_file.readlines()

for prefix in prefixes :
    output_file.write(prefix)
output_file.write("\n")

# K: parameterize this, too?
code_mappings_url = 'https://raw.githubusercontent.com/tetherless-world/chear-ontology/master/code_mappings.csv'
#code_mappings_response = urllib2.urlopen(code_mappings_url)
code_mappings_reader = pd.read_csv(code_mappings_url)

column_ind = None
attr_ind = None
attr_of_ind = None
entity_ind = None
unit_ind = None
time_ind = None
role_ind = None
relation_ind = None
relation_to_ind = None
derived_from_ind = None
generated_by_ind = None
position_ind = None
label_ind = None
comment_ind = None

sdd_key = None
cb_key = None
data_key = None

unit_code_list = []
unit_uri_list = []
unit_label_list = []

actual_list = []
virtual_list = []

actual_tuples = []
virtual_tuples = []
cb_tuple = {}

try :
    sdd_file = pd.read_csv(sdd_fn)
except:
    print "Error: The specified SDD file does not exist."
    sys.exit(1)

try: 
    #dialect = csv.Sniffer().sniff(sdd_file.read(),delimiters=',\t')
    #sdd_file.seek(0)
    #sdd_reader=csv.reader(sdd_file,dialect)    
    #sdd_reader=csv.reader(sdd_file)
    row_num=0
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

# Remove list labels
#del unit_code_list[0]
#del unit_uri_list[0] 
#del unit_label_list[0]

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
        # Need to implement check for entry in column list
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

# Set indices
#try :
#    attr_ind = sdd_key.index("Attribute")
#except :
#    print "Error: The SDD must have a column named 'Attribute'"
#    sys.exit(1)
#
#try :
#    attr_of_ind = sdd_key.index("attributeOf")
#except :
#    print "Error: The SDD must have a column named 'attributeOf'"
#    sys.exit(1)
#
#try :
#    entity_ind = sdd_key.index("Entity")
#except :
#    print "Error: The SDD must have a column named 'Entity'"
#    sys.exit(1)
#
#try :
#    unit_ind = sdd_key.index("Unit")
#except :
#    print "Error: The SDD must have a column named 'Unit'"
#    sys.exit(1)
#
#try :
#    time_ind = sdd_key.index("Time")
#except :
#    print "Warning: Optional SDD column 'Time' is missing"
#
#try :
#    role_ind = sdd_key.index("Role")
#except :
#    print "Error: The SDD must have a column named 'Role'"
#    sys.exit(1)
#
#try:
#    relation_ind = sdd_key.index("Relation")
#except :
#    print "Error: The SDD must have a column named 'Relation'"
#    sys.exit(1)
#
#try :
#    relation_to_ind = sdd_key.index("inRelationTo")
#except :
#    print "Error: The SDD must have a column named 'inRelationTo'"
#    sys.exit(1)
#
#try:
#    derived_from_ind = sdd_key.index("wasDerivedFrom")
#except :
#    print "Warning: Optional SDD column 'wasDerivedFrom' is missing"
#
#try:
#    generated_by_ind = sdd_key.index("wasGeneratedBy")
#except :
#    print "Warning: Optional SDD column 'wasGeneratedBy' is missing"
#
#try:
#    position_ind = sdd_key.index("hasPosition")    
#except :
#    print "Warning: Optional SDD column 'hasPosition' is missing"
#
#try:
#    label_ind = sdd_key.index("Label")    
#except :
#    print "Warning: Optional SDD column 'Label' is missing"
#
#try:
#    comment_ind = sdd_key.index("Comment")    
#except :
#    print "Warning: Optional SDD column 'Comment' is missing"


#virtual_list is a list of tuples
def writeVirtualRDF(virtual_list, virtual_tuples, output_file) :
    for item in virtual_list :
        virtual_tuple = {}
        output_file.write(kb + item.Column[2:] + " a owl:Class ")
        output_file.write(" ;\n\trdfs:label \"" + item.Column[2:] + "\"")
        # Set the rdf:type of the virtual row to either the Attribute or Entity value (or else owl:Individual)
        if (pd.notnull(item.Entity)) and (pd.isnull(item.Attribute)) :
            output_file.write(" ;\n\trdfs:subClassOf " + codeMapper(item.Entity))
            virtual_tuple["Column"]=item.Column
            virtual_tuple["Entity"]=codeMapper(item.Entity)
            if (virtual_tuple["Entity"] == "hasco:Study") :
                global studyRef
                studyRef = item.Column
                virtual_tuple["Study"] = item.Column
        elif (pd.notnull(item.Entity)) and (pd.notnull(item.Attribute)) :
            output_file.write(" ;\n\trdfs:subClassOf " + codeMapper(item.Attribute))
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
                output_file.write(" ;\n\t" + item.Relation + " " + convertVirtualToKGEntry(item.inRelationTo) )
                virtual_tuple["Relation"]=item.Relation
            # If there is a value in the Role column but not the Relation column ...
            elif (pd.isnull(item.Relation)) and (pd.notnull(item.Role)) :
                output_file.write(" ;\n\tsio:hasRole [ a " + item.Role + " ;\n\t\tsio:inRelationTo " + convertVirtualToKGEntry(item.inRelationTo) + " ]")
                virtual_tuple["Role"]=item.Role
            # If there is a value in the Role and Relation columns ...
            elif (pd.notnull(item.Relation)) and (pd.notnull(item.Role)) :
                virtual_tuple["Relation"]=item.Relation
                virtual_tuple["Role"]=item.Role
                output_file.write(" ;\n\tsio:inRelationTo " + convertVirtualToKGEntry(item.inRelationTo) )
        if (pd.notnull(item.wasDerivedFrom)) :
            output_file.write(" ;\n\tprov:wasDerivedFrom " + convertVirtualToKGEntry(item.wasDerivedFrom))
            virtual_tuple["wasDerivedFrom"]=item.wasDerivedFrom
        if (pd.notnull(item.wasGeneratedBy)) :
            output_file.write(" ;\n\tprov:wasGeneratedBy " + convertVirtualToKGEntry(item.wasGeneratedBy))
            virtual_tuple["wasGeneratedBy"]=item.wasGeneratedBy
        output_file.write(" .\n\n")
        virtual_tuples.append(virtual_tuple)

#actual_list is a list of tuples
def writeActualRDF(actual_list, actual_tuples, output_file) :
    for item in actual_list :
        actual_tuple = {}
        output_file.write(kb + item.Column.replace(" ","_") + " a owl:Class ")
        #output_file.write(" ;\n\trdfs:label \"" + item[column_ind] + "\"")
        if (pd.notnull(item.Attribute)) :
            output_file.write(" ;\n\trdfs:subClassOf " + convertVirtualToKGEntry(codeMapper(item.Attribute)))
            actual_tuple["Column"]=item.Column
            actual_tuple["Attribute"]=codeMapper(item.Attribute)
        else :
            output_file.write(" ;\n\trdfs:subClassOf " + convertVirtualToKGEntry(codeMapper("sio:Attribute")))
            actual_tuple["Column"]=item.Column
            actual_tuple["Attribute"]=codeMapper("sio:Attribute")
            print "WARN: Actual column not assigned an Attribute value."
            #sys.exit(1)
            #output_file.write(kb + item[column_ind] + " a owl:Individual")
        if (pd.notnull(item.attributeOf)) :
            output_file.write(" ;\n\tsio:isAttributeOf " + convertVirtualToKGEntry(item.attributeOf))
            actual_tuple["isAttributeOf"]=item.attributeOf
        else :
	    print "WARN: Actual column not assigned an isAttributeOf value. Skipping...."
	    output_file.write(" ;\n\n")
	    continue
            #print "Error: Actual column not assigned an isAttributeOf value."
            #sys.exit(1)
        if (pd.notnull(item.Unit)) :
            output_file.write(" ;\n\tsio:hasUnit " + codeMapper(item.Unit))
            actual_tuple["Unit"] = codeMapper(item.Unit)
        if (pd.notnull(item.wasDerivedFrom)) :
            output_file.write(" ;\n\tprov:wasDerivedFrom " + convertVirtualToKGEntry(item.wasDerivedFrom))
            actual_tuple["wasDerivedFrom"]=item.wasDerivedFrom
        if (pd.notnull(item.wasGeneratedBy)) :
            output_file.write(" ;\n\tprov:wasGeneratedBy " + convertVirtualToKGEntry(item.wasGeneratedBy))
            actual_tuple["wasGeneratedBy"]=item.wasGeneratedBy
        if (pd.notnull(item.hasPosition)) :
            output_file.write(" ;\n\thasco:hasPosition \"" + item.hasPosition + "\"^^xsd:Integer")
            actual_tuple["hasPosition"]=item.hasPosition
        if (pd.notnull(item.Time)) :
            output_file.write(" ;\n\tsio:existsAt " + convertVirtualToKGEntry(item.Time))
            actual_tuple["Time"]=item.Time
        if (pd.notnull(item.inRelationTo)):
            output_file.write(" ;\n\tsio:inRelationTo " + convertVirtualToKGEntry(item.inRelationTo))
            actual_tuple["inRelationTo"]=item.inRelationTo
        if (pd.notnull(item.Relation) and pd.notnull(item.inRelationTo)) :
            output_file.write(" ;\n\t" + item.Relation + " " + convertVirtualToKGEntry(item.inRelationTo))
            actual_tuple["Relation"]=item.Relation
        if (pd.notnull(item.Label)) :
            output_file.write(" ;\n\trdfs:label \"" + item.Label + "\"^^xsd:String" )
            actual_tuple["Label"]=item.Label
        if (pd.notnull(item.Comment)) :
            output_file.write(" ;\n\trdfs:comment \"" + item.Comment + "\"^^xsd:String")
            actual_tuple["Comment"]=item.Comment
        output_file.write(" .\n\n")
        actual_tuples.append(actual_tuple)

writeVirtualRDF(virtual_list, virtual_tuples, output_file)
writeActualRDF(actual_list, actual_tuples, output_file)

if cb_fn is not None :
    #cb_column_ind = None
    #cb_code_ind = None
    #cb_label_ind = None
    #cb_class_ind = None
    #cb_comment_ind = None
    #cb_note_ind = None
    try :
        #cb_file = open(cb_fn, 'r')
	cb_file = pd.read_csv(cb_fn)
    except :
        print "Error: The specified Codebook file does not exist."
        sys.exit(1)
    try :
        #dialect = csv.Sniffer().sniff(cb_file.read(),delimiters=',\t')
        #cb_file.seek(0)
        #cb_reader=csv.reader(cb_file,dialect)
        #cb_reader=csv.reader(cb_file)

        inner_tuple_list = []
        row_num=0
        for row in cb_file.itertuples():
            #if row_num == 0:
            #    cb_key = row
            #    try :
            #        cb_column_ind = cb_key.index("Column")
            #    except :
            #        print "Error: The Codebook must have a column named 'Column'"
            #        sys.exit(1)
            #    try :
            #        cb_code_ind = cb_key.index("Code")
            #    except :
            #        print "Error: The Codebook must have a column named 'Code'"
            #        sys.exit(1)
            #    try :
            #        cb_label_ind = cb_key.index("Label")
            #    except :
            #        print "Error: The Codebook must have a column named 'Label'"
            #        sys.exit(1)
            #    try :
            #        cb_class_ind = cb_key.index("Class")
            #    except :
            #        print "Warning: The Codebook is missing an optional column named 'Class'"
            #    try :
            #        cb_comment_ind = cb_key.index("Comment")
            #    except :
            #        print "Warning: The Codebook is missing an optional column named 'Comment'"
            #    try :
            #        cb_note_ind = cb_key.index("Note")
            #    except :
            #        print "Warning: The Codebook is missing an optional column named 'Note'"
            #else :
            if (pd.notnull(row.Column) and row.Column not in cb_tuple) :
                inner_tuple_list=[]
            inner_tuple = {}
            inner_tuple["Code"]=row.Code
            inner_tuple["Label"]=row.Label
            if(pd.notnull(row.Class)) :
                inner_tuple["Class"]=row.Class
            inner_tuple_list.append(inner_tuple)
            cb_tuple[row.Column]=inner_tuple_list
            row_num += 1
    except :
        print "Warning: Unable to process Codebook file"

def convertFromCB(dataVal,column_name) :
    if column_name in cb_tuple :
        for tuple_row in cb_tuple[column_name] :
            #print tuple_row
            #if (tuple_row['Code'].__str__() is dataVal.__str__()) :
            if ("Code" in tuple_row) :
                if ("Class" in tuple_row) and (tuple_row['Class'] is not "") :
                    return tuple_row['Class']     
                else :
                    return "\"" + tuple_row['Label'] + "\"^^xsd:String"
        return "\"" + dataVal + "\""
    else :
        return "\"" + dataVal + "\""

def writeVirtualEntry(output_file, v_column, index) : 
    try :
        for v_tuple in virtual_tuples :
            if (v_tuple["Column"] == v_column) :
                if "Study" in v_tuple :
                    continue
                else :
                    output_file.write(kb + v_tuple["Column"][2:] + "-" + index + " a " + kb + v_tuple["Column"][2:])
                if "Entity" in v_tuple :
                    output_file.write(";\n\ta " + v_tuple["Entity"])
                if "Attribute" in v_tuple :
                    output_file.write(";\n\ta " + v_tuple["Attribute"])
                if "Subject" in v_tuple :
                    output_file.write(";\n\tsio:hasIdentifier " + kb + v_tuple["Subject"] + "-" + index)
                if "inRelationTo" in v_tuple :
                    if ("Role" in v_tuple) and ("Relation" not in v_tuple) :
                        output_file.write(" ;\n\tsio:hasRole [ a " + v_tuple["Role"] + " ;\n\t\tsio:inRelationTo " + convertVirtualToKGEntry(v_tuple["inRelationTo"], index) + " ]")
                    elif ("Role" not in v_tuple) and ("Relation" in v_tuple) :
                        output_file.write(" ;\n\t" + v_tuple["Relation"] + " " + convertVirtualToKGEntry(v_tuple["inRelationTo"],index))
                    elif ("Role" not in v_tuple) and ("Relation" not in v_tuple) :
                        output_file.write(" ;\n\tsio:inRelationTo " + convertVirtualToKGEntry(v_tuple["inRelationTo"],index))
                if "wasGeneratedBy" in v_tuple : 
                    output_file.write(" ;\n\tprov:wasGeneratedBy " + convertVirtualToKGEntry(v_tuple["wasGeneratedBy"],index))
                if "wasDerivedFrom" in v_tuple : 
                    output_file.write(" ;\n\tprov:wasDerivedFrom " + convertVirtualToKGEntry(v_tuple["wasDerivedFrom"],index))
                #output_file.write(";\n\tprov:wasDerivedFrom " + kb + v_tuple["Column"][2:])
                output_file.write(" .\n\n")
                if ("wasGeneratedBy" in v_tuple ) and (checkVirtual(v_tuple["wasGeneratedBy"])) :
                    writeVirtualEntry(output_file, v_tuple["wasGeneratedBy"], index)
                if ("wasDerivedFrom" in v_tuple) and (checkVirtual(v_tuple["wasDerivedFrom"])) :
                    writeVirtualEntry(output_file, v_tuple["wasDerivedFrom"], index)
    except :
        print "Warning: Unable to create virtual entry."

if data_fn is not None :
    try :
        #data_file = open(data_fn, 'r')
	data_file = pd.read_csv(data_fn)
    except :
        print "Error: The specified Data file does not exist."
        sys.exit(1)
    try :
        #dialect = csv.Sniffer().sniff(data_file.read(),delimiters=',\t')
        #data_file.seek(0)
        #data_reader=csv.reader(data_file,dialect)
        #data_reader=csv.reader(data_file)

	# ensure that there is a column annotated as the sio:Identifier or hasco:originalID in the data file:
	# TODO make sure this is getting the first available ID property for the _subject_ (and not anything else)
        id_index=None
	col_headers=list(data_file.columns.values)
	try:
	    for a_tuple in actual_tuples :
		#print a_tuple["Column"]
		if ((a_tuple["Attribute"] == "hasco:originalID") or (a_tuple["Attribute"] == "sio:Identifier")) :
		    if(a_tuple["Column"] in col_headers) :
			print a_tuple["Column"]
			id_index = col_headers.index(a_tuple["Column"])
		        for v_tuple in virtual_tuples :
		            if (a_tuple["isAttributeOf"] == v_tuple["Column"]) :
		                v_tuple["Subject"]=a_tuple["Column"].replace(" ","_")
		if (id_index is None) :
		    print "Error: To process Data it is necessary to have a \"hasco:originalID\" or \"sio:Identifier\" Attribute in the SDD."
		    sys.exit(1)
	except: 
	    print "Error processing column headers"

        for row in data_file.itertuples() :
                try :
                    vref_list = []
                    for a_tuple in actual_tuples :
                        #print a_tuple
                        if (a_tuple["Column"] in data_key ) :
                            try :
                                output_file.write(kb + a_tuple["Column"].replace(" ","_") + "-" + row[id_index] + " a " + a_tuple["Attribute"])
                                output_file.write(" ;\n\ta " + kb + a_tuple["Column"].replace(" ","_"))
                                output_file.write(" ;\n\tsio:isAttributeOf " + convertVirtualToKGEntry(a_tuple["isAttributeOf"],row[id_index]))
                                if checkVirtual(a_tuple["isAttributeOf"]) :
                                    if a_tuple["isAttributeOf"] not in vref_list :
                                        vref_list.append(a_tuple["isAttributeOf"])
                                if "Unit" in a_tuple :
                                    output_file.write(" ;\n\tsio:hasUnit " + a_tuple["Unit"])
                                if "Time" in a_tuple :
                                    output_file.write(" ;\n\tsio:existsAt " + convertVirtualToKGEntry(a_tuple["Time"], row[id_index]))
                                    if checkVirtual(a_tuple["Time"]) :
                                        if a_tuple["Time"] not in vref_list :
                                            vref_list.append(a_tuple["Time"])
                                if "wasDerivedFrom" in a_tuple :
                                    output_file.write(" ;\n\tprov:wasDerivedFrom " + convertVirtualToKGEntry(a_tuple["wasDerivedFrom"], row[id_index]))
                                    if checkVirtual(a_tuple["wasDerivedFrom"]) :
                                        if a_tuple["wasDerivedFrom"] not in vref_list :
                                            vref_list.append(a_tuple["wasDerivedFrom"])
                                if "wasGeneratedBy" in a_tuple :
                                    output_file.write(" ;\n\tprov:wasGeneratedBy " + convertVirtualToKGEntry(a_tuple["wasGeneratedBy"], row[id_index]))
                                    if checkVirtual(a_tuple["wasGeneratedBy"]) :
                                        if a_tuple["wasGeneratedBy"] not in vref_list :
                                            vref_list.append(a_tuple["wasGeneratedBy"])
                                if "inRelationTo" in a_tuple :
                                    if checkVirtual(a_tuple["inRelationTo"]) :
                                        if a_tuple["inRelationTo"] not in vref_list :
                                            vref_list.append(a_tuple["inRelationTo"])
                                    if "Relation" in a_tuple :
                                        output_file.write(" ;\n\t" + a_tuple["Relation"] + " " + convertVirtualToKGEntry(a_tuple["inRelationTo"], row[id_index]))
                                    else :
                                        output_file.write(" ;\n\tsio:inRelationTo " + convertVirtualToKGEntry(a_tuple["inRelationTo"], row[id_index]))
                                if "hasPosition" in a_tuple :
                                    output_file.write(" ;\n\thasco:hasPosition " + a_tuple["hasPosition"])
                                if "Label" in a_tuple :
                                    output_file.write(" ;\n\trdfs:label \"" + a_tuple["Label"] + "\"^^xsd:String")
                                if "Comment" in a_tuple :
                                        output_file.write(" ;\n\trdfs:comment \"" + a_tuple["Comment"] + "\"^^xsd:String")
                                try :
                                    if (row[data_key.index(a_tuple["Column"])] != "") :
                                        #print row[data_key.index(a_tuple["Column"])]
                                        if (cb_fn is not None) :
                                            output_file.write(" ;\n\tsio:hasValue " + convertFromCB(row[data_key.index(a_tuple["Column"])],a_tuple["Column"]))
                                        else :
                                            output_file.write(" ;\n\tsio:hasValue \"" + row[data_key.index(a_tuple["Column"])] + "\"")
                                except :
                                    print "Error writing data value"
                                    #print row[data_key.index(a_tuple["Column"])]
                                output_file.write(" .\n\n")
                            except :
                                print "Unable to process tuple" + a_tuple.__str__()
                    #print vref_list
                    for vref in vref_list :
                        writeVirtualEntry(output_file, vref, row[id_index])
                        #print vref
                except:
                    print "Error: Something went wrong when processing actual tuples."
                    sys.exit(1)
           
    except :
        print "Warning: Unable to process Data file"

#sdd_file.close()
output_file.close()

#if cb_fn is not None :
#    cb_file.close()

#if data_fn is not None : 
#    data_file.close()
