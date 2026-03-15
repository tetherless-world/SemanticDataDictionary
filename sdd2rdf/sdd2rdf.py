"""
sdd2rdf.py - Converts Semantic Data Dictionary (SDD) files to RDF.

Usage:
    python sdd2rdf.py <configuration_file>
"""

import csv
import sys
import re
import hashlib
import os
import logging
from datetime import datetime, timezone

import pandas as pd
import configparser
import rdflib

logging.getLogger().disabled = True

# ---------------------------------------------------------------------------
# Namespace declarations
# ---------------------------------------------------------------------------
whyis = rdflib.Namespace("http://vocab.rpi.edu/whyis/")
np    = rdflib.Namespace("http://www.nanopub.org/nschema#")
prov  = rdflib.Namespace("http://www.w3.org/ns/prov#")
dc    = rdflib.Namespace("http://purl.org/dc/terms/")
sio   = rdflib.Namespace("http://semanticscience.org/resource/")
setl  = rdflib.Namespace("http://purl.org/twc/vocab/setl/")
pv    = rdflib.Namespace("http://purl.org/net/provenance/ns#")
skos  = rdflib.Namespace("http://www.w3.org/2008/05/skos#")
rdfs  = rdflib.RDFS
rdf   = rdflib.RDF
owl   = rdflib.OWL
xsd   = rdflib.XSD


# ---------------------------------------------------------------------------
# Global mutable state (populated during startup and main())
# ---------------------------------------------------------------------------
studyRef         = None
properties_tuple = {}
prefixes         = {}
kb               = ":"
cmap_fn          = None
explicit_entry_list = []
implicit_entry_list = []
nanopublication_option = "enabled"

unit_code_list  = []
unit_uri_list   = []
unit_label_list = []


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def utc_now_string():
    """Return current UTC time as an xsd:dateTime-compatible string."""
    now = datetime.now(timezone.utc)
    return "{:4d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}Z".format(
        now.year, now.month, now.day,
        now.hour, now.minute, now.second
    )


def sanitize_term(term):
    """Sanitize a column name for use as a URI fragment."""
    return (term
            .replace(" ", "_")
            .replace(",", "")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "-")
            .replace("\\", "-"))


def read_csv_safe(filepath, **kwargs):
    """
    Read a CSV with UTF-8 encoding and a fallback to latin-1.
    This prevents UnicodeDecodeError on Windows when files contain
    non-ASCII characters.
    """
    try:
        return pd.read_csv(filepath, encoding="utf-8", **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(filepath, encoding="latin-1", **kwargs)


def parse_delimited_string(input_string, delim):
    """Split a string on delim and strip whitespace from each token."""
    return [item.strip() for item in input_string.split(delim)]


def isfloat(term):
    try:
        float(term)
        return True
    except (ValueError, TypeError):
        return False


def isURI(term):
    """Return True if term looks like an HTTP(S) URI."""
    try:
        return "http://" in term or "https://" in term
    except TypeError:
        return False


def checkTemplate(term):
    """Return True if term contains a {key} template placeholder."""
    return "{" in term and "}" in term


def checkImplicit(input_word):
    """Return True if input_word starts with '??', marking an implicit entry."""
    try:
        return str(input_word)[:2] == "??"
    except Exception as e:
        print("Something went wrong in checkImplicit(): " + str(e))
        sys.exit(1)


def isSchemaVar(term):
    for entry in explicit_entry_list:
        if term == entry[1]:
            return True
    return False


# ---------------------------------------------------------------------------
# Code mapper
# ---------------------------------------------------------------------------

def codeMapper(input_word):
    """Map a unit label or code string to its URI equivalent."""
    unitVal = input_word
    for i, label in enumerate(unit_label_list):
        if label == input_word:
            unitVal = unit_uri_list[i]
            return unitVal
    for i, code in enumerate(unit_code_list):
        if code == input_word:
            unitVal = unit_uri_list[i]
            return unitVal
    return unitVal


# ---------------------------------------------------------------------------
# Term conversion helpers
# ---------------------------------------------------------------------------

def convertImplicitToKGEntry(*args):
    """
    Convert an implicit column reference (??name) or plain string to a
    bracketed URI or literal suitable for embedding in a Turtle string.
    """
    term = args[0]
    if str(term)[:2] == "??":
        if studyRef is not None and term == studyRef:
            return "<" + prefixes[kb] + str(term)[2:] + ">"
        if len(args) == 2:
            return "<" + prefixes[kb] + str(term)[2:] + "-" + str(args[1]) + ">"
        else:
            return "<" + prefixes[kb] + str(term)[2:] + ">"
    elif not isURI(str(term)):
        for item in explicit_entry_list:
            if term == item.Column:
                safe = sanitize_term(str(term))
                if len(args) == 2:
                    return "<" + prefixes[kb] + safe + "-" + str(args[1]) + ">"
                else:
                    return "<" + prefixes[kb] + safe + ">"
        return str(term)
    else:
        return str(term)


def extractTemplate(col_headers, row, term):
    """
    Expand {ColumnName} placeholders in term using the current data row.
    col_headers is a plain list; row is an itertuples namedtuple (1-indexed).
    """
    while checkTemplate(term):
        open_idx  = term.index("{")
        close_idx = term.index("}")
        key  = term[open_idx + 1:close_idx]
        try:
            val = str(row[col_headers.index(key) + 1])
        except (ValueError, IndexError):
            val = key  # leave as-is if column not found
        term = term[:open_idx] + val + term[close_idx + 1:]
    return term


def extractExplicitTerm(col_headers, row, term):
    """
    Like extractTemplate but resolves schema variable references via
    explicit_entry_list first.
    """
    while checkTemplate(term):
        open_idx  = term.index("{")
        close_idx = term.index("}")
        key = term[open_idx + 1:close_idx]
        if isSchemaVar(key):
            for entry in explicit_entry_list:
                if entry.Column == key:
                    if pd.notnull(entry.Template):
                        term = extractTemplate(col_headers, row, entry.Template)
                    else:
                        typeString = ""
                        for attr in ("Attribute", "Entity", "Label", "Unit",
                                     "Time", "inRelationTo", "wasGeneratedBy",
                                     "wasDerivedFrom"):
                            val = getattr(entry, attr, None)
                            if val is not None and pd.notnull(val):
                                typeString += str(val)
                        id_key = hashlib.md5(
                            (str(row[col_headers.index(key) + 1]) + typeString
                             ).encode("utf-8")
                        ).hexdigest()
                        term = (sanitize_term(entry.Column)
                                + "-" + id_key)
        else:
            print("Warning: Template reference " + term
                  + " is not a schema variable")
            try:
                val = str(row[col_headers.index(key) + 1])
            except (ValueError, IndexError):
                val = key
            term = term[:open_idx] + val + term[close_idx + 1:]
    return term


# ---------------------------------------------------------------------------
# VID / term assignment helpers
# ---------------------------------------------------------------------------

def assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, column, npubIdentifier):
    """Derive a stable hash ID for an implicit/timeline entry."""
    v_id = npubIdentifier
    for v_tuple in implicit_entry_tuples:
        if v_tuple["Column"] == a_tuple[column]:
            v_id = hashlib.md5(
                (str(v_tuple) + str(npubIdentifier)).encode("utf-8")
            ).hexdigest()
            return v_id
    if v_id == npubIdentifier:
        for t_tuple in timeline_tuple:
            if t_tuple["Column"] == a_tuple[column]:
                v_id = hashlib.md5(
                    (str(t_tuple) + str(npubIdentifier)).encode("utf-8")
                ).hexdigest()
                return v_id
    if v_id == npubIdentifier:
        print("Warning, " + column + " ID assigned to nanopub ID: "
              + str(a_tuple[column]))
    return v_id


def assignTerm(col_headers, column, implicit_entry_tuples, a_tuple, row, v_id):
    """Determine the URI for a field that references an implicit entry."""
    termURI = None
    for v_tuple in implicit_entry_tuples:
        if v_tuple["Column"] == a_tuple[column]:
            if "Template" in v_tuple:
                template_term = extractTemplate(col_headers, row, v_tuple["Template"])
                termURI = "<" + prefixes[kb] + template_term + ">"
    if termURI is None:
        termURI = convertImplicitToKGEntry(a_tuple[column], v_id)
    return termURI


# ---------------------------------------------------------------------------
# Class-level assertion/provenance writers
# ---------------------------------------------------------------------------

def writeClassAttributeOrEntity(item, term, input_tuple, assertionString,
                                whereString, swrlString):
    entity_val    = getattr(item, "Entity", None)
    attribute_val = getattr(item, "Attribute", None)
    has_entity    = entity_val is not None and pd.notnull(entity_val)
    has_attribute = attribute_val is not None and pd.notnull(attribute_val)

    if has_entity and not has_attribute:
        values = parse_delimited_string(entity_val, ",") if "," in entity_val else [entity_val]
        for v in values:
            mapped = codeMapper(v)
            assertionString += " ;\n        <" + str(rdfs.subClassOf) + ">    " + mapped
            whereString     += codeMapper(v) + " "
            swrlString      += codeMapper(v) + "(" + term + ") ^ "
            if len(values) > 1 and values.index(v) + 1 != len(values):
                whereString += ", "
        input_tuple["Entity"] = codeMapper(entity_val)
        if input_tuple["Entity"] == "hasco:Study":
            global studyRef
            studyRef = item.Column
            input_tuple["Study"] = item.Column
    elif has_attribute and not has_entity:
        values = parse_delimited_string(attribute_val, ",") if "," in attribute_val else [attribute_val]
        for v in values:
            mapped = codeMapper(v)
            assertionString += " ;\n        <" + str(rdfs.subClassOf) + ">    " + mapped
            whereString     += mapped + " "
            swrlString      += mapped + "(" + term + ") ^ "
            if len(values) > 1 and values.index(v) + 1 != len(values):
                whereString += ", "
        input_tuple["Attribute"] = codeMapper(attribute_val)
    else:
        print("Warning: Entry not assigned an Entity or Attribute value, "
              "or was assigned both.")
        input_tuple["Attribute"] = codeMapper("sio:Attribute")
        assertionString += " ;\n        <" + str(rdfs.subClassOf) + ">    sio:Attribute"
        whereString += "sio:Attribute "
        swrlString  += "sio:Attribute(" + term + ") ^ "
    return [input_tuple, assertionString, whereString, swrlString]


def writeClassAttributeOf(item, term, input_tuple, assertionString,
                          whereString, swrlString):
    val = getattr(item, "attributeOf", None)
    if val is None or pd.isnull(val):
        return [input_tuple, assertionString, whereString, swrlString]

    if checkTemplate(val):
        open_idx  = val.index("{")
        close_idx = val.index("}")
        key = val[open_idx + 1:close_idx]
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.allValuesFrom) + ">    " + convertImplicitToKGEntry(key) + " ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["attributeOf"]) + "> ]"
        )
        whereString += " ;\n    <" + str(properties_tuple["attributeOf"]) + ">    ?" + key.lower() + "_E"
        swrlString  += (str(properties_tuple["attributeOf"]) + "(" + term + " , "
                        + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ ")
    else:
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.allValuesFrom) + ">    " + convertImplicitToKGEntry(val) + " ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["attributeOf"]) + "> ]"
        )
        whereString += (" ;\n    <" + str(properties_tuple["attributeOf"]) + ">    "
                        + ([val + " ", val[1:] + "_V "][checkImplicit(val)]))
        swrlString  += (str(properties_tuple["attributeOf"]) + "(" + term + " , "
                        + ([val, val[1:] + "_V"][checkImplicit(val)]) + ") ^ ")
    input_tuple["isAttributeOf"] = val
    return [input_tuple, assertionString, whereString, swrlString]


def writeClassUnit(item, term, input_tuple, assertionString,
                   whereString, swrlString):
    val = getattr(item, "Unit", None)
    if val is None or pd.isnull(val):
        return [input_tuple, assertionString, whereString, swrlString]

    if checkTemplate(val):
        open_idx  = val.index("{")
        close_idx = val.index("}")
        key = val[open_idx + 1:close_idx]
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.hasValue) + ">    " + convertImplicitToKGEntry(key) + " ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Unit"]) + "> ]"
        )
        whereString += " ;\n    <" + str(properties_tuple["Unit"]) + ">    ?" + key.lower() + "_E"
        swrlString  += (str(properties_tuple["Unit"]) + "(" + term + " , "
                        + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ ")
        input_tuple["Unit"] = key
    else:
        mapped = str(codeMapper(val))
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.hasValue) + ">    " + mapped + " ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Unit"]) + "> ]"
        )
        whereString += " ;\n    <" + str(properties_tuple["Unit"]) + ">    " + mapped
        swrlString  += str(properties_tuple["Unit"]) + "(" + term + " , " + mapped + ") ^ "
        input_tuple["Unit"] = codeMapper(val)
    return [input_tuple, assertionString, whereString, swrlString]


def writeClassTime(item, term, input_tuple, assertionString,
                   whereString, swrlString):
    val = getattr(item, "Time", None)
    if val is None or pd.isnull(val):
        return [input_tuple, assertionString, whereString, swrlString]

    if checkTemplate(val):
        open_idx  = val.index("{")
        close_idx = val.index("}")
        key = val[open_idx + 1:close_idx]
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Time"]) + "> ;\n"
            "                <" + str(owl.someValuesFrom) + ">    " + convertImplicitToKGEntry(key) + " ]"
        )
        whereString += " ;\n    <" + str(properties_tuple["Time"]) + ">    ?" + key.lower() + "_E"
        swrlString  += (str(properties_tuple["Time"]) + "(" + term + " , "
                        + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ ")
    else:
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Time"]) + "> ;\n"
            "                <" + str(owl.someValuesFrom) + ">    " + convertImplicitToKGEntry(val) + " ]"
        )
        whereString += (" ;\n    <" + str(properties_tuple["Time"]) + ">     "
                        + ([val + " ", val[1:] + "_V "][checkImplicit(val)]))
        swrlString  += (str(properties_tuple["Time"]) + "(" + term + " , "
                        + ([val + " ", val[1:] + "_V "][checkImplicit(val)]) + ") ^ ")
    input_tuple["Time"] = val
    return [input_tuple, assertionString, whereString, swrlString]


def writeClassRelation(item, term, input_tuple, assertionString,
                       whereString, swrlString):
    inrel_val  = getattr(item, "inRelationTo", None)
    role_val   = getattr(item, "Role", None)
    rel_val    = getattr(item, "Relation", None)
    has_inrel  = inrel_val is not None and pd.notnull(inrel_val)
    has_role   = role_val  is not None and pd.notnull(role_val)
    has_rel    = rel_val   is not None and pd.notnull(rel_val)

    if has_inrel:
        input_tuple["inRelationTo"] = inrel_val
        key = inrel_val
        if checkTemplate(inrel_val):
            open_idx  = inrel_val.index("{")
            close_idx = inrel_val.index("}")
            key = inrel_val[open_idx + 1:close_idx]

        if has_rel and not has_role:
            assertionString += " ;\n        " + rel_val + " " + convertImplicitToKGEntry(key)
            if isSchemaVar(key):
                whereString += " ;\n    " + rel_val + " ?" + key.lower() + "_E "
                swrlString  += rel_val + "(" + term + " , ?" + key.lower() + "_E) ^ "
            else:
                whereString += " ;\n    " + rel_val + " " + ([key + " ", key[1:] + "_V "][checkImplicit(key)])
                swrlString  += rel_val + "(" + term + " , " + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ "
            input_tuple["Relation"] = rel_val

        elif not has_rel and has_role:
            assertionString += (
                " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
                "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Role"]) + "> ;\n"
                "                <" + str(owl.someValuesFrom) + ">    [ <" + str(rdf.type) + ">    <" + str(owl.Class) + "> ;\n"
                "                    <" + str(owl.intersectionOf) + "> ( \n"
                "                        [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                "                        <" + str(owl.allValuesFrom) + "> " + ([key, convertImplicitToKGEntry(key)][checkImplicit(key)]) + " ;\n"
                "                        <" + str(owl.onProperty) + "> <" + str(properties_tuple["inRelationTo"]) + "> ] <" + role_val + "> ) ]    ]"
            )
            whereString += (" ;\n    <" + str(properties_tuple["Role"]) + ">    [ <" + str(rdf.type) + "> " + role_val + " ;\n"
                            "      <" + str(properties_tuple["inRelationTo"]) + ">    "
                            + ([key + " ", key[1:] + "_V "][checkImplicit(key)]) + " ]")
            input_tuple["Role"] = role_val

        elif has_rel and has_role:
            input_tuple["Relation"] = rel_val
            input_tuple["Role"]     = role_val
            assertionString += (
                " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
                "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Role"]) + "> ;\n"
                "                <" + str(owl.someValuesFrom) + ">    [ <" + str(rdf.type) + ">    <" + str(owl.Class) + "> ;\n"
                "                    <" + str(owl.intersectionOf) + "> ( \n"
                "                        [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                "                        <" + str(owl.allValuesFrom) + "> " + ([key, convertImplicitToKGEntry(key)][checkImplicit(key)]) + " ;\n"
                "                        <" + str(owl.onProperty) + "> <" + rel_val + "> ] <" + role_val + "> ) ]    ]"
            )
            if isSchemaVar(key):
                whereString += " ;\n    <" + str(properties_tuple["inRelationTo"]) + ">    ?" + key.lower() + "_E "
            else:
                whereString += (" ;\n    <" + str(properties_tuple["inRelationTo"]) + ">    "
                                + ([key + " ", key[1:] + "_V "][checkImplicit(key)]))

        else:  # neither rel nor role
            assertionString += (
                " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
                "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                "                <" + str(owl.allValuesFrom) + ">    " + convertImplicitToKGEntry(key) + " ;\n"
                "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["inRelationTo"]) + "> ]"
            )
            if isSchemaVar(key):
                whereString += " ;\n    <" + str(properties_tuple["inRelationTo"]) + ">    ?" + key.lower() + "_E "
                swrlString  += (str(properties_tuple["inRelationTo"]) + "(" + term + " , ?"
                                + key.lower() + "_E) ^ ")
            else:
                whereString += (" ;\n    <" + str(properties_tuple["inRelationTo"]) + ">    "
                                + ([key + " ", key[1:] + "_V "][checkImplicit(key)]))
                swrlString  += (str(properties_tuple["inRelationTo"]) + "(" + term + " , "
                                + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ ")

    elif has_role:
        input_tuple["Role"] = role_val
        assertionString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Role"]) + "> ;\n"
            "                <" + str(owl.someValuesFrom) + ">    [ <" + str(rdf.type) + ">    <" + role_val + ">    ]    ]"
        )
        whereString += (" ;\n    <" + str(properties_tuple["Role"]) + ">    [ <"
                        + str(rdf.type) + "> " + role_val + " ]")

    return [input_tuple, assertionString, whereString, swrlString]


def _write_derived_from_item(derivative, term, input_tuple, provenanceString,
                             whereString, swrlString):
    """Helper: write a single wasDerivedFrom value into Turtle strings."""
    provenanceString += (
        " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
        "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
        "                <" + str(owl.someValuesFrom) + ">    " + convertImplicitToKGEntry(derivative) + " ;\n"
        "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["wasDerivedFrom"]) + "> ]"
    )
    input_tuple["wasDerivedFrom"] = derivative
    if isSchemaVar(derivative):
        whereString += " ;\n    <" + str(properties_tuple["wasDerivedFrom"]) + ">    ?" + derivative.lower() + "_E "
        swrlString  += str(properties_tuple["wasDerivedFrom"]) + "(" + term + " , ?" + derivative.lower() + "_E) ^ "
    elif checkTemplate(derivative):
        open_idx  = derivative.index("{")
        close_idx = derivative.index("}")
        key = derivative[open_idx + 1:close_idx]
        provenanceString += (
            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
            "                <" + str(owl.someValuesFrom) + ">    " + convertImplicitToKGEntry(key) + " ;\n"
            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["wasDerivedFrom"]) + "> ]"
        )
        whereString += " ;\n    <" + str(properties_tuple["wasDerivedFrom"]) + ">    ?" + key.lower() + "_E"
        swrlString  += (str(properties_tuple["wasDerivedFrom"]) + "(" + term + " , "
                        + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ ")
    else:
        whereString += " ;\n    <" + str(properties_tuple["wasDerivedFrom"]) + ">    " + ([derivative + " ", derivative[1:] + "_V "][checkImplicit(derivative)])
        swrlString  += (str(properties_tuple["wasDerivedFrom"]) + "(" + term + " , "
                        + ([derivative, derivative[1:] + "_V"][checkImplicit(derivative)]) + ") ^ ")
    return [input_tuple, provenanceString, whereString, swrlString]


def writeClassWasDerivedFrom(item, term, input_tuple, provenanceString,
                             whereString, swrlString):
    val = getattr(item, "wasDerivedFrom", None)
    if val is None or pd.isnull(val):
        return [input_tuple, provenanceString, whereString, swrlString]

    if "," in val:
        for derivative in parse_delimited_string(val, ","):
            [input_tuple, provenanceString, whereString, swrlString] = \
                _write_derived_from_item(derivative, term, input_tuple,
                                        provenanceString, whereString, swrlString)
    else:
        [input_tuple, provenanceString, whereString, swrlString] = \
            _write_derived_from_item(val, term, input_tuple,
                                    provenanceString, whereString, swrlString)
    return [input_tuple, provenanceString, whereString, swrlString]


def _write_generated_by_item(generator, term, input_tuple, provenanceString,
                             whereString, swrlString):
    """Helper: write a single wasGeneratedBy value into Turtle strings."""
    provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                         + convertImplicitToKGEntry(generator))
    input_tuple["wasGeneratedBy"] = generator
    if isSchemaVar(generator):
        whereString += " ;\n    <" + str(properties_tuple["wasGeneratedBy"]) + ">    ?" + generator.lower() + "_E "
        swrlString  += str(properties_tuple["wasGeneratedBy"]) + "(" + term + " , ?" + generator.lower() + "_E) ^ "
    elif checkTemplate(generator):
        open_idx  = generator.index("{")
        close_idx = generator.index("}")
        key = generator[open_idx + 1:close_idx]
        # NOTE: wasGeneratedBy template expansion belongs in provenanceString,
        # not assertionString (bug fix: original code referenced assertionString
        # which was out of scope in this function)
        provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                             + convertImplicitToKGEntry(key))
        whereString += " ;\n    <" + str(properties_tuple["wasGeneratedBy"]) + ">    ?" + key.lower() + "_E"
        swrlString  += (str(properties_tuple["wasGeneratedBy"]) + "(" + term + " , "
                        + ([key, key[1:] + "_V"][checkImplicit(key)]) + ") ^ ")
    else:
        whereString += (" ;\n    <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                        + ([generator + " ", generator[1:] + "_V "][checkImplicit(generator)]))
        swrlString  += (str(properties_tuple["wasGeneratedBy"]) + "(" + term + " , "
                        + ([generator, generator[1:] + "_V"][checkImplicit(generator)]) + ") ^ ")
    return [input_tuple, provenanceString, whereString, swrlString]


def writeClassWasGeneratedBy(item, term, input_tuple, provenanceString,
                             whereString, swrlString):
    val = getattr(item, "wasGeneratedBy", None)
    if val is None or pd.isnull(val):
        return [input_tuple, provenanceString, whereString, swrlString]

    if "," in val:
        for generator in parse_delimited_string(val, ","):
            [input_tuple, provenanceString, whereString, swrlString] = \
                _write_generated_by_item(generator, term, input_tuple,
                                        provenanceString, whereString, swrlString)
    else:
        [input_tuple, provenanceString, whereString, swrlString] = \
            _write_generated_by_item(val, term, input_tuple,
                                    provenanceString, whereString, swrlString)
    return [input_tuple, provenanceString, whereString, swrlString]


# ---------------------------------------------------------------------------
# Implicit entry writer (called during data processing)
# ---------------------------------------------------------------------------

def writeImplicitEntry(assertionString, provenanceString, publicationInfoString,
                       explicit_entry_tuples, implicit_entry_tuples, timeline_tuple,
                       vref_list, v_column, index, row, col_headers):
    try:
        if timeline_tuple != {}:
            if v_column in timeline_tuple:
                v_id = hashlib.md5(
                    (str(timeline_tuple[v_column]) + str(index)).encode("utf-8")
                ).hexdigest()
                assertionString += ("\n    " + convertImplicitToKGEntry(v_column, v_id)
                                    + "    <" + str(rdf.type) + ">    "
                                    + convertImplicitToKGEntry(v_column))
                for timeEntry in timeline_tuple[v_column]:
                    if "Type" in timeEntry:
                        assertionString += " ;\n        <" + str(rdf.type) + ">    " + timeEntry["Type"]
                    if "Label" in timeEntry:
                        assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                            + timeEntry["Label"] + "\"^^xsd:string")
                    if ("Start" in timeEntry and "End" in timeEntry
                            and timeEntry["Start"] == timeEntry["End"]):
                        assertionString += (" ;\n        <" + str(properties_tuple["Value"]) + "> "
                                            + str(timeEntry["Start"]))
                    if "Start" in timeEntry:
                        assertionString += (" ;\n        <" + str(properties_tuple["Start"]) + "> [ <"
                                            + str(properties_tuple["Value"]) + "> "
                                            + str(timeEntry["Start"]) + " ]")
                    if "End" in timeEntry:
                        assertionString += (" ;\n        <" + str(properties_tuple["End"]) + "> [ <"
                                            + str(properties_tuple["Value"]) + "> "
                                            + str(timeEntry["End"]) + " ]")
                    if "Unit" in timeEntry:
                        assertionString += (
                            " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
                            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                            "                <" + str(owl.hasValue) + ">    " + str(codeMapper(timeEntry["Unit"])) + " ;\n"
                            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Unit"]) + "> ]"
                        )
                    if "inRelationTo" in timeEntry:
                        assertionString += (" ;\n        <" + str(properties_tuple["inRelationTo"]) + ">    "
                                            + convertImplicitToKGEntry(timeEntry["inRelationTo"], v_id))
                        if checkImplicit(timeEntry["inRelationTo"]) and timeEntry["inRelationTo"] not in vref_list:
                            vref_list.append(timeEntry["inRelationTo"])
                assertionString += " .\n"

        for v_tuple in implicit_entry_tuples:
            if v_tuple["Column"] == v_column:
                if "Study" in v_tuple:
                    continue
                v_id = hashlib.md5(
                    (str(v_tuple) + str(index)).encode("utf-8")
                ).hexdigest()
                if "Template" in v_tuple:
                    template_term = extractTemplate(col_headers, row, v_tuple["Template"])
                    termURI = "<" + prefixes[kb] + template_term + ">"
                else:
                    termURI = "<" + prefixes[kb] + v_tuple["Column"][2:] + "-" + v_id + ">"

                assertionString += ("\n    " + termURI + "    <" + str(rdf.type) + ">    <"
                                    + prefixes[kb] + v_tuple["Column"][2:] + ">")

                for field, prop_key in (("Entity", None), ("Attribute", None)):
                    if field in v_tuple:
                        values = parse_delimited_string(v_tuple[field], ",") if "," in v_tuple[field] else [v_tuple[field]]
                        for v in values:
                            assertionString += " ;\n        <" + str(rdf.type) + ">    " + v

                if "Label" in v_tuple:
                    values = parse_delimited_string(v_tuple["Label"], ",") if "," in v_tuple["Label"] else [v_tuple["Label"]]
                    for label in values:
                        assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                            + label + "\"^^xsd:string")

                if "Time" in v_tuple:
                    if checkImplicit(v_tuple["Time"]):
                        timeID = None
                        for vr_tuple in implicit_entry_tuples:
                            if vr_tuple["Column"] == v_tuple["Time"]:
                                timeID = hashlib.md5(
                                    (str(vr_tuple) + str(index)).encode("utf-8")
                                ).hexdigest()
                        assertionString += (" ;\n        <" + str(properties_tuple["Time"]) + ">    "
                                            + convertImplicitToKGEntry(v_tuple["Time"], timeID))
                        if v_tuple["Time"] not in vref_list:
                            vref_list.append(v_tuple["Time"])
                    else:
                        assertionString += (" ;\n        <" + str(properties_tuple["Time"]) + ">    "
                                            + convertImplicitToKGEntry(v_tuple["Time"], v_id))

                if "inRelationTo" in v_tuple:
                    relationToID = None
                    for vr_tuple in implicit_entry_tuples:
                        if vr_tuple["Column"] == v_tuple["inRelationTo"]:
                            relationToID = hashlib.md5(
                                (str(vr_tuple) + str(index)).encode("utf-8")
                            ).hexdigest()
                    has_role     = "Role" in v_tuple
                    has_relation = "Relation" in v_tuple
                    if has_role and not has_relation:
                        assertionString += (
                            " ;\n        <" + str(properties_tuple["Role"]) + ">    [ <" + str(rdf.type) + ">    "
                            + v_tuple["Role"] + " ;\n            <" + str(properties_tuple["inRelationTo"]) + ">    "
                            + convertImplicitToKGEntry(v_tuple["inRelationTo"], relationToID) + " ]"
                        )
                    elif has_relation and not has_role:
                        assertionString += (" ;\n        " + v_tuple["Relation"] + " "
                                            + convertImplicitToKGEntry(v_tuple["inRelationTo"], v_id))
                        assertionString += (" ;\n        " + v_tuple["Relation"] + " "
                                            + convertImplicitToKGEntry(v_tuple["inRelationTo"], relationToID))
                    else:
                        assertionString += (" ;\n        <" + str(properties_tuple["inRelationTo"]) + ">    "
                                            + convertImplicitToKGEntry(v_tuple["inRelationTo"], relationToID))
                elif "Role" in v_tuple:
                    assertionString += (" ;\n        <" + str(properties_tuple["Role"]) + ">    [ <"
                                        + str(rdf.type) + ">    " + v_tuple["Role"] + " ]")

                assertionString += " .\n"

                provenanceString += ("\n    " + termURI + "    <" + str(prov.generatedAtTime) + ">    \""
                                     + utc_now_string() + "\"^^xsd:dateTime")

                if "wasGeneratedBy" in v_tuple:
                    gens = parse_delimited_string(v_tuple["wasGeneratedBy"], ",") if "," in v_tuple["wasGeneratedBy"] else [v_tuple["wasGeneratedBy"]]
                    for gen in gens:
                        provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                                             + convertImplicitToKGEntry(gen, v_id))
                        if checkImplicit(gen) and gen not in vref_list:
                            vref_list.append(gen)

                if "wasDerivedFrom" in v_tuple:
                    derivs = parse_delimited_string(v_tuple["wasDerivedFrom"], ",") if "," in v_tuple["wasDerivedFrom"] else [v_tuple["wasDerivedFrom"]]
                    for deriv in derivs:
                        provenanceString += (" ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    "
                                             + convertImplicitToKGEntry(deriv, v_id))
                        if checkImplicit(deriv) and deriv not in vref_list:
                            vref_list.append(deriv)

                provenanceString += " .\n"

        return [assertionString, provenanceString, publicationInfoString, vref_list]
    except Exception as e:
        print("Warning: Unable to create implicit entry: " + str(e))
        return [assertionString, provenanceString, publicationInfoString, vref_list]


# ---------------------------------------------------------------------------
# Schema-level tuple writers
# ---------------------------------------------------------------------------

def writeExplicitEntryTuples(explicit_entry_list, output_file, query_file,
                             swrl_file, dm_fn):
    explicit_entry_tuples = []
    assertionString       = ""
    provenanceString      = ""
    publicationInfoString = ""
    selectString          = "SELECT DISTINCT "
    whereString           = "WHERE {\n"
    swrlString            = ""

    datasetIdentifier = hashlib.md5(dm_fn.encode("utf-8")).hexdigest()
    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "head-explicit_entry-" + datasetIdentifier + "> { ")
        output_file.write("\n    <" + prefixes[kb] + "nanoPub-explicit_entry-" + datasetIdentifier + ">    <" + str(rdf.type) + ">    <" + str(np.Nanopublication) + ">")
        output_file.write(" ;\n        <" + str(np.hasAssertion) + ">    <" + prefixes[kb] + "assertion-explicit_entry-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasProvenance) + ">    <" + prefixes[kb] + "provenance-explicit_entry-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasPublicationInfo) + ">    <" + prefixes[kb] + "pubInfo-explicit_entry-" + datasetIdentifier + ">")
        output_file.write(" .\n}\n\n")

    col_headers = list(read_csv_safe(dm_fn).columns.values)
    for item in explicit_entry_list:
        explicit_entry_tuple = {}
        if "Template" in col_headers and pd.notnull(item.Template):
            explicit_entry_tuple["Template"] = item.Template

        term = sanitize_term(item.Column)
        assertionString += "\n    <" + prefixes[kb] + term + ">    <" + str(rdf.type) + ">    owl:Class"
        selectString    += "?" + term.lower() + " "
        whereString     += "  ?" + term.lower() + "_E <" + str(rdf.type) + "> "
        term_expl        = "?" + term.lower() + "_E"

        explicit_entry_tuple["Column"] = item.Column
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassAttributeOrEntity(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassAttributeOf(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassUnit(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassTime(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)
        [explicit_entry_tuple, assertionString, whereString, swrlString] = writeClassRelation(item, term_expl, explicit_entry_tuple, assertionString, whereString, swrlString)

        if "Label" in col_headers and pd.notnull(item.Label):
            labels = parse_delimited_string(item.Label, ",") if "," in item.Label else [item.Label]
            for label in labels:
                assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                    + label + "\"^^xsd:string")
            explicit_entry_tuple["Label"] = item.Label

        if "Comment" in col_headers and pd.notnull(item.Comment):
            assertionString += (" ;\n        <" + str(properties_tuple["Comment"]) + ">    \""
                                + item.Comment + "\"^^xsd:string")
            explicit_entry_tuple["Comment"] = item.Comment

        if "Format" in col_headers and pd.notnull(item.Format):
            explicit_entry_tuple["Format"] = item.Format

        assertionString  += " .\n"
        provenanceString += ("\n    <" + prefixes[kb] + term + ">\n        <"
                             + str(prov.generatedAtTime) + ">    \""
                             + utc_now_string() + "\"^^xsd:dateTime")
        [explicit_entry_tuple, provenanceString, whereString, swrlString] = writeClassWasGeneratedBy(item, term_expl, explicit_entry_tuple, provenanceString, whereString, swrlString)
        [explicit_entry_tuple, provenanceString, whereString, swrlString] = writeClassWasDerivedFrom(item, term_expl, explicit_entry_tuple, provenanceString, whereString, swrlString)
        provenanceString += " .\n"
        whereString      += " ;\n    <" + str(properties_tuple["Value"]) + "> ?" + term.lower() + " .\n\n"

        if "hasPosition" in col_headers and pd.notnull(item.hasPosition):
            publicationInfoString += ("\n    <" + prefixes[kb] + term + ">    hasco:hasPosition    \""
                                      + str(item.hasPosition) + "\"^^xsd:integer .")
            explicit_entry_tuple["hasPosition"] = item.hasPosition

        explicit_entry_tuples.append(explicit_entry_tuple)

    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "assertion-explicit_entry-" + datasetIdentifier + "> {" + assertionString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "provenance-explicit_entry-" + datasetIdentifier + "> {")
        provenanceString = ("\n    <" + prefixes[kb] + "assertion-explicit_entry-" + datasetIdentifier + ">    <"
                            + str(prov.generatedAtTime) + ">    \"" + utc_now_string()
                            + "\"^^xsd:dateTime .\n" + provenanceString)
        output_file.write(provenanceString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "pubInfo-explicit_entry-" + datasetIdentifier + "> {\n    <"
                          + prefixes[kb] + "nanoPub-explicit_entry-" + datasetIdentifier + ">    <"
                          + str(prov.generatedAtTime) + ">    \"" + utc_now_string() + "\"^^xsd:dateTime .")
        output_file.write(publicationInfoString + "\n}\n\n")
    else:
        output_file.write(assertionString + "\n")
        output_file.write(provenanceString + "\n")

    query_file.write(selectString)
    query_file.write(whereString)
    swrl_file.write(swrlString)
    return explicit_entry_tuples


def writeImplicitEntryTuples(implicit_entry_list, timeline_tuple, output_file,
                             query_file, swrl_file, dm_fn):
    implicit_entry_tuples = []
    assertionString       = ""
    provenanceString      = ""
    whereString           = "\n"
    swrlString            = ""

    datasetIdentifier = hashlib.md5(dm_fn.encode("utf-8")).hexdigest()
    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "head-implicit_entry-" + datasetIdentifier + "> { ")
        output_file.write("\n    <" + prefixes[kb] + "nanoPub-implicit_entry-" + datasetIdentifier + ">    <" + str(rdf.type) + ">    <" + str(np.Nanopublication) + ">")
        output_file.write(" ;\n        <" + str(np.hasAssertion) + ">    <" + prefixes[kb] + "assertion-implicit_entry-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasProvenance) + ">    <" + prefixes[kb] + "provenance-implicit_entry-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasPublicationInfo) + ">    <" + prefixes[kb] + "pubInfo-implicit_entry-" + datasetIdentifier + ">")
        output_file.write(" .\n}\n\n")

    col_headers = list(read_csv_safe(dm_fn).columns.values)
    for item in implicit_entry_list:
        implicit_tuple = {}
        if "Template" in col_headers and pd.notnull(item.Template):
            implicit_tuple["Template"] = item.Template

        assertionString += "\n    <" + prefixes[kb] + item.Column[2:] + ">    <" + str(rdf.type) + ">    owl:Class"
        term_implicit    = item.Column[1:] + "_V"
        whereString     += "  " + term_implicit + " <" + str(rdf.type) + "> "
        implicit_tuple["Column"] = item.Column

        if hasattr(item, "Label") and pd.notnull(item.Label):
            implicit_tuple["Label"] = item.Label
            labels = parse_delimited_string(item.Label, ",") if "," in item.Label else [item.Label]
            for label in labels:
                assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                    + label + "\"^^xsd:string")
        else:
            assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                + item.Column[2:] + "\"^^xsd:string")
            implicit_tuple["Label"] = item.Column[2:]

        if hasattr(item, "Comment") and pd.notnull(item.Comment):
            assertionString += (" ;\n        <" + str(properties_tuple["Comment"]) + ">    \""
                                + item.Comment + "\"^^xsd:string")
            implicit_tuple["Comment"] = item.Comment

        [implicit_tuple, assertionString, whereString, swrlString] = writeClassAttributeOrEntity(item, term_implicit, implicit_tuple, assertionString, whereString, swrlString)
        [implicit_tuple, assertionString, whereString, swrlString] = writeClassAttributeOf(item, term_implicit, implicit_tuple, assertionString, whereString, swrlString)
        [implicit_tuple, assertionString, whereString, swrlString] = writeClassUnit(item, term_implicit, implicit_tuple, assertionString, whereString, swrlString)
        [implicit_tuple, assertionString, whereString, swrlString] = writeClassTime(item, term_implicit, implicit_tuple, assertionString, whereString, swrlString)
        [implicit_tuple, assertionString, whereString, swrlString] = writeClassRelation(item, term_implicit, implicit_tuple, assertionString, whereString, swrlString)

        assertionString  += " .\n"
        provenanceString += ("\n    <" + prefixes[kb] + item.Column[2:] + ">\n        <"
                             + str(prov.generatedAtTime) + ">    \""
                             + utc_now_string() + "\"^^xsd:dateTime")
        [implicit_tuple, provenanceString, whereString, swrlString] = writeClassWasGeneratedBy(item, term_implicit, implicit_tuple, provenanceString, whereString, swrlString)
        [implicit_tuple, provenanceString, whereString, swrlString] = writeClassWasDerivedFrom(item, term_implicit, implicit_tuple, provenanceString, whereString, swrlString)
        provenanceString += " .\n"
        whereString      += ".\n\n"
        implicit_entry_tuples.append(implicit_tuple)

    if timeline_tuple != {}:
        for key in timeline_tuple:
            assertionString += "\n    " + convertImplicitToKGEntry(key) + "    <" + str(rdf.type) + ">    owl:Class "
            for timeEntry in timeline_tuple[key]:
                if "Type" in timeEntry:
                    assertionString += " ;\n        rdfs:subClassOf    " + timeEntry["Type"]
                if "Label" in timeEntry:
                    assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                        + timeEntry["Label"] + "\"^^xsd:string")
                if ("Start" in timeEntry and "End" in timeEntry
                        and timeEntry["Start"] == timeEntry["End"]):
                    assertionString += (" ;\n        <" + str(properties_tuple["Value"]) + "> "
                                        + str(timeEntry["Start"]))
                if "Start" in timeEntry:
                    if "Unit" in timeEntry:
                        assertionString += (
                            " ;\n        <" + str(rdfs.subClassOf) + ">\n"
                            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                            "                <" + str(owl.allValuesFrom) + ">\n"
                            "                    [ <" + str(rdf.type) + ">    <" + str(owl.Class) + "> ;\n"
                            "                        <" + str(owl.intersectionOf) + "> ( [ <" + str(rdf.type) + "> <" + str(owl.Restriction) + "> ;\n"
                            "                            <" + str(owl.hasValue) + "> " + str(timeEntry["Start"]) + " ;\n"
                            "                            <" + str(owl.onProperty) + "> <" + str(properties_tuple["Value"]) + "> ] "
                            + str(codeMapper(timeEntry["Unit"])) + " ) ] ;\n"
                            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Start"]) + "> ] "
                        )
                    else:
                        assertionString += (" ;\n        <" + str(properties_tuple["Start"]) + "> [ <"
                                            + str(properties_tuple["Value"]) + "> " + str(timeEntry["Start"]) + " ]")
                if "End" in timeEntry:
                    if "Unit" in timeEntry:
                        assertionString += (
                            " ;\n        <" + str(rdfs.subClassOf) + ">\n"
                            "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                            "                <" + str(owl.allValuesFrom) + ">\n"
                            "                    [ <" + str(rdf.type) + ">    <" + str(owl.Class) + "> ;\n"
                            "                        <" + str(owl.intersectionOf) + "> ( [ <" + str(rdf.type) + "> <" + str(owl.Restriction) + "> ;\n"
                            "                            <" + str(owl.hasValue) + "> " + str(timeEntry["End"]) + " ;\n"
                            "                            <" + str(owl.onProperty) + "> <" + str(properties_tuple["Value"]) + "> ] "
                            + str(codeMapper(timeEntry["Unit"])) + " ) ] ;\n"
                            "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["End"]) + "> ] "
                        )
                    else:
                        assertionString += (" ;\n        <" + str(properties_tuple["End"]) + "> [ <"
                                            + str(properties_tuple["Value"]) + "> " + str(timeEntry["End"]) + " ]")
                if "Unit" in timeEntry:
                    assertionString += (
                        " ;\n        <" + str(rdfs.subClassOf) + ">    \n"
                        "            [ <" + str(rdf.type) + ">    <" + str(owl.Restriction) + "> ;\n"
                        "                <" + str(owl.hasValue) + ">    " + str(codeMapper(timeEntry["Unit"])) + " ;\n"
                        "                <" + str(owl.onProperty) + ">    <" + str(properties_tuple["Unit"]) + "> ]"
                    )
                if "inRelationTo" in timeEntry:
                    assertionString += (" ;\n        <" + str(properties_tuple["inRelationTo"]) + ">    "
                                        + convertImplicitToKGEntry(timeEntry["inRelationTo"]))
                assertionString += " .\n"
            provenanceString += ("\n    " + convertImplicitToKGEntry(key) + "    <"
                                 + str(prov.generatedAtTime) + ">    \""
                                 + utc_now_string() + "\"^^xsd:dateTime .\n")

    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "assertion-implicit_entry-" + datasetIdentifier + "> {" + assertionString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "provenance-implicit_entry-" + datasetIdentifier + "> {")
        provenanceString = ("\n    <" + prefixes[kb] + "assertion-implicit_entry-" + datasetIdentifier + ">    <"
                            + str(prov.generatedAtTime) + ">    \"" + utc_now_string()
                            + "\"^^xsd:dateTime .\n" + provenanceString)
        output_file.write(provenanceString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "pubInfo-implicit_entry-" + datasetIdentifier + "> {\n    <"
                          + prefixes[kb] + "nanoPub-implicit_entry-" + datasetIdentifier + ">    <"
                          + str(prov.generatedAtTime) + ">    \"" + utc_now_string()
                          + "\"^^xsd:dateTime .\n}\n\n")
    else:
        output_file.write(assertionString + "\n")
        output_file.write(provenanceString + "\n")

    whereString += "}"
    query_file.write(whereString)
    swrl_file.write(swrlString[:-2] if len(swrlString) >= 2 else swrlString)
    return implicit_entry_tuples


# ---------------------------------------------------------------------------
# Source file processors
# ---------------------------------------------------------------------------

def processPrefixes(output_file, query_file):
    """Read prefix CSV and write @prefix declarations to output files."""
    loaded_prefixes = {}
    prefix_fn = config["Prefixes"].get("prefixes", "prefixes.csv")
    try:
        prefix_file = read_csv_safe(prefix_fn, dtype=object)
        for row in prefix_file.itertuples():
            loaded_prefixes[row.prefix] = row.url
        for prefix, url in loaded_prefixes.items():
            output_file.write("@prefix " + prefix + ": <" + url + "> .\n")
            query_file.write("prefix " + prefix + ": <" + url + "> \n")
        query_file.write("\n")
        output_file.write("\n")
    except Exception as e:
        print("Warning: Something went wrong when trying to read the prefixes file: " + str(e))
    return loaded_prefixes


def processProperties():
    """Build the properties lookup dict, optionally overriding from a CSV."""
    props = {
        "Comment":       rdfs.comment,
        "attributeOf":   sio.isAttributeOf,
        "Attribute":     rdf.type,
        "Definition":    skos.definition,
        "Value":         sio.hasValue,
        "wasDerivedFrom": prov.wasDerivedFrom,
        "Label":         rdfs.label,
        "inRelationTo":  sio.inRelationTo,
        "Role":          sio.hasRole,
        "Start":         sio.hasStartTime,
        "End":           sio.hasEndTime,
        "Time":          sio.existsAt,
        "Entity":        rdf.type,
        "Unit":          sio.hasUnit,
        "wasGeneratedBy": prov.wasGeneratedBy,
    }
    if "properties" not in config["Source Files"]:
        return props

    properties_fn = config["Source Files"]["properties"]
    try:
        properties_file = read_csv_safe(properties_fn, dtype=object)
    except Exception as e:
        print("Warning: The specified Properties file does not exist or is unreadable: " + str(e))
        return props

    for row in properties_file.itertuples():
        if not (hasattr(row, "Property") and pd.notnull(row.Property)):
            continue
        prop = row.Property
        if "http://" in prop or "https://" in prop:
            props[row.Column] = prop
        elif ":" in prop:
            parts = prop.split(":", 1)
            props[row.Column] = rdflib.term.URIRef(prefixes[parts[0]] + parts[1])
        elif "." in prop:
            parts = prop.split(".", 1)
            props[row.Column] = rdflib.term.URIRef(prefixes[parts[0]] + parts[1])
    return props


def processCodeMappings(cmap_fn):
    """Load unit code/label/URI lists from a code mappings CSV."""
    unit_code_list  = []
    unit_uri_list   = []
    unit_label_list = []
    if cmap_fn is None:
        return [unit_code_list, unit_uri_list, unit_label_list]
    try:
        code_mappings_reader = read_csv_safe(cmap_fn)
        for code_row in code_mappings_reader.itertuples():
            if pd.notnull(code_row.code):
                unit_code_list.append(code_row.code)
            if pd.notnull(code_row.uri):
                unit_uri_list.append(code_row.uri)
            if pd.notnull(code_row.label):
                unit_label_list.append(code_row.label)
    except Exception as e:
        print("Warning: Something went wrong when trying to read the Code Mappings file: " + str(e))
    return [unit_code_list, unit_uri_list, unit_label_list]


def processInfosheet(output_file, dm_fn, cb_fn, cmap_fn, timeline_fn):
    """Parse infosheet metadata and write collection-level nanopub triples."""
    if "infosheet" not in config["Source Files"]:
        return [dm_fn, cb_fn, cmap_fn, timeline_fn]

    infosheet_fn = config["Source Files"]["infosheet"]
    try:
        infosheet_file = read_csv_safe(infosheet_fn, dtype=object)
    except Exception as e:
        print("Warning: Collection metadata will not be written.\n"
              "The specified Infosheet file does not exist or is unreadable: " + str(e))
        return [dm_fn, cb_fn, cmap_fn, timeline_fn]

    infosheet_tuple = {}
    for row in infosheet_file.itertuples():
        if pd.notnull(row.Value):
            infosheet_tuple[row.Attribute] = row.Value

    # SDD file declarations in infosheet override config
    for key, var in (("Dictionary Mapping", "dm_fn"),
                     ("Codebook",           "cb_fn"),
                     ("Code Mapping",       "cmap_fn"),
                     ("Timeline",           "timeline_fn")):
        if key in infosheet_tuple:
            if var == "dm_fn":       dm_fn       = infosheet_tuple[key]
            elif var == "cb_fn":     cb_fn       = infosheet_tuple[key]
            elif var == "cmap_fn":   cmap_fn     = infosheet_tuple[key]
            elif var == "timeline_fn": timeline_fn = infosheet_tuple[key]

    datasetIdentifier = hashlib.md5(dm_fn.encode("utf-8")).hexdigest()
    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "head-collection_metadata-" + datasetIdentifier + "> { ")
        output_file.write("\n    <" + prefixes[kb] + "nanoPub-collection_metadata-" + datasetIdentifier + ">    <" + str(rdf.type) + ">    <" + str(np.Nanopublication) + ">")
        output_file.write(" ;\n        <" + str(np.hasAssertion) + ">    <" + prefixes[kb] + "assertion-collection_metadata-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasProvenance) + ">    <" + prefixes[kb] + "provenance-collection_metadata-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasPublicationInfo) + ">    <" + prefixes[kb] + "pubInfo-collection_metadata-" + datasetIdentifier + ">")
        output_file.write(" .\n}\n\n")

    assertionString  = "<" + prefixes[kb] + "collection-" + datasetIdentifier + ">"
    provenanceString = ("    <" + prefixes[kb] + "collection-" + datasetIdentifier + ">    <http://www.w3.org/ns/prov#generatedAtTime>    \""
                        + utc_now_string() + "\"^^xsd:dateTime")

    def uri_or_literal(val):
        return ("<" + val + ">") if isURI(val) else ("\"" + val + "\"^^xsd:string")

    if "Type" in infosheet_tuple:
        t = infosheet_tuple["Type"]
        assertionString += "    <" + str(rdf.type) + ">    " + (("<" + t + ">") if isURI(t) else t)
    else:
        assertionString += "    <" + str(rdf.type) + ">    <http://purl.org/dc/dcmitype/Collection>"

    _simple_assertion_fields = {
        "Title":       "http://purl.org/dc/terms/title",
        "Comment":     "http://www.w3.org/2000/01/rdf-schema#comment",
        "Description": "http://purl.org/dc/terms/description",
        "Language":    "http://purl.org/dc/terms/language",
        "File Format": "http://purl.org/dc/terms/format",
    }
    for field, uri in _simple_assertion_fields.items():
        if field in infosheet_tuple:
            assertionString += " ;\n        <" + uri + ">    \"" + infosheet_tuple[field] + "\"^^xsd:string"

    _simple_provenance_fields = {
        "Date Created":    "http://purl.org/dc/terms/created",
        "Date of Issue":   "http://purl.org/dc/terms/issued",
    }
    for field, uri in _simple_provenance_fields.items():
        if field in infosheet_tuple:
            provenanceString += " ;\n        <" + uri + ">    \"" + infosheet_tuple[field] + "\"^^xsd:date"

    _multi_assertion_fields = {
        "Alternative Title": "http://purl.org/dc/terms/alternative",
        "Keywords":          "http://www.w3.org/ns/dcat#keyword",
        "Standards":         "http://purl.org/dc/terms/conformsTo",
        "License":           "http://purl.org/dc/terms/license",
        "Rights":            "http://purl.org/dc/terms/rights",
        "Imports":           "http://www.w3.org/2002/07/owl#imports",
    }
    for field, uri in _multi_assertion_fields.items():
        if field in infosheet_tuple:
            vals = parse_delimited_string(infosheet_tuple[field], ",") if "," in infosheet_tuple[field] else [infosheet_tuple[field]]
            for v in vals:
                assertionString += " ;\n        <" + uri + ">    " + uri_or_literal(v)

    _multi_provenance_fields = {
        "Creators":      "http://purl.org/dc/terms/creator",
        "Contributors":  "http://purl.org/dc/terms/contributor",
        "Publisher":     "http://purl.org/dc/terms/publisher",
        "Source":        "http://purl.org/dc/terms/source",
    }
    for field, uri in _multi_provenance_fields.items():
        if field in infosheet_tuple:
            vals = parse_delimited_string(infosheet_tuple[field], ",") if "," in infosheet_tuple[field] else [infosheet_tuple[field]]
            for v in vals:
                provenanceString += " ;\n        <" + uri + ">    " + uri_or_literal(v)

    if "Link" in infosheet_tuple:
        assertionString += " ;\n        <http://xmlns.com/foaf/0.1/page>    <" + infosheet_tuple["Link"] + ">"
    if "Documentation" in infosheet_tuple:
        provenanceString += " ;\n        <http://www.w3.org/ns/dcat#landingPage>    <" + infosheet_tuple["Documentation"] + ">"
    if "Identifier" in infosheet_tuple:
        assertionString += (
            " ;\n        <http://semanticscience.org/resource/hasIdentifier>    \n"
            "            [ <" + str(rdf.type) + ">    <http://semanticscience.org/resource/Identifier> ; \n"
            "            <http://semanticscience.org/resource/hasValue>    \""
            + infosheet_tuple["Identifier"] + "\"^^xsd:string ]"
        )
    if "Version" in infosheet_tuple:
        provenanceString += " ;\n        <http://purl.org/pav/version>    " + uri_or_literal(infosheet_tuple["Version"])
        provenanceString += " ;\n        <http://www.w3.org/2002/07/owl/versionInfo>    " + uri_or_literal(infosheet_tuple["Version"])
    if "Previous Version" in infosheet_tuple:
        provenanceString += " ;\n        <http://purl.org/pav/previousVersion>    " + uri_or_literal(infosheet_tuple["Previous Version"])
    if "Version Of" in infosheet_tuple:
        provenanceString += " ;\n        <http://purl.org/dc/terms/isVersionOf>    " + uri_or_literal(infosheet_tuple["Version Of"])

    assertionString  += " .\n"
    provenanceString += " .\n"

    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "assertion-collection_metadata-" + datasetIdentifier + "> {\n    " + assertionString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "provenance-collection_metadata-" + datasetIdentifier + "> {\n    <"
                          + prefixes[kb] + "assertion-dataset_metadata-" + datasetIdentifier + ">    <http://www.w3.org/ns/prov#generatedAtTime>    \""
                          + utc_now_string() + "\"^^xsd:dateTime .\n" + provenanceString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "pubInfo-collection_metadata-" + datasetIdentifier + "> {")
        publicationInfoString = ("\n    <" + prefixes[kb] + "nanoPub-collection_metadata-" + datasetIdentifier + ">    <http://www.w3.org/ns/prov#generatedAtTime>    \""
                                 + utc_now_string() + "\"^^xsd:dateTime .\n")
        output_file.write(publicationInfoString + "\n}\n\n")
    else:
        output_file.write(assertionString + "\n\n")
        output_file.write(provenanceString + "\n")

    return [dm_fn, cb_fn, cmap_fn, timeline_fn]


def processTimeline(timeline_fn):
    """Load the timeline CSV into a nested dict keyed by Name."""
    timeline_tuple = {}
    if timeline_fn is None:
        return timeline_tuple
    try:
        timeline_file = read_csv_safe(timeline_fn, dtype=object)
        try:
            for row in timeline_file.itertuples():
                if pd.notnull(row.Name) and row.Name not in timeline_tuple:
                    timeline_tuple[row.Name] = []
                inner = {"Type": row.Type}
                if hasattr(row, "Label") and pd.notnull(row.Label):
                    inner["Label"] = row.Label
                if pd.notnull(row.Start):
                    inner["Start"] = row.Start
                if pd.notnull(row.End):
                    inner["End"] = row.End
                if hasattr(row, "Unit") and pd.notnull(row.Unit):
                    inner["Unit"] = row.Unit
                if hasattr(row, "inRelationTo") and pd.notnull(row.inRelationTo):
                    inner["inRelationTo"] = row.inRelationTo
                timeline_tuple[row.Name].append(inner)
        except Exception as e:
            print("Warning: Unable to process Timeline file: " + str(e))
    except Exception as e:
        print("Warning: The specified Timeline file does not exist: " + str(e))
    return timeline_tuple


def processDictionaryMapping(dm_fn):
    """Parse the dictionary mapping CSV into explicit/implicit entry lists."""
    try:
        dm_file = read_csv_safe(dm_fn, dtype=object)
    except Exception as e:
        print("Current directory: " + os.getcwd() + " - " + str(os.path.isfile(dm_fn)))
        print("Error processing DM file \"" + dm_fn + "\": " + str(e))
        sys.exit(1)

    local_explicit = []
    local_implicit = []
    try:
        for row in dm_file.itertuples():
            if pd.isnull(row.Column):
                print("Error: The DM must have a column named 'Column'")
                sys.exit(1)
            if row.Column.startswith("??"):
                local_implicit.append(row)
            else:
                local_explicit.append(row)
    except Exception as e:
        print("Something went wrong when trying to read the DM: " + str(e))
        sys.exit(1)
    return [local_explicit, local_implicit]


def processCodebook(cb_fn):
    """Load the codebook CSV into a nested dict keyed by Column."""
    cb_tuple = {}
    if cb_fn is None:
        return cb_tuple
    try:
        cb_file = read_csv_safe(cb_fn, dtype=object)
    except Exception as e:
        print("Error processing Codebook file: " + str(e))
        sys.exit(1)
    try:
        for row in cb_file.itertuples():
            if pd.notnull(row.Column) and row.Column not in cb_tuple:
                cb_tuple[row.Column] = []
            inner = {"Code": row.Code}
            if hasattr(row, "Label") and pd.notnull(row.Label):
                inner["Label"] = row.Label
            if hasattr(row, "Class") and pd.notnull(row.Class):
                inner["Class"] = row.Class
            if hasattr(row, "Resource") and pd.notnull(row.Resource):
                inner["Resource"] = row.Resource
            if hasattr(row, "Comment") and pd.notnull(row.Comment):
                inner["Comment"] = row.Comment
            if hasattr(row, "Definition") and pd.notnull(row.Definition):
                inner["Definition"] = row.Definition
            cb_tuple[row.Column].append(inner)
    except Exception as e:
        print("Warning: Unable to process Codebook file: " + str(e))
    return cb_tuple


def writeCodebookEntryTuples(cb_tuple, cb_fn, explicit_entry_tuples, output_file):
    """
    Generate ontology-level named individuals for every codebook entry.

    For each (Column, Code) pair this function produces:
      - A named individual typed as the OWL class that the DM declared for
        that column, plus any extra Class values listed in the codebook row.
      - If the codebook row supplies a Resource URI, that URI is used as the
        individual and owl:sameAs links it back to the generated URI so the
        generated URI remains a stable, resolvable node.
      - rdfs:label, rdfs:comment, and skos:definition annotations where present.
      - A sio:hasValue triple carrying the raw code as an xsd:string, making
        the code itself recoverable from the individual without the data file.
      - Provenance (prov:generatedAtTime) for each individual, consistent with
        explicit and implicit entry nanopubs.

    The entire block is wrapped in its own nanopub keyed by a hash of the
    codebook filename (same pattern as explicit_entry and implicit_entry nanopubs).
    When nanopublication is disabled the triples are written flat.
    """
    if not cb_tuple or cb_fn is None:
        return

    # Build a fast lookup: column name -> class URI string declared in the DM.
    # We prefer "Entity" over "Attribute" to match writeClassAttributeOrEntity.
    column_class_map = {}
    for a_tuple in explicit_entry_tuples:
        col = a_tuple.get("Column")
        if col is None:
            continue
        if "Entity" in a_tuple:
            column_class_map[col] = str(a_tuple["Entity"])
        elif "Attribute" in a_tuple:
            column_class_map[col] = str(a_tuple["Attribute"])

    datasetIdentifier = hashlib.md5(cb_fn.encode("utf-8")).hexdigest()
    assertionString   = ""
    provenanceString  = ""

    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "head-codebook-" + datasetIdentifier + "> { ")
        output_file.write("\n    <" + prefixes[kb] + "nanoPub-codebook-" + datasetIdentifier + ">    <" + str(rdf.type) + ">    <" + str(np.Nanopublication) + ">")
        output_file.write(" ;\n        <" + str(np.hasAssertion) + ">    <" + prefixes[kb] + "assertion-codebook-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasProvenance) + ">    <" + prefixes[kb] + "provenance-codebook-" + datasetIdentifier + ">")
        output_file.write(" ;\n        <" + str(np.hasPublicationInfo) + ">    <" + prefixes[kb] + "pubInfo-codebook-" + datasetIdentifier + ">")
        output_file.write(" .\n}\n\n")

    for column, entries in cb_tuple.items():
        col_class = column_class_map.get(column)
        col_term  = sanitize_term(column)

        for entry in entries:
            code = entry.get("Code")
            if code is None:
                continue

            # Stable URI for this individual: <base><ColumnTerm>-<sanitized code>
            code_term   = sanitize_term(str(code))
            generated_uri = "<" + prefixes[kb] + col_term + "-" + code_term + ">"

            # If an external Resource URI is given, use it as the primary node
            # and owl:sameAs the generated URI so it remains resolvable.
            resource = entry.get("Resource", "")
            if resource and str(resource).strip():
                resource_uri = str(resource).strip()
                if not isURI(resource_uri):
                    resource_uri = codeMapper(resource_uri)
                primary_uri = "<" + resource_uri + ">" if not resource_uri.startswith("<") else resource_uri
            else:
                primary_uri = generated_uri
                resource_uri = None

            # --- assertion ---
            assertionString += "\n    " + primary_uri + "\n        <" + str(rdf.type) + ">    owl:NamedIndividual"

            # Type as the DM-declared column class
            if col_class:
                assertionString += " ;\n        <" + str(rdf.type) + ">    " + col_class

            # Additional Class values from the codebook row
            cb_class = entry.get("Class", "")
            if cb_class and str(cb_class).strip():
                class_vals = parse_delimited_string(str(cb_class), ",") if "," in str(cb_class) else [str(cb_class)]
                for cv in class_vals:
                    cv = cv.strip()
                    if cv:
                        assertionString += " ;\n        <" + str(rdf.type) + ">    " + codeMapper(cv)

            # owl:sameAs back to generated URI when Resource was supplied
            if resource_uri is not None and primary_uri != generated_uri:
                assertionString += " ;\n        <" + str(owl.sameAs) + ">    " + generated_uri

            # Store the raw code value on the individual
            assertionString += (" ;\n        <" + str(sio.hasValue) + ">    \""
                                + str(code) + "\"^^xsd:string")

            # Annotation properties
            label = entry.get("Label", "")
            if label and str(label).strip():
                assertionString += (" ;\n        <" + str(rdfs.label) + ">    \""
                                    + str(label).strip() + "\"^^xsd:string")

            comment = entry.get("Comment", "")
            if comment and str(comment).strip():
                assertionString += (" ;\n        <" + str(rdfs.comment) + ">    \""
                                    + str(comment).strip() + "\"^^xsd:string")

            definition = entry.get("Definition", "")
            if definition and str(definition).strip():
                assertionString += (" ;\n        <" + str(skos.definition) + ">    \""
                                    + str(definition).strip() + "\"^^xsd:string")

            assertionString += " .\n"

            # --- provenance ---
            provenanceString += ("\n    " + primary_uri + "    <" + str(prov.generatedAtTime)
                                 + ">    \"" + utc_now_string() + "\"^^xsd:dateTime .\n")

    if nanopublication_option == "enabled":
        output_file.write("<" + prefixes[kb] + "assertion-codebook-" + datasetIdentifier + "> {" + assertionString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "provenance-codebook-" + datasetIdentifier + "> {")
        prov_header = ("\n    <" + prefixes[kb] + "assertion-codebook-" + datasetIdentifier + ">    <"
                       + str(prov.generatedAtTime) + ">    \"" + utc_now_string() + "\"^^xsd:dateTime .\n")
        output_file.write(prov_header + provenanceString + "\n}\n\n")
        output_file.write("<" + prefixes[kb] + "pubInfo-codebook-" + datasetIdentifier + "> {\n    <"
                          + prefixes[kb] + "nanoPub-codebook-" + datasetIdentifier + ">    <"
                          + str(prov.generatedAtTime) + ">    \"" + utc_now_string() + "\"^^xsd:dateTime .\n}\n\n")
    else:
        output_file.write(assertionString + "\n")
        output_file.write(provenanceString + "\n")


# ---------------------------------------------------------------------------
# Data file processor
# ---------------------------------------------------------------------------

def processData(data_fn, output_file, query_file, swrl_file,
                cb_tuple, timeline_tuple, explicit_entry_tuples, implicit_entry_tuples):
    xsd_datatype_list = [
        "anyURI", "base64Binary", "boolean", "date", "dateTime", "decimal",
        "double", "duration", "float", "hexBinary", "gDay", "gMonth",
        "gMonthDay", "gYear", "gYearMonth", "NOTATION", "QName", "string", "time"
    ]
    if data_fn is None:
        return

    try:
        data_file = read_csv_safe(data_fn, dtype=object)
    except Exception as e:
        print("Error: The specified Data file does not exist: " + str(e))
        sys.exit(1)

    try:
        col_headers = list(data_file.columns.values)
        try:
            for a_tuple in explicit_entry_tuples:
                if "Attribute" in a_tuple:
                    if a_tuple["Attribute"] in ("hasco:originalID", "sio:Identifier"):
                        if a_tuple["Column"] in col_headers:
                            for v_tuple in implicit_entry_tuples:
                                if "isAttributeOf" in a_tuple:
                                    if a_tuple["isAttributeOf"] == v_tuple["Column"]:
                                        v_tuple["Subject"] = sanitize_term(a_tuple["Column"])
        except Exception as e:
            print("Error processing column headers: " + str(e))

        for row in data_file.itertuples():
            assertionString       = ""
            provenanceString      = ""
            publicationInfoString = ""
            id_string = "".join(str(t) for t in row[1:] if t is not None)
            npubIdentifier = hashlib.md5(id_string.encode("utf-8")).hexdigest()

            try:
                if nanopublication_option == "enabled":
                    output_file.write("<" + prefixes[kb] + "head-" + npubIdentifier + "> {")
                    output_file.write("\n    <" + prefixes[kb] + "nanoPub-" + npubIdentifier + ">")
                    output_file.write("\n        <" + str(rdf.type) + ">    <" + str(np.Nanopublication) + ">")
                    output_file.write(" ;\n        <" + str(np.hasAssertion) + ">    <" + prefixes[kb] + "assertion-" + npubIdentifier + ">")
                    output_file.write(" ;\n        <" + str(np.hasProvenance) + ">    <" + prefixes[kb] + "provenance-" + npubIdentifier + ">")
                    output_file.write(" ;\n        <" + str(np.hasPublicationInfo) + ">    <" + prefixes[kb] + "pubInfo-" + npubIdentifier + ">")
                    output_file.write(" .\n}\n\n")

                vref_list = []
                for a_tuple in explicit_entry_tuples:
                    if a_tuple["Column"] not in col_headers:
                        continue

                    typeString = "".join(
                        str(a_tuple[f]) for f in ("Attribute", "Entity", "Label", "Unit",
                                                   "Time", "inRelationTo", "wasGeneratedBy",
                                                   "wasDerivedFrom")
                        if f in a_tuple
                    )
                    identifierString = hashlib.md5(
                        (str(row[col_headers.index(a_tuple["Column"]) + 1]) + typeString
                         ).encode("utf-8")
                    ).hexdigest()

                    try:
                        if "Template" in a_tuple:
                            template_term = extractTemplate(col_headers, row, a_tuple["Template"])
                            termURI = "<" + prefixes[kb] + template_term + ">"
                        else:
                            termURI = ("<" + prefixes[kb]
                                       + sanitize_term(a_tuple["Column"])
                                       + "-" + identifierString + ">")

                        try:
                            assertionString += ("\n    " + termURI
                                                + "\n        <" + str(rdf.type) + ">    <"
                                                + prefixes[kb] + sanitize_term(a_tuple["Column"]) + ">")
                            if "Attribute" in a_tuple:
                                attrs = parse_delimited_string(a_tuple["Attribute"], ",") if "," in a_tuple["Attribute"] else [a_tuple["Attribute"]]
                                for attr in attrs:
                                    assertionString += " ;\n        <" + str(properties_tuple["Attribute"]) + ">    " + attr
                            if "Entity" in a_tuple:
                                ents = parse_delimited_string(a_tuple["Entity"], ",") if "," in a_tuple["Entity"] else [a_tuple["Entity"]]
                                for ent in ents:
                                    assertionString += " ;\n        <" + str(properties_tuple["Entity"]) + ">    " + ent
                            if "isAttributeOf" in a_tuple:
                                if checkImplicit(a_tuple["isAttributeOf"]):
                                    v_id = assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, "isAttributeOf", npubIdentifier)
                                    vTermURI = assignTerm(col_headers, "isAttributeOf", implicit_entry_tuples, a_tuple, row, v_id)
                                    assertionString += " ;\n        <" + str(properties_tuple["attributeOf"]) + ">    " + vTermURI
                                    if a_tuple["isAttributeOf"] not in vref_list:
                                        vref_list.append(a_tuple["isAttributeOf"])
                                elif checkTemplate(a_tuple["isAttributeOf"]):
                                    assertionString += (" ;\n        <" + str(properties_tuple["attributeOf"]) + ">    <"
                                                        + prefixes[kb] + str(extractExplicitTerm(col_headers, row, a_tuple["isAttributeOf"])) + ">")
                                else:
                                    assertionString += (" ;\n        <" + str(properties_tuple["attributeOf"]) + ">    "
                                                        + convertImplicitToKGEntry(a_tuple["isAttributeOf"], identifierString))
                            if "Unit" in a_tuple:
                                if checkImplicit(a_tuple["Unit"]):
                                    v_id = assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, "Unit", npubIdentifier)
                                    vTermURI = assignTerm(col_headers, "Unit", implicit_entry_tuples, a_tuple, row, v_id)
                                    assertionString += " ;\n        <" + str(properties_tuple["Unit"]) + ">    " + vTermURI
                                    if a_tuple["Unit"] not in vref_list:
                                        vref_list.append(a_tuple["Unit"])
                                elif checkTemplate(a_tuple["Unit"]):
                                    assertionString += (" ;\n        <" + str(properties_tuple["Unit"]) + ">    <"
                                                        + prefixes[kb] + str(extractExplicitTerm(col_headers, row, a_tuple["Unit"])) + ">")
                                else:
                                    assertionString += " ;\n        <" + str(properties_tuple["Unit"]) + ">    " + a_tuple["Unit"]
                            if "Time" in a_tuple:
                                if checkImplicit(a_tuple["Time"]):
                                    found = any(v["Column"] == a_tuple["Time"] for v in implicit_entry_tuples)
                                    if found:
                                        v_id = assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, "Time", npubIdentifier)
                                        vTermURI = assignTerm(col_headers, "Time", implicit_entry_tuples, a_tuple, row, v_id)
                                        assertionString += " ;\n        <" + str(properties_tuple["Time"]) + ">    " + vTermURI
                                    else:
                                        for t_tuple in timeline_tuple:
                                            if t_tuple == a_tuple["Time"]:
                                                vTermURI = convertImplicitToKGEntry(t_tuple)
                                                assertionString += (" ;\n        <" + str(properties_tuple["Time"]) + ">    [     rdf:type    "
                                                                    + vTermURI + "     ] ")
                                elif checkTemplate(a_tuple["Time"]):
                                    assertionString += (" ;\n        <" + str(properties_tuple["Time"]) + ">    <"
                                                        + prefixes[kb] + str(extractExplicitTerm(col_headers, row, a_tuple["Time"])) + ">")
                                else:
                                    assertionString += (" ;\n        <" + str(properties_tuple["Time"]) + ">    "
                                                        + convertImplicitToKGEntry(a_tuple["Time"], identifierString))
                            if "Label" in a_tuple:
                                labels = parse_delimited_string(a_tuple["Label"], ",") if "," in a_tuple["Label"] else [a_tuple["Label"]]
                                for label in labels:
                                    assertionString += (" ;\n        <" + str(properties_tuple["Label"]) + ">    \""
                                                        + label + "\"^^xsd:string")
                            if "Comment" in a_tuple:
                                assertionString += (" ;\n        <" + str(properties_tuple["Comment"]) + ">    \""
                                                    + a_tuple["Comment"] + "\"^^xsd:string")
                            if "inRelationTo" in a_tuple:
                                if checkImplicit(a_tuple["inRelationTo"]):
                                    v_id = assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, "inRelationTo", npubIdentifier)
                                    vTermURI = assignTerm(col_headers, "inRelationTo", implicit_entry_tuples, a_tuple, row, v_id)
                                    if a_tuple["inRelationTo"] not in vref_list:
                                        vref_list.append(a_tuple["inRelationTo"])
                                    if "Relation" in a_tuple:
                                        assertionString += " ;\n        " + a_tuple["Relation"] + "    " + vTermURI
                                    elif "Role" in a_tuple:
                                        assertionString += (" ;\n        <" + str(properties_tuple["Role"]) + ">    [ <"
                                                            + str(rdf.type) + ">    " + a_tuple["Role"] + " ;\n            <"
                                                            + str(properties_tuple["inRelationTo"]) + ">    " + vTermURI + " ]")
                                    else:
                                        assertionString += (" ;\n        <" + str(properties_tuple["inRelationTo"]) + ">    " + vTermURI)
                                elif checkTemplate(a_tuple["inRelationTo"]):
                                    resolved = "<" + prefixes[kb] + str(extractExplicitTerm(col_headers, row, a_tuple["inRelationTo"])) + ">"
                                    if "Relation" in a_tuple:
                                        assertionString += " ;\n        " + a_tuple["Relation"] + "    " + resolved
                                    elif "Role" in a_tuple:
                                        assertionString += (" ;\n        <" + str(properties_tuple["Role"]) + ">    [ <"
                                                            + str(rdf.type) + ">    " + a_tuple["Role"] + " ;\n            <"
                                                            + str(properties_tuple["inRelationTo"]) + ">    " + resolved + " ]")
                                    else:
                                        assertionString += " ;\n        <" + str(properties_tuple["inRelationTo"]) + ">    " + resolved
                                else:
                                    plain = convertImplicitToKGEntry(a_tuple["inRelationTo"], identifierString)
                                    if "Relation" in a_tuple:
                                        assertionString += " ;\n        " + a_tuple["Relation"] + "    " + plain
                                    elif "Role" in a_tuple:
                                        assertionString += (" ;\n        <" + str(properties_tuple["Role"]) + ">    [ <"
                                                            + str(rdf.type) + ">    " + a_tuple["Role"] + " ;\n            <"
                                                            + str(properties_tuple["inRelationTo"]) + ">    " + plain + " ]")
                                    else:
                                        assertionString += " ;\n        <" + str(properties_tuple["inRelationTo"]) + ">    " + plain
                        except Exception as e:
                            print("Error writing initial assertion elements: " + str(e))

                        try:
                            cell_value = row[col_headers.index(a_tuple["Column"]) + 1]
                            if cell_value != "":
                                if cb_tuple and a_tuple["Column"] in cb_tuple:
                                    for tuple_row in cb_tuple[a_tuple["Column"]]:
                                        if "Code" in tuple_row and str(tuple_row["Code"]) == str(cell_value):
                                            if "Class" in tuple_row and tuple_row["Class"] != "":
                                                classTerms = parse_delimited_string(tuple_row["Class"], ",") if "," in tuple_row["Class"] else [tuple_row["Class"]]
                                                for classTerm in classTerms:
                                                    assertionString += " ;\n        <" + str(rdf.type) + ">    " + classTerm
                                            if "Resource" in tuple_row and tuple_row["Resource"] != "":
                                                assertionString += " ;\n        <" + str(sio.hasValue) + ">    <" + tuple_row["Resource"] + ">"
                                            if "Label" in tuple_row and tuple_row["Label"] != "":
                                                assertionString += (" ;\n        <" + str(rdfs.label) + ">    \""
                                                                    + tuple_row["Label"] + "\"^^xsd:string")
                                            if "Comment" in tuple_row and tuple_row["Comment"] != "":
                                                assertionString += (" ;\n        <" + str(rdfs.comment) + ">    \""
                                                                    + tuple_row["Comment"] + "\"^^xsd:string")
                                            if "Definition" in tuple_row and tuple_row["Definition"] != "":
                                                assertionString += (" ;\n        <" + str(skos.definition) + ">    \""
                                                                    + tuple_row["Definition"] + "\"^^xsd:string")
                                else:
                                    # Determine appropriate literal type
                                    if "Format" in a_tuple:
                                        fmt = a_tuple["Format"]
                                        if fmt in xsd_datatype_list:
                                            assertionString += (" ;\n        <" + str(sio.hasValue) + ">    \""
                                                                + str(cell_value) + "\"^^xsd:" + fmt)
                                        elif isURI(fmt):
                                            assertionString += (" ;\n        <" + str(sio.hasValue) + ">    \""
                                                                + str(cell_value) + "\"^^<" + fmt + ">")
                                        else:
                                            assertionString += (" ;\n        <" + str(sio.hasValue) + ">    \""
                                                                + str(cell_value) + "\"^^xsd:string")
                                    elif isfloat(str(cell_value)):
                                        assertionString += (" ;\n        <" + str(sio.hasValue) + ">    \""
                                                            + str(cell_value) + "\"^^xsd:float")
                                    else:
                                        assertionString += (" ;\n        <" + str(sio.hasValue) + ">    \""
                                                            + str(cell_value) + "\"^^xsd:string")
                            assertionString += " .\n"
                        except Exception as e:
                            print("Error writing value: " + str(e))

                        try:
                            provenanceString += ("\n    " + termURI + "    <" + str(prov.generatedAtTime) + ">    \""
                                                 + utc_now_string() + "\"^^xsd:dateTime")
                            if "wasDerivedFrom" in a_tuple:
                                if "," in a_tuple["wasDerivedFrom"]:
                                    derivedFromTerms = parse_delimited_string(a_tuple["wasDerivedFrom"], ",")
                                    for dfTerm in derivedFromTerms:
                                        if checkImplicit(dfTerm):
                                            provenanceString += (" ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    "
                                                                 + convertImplicitToKGEntry(dfTerm, npubIdentifier))
                                            if dfTerm not in vref_list:
                                                vref_list.append(dfTerm)
                                        elif checkTemplate(dfTerm):
                                            provenanceString += (" ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    <"
                                                                 + prefixes[kb] + str(extractExplicitTerm(col_headers, row, dfTerm)) + ">")
                                        else:
                                            provenanceString += (" ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    "
                                                                 + convertImplicitToKGEntry(a_tuple["wasDerivedFrom"], identifierString))
                                elif checkImplicit(a_tuple["wasDerivedFrom"]):
                                    v_id = assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, "wasDerivedFrom", npubIdentifier)
                                    vTermURI = assignTerm(col_headers, "wasDerivedFrom", implicit_entry_tuples, a_tuple, row, v_id)
                                    provenanceString += " ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    " + vTermURI
                                    if a_tuple["wasDerivedFrom"] not in vref_list:
                                        vref_list.append(a_tuple["wasDerivedFrom"])
                                elif checkTemplate(a_tuple["wasDerivedFrom"]):
                                    provenanceString += (" ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    <"
                                                         + prefixes[kb] + str(extractExplicitTerm(col_headers, row, a_tuple["wasDerivedFrom"])) + ">")
                                else:
                                    provenanceString += (" ;\n        <" + str(properties_tuple["wasDerivedFrom"]) + ">    "
                                                         + convertImplicitToKGEntry(a_tuple["wasDerivedFrom"], identifierString))
                            if "wasGeneratedBy" in a_tuple:
                                v_id = assignVID(implicit_entry_tuples, timeline_tuple, a_tuple, "wasGeneratedBy", npubIdentifier)
                                if "," in a_tuple["wasGeneratedBy"]:
                                    generatedByTerms = parse_delimited_string(a_tuple["wasGeneratedBy"], ",")
                                    for gbTerm in generatedByTerms:
                                        if checkImplicit(gbTerm):
                                            provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                                                                 + convertImplicitToKGEntry(gbTerm, v_id))
                                            if gbTerm not in vref_list:
                                                vref_list.append(gbTerm)
                                        elif checkTemplate(gbTerm):
                                            provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    <"
                                                                 + prefixes[kb] + str(extractExplicitTerm(col_headers, row, gbTerm)) + ">")
                                        else:
                                            provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                                                                 + convertImplicitToKGEntry(gbTerm, identifierString))
                                elif checkImplicit(a_tuple["wasGeneratedBy"]):
                                    vTermURI = assignTerm(col_headers, "wasGeneratedBy", implicit_entry_tuples, a_tuple, row, v_id)
                                    provenanceString += " ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    " + vTermURI
                                    if a_tuple["wasGeneratedBy"] not in vref_list:
                                        vref_list.append(a_tuple["wasGeneratedBy"])
                                elif checkTemplate(a_tuple["wasGeneratedBy"]):
                                    provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    <"
                                                         + prefixes[kb] + str(extractExplicitTerm(col_headers, row, a_tuple["wasGeneratedBy"])) + ">")
                                else:
                                    provenanceString += (" ;\n        <" + str(properties_tuple["wasGeneratedBy"]) + ">    "
                                                         + convertImplicitToKGEntry(a_tuple["wasGeneratedBy"], identifierString))
                            provenanceString += " .\n"
                            if "hasPosition" in a_tuple:
                                publicationInfoString += ("\n    " + termURI + "\n        hasco:hasPosition    \""
                                                          + str(a_tuple["hasPosition"]) + "\"^^xsd:integer .")
                        except Exception as e:
                            print("Error writing provenance or publication info: " + str(e))
                    except Exception as e:
                        print("Unable to process tuple " + str(a_tuple) + ": " + str(e))

                try:
                    for vref in vref_list:
                        [assertionString, provenanceString, publicationInfoString, vref_list] = \
                            writeImplicitEntry(assertionString, provenanceString, publicationInfoString,
                                               explicit_entry_tuples, implicit_entry_tuples, timeline_tuple,
                                               vref_list, vref, npubIdentifier, row, col_headers)
                except Exception as e:
                    print("Warning: Something went wrong writing implicit entries: " + str(e))

            except Exception as e:
                print("Error: Something went wrong when processing explicit tuples: " + str(e))
                sys.exit(1)

            if nanopublication_option == "enabled":
                output_file.write("<" + prefixes[kb] + "assertion-" + npubIdentifier + "> {" + assertionString + "\n}\n\n")
                output_file.write("<" + prefixes[kb] + "provenance-" + npubIdentifier + "> {")
                provenanceString = ("\n    <" + prefixes[kb] + "assertion-" + npubIdentifier + ">    <"
                                    + str(prov.generatedAtTime) + ">    \""
                                    + utc_now_string() + "\"^^xsd:dateTime .\n" + provenanceString)
                output_file.write(provenanceString + "\n}\n\n")
                output_file.write("<" + prefixes[kb] + "pubInfo-" + npubIdentifier + "> {")
                publicationInfoString = ("\n    <" + prefixes[kb] + "nanoPub-" + npubIdentifier + ">    <"
                                         + str(prov.generatedAtTime) + ">    \""
                                         + utc_now_string() + "\"^^xsd:dateTime .\n" + publicationInfoString)
                output_file.write(publicationInfoString + "\n}\n\n")
            else:
                output_file.write(assertionString + "\n")
                output_file.write(provenanceString + "\n")

    except Exception as e:
        print("Warning: Unable to process Data file: " + str(e))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    global kb, cmap_fn, nanopublication_option, prefixes, properties_tuple
    global explicit_entry_list, implicit_entry_list
    global unit_code_list, unit_uri_list, unit_label_list

    if "dictionary" in config["Source Files"]:
        dm_fn = config["Source Files"]["dictionary"]
    else:
        print("Error: Dictionary Mapping file is not specified in the config.")
        sys.exit(1)

    cb_fn       = config["Source Files"].get("codebook",  None)
    timeline_fn = config["Source Files"].get("timeline",  None)
    data_fn     = config["Source Files"].get("data_file", None)
    local_cmap  = config["Source Files"].get("code_mappings", None)

    if "base_uri" in config["Prefixes"]:
        kb = config["Prefixes"]["base_uri"]
    else:
        kb = ":"

    nanopublication_option = config["Prefixes"].get("nanopublication", "enabled")

    if "out_file" in config["Output Files"]:
        out_fn = config["Output Files"]["out_file"]
    else:
        out_fn = "out.trig" if nanopublication_option == "enabled" else "out.ttl"

    query_fn = config["Output Files"].get("query_file", "queryQ")
    swrl_fn  = config["Output Files"].get("swrl_file",  "swrlModel")

    # Open all output files with explicit UTF-8 encoding (Windows fix)
    with (open(out_fn,   "w", encoding="utf-8") as output_file,
          open(query_fn, "w", encoding="utf-8") as query_file,
          open(swrl_fn,  "w", encoding="utf-8") as swrl_file):

        prefixes         = processPrefixes(output_file, query_file)
        properties_tuple = processProperties()

        [unit_code_list, unit_uri_list, unit_label_list] = processCodeMappings(local_cmap)

        [dm_fn, cb_fn, local_cmap, timeline_fn] = processInfosheet(
            output_file, dm_fn, cb_fn, local_cmap, timeline_fn
        )

        [explicit_entry_list, implicit_entry_list] = processDictionaryMapping(dm_fn)

        cb_tuple       = processCodebook(cb_fn)
        timeline_tuple = processTimeline(timeline_fn)

        explicit_entry_tuples = writeExplicitEntryTuples(
            explicit_entry_list, output_file, query_file, swrl_file, dm_fn
        )
        implicit_entry_tuples = writeImplicitEntryTuples(
            implicit_entry_list, timeline_tuple, output_file, query_file, swrl_file, dm_fn
        )

        writeCodebookEntryTuples(cb_tuple, cb_fn, explicit_entry_tuples, output_file)

        processData(
            data_fn, output_file, query_file, swrl_file,
            cb_tuple, timeline_tuple, explicit_entry_tuples, implicit_entry_tuples
        )


# ---------------------------------------------------------------------------
# Script bootstrap
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sdd2rdf.py <configuration_file>")
        sys.exit(1)

    config = configparser.ConfigParser()
    try:
        config.read(sys.argv[1], encoding="utf-8")
    except Exception as e:
        print("Error: Unable to open configuration file: " + str(e))
        sys.exit(1)

    main()
