
from jinja2 import Environment, PackageLoader, select_autoescape

import pandas as pd
import argparse
import numpy as np
import re
from setlr import isempty
from slugify import slugify
import io
import magic

base_context = {
    "void" : "http://rdfs.org/ns/void#",
    "csvw" : "http://www.w3.org/ns/csvw#",
    "sio": "http://semanticscience.org/resource/",
    "owl": "http://www.w3.org/2002/07/owl#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "prov": "http://www.w3.org/ns/prov#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "Attribute" : { "@id" : "rdf:type", "@type" : "@id"},
    "Entity" : { "@id" : "rdf:type", "@type" : "@id"},
    "attributeOf" : { "@id" : "sio:isAttributeOf", "@type" : "@id"},
    "Entity" : { "@id" : "rdf:type", "@type" : "@id"},
    "inRelationTo" : { "@id" : "sio:inRelationTo", "@type" : "@id"},
    "Role" : { "@id" : "sio:hasRole", "@type" : "@id"},
    "Time" : { "@id" : "sio:existsAt", "@type" : "@id"},
    "Unit" : { "@id" : "sio:hasUnit", "@type" : "@id"},
    "Value" : { "@id" : "sio:hasValue"},
    "hasStart" : { "@id" : "sio:hasStartTime"},
    "hasEnd" : { "@id" : "sio:hasEndTime"},
    "TimeInterval" : {"@id" : "sio:TimeInterval"},
    "wasDerivedFrom" : { "@id" : "prov:wasDerivedFrom", "@type" : "@id"},
    "wasGeneratedBy" : { "@id" : "prov:wasGeneratedBy", "@type" : "@id"},
}

class SemanticDataDictionary:
    columns = None
    codebook = None
    resource_codebook = None
    context = None
    codemap = None
    timeline = None
    infosheet = None

    def __init__(self, sdd_path, prefix):
        self.sdd_path = sdd_path
        self.prefix = prefix
        self.load()

    def load(self):
        infosheet = self._get_table()
        self.infosheet = dict([(row.Attribute, row.Value)
                               for i, row in infosheet.iterrows()
                               if not isempty(row.Value)])

        codemap = self._get_table('Code Mapping')

        self.codemap = dict([(row.code, row.uri)
                               for i, row in codemap.iterrows()
                               if not isempty(row.uri)])

        codebook = self._get_table('Codebook')
        self.codebook = dict([((row.Column, row.Code),
                                self._split_and_map(row.Class))
                               for i, row in codebook.iterrows()
                               if not isempty(row.Class)])
        self.resource_codebook = {}
        for i, row in codebook.iterrows():
            if not isempty(row.Resource):
                if row.Column not in self.resource_codebook:
                    self.resource_codebook[row.Column] = {}
                self.resource_codebook[row.Column][row.Code] = self.codemap.get(row.Resource,row.Resource)

        self.context = {}
        self.context.update(base_context)
        self.context['@base'] = self.prefix
        if 'Prefixes' in self.infosheet:
            prefixes = self._get_table('Prefixes')
            self.context.update(dict([(str(row.prefix), row.url)
                                       for i, row in prefixes.iterrows()
                                       if not isempty(row.prefix) and not isempty(row.url)]))

        dm = self._get_table('Dictionary Mapping')
        self.columns = dict([(col.Column, col.to_dict()) for i, col in dm.iterrows()])
        timeline = self._get_table('Timeline')
        for i, t in timeline.iterrows():
            if t.Name in self.columns:
                self.columns[t.Name].update(t.to_dict())
            else:
                self.columns[t.Name] = t.to_dict()
                self.columns[t.Name]['Column'] = t.Name
        self.column_templates = {}
        for key, col in self.columns.items():
            for annotation in ['Unit','Format','Role','Relation']:
                if annotation in col and not isempty(col[annotation]):
                    col[annotation] = self.codemap.get(col[annotation],col[annotation])
            for annotation in ['wasDerivedFrom','wasGeneratedBy']:
                if annotation in col and not isempty(col[annotation]):
                    col[annotation] = self._split(col[annotation])
            for annotation in ['Attribute','Entity','Type']:
                if annotation in col and not isempty(col[annotation]):
                    col[annotation] = self._split_and_map(col[annotation])
            self.column_templates[col['Column']] = "{{row.get('%s')}}"%col['Column']
        for col in self.columns.values():
            template = slugify(col['Column'])+'-{i}'
            template  =col.get('Template',template)
            col['uri_template'] = template.format(i='{{name}}',**self.column_templates)

    loaders = {
        "text/csv" : pd.read_csv,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': pd.read_excel
    }

    def _get_table(self, entry=None):
        if entry is not None:
            path = self.infosheet[entry]
            if path.startswith('#'):
                sheetname = path.split('#')[1]
                location = self.sdd_path
                local = True
            else:
                sheetname = None
                location = path
                local = False
        else:
            path = None
            location = self.sdd_path
            local = True
            sheetname = "InfoSheet"

        if isinstance(location, io.IOBase):
            if self.sdd_format is None:
                self.sdd_format = magic.from_buffer(location.read(2048), mime=True)
                location.seek(0)
            kwargs = {}
            if sheetname is not None:
                kwargs['sheet_name'] = sheetname
            result = loaders[self.sdd_format](location, **kwargs)
            location.seek(0)
        else:
            if local:
                return pd.read_excel(location, sheet_name=sheetname)
            else:
                return pd.read_csv(location)

    def _split(self, value):
        if isempty(value):
            return []
        return re.split('\\\\s*[,;&]\\\\s*', value)

    def _split_and_map(self, value):
        if isempty(value):
            return []
        return [self.codemap.get(x,x) for x in self._split(value)]

file_types = {
    "text/csv" : "csvw:Table",
    "csv" : "csvw:Table",
    "excel" : "setl:Excel",
    'application/vnd.ms-excel': "setl:Excel",
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': "setl:Excel",
}

def sdd2setl(semantic_data_dictionary, prefix, datafile,
             format='csv', delimiter=',', sheetname=None, output=None, dataset_uri=None):
    if dataset_uri is None:
        dataset_uri = prefix+'dataset'
    sdd = SemanticDataDictionary(semantic_data_dictionary,prefix)
    env = Environment(loader=PackageLoader('sdd2rdf', 'templates'))
    template = env.get_template('sdd_setl_template.jinja')
    output = template.render(sdd=sdd,
                             prefix=prefix,
                             data=datafile,
                             data_type=file_types[format],
                             delimiter=delimiter,
                             sheetname=sheetname,
                             data_out = output,
                             str=str,
                             dataset=dataset_uri,
                             isempty=isempty)
    return output

def sdd2setl_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("semantic_data_dictionary")
    parser.add_argument("prefix")
    parser.add_argument("data_file")
    parser.add_argument("setl_output")
    parser.add_argument('-o', "--output")
    parser.add_argument('-f', "--format",default='csv', choices=['csv','excel'])
    parser.add_argument('-d', "--delimiter", default=',')
    parser.add_argument('-s', '--sheetname')
    parser.add_argument("--dataset_uri")

    args = parser.parse_args()
    output = sdd2setl(args.semantic_data_dictionary,
                      args.prefix,
                      args.data_file,
                      args.format,
                      args.delimiter,
                      args.sheetname,
                      args.output)
    with open(args.setl_output, 'wb') as o:
        o.write(output.encode('utf8'))
