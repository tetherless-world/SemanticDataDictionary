
from jinja2 import Environment, PackageLoader, select_autoescape

import pandas as pd
import argparse
import numpy as np
import re
from slugify import slugify

base_context = {
    "Attribute" : { "@id" : "rdf:type", "@type" : "@id"},
    "Entity" : { "@id" : "rdf:type", "@type" : "@id"},
    "attributeOf" : { "@id" : "sio:isAttributeOf", "@type" : "@id"},
}

class SemanticDataDictionary:
    _columns = None
    _codebook = None
    _resource_codebook = None
    _context = None
    _codemap = None
    _timeline = None
    _infosheet = None

    def __init(self, sdd_path, prefix):
        self.sdd_path = sdd_path
        self.prefix = prefix

    def _get_table(self, entry=None):
        if entry is None:
            path = self.sdd_path
        else:
            path = self.infosheet[entry]
        if '#' in path:
            infosheet_path, sheetname  = self.sdd_path.split('#')
            sheetname = path.replace('#','',1)
            return pd.read_excel(infosheet_path, sheetname=sheetname)
        else:
            return pd.read_csv(path)

    def _split(self, value):
        return re.split('\\\\s*[,;&]\\\\s*', value)

    def _split_and_map(self, value):
        return [self.codemap.get(x,x) for x in self._split(value)]

    @property
    def infosheet(self):
        if self._infosheet is  None:
            infosheet = self._get_table()
            self._infosheet = dict([(row.Attribute, row.Value)
                                   for i, row in infosheet.iterrows()
                                   if not np.is_nan(row.Value)])
        return self._infosheet

    @property
    def codemap(self):
        if self._codemap is None:
            codemap = self._get_table('Code Mapping')
            self._codemap = dict([(row.code, row.uri)
                                   for i, row in codemap.iterrows()
                                   if not np.is_nan(row.uri)])
        return self._codemap

    @property
    def codebook(self):
        if self._codebook is None:
            codebook = self._get_table('Codebook')
            self._codebook = dict([((row.Column, row.Code),
                                    self._split_and_map(row.Class))
                                   for i, row in codebook.iterrows()
                                   if not np.is_nan(row.Class)])
        return self._codebook

    @property
    def resource_codebook(self):
        if self._resource_codebook is None:
            codebook = self._get_table('Codebook')
            self._resource_codebook = dict([((row.Column, row.Code),
                                             self._split_and_map(row.Resource))
                                            for i, row in codebook.iterrows()
                                            if not np.is_nan(row.Resource)])
        return self._resource_codebook

    @property
    def context(self):
        if self._context is None:
            self._context = {}
            self._context.update(base_context)
            self._context['@base'] = self.prefix
            if 'Prefixes' in self.infosheet:
                codebook = self._get_table('Prefixes')
                self._context.update(dict([(row.code, row.uri)
                                           for i, row in codebook.iterrows()]))
        return self._context

    @property
    def columns(self):
        if self._columns is None:
            dm = self._get_table('Dictionary Mapping')
            self._columns = {}
            self._column_templates = {}
            for i, column in dm.iterrows():
                col = column.as_dict()
                for annotation in ['Unit','Format','Role']:
                    if annotation in col and not np.is_nan(col[annotation]):
                        col[annotation] = self._split(col[annotation])
                for annotation in ['inRelationTo','wasDerivedFrom','wasGeneratedBy']:
                    if annotation in col and not np.is_nan(col[annotation]):
                        col[annotation] = self._split(col[annotation])
                for annotation in ['Attribute','Entity','Role']:
                    if annotation in col and not np.is_nan(col[annotation]):
                        col[annotation] = self._split_and_map(col[annotation])
                self._column_templates[col['Column']] = "{{row.get('%s')}}"%col['Column']
                self._columns[col['Column']] = col
            for col in self._columns.values():
                template = slugify(col['Column'])+'-{i}'
                template  =col.get('Template',template)
                col['uri_template'] = template.format(i='{{i}}'**self._column_templates)
        return self._columns


def sdd2setl(semantic_data_dictionary, prefix, datafile, **kwargs):
    sdd = SemanticDataDictionary(semantic_data_dictionary,prefix)
    env = Environment(loader=PackageLoader('sdd2rdf', 'templates'))
    template = env.get_template('sdd_setl_template.jinja')
    output = template.render(sdd=sdd, prefix=prefix, data=datafile)
    return output

def sdd2setl_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("semantic_data_dictionary")
    parser.add_argument("prefix")
    parser.add_argument("data_file")
    parser.add_argument("output_file")

    args = parser.parse_args()
    output = sdd2setl(args['semantic_data_dictionary'],
                      args['prefix'], args['datafile'])
    with open(args['output_file'], 'wb') as o:
        o.write(output.encode('utf8'))
