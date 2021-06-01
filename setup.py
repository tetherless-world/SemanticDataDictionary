import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "sdd2rdf",
    version = "0.1.6",
    author = "Jamie McCusker",
    author_email = "mccusj@cs.rpi.edu",
    description = ("sdd2rdf generates RDF graphs from semantically annotated data."),
    license = "Apache License 2.0",
    keywords = "rdf semantic etl",
    url = "http://packages.python.org/sdd2rdf",
    packages=['sdd2rdf'],
    long_description='''SETLr is a tool for generating RDF graphs, including named graphs, from almost any kind of tabular data.''',
    include_package_data = True,
    package_data={'sdd2rdf': ['templates/*.jinja']},
    install_requires = [
        'future',
        'rdflib',
        'rdflib-jsonld',
        'setlr',
        'bsddb3',
        'jinja2',
        'openpyxl',
        'python-magic',
        'python-slugify',
        'pandas>=0.23.0',
    ],
    entry_points = {
        'console_scripts': ['sdd2rdf=sdd2rdf:main'],
        'console_scripts': ['sdd2setl=sdd2rdf:sdd2setl_main'],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
)
