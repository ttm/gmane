# gmane
the python package to expressing public email groups data from Gmane database as RDF linked data.

## core features
  - consistent downloading routines of data from email groups.
  - routines to render RDF data from the downloaded email data.
  - compliance to the participation ontology and the percolation package for harnessing open linked social data.

## install with
    $ pip install gmane
or

    $ python setup.py gmane

For greater control of customization (and debugging), clone the repo and install with pip with -e:

    $ git clone https://github.com/ttm/gmane.git
    $ pip install -e <path_to_repo>
This install method is especially useful when reloading modified module in subsequent runs of gmane.

Gmane is integrated to the participation ontology and the percolation package
to enable anthropological physics experiments and social harnessing:
- https://github.com/ttm/percolation

## coding conventions
A function name has a verb if it changes state of initialized objects, if it only "returns something", it is has no verb in name.

Classes, functions and variables are writen in CamelCase, headlessCamelCase and lowercase, respectively.
Underline is used only in variable names where the words in variable name make something unreadable (usually because the resulting name is big).

The code is the documentation. Code should be very readable to avoid writing unnecessary documentation and duplicating routine representations. This adds up to using docstrings to give context to the objects or omitting the docstrings.

Tasks might have c("some status message") which are printed with time interval in seconds between P.check calls.
These messages are turned of by setting P.QUIET=True or calling P.silence() which just sets P.QUIET=True

The usual variables in scripts are: P for percolation, NS for P.rdf.NS for namespace, a for NS.rdf.type, c for P.utils.check, S for social, r for rdflib, x for networkx, k for nltk... Variables have larger names to better describe the routine.

Every feature should be related to at least one legacy/ outline.

Routines should be oriented towards making RDF data from Gmane data, which is currently accessed in this gmane python package through the mbox email format.

Two peculiarities made appropriate that this minimalist package be isolated:
1. Debugging. E.g. the Gmane platform/server capabilities are not clear with respect to what can be downloaded with one request (although it is open source and one might verify or correct this functionality);
when the quantitiy of emails requested increases, or their size, some existent emails can be received as empty,
therefore Emails are downloaded one-by-one; the datetime format and other fields have non-trivial patterns in the dataset, specially "get_payload".
2. Gmane package is dedicated to publishing data already published as public. The other packages related to the participation ontology and the percolation framework render public data from different provenance.

### package structure
Data not in RDF are kept in the data/ directory.
Rendered RDF data should be in G.PERCOLATIONDIR="~./percolation/rdf/" unless otherwise specified.
Each platform/protocol has an umbrella module in which are modules for accessing current data in platforms
and expressing them as RDF.
This package relies heavily in the percolation package to assist rendering of RDF data.


#### the modules are:
bootstrap.py for starting system with basic variables and routines

render.py for expressing contents of gml, gdf and tab files in RDF. 
access.py for access to data in the facebook platform (through bots and other interfaces)
ontology.py for ontological relations used in facebook data
legacy.py for legacy routines and enrichment triples
utils.py for small functionalities that fit nowhere else

## usage example
```python
import gmane as G

G.publishLegacy() # publish as rdf all emails in data/

```

## further information
Analyses and media rendering of the published RDF data is dealt with by the percolation package: https://github.com/ttm/percolation

Social package for expressing data from Facebook, Twitter and IRC: https://github.com/ttm/gmane

Participation package for expressing data from participatory platforms such as AA, Particpabr and Cidade Democrática:
https://github.com/ttm/participation
