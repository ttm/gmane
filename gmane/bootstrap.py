import os, percolation as P
PERCOLATIONDIR="~/.percolation/rdf/"
PACKAGEDIR=os.path.dirname(__file__)
DATADIR=PACKAGEDIR+"/../data/"
P.start(start_session=False)
P.percolation_graph.bind("po",P.rdf.NS.po)
