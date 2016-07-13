# import sys
# keys=tuple(sys.modules.keys())
# for key in keys:
#     if "gmane" in key or "percolation" in key:
#         del sys.modules[key]
import gmane as G
from percolation.rdf import c

# ss=G.access.parseLegacyFiles()
ss = G.access.parseLegacyFiles("/home/r/.gmane4/")
# ss = [i for i in ss if 'users' in i or 'metar' in i]
c("finished .gmane", ss)
# ss.union(G.access.parseLegacyFiles("/home/r/.gmane2/")); c("finished .gmane2")
# ss.union(G.access.parseLegacyFiles("/home/r/.gmane3/")); c("finished .gmane3")
# ss.union(G.access.parseLegacyFiles("/home/r/.gmane4/")); c("finished .gmane4")
# last_triplification_classes+=G.render.publishAll(ss); c("finished publication of all")
triplification_classes = G.render.publishAll(ss)
c("finished publication of all")
