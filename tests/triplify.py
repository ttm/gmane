import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "gmane" in key or "percolation" in key:
        del sys.modules[key]
import gmane as G, percolation as P
from percolation.rdf import NS, a, po

#ss=S.facebook.access.parseLegacyFiles()
##ss=[i for i in ss if i.endswith("gdf_fb")]
#last_triplification_class=S.facebook.render.publishAll(ss)

#ss=S.twitter.access.parseLegacyFiles()
##ss=[i for i in ss if i.endswith("gdf_fb")]
#last_triplification_class=S.twitter.render.publishAll(ss)

ss=G.access.parseLegacyFiles()
ss=G.access.parseLegacyFiles("/home/r/.gmane/")
#ss=[i for i in ss if i.endswith("gdf_fb")]
last_triplification_class=G.render.publishAll(ss)
