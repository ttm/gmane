import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "gmane" in key or "percolation" in key:
        del sys.modules[key]
import gmane as G, percolation as P
from percolation.rdf import NS, a, po, c

#ss=S.facebook.access.parseLegacyFiles()
##ss=[i for i in ss if i.endswith("gdf_fb")]
#last_triplification_class=S.facebook.render.publishAll(ss)

#ss=S.twitter.access.parseLegacyFiles()
##ss=[i for i in ss if i.endswith("gdf_fb")]
#last_triplification_class=S.twitter.render.publishAll(ss)

#ss=G.access.parseLegacyFiles()
ss=G.access.parseLegacyFiles("/home/r/.gmane3/"); c("finished .gmane")
#ss.union(G.access.parseLegacyFiles("/home/r/.gmane2/")); c("finished .gmane2")
#ss.union(G.access.parseLegacyFiles("/home/r/.gmane3/")); c("finished .gmane3")
#ss.union(G.access.parseLegacyFiles("/home/r/.gmane4/")); c("finished .gmane4")
#ss=[i for i in ss if i.endswith("gdf_fb")]
#last_triplification_classes+=G.render.publishAll(ss); c("finished publication of all")
triplification_classes=G.render.publishAll(ss); c("finished publication of all")
