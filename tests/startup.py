import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "gmane" in key or "percolation" in key:
        del sys.modules[key]
import gmane as G, percolation as P
