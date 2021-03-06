import percolation as P
from percolation.rdf import NS, a, po, c
from .mbox2rdf import MboxPublishing


def publishAll(snapshoturis=None):
    """express emails as RDF for publishing"""
    if not snapshoturis:
        c("getting email snapshots, implementation needs verification TTM")
        uridict = {}
        for snapshoturi in P.get(None, a, NS.po.Snapshot, minimized=True):
            uridict[snapshoturi] = 0
            for rawFile in P.get(snapshoturi, NS.po.rawFile, strict=True, minimized=True):
                uridict[snapshoturi] += P.get(rawFile, NS.po.directorySize, minimized=True).toPython()
        snapshoturis.sort(key=lambda x: uridict[x])
    c("on triplification")
    triplification_classes = []
    for snapshoturi in list(snapshoturis)[:10]:
        triplification_classes += [publishAny(snapshoturi)]
    # writePublishingReadme()
    return triplification_classes


def publishAny(snapshoturi):
    # publish to umbrelladir
    triples = [
            (snapshoturi,      po.dataDir, "?datadir"),
            (snapshoturi,      po.snapshotID, "?snapshotid"),
            (snapshoturi,      po.rawDirectory, "?directoryurifoo"),
            ("?directoryurifoo",    po.directoryName, "?directoryname"),
            ]
    data_dir, directory, snapshotid = P.get(triples)
    return MboxPublishing(snapshoturi, snapshotid, directory, data_dir)
