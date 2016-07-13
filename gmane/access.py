from gmane import DATADIR, os
import re
import percolation as P
from percolation.rdf import NS, a, po, c


def parseLegacyFiles(data_dir=DATADIR):
    """Parse legacy mbox files with emails from the Gmane database"""
    data_dir = os.path.expanduser(data_dir)
    directories = os.listdir(data_dir)
    directories = [i for i in directories if os.path.isdir(data_dir+i)]
    snapshots = set()
    triples = []
    for directory in directories:
        all_files = [i for i in os.listdir(data_dir+directory) if i.isdigit()]
        if all_files:
            all_files.sort()
            foo = all_files[0].lstrip("0")
            if not foo:
                foo = "0"
            snapshotid = re.sub('^gmane', 'gmane-legacy-', directory)+foo+"-"+all_files[-1].lstrip("0")
            snapshoturi = po.GmaneSnapshot+"#"+snapshotid
            expressed_classes = [po.GmaneParticipant, po.EmailPeer, po.EmailMessage]
            expressed_reference = directory
            name_humanized = "Gmane email list with id "+expressed_reference
            directorysize = sum(os.path.getsize(data_dir+directory+"/"+filename) for filename in os.listdir(data_dir+directory))/10**6
            fileformat = "mbox"
            directoryuri = po.Directory+"#gmane-"+directory
            triples.extend([
                     (snapshoturi, a, po.Snapshot),
                     (snapshoturi, a, po.GmaneSnapshot),
                     (snapshoturi, po.dataDir, data_dir),
                     (snapshoturi, po.snapshotID, snapshotid),
                     (snapshoturi, po.isEgo, False),
                     (snapshoturi, po.isGroup, True),
                     (snapshoturi, po.isFriendship, False),
                     (snapshoturi, po.isInteraction, True),
                     (snapshoturi, po.isPost, True),
                     (snapshoturi, po.humanizedName, name_humanized),
                     (snapshoturi, po.expressedReference, expressed_reference),
                     (snapshoturi, po.rawDirectory, directoryuri),
                     (directoryuri,     po.directorySize, directorysize),
                     (directoryuri,     po.directoryName, directory),
                     (directoryuri,     po.fileFormat, fileformat),
                     ]+[
                     (directoryuri,    po.expressedClass, expressed_class) for expressed_class in expressed_classes
                     ])
            snapshots.add(snapshoturi)
    nsnapshots = ndirectories = len(directories)
    P.context("gmane", "remove")
    platformuri = P.rdf.ic(po.Platform, "Gmane", context="gmane")
    triples.extend([
             (NS.social.Session, NS.social.nGmaneParsedDirectories, ndirectories),
             (NS.social.Session, NS.social.nGmaneSnapshots, nsnapshots),
             (NS.social.Session, po.platform, platformuri),
    ])
    P.add(triples, context="gmane")
    c("parsed {} gmane data directories (=={} snapshots) are in percolation graph and 'gmane' context".format(ndirectories, nsnapshots))
    c("percolation graph have {} triples ({} in gmane context)".format(len(P.percolation_graph), len(P.context("gmane"))))
    negos = P.query(r" SELECT (COUNT(?s) as ?cs) WHERE         { GRAPH <gmane> { ?s po:isEgo true         } } ")
    ngroups = P.query(r" SELECT (COUNT(?s) as ?cs) WHERE       { GRAPH <gmane> { ?s po:isGroup true       } } ")
    nfriendships = P.query(r" SELECT (COUNT(?s) as ?cs) WHERE  { GRAPH <gmane> { ?s po:isFriendship true  } } ")
    ninteractions = P.query(r" SELECT (COUNT(?s) as ?cs) WHERE { GRAPH <gmane> { ?s po:isInteraction true } } ")
    nposts = P.query(r" SELECT (COUNT(?s) as ?cs) WHERE        { GRAPH <gmane> { ?s po:isPost true        } } ")
    totalsize = sum(P.query(r" SELECT ?size WHERE              { GRAPH <gmane> { ?s po:directorySize ?size     } } "))
    c("""{} are ego snapshots, {} are group snapshots
{} have a friendship structures. {} have an interaction structures. {} have texts
Total raw data size is {:.2f}MB""".format(negos, ngroups, nfriendships, ninteractions, nposts, totalsize))
    return snapshots
