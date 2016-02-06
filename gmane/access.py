from gmane import DATADIR, os
import percolation as P
from percolation.rdf import NS, a, po, c

def parseLegacyFiles(data_dir=DATADIR):
    """Parse legacy txt files with irc logs"""
    directories=os.listdir(data_dir)
    directories=[i for i in directories if i!="ipython_log.py" and not i.endswith(".swp") and i!="lists.pickle"]

    snapshots=set()
    triples=[]
    for directory in directories:
        snapshotid="gmane-legacy-"+directory
        snapshoturi=po.GmaneSnapshot+"#"+snapshotid
        expressed_classes=[po.Participant,po.EmailMessage]
        expressed_reference=directory
        name_humanized="Gmane email list with id "+expressed_reference
        # get size for all files in dir
        directorysize=sum(os.path.getsize(data_dir+directory+"/"+filename) for filename in os.listdir(data_dir+directory))/10**6
        nfiles=len(os.listdir(data_dir+directory))
        fileformat="mbox"
        directoryuri=po.Directory+"#gmane-"+directory
        triples+=[
                 (snapshoturi,a,po.Snapshot),
                 (snapshoturi,a,po.GmaneSnapshot),
                 (snapshoturi,po.snapshotID,snapshotid),
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
                 ]
        snapshots.add(snapshoturi)
    nsnapshots=ndirectories=len(directories)
    P.context("gmane","remove")
    platformuri=P.rdf.ic(po.Platform,"#Gmane",context="gmane")
    triples+=[
             (NS.social.Session,NS.social.nGmaneParsedDirectories,ndirectories),
             (NS.social.Session,NS.social.nGmaneSnapshots,nsnapshots),
             (platformuri, po.dataDir,data_dir),
             ]
    P.add(triples,context="gmane")
    c("parsed {} gmane data directories (=={} snapshots) are in percolation graph and 'gmane' context".format(ndirectories,nsnapshots))
    c("percolation graph have {} triples ({} in gmane context)".format(len(P.percolation_graph),len(P.context("gmane"))))
    negos=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE         { GRAPH <gmane> { ?s po:isEgo true         } } ")
    ngroups=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE       { GRAPH <gmane> { ?s po:isGroup true       } } ")
    nfriendships=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE  { GRAPH <gmane> { ?s po:isFriendship true  } } ")
    ninteractions=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE { GRAPH <gmane> { ?s po:isInteraction true } } ")
    nposts=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE        { GRAPH <gmane> { ?s po:isPost true        } } ")
    totalsize=sum(P.query(r" SELECT ?size WHERE              { GRAPH <gmane> { ?s po:directorySize ?size     } } "))
    c("""{} are ego snapshots, {} are group snapshots
{} have a friendship structures. {} have an interaction structures. {} have texts 
Total raw data size is {:.2f}MB""".format(negos,ngroups,nfriendships,ninteractions,nposts,totalsize))
    return snapshots




