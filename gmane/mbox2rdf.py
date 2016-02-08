import mailbox
import percolation as P, social as S, numpy as n, pickle, dateutil, nltk as k, os, datetime, shutil, rdflib as r, codecs, re, string, random
from percolation.rdf import NS, U, a, po, c
class MboxPublishing:
    def __init__(self,snapshoturi,snapshotid,directory="somedir/",\
            data_path="../data/",final_path="./gmane_snapshots/",umbrella_dir="gmane_snapshotsX/"):
        c(snapshoturi,snapshotid,directory)
        isego=False
        isgroup=True
        isfriendship=False
        isinteraction=True
        hastext=True
        interactions_anonymized=False

        translation_graph="translation"
        meta_graph="translation_meta"
        gmane_graph="gmane"
        P.context(translation_graph,"remove")
        P.context(meta_graph,"remove")
        final_path_="{}{}/".format(final_path,snapshotid)
        online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,snapshotid)
        nurls=nreplies=nmessages=nempty=0
        dates=[]; nchars_all=[]; ntokens_all=[]; nsentences_all=[]
        participantvars=["email","name"]
        messagevars=["author","createdAt","replyTo","messageText","cleanMessageText","nChars","nTokens","nSentences","url","emptyMessage"]
        messagevars.sort()
        files=os.listdir(data_path+directory)
        locals_=locals().copy(); del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i,i))
        self.rdfLog()
        self.makeMetadata()
        self.writeAllGmane()
    def rdfLog(self):
        self.mboxs=[]
        self.messages=[]
        self.messagesB=[]
        self.charsets=[]
        for file_ in self.files:
            mbox = mailbox.mbox(self.data_path+self.directory+"/"+file_)
            self.mboxs+=[mbox]
            keys=mbox.keys()
            if not keys:
                self.nempty+=1
                mbox.close()
                continue
            keys.sort()
            for key in keys[1:]:
                if not mbox[key] or set(mbox[key])==set(('From', 'Date', 'Subject')):
                    pass
                else:
                    raise KeyError("keys in mbox instance not understood")
            self.messages.append(mbox[0])
            self.charsets.append(mbox[0].get_charsets())
            mbox.close()
    def makeMetadata(self):
        pass
    def writeAllGmane(self):
        pass

