import os
import shutil
import mailbox
import re
import string
from email.header import decode_header
import datetime
import dateutil.parser
# import urllib
# import unicodedata
# import pickle
# import codecs
# import random
import bs4
import pytz
import numpy as n
import nltk as k
# import rdflib as r
from validate_email import validate_email
import percolation as P
import gmane as G
from percolation.rdf import NS, a, po, c


class MboxPublishing:
    def __init__(self, snapshoturi, snapshotid, directory="somedir/",
                 data_path="../data/", final_path="./gmane_snapshots/",
                 umbrella_dir="gmane_snapshotsX/"):
        c(snapshoturi, snapshotid, directory)
        isego = False
        isgroup = True
        isfriendship = False
        isinteraction = True
        hastext = True
        interactions_anonymized = False

        translation_graph = "translation"
        meta_graph = "translation_meta"
        gmane_graph = "gmane"
        P.context(translation_graph, "remove")
        P.context(meta_graph, "remove")
        final_path_ = "{}{}/".format(final_path, snapshotid)
        online_prefix = "https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir, snapshotid)
        ncc = nto = nlines = nremoved_lines = nurls = nlost_messages = nparticipants = nreferences = totalchars = nurls = nreplies = nmessages = nempty = 0
        dates = []
        nchars_all = []
        ntokens_all = []
        nsentences_all = []
        participantvars = ["emailAddress", "name"]
        messagevars = ["author", "createdAt", "replyTo", "messageText",
                       "cleanMessageText", "nCharsClean", "nTokensClean",
                       "nSentencesClean", "hasUrl", "nChars", "nTokens",
                       "nSentences", "emptyMessage", "gmaneID", "subject",
                       "cc", "to", "hasReference", "contentType", "organization",
                       "unparsedCC", "unparsedTo", "emailList"]
        messagevars.sort()
        files = os.listdir(data_path+directory)
        if not files:
            self.comment = "no files on the snapshot id"
            return
        files.sort()
        nchars_all = []
        ntokens_all = []
        nsentences_all = []
        nchars_clean_all = []
        ntokens_clean_all = []
        nsentences_clean_all = []
        locals_ = locals().copy()
        del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i, i))
        self.rdfMbox()
        if len(self.files) > self.nempty:
            if not os.path.isdir(final_path_):
                os.mkdir(final_path_)
            self.email_xml, self.size_xml, self.email_ttl, self.size_ttl = P.rdf.writeByChunks(
                self.final_path_+self.snapshotid+"Email", context=self.translation_graph, ntriples=100000)
            self.makeMetadata()
            self.writeAllGmane()

    def rdfMbox(self):
        self.messages = []
        for filecount, file_ in enumerate(self.files):
            if filecount % 100 == 0:
                c(self.snapshoturi, filecount)
            mbox = mailbox.mbox(self.data_path+self.directory+"/"+file_)
            if not mbox.keys():
                self.nempty += 1
                mbox.close()
                # c("||||||||||| EMPTY MESSAGE |||||||||||||||||||||", self.snapshotid, file_, "(", filecount, ")")
                continue
            assert mbox[0]["Message-Id"], "What to do with nonempy messages without id?"
            message = mbox[0]
            self.messages += [message]
            gmaneid = self.makeId(message["Message-Id"])
            # c("gmaneid",gmaneid)
            if not gmaneid:
                raise ValueError("Message without id")
            messageuri = P.rdf.ic(po.EmailMessage, gmaneid, self.translation_graph, self.snapshoturi)
            self.nmessages += 1
            triples = [
                     (messageuri, po.gmaneID, gmaneid),
                     ]
            email, name = self.parseParticipant(message["From"])
            if not email:
                raise ValueError("message without author")
            participanturi = P.rdf.ic(po.Participant, email, self.translation_graph, self.snapshoturi)
            # if not P.get(participanturi, po.emailAddress, None, self.translation_graph):
            #     self.nparticipants += 1
            #     if self.nparticipants == 100:
            #         pass
            triples.extend((
                     (messageuri, po.author, participanturi),
                     (participanturi, po.emailAddress, email),
            ))
            if name:
                triples.append((participanturi, po.name, name))
            subject = message["Subject"]
            if subject:
                subject = decodeHeader(subject)
                assert isinstance(subject, str)
                triples.append((messageuri, po.subject, subject))
            replyid_ = message["In-Reply-To"]
            saneid = self.makeId(replyid_)
            if bool(replyid_) and not bool(saneid):
                # self.nreplies += 1
                replyid = self.snapshotid+"-"+str(self.nlost_messages)
                self.nlost_messages += 1
                replymessageuri = P.rdf.ic(po.LostEmailMessage, replyid, self.translation_graph, self.snapshoturi)
                triples.extend((
                         (replymessageuri, a, po.EmailMessage),
                         (replymessageuri, NS.rdfs.comment, "This message registered as having a reply,  but the field might be ill-formed: "+replyid_),
                         (messageuri, po.replyTo, replymessageuri),
                ))
            elif saneid:
                # self.nreplies += 1
                replymessageuri = P.rdf.ic(po.EmailMessage, saneid, self.translation_graph, self.snapshoturi)
                triples.extend((
                         (replymessageuri, po.gmaneID, saneid),
                         (messageuri, po.replyTo, replymessageuri),
                ))
            if isinstance(message["Date"], str):
                datetime = parseDate(message["Date"])
            elif isinstance(message["Date"], mailbox.email.header.Header):
                datetimestring = decodeHeader(message["Date"])
                if False in [i in string.printable for i in datetimestring]:
                    datetime = None
                    triples.append((messageuri, po.lostCreatedAt, True))
                else:
                    datetime_ = re.findall(r"(.*\d\d:\d\d:\d\d).*", datetimestring)[0]
                    datetime = parseDate(datetime_)
            else:
                raise ValueError("datetime not understood")
            if datetime:
                # self.dates += [datetime]
                triples.append((messageuri, po.createdAt, datetime))
            if message["References"]:
                references = message["References"].replace("\n", "").replace("\t", "").replace(" ", "")
                if not re.findall(r"\A<(.*?)>\Z", references):
                    c("::: ::: ::: references field not understood",  message["References"])
                    triples.extend((
                             (messageuri, po.comment, "the references are not understood (<.*> ids are added anyway): "+message["References"]),
                             (messageuri, po.referencesLost, True),
                    ))
                for reference in re.findall(r"<(.*?)>", references):
                    self.nreferences += 1
                    referenceuri = P.rdf.ic(po.EmailMessage, reference, self.translation_graph, self.snapshoturi)
                    triples.extend((
                             (referenceuri, po.gmaneID, reference),
                             (messageuri, po.hasReference, referenceuri),
                    ))
                for part in message["References"].replace("\n", "").replace("\t", "").split():
                    if validate_email(part):
                        self.nreferences += 1
                        referenceuri = P.rdf.ic(po.EmailMessage, part, self.translation_graph, self.snapshoturi)
                        triples.extend((
                                 (referenceuri, po.gmaneID, reference),
                                 (messageuri, po.hasReference, referenceuri),
                        ))
            text = getText(message)
            if text:
                nchars = len(text)
                # ntokens = len(k.wordpunct_tokenize(text))
                # nsentences = len(k.sent_tokenize(text))
                self.nchars_all += [nchars]
                # self.ntokens_all += [ntokens]
                # self.nsentences_all += [nsentences]
                triples.extend((
                         (messageuri, po.text, text),
                         (messageuri, po.nChars, nchars),
                         # (messageuri, po.nTokens, ntokens),
                         # (messageuri, po.nSentences, nsentences),
                ))
                clean_text = cleanEmailBody(text)
                # self.nremoved_lines += text.count("\n")-clean_text.count("\n")
                self.nlines += text.count("\n")
                nchars_clean = len(clean_text)
                # ntokens_clean = len(k.wordpunct_tokenize(clean_text))
                # nsentences_clean = len(k.sent_tokenize(clean_text))
                self.nchars_clean_all.append(nchars_clean)
                # self.ntokens_clean_all += [ntokens_clean]
                # self.nsentences_clean_all += [nsentences_clean]
                triples.extend((
                         (messageuri, po.cleanText, clean_text),
                         (messageuri, po.nCharsClean, nchars_clean),
                         # (messageuri, po.nTokensClean, ntokens_clean),
                         # (messageuri, po.nSentencesClean, nsentences_clean),
                ))

                for url in re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', clean_text):
                    self.nurls += 1
                    triples.append((messageuri, po.hasUrl, url))
            content_type = message.get_content_type()
            if content_type:
                triples.append((messageuri, po.contentType, content_type))
            else:
                raise ValueError("/\/\/\/\/\ message without content type")
            organization = message["Organization"]
            if organization:
                if not isinstance(organization, str):
                    organization = "".join(i for i in str(organization) if i in string.printable)
                triples.append((messageuri, po.organization, organization))
            if message["cc"]:
                cc, unparsed = parseAddresses(message["cc"])
                if unparsed:
                    triples.append((messageuri, po.unparsedCC, unparsed))
                for peeraddress, peername in cc:
                    peeraddress = peeraddress.strip()
                    assert bool(peeraddress)
                    peeruri = P.rdf.ic(po.EmailPeer, peeraddress, self.translation_graph, self.snapshoturi)
                    triples.extend((
                             (messageuri, po.cc, peeruri),
                             (peeruri, po.emailAddress, peeraddress),
                    ))
                    self.ncc += 1
                    if peername:
                        triples.append((peeruri, po.name, peername.strip()))
            if message["to"]:
                to, unparsed = parseAddresses(message["to"])
                if unparsed:
                    triples.append((messageuri, po.unparsedTo, unparsed))
                for peeraddress, peername in to:
                    peeraddress = peeraddress.strip()
                    assert bool(peeraddress)
                    peeruri = P.rdf.ic(po.EmailPeer, peeraddress, self.translation_graph, self.snapshoturi)
                    triples.extend((
                             (messageuri, po.to, peeruri),
                             (peeruri, po.emailAddress, peeraddress),
                    ))
                    self.nto += 1
                    if peername:
                        triples.append((peeruri, po.name, peername.strip()))
            listid = message["list-id"]
            if listid:
                assert isinstance(listid, str)
                listid = listid.replace("\n", "").replace("\t", "")
                if listid.count("<") == listid.count(">") == listid.count(" ") == 0:
                    listname = ""
                    listid_ = listid
                elif listid.count("<") == listid.count(">") == 0:
                    parts = listid.split()
                    lens = [len(i) for i in parts]
                    listid_ = [i for i in parts if len(i) == max(lens)][0]
                    listname = " ".join(i for i in parts if len(i) != max(lens))
                elif listid.count("<") == listid.count(">") == 1:
                    listname, listid_ = re.findall(r"(.*) {0,1}<(.*)>", listid)[0]
                else:
                    raise ValueError("Unexpected listid string format")
                listuri = P.rdf.ic(po.EmailList, listid_, self.translation_graph, self.snapshoturi)
                triples.extend((
                         (messageuri, po.emailList, listuri),
                         (listuri, po.listID, listid_),
                ))
                if listname:
                    triples.append((listuri, po.name, listname.strip()))
            P.add(triples, self.translation_graph)
            mbox.close()

    def makeMetadata(self):
        triples = P.get(self.snapshoturi, None, None, self.gmane_graph)
        self.totalchars = sum(self.nchars_all)
        self.mchars_messages = n.mean(self.nchars_all)
        self.dchars_messages = n.std(self.nchars_all)
        # self.totaltokens = sum(self.ntokens_all)
        # self.mtokens_messages = n.mean(self.ntokens_all)
        # self.dtokens_messages = n.std(self.ntokens_all)
        # self.totalsentences = sum(self.nsentences_all)
        # self.msentences_messages = n.mean(self.nsentences_all)
        # self.dsentences_messages = n.std(self.nsentences_all)

        self.totalchars_clean = sum(self.nchars_clean_all)
        self.mchars_messages_clean = n.mean(self.nchars_clean_all)
        self.dchars_messages_clean = n.std(self.nchars_clean_all)
        # self.totaltokens_clean = sum(self.ntokens_clean_all)
        # self.mtokens_messages_clean = n.mean(self.ntokens_clean_all)
        # self.dtokens_messages_clean = n.std(self.ntokens_clean_all)
        # self.totalsentences_clean = sum(self.nsentences_clean_all)
        # self.msentences_messages_clean = n.mean(self.nsentences_clean_all)
        # self.dsentences_messages_clean = n.std(self.nsentences_clean_all)
        # fremoved_lines = self.nremoved_lines/self.nlines

        triples += [
                # (self.snapshoturi, po.numberOfParticipants,           self.nparticipants),
                # (self.snapshoturi, po.numberOfMessages,                 self.nmessages),
                (self.snapshoturi, po.numberOfEmptyMessages,                 self.nempty),
                # (self.snapshoturi, po.numberOfReplies,              self.nreplies),
                # (self.snapshoturi, po.numberOfCC,                 self.ncc),
                # (self.snapshoturi, po.numberOfTo,              self.nto),
                # (self.snapshoturi, po.numberOfReferences,               self.nreferences),
                # (self.snapshoturi, po.numberOfUrls,               self.nurls),
                # (self.snapshoturi, po.numberOfChars, self.totalchars),
                # (self.snapshoturi, po.meanChars, self.mchars_messages),
                # (self.snapshoturi, po.deviationChars, self.dchars_messages),
                # (self.snapshoturi, po.numberOfTokens, self.totaltokens),
                # (self.snapshoturi, po.meanTokens, self.mtokens_messages),
                # (self.snapshoturi, po.deviationTokens, self.dtokens_messages),
                # (self.snapshoturi, po.numberOfSentences, self.totalsentences),
                # (self.snapshoturi, po.meanSentences, self.msentences_messages),
                # (self.snapshoturi, po.deviationSentences, self.dsentences_messages),

                # (self.snapshoturi,  po.numberOfCharsClean,      self.totalchars_clean),
                # (self.snapshoturi,  po.meanCharsClean,  self.mchars_messages_clean),
                # (self.snapshoturi,  po.deviationCharsClean,  self.dchars_messages_clean),
                # (self.snapshoturi, po.numberOfTokensClean,     self.totaltokens_clean),
                # (self.snapshoturi, po.meanTokensClean, self.mtokens_messages_clean),
                # (self.snapshoturi, po.deviationTokensClean, self.dtokens_messages_clean),
                # (self.snapshoturi, po.numberOfSentencesClean,     self.totalsentences_clean),
                # (self.snapshoturi, po.meanSentencesClean, self.msentences_messages_clean),
                # (self.snapshoturi, po.deviationSentencesClean, self.dsentences_messages_clean),
                # (self.snapshoturi, po.fractionOfRemovedLines, fremoved_lines),
                ]
        P.add(triples, context=self.meta_graph)
        # P.rdf.triplesScaffolding(self.snapshoturi,
        #                          [po.gmaneParticipantAttribute]*len(self.participantvars),
        #                          self.participantvars, context=self.meta_graph)
        # P.rdf.triplesScaffolding(self.snapshoturi,
        #                          [po.gmaneMessageAttribute]*len(self.messagevars),
        #                          self.messagevars, context=self.meta_graph)
        # P.rdf.triplesScaffolding(self.snapshoturi,
        #                          [po.emailXMLFilename]*len(self.email_xml)+[po.emailTTLFilename]*len(self.email_ttl),
        #                          self.email_xml+self.email_ttl, context=self.meta_graph)
        # P.rdf.triplesScaffolding(self.snapshoturi,
        #                          [po.onlineEmailXMLFile]*len(self.email_xml)+[po.onlineEmailTTLFile]*len(self.email_ttl),
        #                          [self.online_prefix+i for i in self.email_xml+self.email_ttl], context=self.meta_graph)
        self.mrdf = self.snapshotid+"Meta.rdf"
        self.mttl = self.snapshotid+"Meta.ttl"
        self.desc = "gmane public email list dataset with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid, self.snapshoturi, self.isego, self.isgroup, )
        self.desc += "\nisFriendship: {}; ".format(self.isfriendship)
        self.desc += "isInteraction: {}.".format(self.isinteraction)
        # self.desc += "\nnParticipants: {}; nInteractions: {} (replies+references+cc+to).".format(
        #     self.nparticipants, self.nreplies+self.nreferences+self.ncc+self.nto)
        self.desc += "\nisPost: {}".format(self.hastext)
        # self.desc += "\nnMessages: {} (+ empty: {}); ".format(self.nmessages, self.nempty)
        # self.desc += "nReplies: {}; nReferences: {}; nTo {}; nCC: {}.".format(self.nreplies, self.nreferences, self.ncc, self.nto)
        # self.desc += "\nnumberOfChars: {}; meanChars: {}; deviationChars: {}.".format(self.totalchars, self.mchars_messages, self.dchars_messages)
        # self.desc += "\nnTokens: {}; mTokens: {}; dTokens: {};".format(self.totaltokens, self.mtokens_messages, self.dtokens_messages)
        # self.desc += "\nnSentences: {}; mSentences: {}; dSentences: {}.".format(self.totalsentences, self.msentences_messages, self.dsentences_messages)
        # self.desc += "\nnumberOfCharsClean: {}; meanCharsClean: {}; deviationCharsClean: {}.".format(self.totalchars_clean, self.mchars_messages_clean, self.dchars_messages_clean)
        # self.desc += "\nnTokensClean: {}; mTokensClean: {}; dTokensClean: {};".format(self.totaltokens_clean, self.mtokens_messages_clean, self.dtokens_messages_clean)
        # self.desc += "\nnSentencesClean: {}; mSentencesClean: {}; dSentencesClean: {}.".format(self.totalsentences_clean, self.msentences_messages_clean, self.dsentences_messages_clean)
        # self.desc += "\nnumberOfUrls: {}"  # ;  fRemovedLines {};.".format(self.nurls, fremoved_lines)
        self.ntriples = len(P.context(self.translation_graph))
        triples = [
                (self.snapshoturi, po.triplifiedIn,      datetime.datetime.now()),
                # (self.snapshoturi, po.triplifiedBy,      "scripts/"),
                # (self.snapshoturi, po.donatedBy,         self.snapshotid),
                # (self.snapshoturi, po.availableAt,       self.online_prefix),
                # (self.snapshoturi, po.onlineMetaXMLFile, self.online_prefix+self.mrdf),
                # (self.snapshoturi, po.onlineMetaTTLFile, self.online_prefix+self.mttl),
                # (self.snapshoturi, po.metaXMLFileName,   self.mrdf),
                # (self.snapshoturi, po.metaTTLFileName,   self.mttl),
                # (self.snapshoturi, po.totalXMLFileSizeMB, sum(self.size_xml)),
                # (self.snapshoturi, po.totalTTLFileSizeMB, sum(self.size_ttl)),
                (self.snapshoturi, po.acquiredThrough,   "Gmane public mailing list archive RSS feed"),
                (self.snapshoturi, po.socialProtocol, "Gmane"),
                # (self.snapshoturi, po.socialProtocolTag, "Gmane"),
                # (self.snapshoturi, po.socialProtocol,    P.rdf.ic(po.Platform, "Gmane", self.meta_graph, self.snapshoturi)),
                # (self.snapshoturi, po.numberOfTriples,         self.ntriples),
                (self.snapshoturi, po.comment,         self.desc),
                (self.snapshoturi, po.gmaneID, self.directory),
                ]
        P.add(triples, context=self.meta_graph)

    def writeAllGmane(self):
        g = P.context(self.meta_graph)
        g.namespace_manager.bind("po", po)
        # ntriples = len(g)
        # triples = [
        #          (self.snapshoturi, po.nMetaTriples, ntriples),
        #          ]
        # P.add(triples, context=self.meta_graph)
        g.namespace_manager.bind("po", po)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl", "turtle")
        c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf", "xml")
        c("serialized meta")
        # copia o script que gera este codigo
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(G.PACKAGEDIR+"/../tests/triplify.py", self.final_path_+"scripts/triplify.py")
        # copia do base data
        tinteraction = """\n\n{} individuals with metadata {}
and {} interactions (replies: {}, references: {}, to: {}, cc: {})
constitute the interaction
structure in the RDF/XML file(s):
{}
and the Turtle file(s):
    {}
(anonymized: {}).""".format(self.nparticipants, str(self.participantvars),
                            self.nreplies+self.nreferences+self.ncc+self.nto, self.nreplies, self.nreferences, self.nto, self.ncc,
                            self.email_xml,
                            self.email_ttl,
                            self.interactions_anonymized)
        tposts = """\n\nThe dataset consists of {} messages with metadata {}
{:.3f} characters in average (std: {:.3f}) and total chars in snapshot: {}
{:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}""".format(
                        self.nmessages, str(self.messagevars),
                        self.mchars_messages, self.dchars_messages, self.totalchars,
                        self.mtokens_messages, self.dtokens_messages, self.totaltokens,
                        )
        # self.dates = [i.isoformat() for i in self.dates]
        date1 = 0 # min(self.dates)
        date2 = 0 # max(self.dates)
        with open(self.final_path_+"README", "w") as f:
            f.write("""::: Open Linked Social Data publication
\nThis repository is a RDF data expression of the gmane public email list with
snapshot {snapid} with emails from {date1} to {date2}
(total of {ntrip} triples).{tinteraction}{tposts}
\nMetadata for discovery in the RDF/XML file:
{mrdf} \nor in the Turtle file:\n{mttl}
\nEgo network: {ise}
Group network: {isg}
Friendship network: {isf}
Interaction network: {isi}
Has text/posts: {ist}
\nAll files should be available at the git repository:
{ava}
\n{desc}

The script that rendered this data publication is on the script/ directory.\n:::""".format(
                snapid=self.snapshotid, date1=date1, date2=date2, ntrip=self.ntriples,
                tinteraction=tinteraction,
                tposts=tposts,
                mrdf=self.mrdf,
                mttl=self.mttl,
                ise=self.isego,
                isg=self.isgroup,
                isf=self.isfriendship,
                isi=self.isinteraction,
                ist=self.hastext,
                ava=self.online_prefix,
                desc=self.desc
                ))
        # triples = [
        #         (self.snapshotid, po.published, True),
        #         ]

    def parseParticipant(self, fromstring):
        fromstring = decodeHeader(fromstring)
#            fromstring = "".join(i for i in str(fromstring) if i in string.printable)
        fromstring = fromstring.replace("\n", "").replace("\t", "")
        if ">" in fromstring and "<" not in fromstring:
            fromstring = re.sub(r"(.*[ ^]*)(.*>)",   r"\1<\2", fromstring)
            c("-|-|-|-| corrected fromstring:", fromstring)
        elif "<" in fromstring and ">" not in fromstring:
            fromstring = re.sub(r"(<.*)([ $]*.*)",   r"\1>\2", fromstring)
            c("-|-|-|-| corrected fromstring:", fromstring)
        if fromstring.count(">") == fromstring.count("<") > 0:
            name, email = re.findall(r"(.*?) {0,1}<(.*?)>", fromstring)[0]
            if not email:
                name, email = re.findall(r"(.*?) {0,1}<(.*?)>", fromstring)[1]
        elif "(" in fromstring:
            email, name = re.findall(r"(.*?) {0,1}\((.*)\)", fromstring)[0]
        elif " " in fromstring:
            raise ValueError("new author field pattern")
        else:
            email = fromstring
            name = ""
        email = email.replace("..", ".")
        try:
            assert validate_email(email)
        except:
            if "cardecovil.co.kr" in email:
                email = "foo@cardecovil.co.kr"
                name = ""
            elif 'akinobu.mita"@gmail.com' == email:
                email = 'akinobu.mita@gmail.com'
                name = ''
            elif re.findall(r"(.*):(.*)", email):
                name, email = re.findall(r"(.*):(.*)", email)[0]
            else:
                raise ValueError("bad email")
        assert validate_email(email)
        return email, name.strip().replace("'", "").replace('"', '')

    def makeId(self, gmaneid):
        if not gmaneid:
            return None
        gmaneid = decodeHeader(gmaneid)
        if not gmaneid or gmaneid.count(">") > 1:
            return None
        if gmaneid:
            gmaneid = re.findall(r"<(.*)>", gmaneid)
        if not gmaneid:
            return None
        assert len(gmaneid) == 1
        gmaneid_ = gmaneid[0].replace(" ", "")
        if not gmaneid_:
            return None
            # raise ValueError("Strange id!")
        return gmaneid_


def getText(message):
    while message.is_multipart():
        message = message.get_payload()[0]
    charsets = message.get_charsets()
    try:
        text = message.get_payload(decode=True)
    except AssertionError:
        text = ""
    if len(charsets) == 1 and text:
        charset = charsets[0]
        if charset:
            try:
                text = text.decode(charset)
            except LookupError:
                c("+++ lookup error in decoding messsage; charset:", charset)
                try:
                    text = text.decode()
                except UnicodeDecodeError:
                    try:
                        text = text.decode("latin1")
                        c("+++ used latin1 (no errors)", charset)
                    except UnicodeDecodeError:
                        text = text.decode(errors="ignore")
                        c("+-- unicode decode error in decoding messsage; used utf8 but charset:", charset)
            except UnicodeDecodeError:
                # c(text,charset)
                c("--- unicode error:", charset)
                try:
                    text = text.decode("latin1")
                    c("--- used latin1 (no errors)", charset)
                except UnicodeDecodeError:
                    try:
                        text = text.decode(charset, errors="ignore")
                        c("--+ removed errors in decoding message; charset:", charset)
                    except LookupError:
                        text = text.decode(errors="ignore")
                        c("-++ lookup error in decoding messsage; used utf8 but charset:", charset)
        else:
            # c("*** charset is empty string or None. Might need encoding.")
            try:
                text = text.decode()
            except UnicodeDecodeError:
                try:
                    text = text.decode("latin1")
                    c("**+ used latin1 (no errors)", charset)
                except UnicodeDecodeError:
                    text = text.decode(errors="ignore")
                    c("*++ decoded with utf8 and removed errors", charset)
    elif len(charsets) == 0 and text:
        text = text.decode()
    elif text:
        raise ValueError("more than one charset at the lowest payload leaf")
    elif not text:
        text = ""
    assert isinstance(text, str)
    content_type = message.get_content_type()
    if content_type == "text/html":
        text = ''.join(bs4.BeautifulSoup(text).findAll(text=True))
    elif content_type == "text/plain":
        pass
    # elif "text/plain" in content_type:
    elif "text" in content_type:
        c("WARNING: admitted text without fully understood content type")
    else:
        text = ""
        c(" == = Lowest not multipart payload. Should not be translated to rdf")
        c("content_type", content_type)
    return P.utils.cleanText(text)


def parseDate(datetimestring):
    date = datetimestring
    if date.split(" ")[-1].islower():
        date = date.replace(date.split(" ")[-1], date.split(" ")[-1].upper())
#    if date.split(" ")[-1].isupper() and date.split(" ")[-1].isalpha():
#        date=date.replace(date.split(" ")[-1],"")
    # date=date.replace("GMT","")
    # date=date.replace(" CST","")
    # date=date.replace(" CDT","")
    # date=date.replace("(KST)","")
    # date=date.replace("(METDST)","")
    date = date.replace("Thur", "Thu")
    date = date.replace("--", "-")
    if "+-" in date:
        date = date.split("+-")[0][:-1]
    if "-" in date and len(date.split("-")[-1]) == 3:
        date = date+"0"
    if "+" in date and len(date.split("+")[-1]) == 3:
        date = date+"0"
    if "\n" in date:
        date = date.split("\n")[0]
    if date.startswith("So, "):
        date = date[4:]
    try:
        date = dateutil.parser.parse(date)

    except ValueError:
        usual_pattern = re.findall(r"(.*?) {0,1}\(.*\)$", date)
        if usual_pattern:
            assert len(usual_pattern) == 1
            date = usual_pattern[0]
        elif re.findall(r"(.*) [A-Z]{3,4}$", date):
            date = re.findall(r"(.*) [A-Z]{3,4}$", date)[0]
        elif re.findall(r"\d{2} \d{4}$", date):
            date = date.replace(date[-4:], "+"+date[-4:])
        else:
            raise ValueError("New datetime or invalid string pattern")
        if re.findall(r" [+-]\d{3}$", date):
            date = date[:-3]+"0"+date[-3:]
        date = dateutil.parser.parse(date)
    if date.tzinfo is None:  # colocando localizador em que não tem, para poder comparar
        date = pytz.UTC.localize(date)
    try:
        date.utcoffset()  # test
        date.isoformat()  # test
    except:
        date = date.replace(tzinfo=None)
        date = pytz.UTC.localize(date)
    return date


def cleanEmailBody(text):
    # return text
    lines = text.splitlines()
    lines_with_content = [line for line in lines if line]
    jump_starts = [">", "<", "return ", "\./", "~/", "//", " |", "| ", "On Mon", "On Jan", "On Tue", "On Wed", "On Thu", "On Fri", "On Sat", "On Sun", "From:", "Subject", "To", "Reply-To:", "WARNING", "-----BEGIN", "Hash: "]
    jump_ends = ["wrote: "]
    jump_present = "style=", "]$", "=", "INFO", "----"
    jump_present_set = "if", "while", "for", ")", "(", "else"  # > = 3
    jump_present_combo = "FLAGS", "="
    relevant_lines = []
    for line in lines_with_content:
        line = line.strip()
        if sum([line.startswith(i) for i in jump_starts]):
            pass
        elif sum([line.endswith(i) for i in jump_ends]):
            pass
        elif line.startswith("--"):
            break
        elif line[:4].count("-") >= 3:
            break
        elif sum([i in line for i in jump_present]):
            pass
        elif sum([i in line for i in jump_present_set]) >= 3:
            pass
        elif sum([i in line for i in jump_present_combo]) == 2:
            pass
        elif len(line.split()) == 1 and line[-1] != ".":  # often a signature?
            pass
        elif line.istitle():
            pass
        else:
            relevant_lines += [line]
    clean_text = "\n".join(relevant_lines)
    return clean_text


def parseAddresses(string_):
    string_ = decodeHeader(string_)
    string_ = string_.replace("\n", "").replace("\t", "")
    unparsed = ""
    # if string_.count("<") == string_.count(">") == 1:
    #     addresses_all=[re.findall(r"(.*) {0,1}<(.*?)>",string_)[0][::-1]]
    # elif string_.count("<") == string_.count(">") == 0 and string_.count("@") == 1:
    #     address=[part for part in string_.split() if "@" in part][0]
    #     name=" ".join([part for part in string_.split() if "@" not in part])
    #     addresses_all=[(address,name)]
    # else:
    candidates = re.split(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''', string_)[1::2]  # ?? pra que isso?
    candidates = [i.strip() for i in candidates]
    addresses_all = []
    for candidate in candidates:
        if candidate.count("<") == candidate.count(">") > 0:
            # assume name <address> format
            name, address = re.findall(r"(.*) {0,1}<(.*?)>", candidate)[0]
        elif "@" in candidate:
            address = [part for part in candidate.split() if "@" in part][0]
            name = " ".join([part for part in candidate.split() if "@" not in part])
        else:
            unparsed += candidate
            address = ""
        if address:
            try:
                validate_email(address)
                addresses_all += [(address, name.strip().replace('"', '').replace("'", ""))]
            except:
                unparsed += candidate
    return addresses_all, unparsed


def decodeHeader(header):
    if isinstance(header, str):
        header = header.replace("\n", "")
    decoded_header = decode_header(header)
    final_string = ""
    # c(decode_header(header))
    for part in decoded_header:
        binary_string, codec = part
        if "binarystring" == b'"':
            pass
        if isinstance(binary_string, str):
            final_string += binary_string
            continue
        if not codec or "unknown" in codec:
            codec = "latin1"
        try:
            string_ = binary_string.decode(codec)
        except:
            try:
                string_ = binary_string.decode()
            except:
                try:
                    string_ = binary_string.decode(codec, errors="ignore")
                except:
                    try:
                        string_ = binary_string.decode(errors="ignore")
                    except:
                        string_ = "".join(i for i in str(header) if i in string.printable)
        final_string += string_
    if header != final_string:
        final_string = decodeHeader(final_string)
    return final_string
