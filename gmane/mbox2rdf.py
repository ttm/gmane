import mailbox, bs4, urllib
from validate_email import validate_email
import percolation as P, social as S, numpy as n, pickle, dateutil, nltk as k, os, datetime, shutil, rdflib as r, codecs, re, string, random, pytz
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
        nlost_messages=nparticipants=nreferences=totalchars=nurls=nreplies=nmessages=nempty=0
        dates=[]; nchars_all=[]; ntokens_all=[]; nsentences_all=[]
        participantvars=["email","name"]
        messagevars=["author","createdAt","replyTo","messageText","cleanMessageText","nChars","nTokens","nSentences","url","emptyMessage"]
        messagevars.sort()
        files=os.listdir(data_path+directory)
        nchars_all=[]; ntokens_all=[]; nsentences_all=[]; nchars_clean_all=[]; ntokens_clean_all=[]; nsentences_clean_all=[]
        locals_=locals().copy(); del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i,i))
        self.rdfLog()
        self.makeMetadata()
        self.writeAllGmane()
    def rdfLog(self):
        self.messages=[]
        for filecount,file_ in enumerate(self.files):
            if filecount%1000==0:
                c(self.snapshoturi,filecount)
            mbox = mailbox.mbox(self.data_path+self.directory+"/"+file_)
            if not mbox.keys():
                self.nempty+=1
                mbox.close()
                continue
            if not mbox[0]["Message-Id"]:
                raise ValueError("What to do with nonempy messages without id?")
            message=mbox[0]
            self.messages+=[message]
            gmaneid,messageid=self.makeId(message["Message-Id"])
            #c("gmaneid",gmaneid)
            if not gmaneid:
                raise ValueError("Message without id")
            messageuri=P.rdf.ic(po.EmailMessage,messageid,self.translation_graph,self.snapshoturi)
            triples=[
                     (messageuri,po.gmaneID,gmaneid),
                     ]
            email,userid,name=self.parseParticipant(message["From"])
            #c("email",email)
            if not email:
                raise ValueError("message without author")
            participanturi=P.rdf.ic(po.Participant,userid,self.translation_graph,self.snapshoturi)
            triples+=[
                     (messageuri,po.author,participanturi),
                     (participanturi,po.email,email),
                     ]
            if name:
                triples+=[
                         (participanturi,po.name,name),
                         ]
            subject=message["Subject"]
            if subject:
                if isinstance(subject,mailbox.email.header.Header):
                    subject="".join(i for i in str(subject) if i in string.printable)
                assert isinstance(subject,str)
                triples+=[
                         (messageuri,po.subject,subject),
                         ]
            replyid_=message["In-Reply-To"]
            saneids=self.makeId(replyid_)
            if bool(replyid_) and not bool(saneids):
                replyid=self.snapshoturi+"-"+str(self.nlost_messages)
                self.nlost_messages+=1
                replymessageuri=P.rdf.ic(po.LostEmailMessage,replyid,self.translation_graph,self.snapshoturi)
                triples+=[
                         (replymessageuri,a,po.EmailMessage),
                         (replymessageuri,NS.rdfs.comment,"This message registered as having a reply, but the field might be ill-formed: "+replyid_),
                         (messageuri,po.replyTo,replymessageuri),
                         ]
            elif replyid_:
                    gmaneid,replyid=saneids
                    replymessageuri=P.rdf.ic(po.EmailMessage,replyid,self.translation_graph,self.snapshoturi)
                    triples+=[
                             (replymessageuri,po.gmaneID,gmaneid),
                             (messageuri,po.replyTo,replymessageuri),
                             ]
            if isinstance(message["Date"],str):
                datetime=parseDate(message["Date"])
            elif isinstance(message["Date"],mailbox.email.header.Header):
                datetime_=re.findall(r"(.*\d\d:\d\d:\d\d).*",str(message["Date"]))[0]
                datetime=parseDate(datetime_)
            else:
                raise ValueError("datetime not understood")
            triples+=[
                     (messageuri,po.createdAt,datetime),
                     ]
            if message["References"]:
                if not re.findall(r"\A<(.*?)>\Z",message["References"].replace("\n","")):
                    c("::: ::: ::: references field not understood", message["References"])
                    triples+=[
                             (messageuri,po.comment,"the references are not understood (<.*> ids are added anyway): "+message["References"]),
                             (messageuri,po.referencesLost,True),
                             ]
                for reference in re.findall(r"<(.*?)>",message["References"]):
                    referenceid=self.snapshotid+"-"+urllib.parse.quote(reference.replace(" ",""))
                    referenceuri=P.rdf.ic(po.EmailMessage,referenceid,self.translation_graph,self.snapshoturi)
                    triples+=[
                             (referenceuri,po.gmaneID,reference),
                             (messageuri,po.hasReference,referenceuri),
                             ]
            # get to? get other references?
            text=getText(message)
            if text:
                nchars=len(text)
                ntokens=len(k.wordpunct_tokenize(text))
                nsentences=len(k.sent_tokenize(text))
                triples+=[
                         (messageuri,po.messageText,text),
                         (messageuri,po.nChars,nchars),
                         (messageuri,po.nTokens,ntokens),
                         (messageuri,po.nSentences,nsentences),
                         ]
                self.nchars_all+=[nchars]
                self.ntokens_all+=[ntokens]
                self.nsentences_all+=[nsentences]

                clean_text=cleanEmailBody(text)
                nchars_clean=len(clean_text)
                ntokens_clean=len(k.wordpunct_tokenize(clean_text))
                nsentences_clean=len(k.sent_tokenize(clean_text))
                triples+=[
                        (messageuri,po.messageTextClean,clean_text),
                        (messageuri,po.nCharsClean,nchars_clean),
                        (messageuri,po.nTokensClean,ntokens_clean),
                        (messageuri,po.nSentencesClean,nsentences_clean),
                        ]
                self.nchars_clean_all+=[nchars_clean]
                self.ntokens_clean_all+=[ntokens_clean]
                self.nsentences_clean_all+=[nsentences_clean]
            content_type=message.get_content_type()
            if content_type:
                triples+=[
                         (messageuri,po.contentType,content_type)
                         ]
            else:
                raise ValueError("/\/\/\/\/\ message without content type")
            P.add(triples,self.translation_graph)
            mbox.close()
    def makeMetadata(self):
        info="nEmpty: "+str(self.nempty)
        self.totalchars=sum(self.nchars_all)
        self.mchars_messages=n.mean(self.nchars_all)
        self.dchars_messages=n.std(self.nchars_all)
        self.totaltokens=sum(self.ntokens_all)
        self.mtokens_messages=n.mean(self.ntokens_all)
        self.dtokens_messages=n.std(self.ntokens_all)
        self.totalsentences=sum(self.nsentences_all)
        self.msentences_messages=n.mean(self.nsentences_all)
        self.dsentences_messages=n.std( self.nsentences_all)

        self.totalchars_clean=sum(self.nchars_clean_all)
        self.mchars_messages_clean=n.mean(self.nchars_clean_all)
        self.dchars_messages_clean=n.std(self.nchars_clean_all)
        self.totaltokens_clean=sum(self.ntokens_clean_all)
        self.mtokens_messages_clean=n.mean(self.ntokens_clean_all)
        self.dtokens_messages_clean=n.std(self.ntokens_clean_all)
        self.totalsentences_clean=sum(self.nsentences_clean_all)
        self.msentences_messages_clean=n.mean(self.nsentences_clean_all)
        self.dsentences_messages_clean=n.std( self.nsentences_clean_all)

        triples=[
                (self.snapshoturi, po.nParticipants,           self.nparticipants),
                (self.snapshoturi, po.nMessages,                 self.nmessages),
                (self.snapshoturi, po.nEmptyMessages,                 self.nempty),
                (self.snapshoturi, po.nReplies,              self.nreplies),
                (self.snapshoturi, po.nReferences,               self.nreferences),
                (self.snapshoturi, po.nCharsOverall, self.totalchars),
                (self.snapshoturi, po.mCharsOverall, self.mchars_messages),
                (self.snapshoturi, po.dCharsOverall, self.dchars_messages),
                (self.snapshoturi, po.nTokensOverall, self.totaltokens),
                (self.snapshoturi, po.mTokensOverall, self.mtokens_messages),
                (self.snapshoturi, po.dTokensOverall, self.dtokens_messages),
                (self.snapshoturi, po.nSentencesOverall, self.totalsentences),
                (self.snapshoturi, po.mSentencesOverall, self.msentences_messages),
                (self.snapshoturi, po.dSentencesOverall, self.dsentences_messages),

                (self.snapshoturi,  po.nCharsOverallClean,      self.totalchars_clean),
                (self.snapshoturi,  po.mCharsOverallClean,  self.mchars_messages_clean),
                (self.snapshoturi,  po.dCharsOverallClean,  self.dchars_messages_clean),
                (self.snapshoturi, po.nTokensOverallClean,     self.totaltokens_clean),
                (self.snapshoturi, po.mTokensOverallClean, self.mtokens_messages_clean),
                (self.snapshoturi, po.dTokensOverallClean, self.dtokens_messages_clean),
                (self.snapshoturi, po.nSentencesOverallClean,     self.totalsentences_clean),
                (self.snapshoturi, po.mSentencesOverallClean, self.msentences_messages_clean),
                (self.snapshoturi, po.dSentencesOverallClean, self.dsentences_messages_clean),
                ]
        P.add(triples,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.gmaneParticipantAttribute]*len(self.participantvars),
                self.participantvars,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.emailXMLFilename]*len(self.email_rdf)+[po.emailTTLFilename]*len(self.email_ttl),
                self.email_rdf+self.email_ttl,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.onlineEmailXMLFile]*len(self.email_rdf)+[po.onlineEmailTTLFile]*len(self.email_ttl),
                [self.online_prefix+i for i in self.tweet_rdf+self.email_ttl],context=self.meta_graph)

        self.mrdf=self.snapshotid+"Meta.rdf"
        self.mttl=self.snapshotid+"Meta.ttl"
        self.desc="twitter dataset with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid,self.snapshoturi,self.isego,self.isgroup,)
        self.desc+="\nisFriendship: {}; ".format(self.isfriendship)
        self.desc+="isInteraction: {}.".format(self.isinteraction)
        self.desc+="\nnParticipants: {}; nInteractions: {} (replies+references).".format(self.nparticipants,self.nreplies+self.nreferences)
        self.desc+="\nisPost: {} (alias hasText: {})".format(self.hastext,self.hastext)
        self.desc+="\nnMessages: {}; ".format(self.nmessages)
        self.desc+="nReplies: {}; nReferences: {}.".format(self.nreplies,self.nreferences)
        self.desc+="\nnTokens: {}; mTokens: {}; dTokens: {};".format(self.totaltokens,self.mtokensmessages,self.dtokensmessages)
        self.desc+="\nnChars: {}; mChars: {}; dChars: {}.".format(self.totalchars,self.mcharsmessages,self.dcharsmessages)
        self.desc+="\nnLinks: {}; fRemovedLines.".format(self.nlinks,fRemovedLines)
        triples=[
                (self.snapshoturi, po.triplifiedIn,      datetime.datetime.now()),
                (self.snapshoturi, po.triplifiedBy,      "scripts/"),
                (self.snapshoturi, po.donatedBy,         self.snapshotid),
                (self.snapshoturi, po.availableAt,       self.online_prefix),
                (self.snapshoturi, po.onlineMetaXMLFile, self.online_prefix+self.mrdf),
                (self.snapshoturi, po.onlineMetaTTLFile, self.online_prefix+self.mttl),
                (self.snapshoturi, po.metaXMLFileName,   self.mrdf),
                (self.snapshoturi, po.metaTTLFileName,   self.mttl),
                (self.snapshoturi, po.totalXMLFileSizeMB, sum(self.size_rdf)),
                (self.snapshoturi, po.totalTTLFileSizeMB, sum(self.size_ttl)),
                (self.snapshoturi, po.acquiredThrough,   "Gmane public mailing list archive RSS feed"),
                (self.snapshoturi, po.socialProtocolTag, "Gmane"),
                (self.snapshoturi, po.socialProtocol,    P.rdf.ic(po.Platform,"Gmane",self.meta_graph,self.snapshoturi)),
                (self.snapshoturi, po.nTriples,         self.ntriples),
                (self.snapshoturi, NS.rdfs.comment,         self.desc),
                (self.snapshoturi, po.gmaneID, self.gmaneid),
                ]
        P.add(triples,context=self.meta_graph)

    def writeAllGmane(self):
        g=P.context(self.meta_graph)
        ntriples=len(g)
        triples=[
                 (self.snapshoturi,po.nMetaTriples,ntriples)      ,
                 ]
        P.add(triples,context=self.meta_graph)
        g.namespace_manager.bind("po",po)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl","turtle"); c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf","xml")
        c("serialized meta")
        # copia o script que gera este codigo
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",self.final_path_+"scripts/triplify.py")
        # copia do base data
        tinteraction="""\n\n{} individuals with metadata {}
and {} interactions (retweets: {}, replies: {}, user_mentions: {}) 
constitute the interaction 
network in the RDF/XML file(s):
{}
and the Turtle file(s):
{}
(anonymized: {}).""".format( self.nparticipants,str(self.participantvars),
                    self.nretweets+self.nreplies+self.nuser_mentions,self.nretweets,self.nreplies,self.nuser_mentions,
                    self.tweet_rdf,
                    self.tweet_ttl,
                    self.interactions_anonymized)
        tposts="""\n\nThe dataset consists of {} tweets with metadata {}
{:.3f} characters in average (std: {:.3f}) and total chars in snapshot: {}
{:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}""".format(
                        self.ntweets,str(self.tweetvars),
                        self.mcharstweets,self.dcharstweets,self.totalchars,
                        self.mtokenstweets,self.dtokenstweets,self.totaltokens,
                        )
        self.dates=[i.isoformat() for i in self.dates]
        date1=min(self.dates)
        date2=max(self.dates)
        with open(self.final_path_+"README","w") as f:
            f.write("""::: Open Linked Social Data publication
\nThis repository is a RDF data expression of the twitter
snapshot {snapid} with tweets from {date1} to {date2}
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
                snapid=self.snapshotid,date1=date1,date2=date2,ntrip=self.ntriples,
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

        pass
    def parseParticipant(self,fromstring):
        if isinstance(fromstring,mailbox.email.header.Header):
            fromstring="".join(i for i in str(fromstring) if i in string.printable)
        fromstring=fromstring.replace("\n","").replace("\t","").replace('"',"")
        if ">" in fromstring and "<" not in fromstring:
            fromstring=re.sub(r"(.*[ ^]*)(.*>)",   r"\1<\2", fromstring)
            c("-|-|-|-| corrected fromstring:", fromstring)
        elif "<" in fromstring and ">" not in fromstring:
            fromstring=re.sub(r"(<.*)([ $]*.*)",   r"\1>\2", fromstring)
            c("-|-|-|-| corrected fromstring:", fromstring)
        if " " in fromstring and ">" in fromstring:
            name,email=re.findall(r"(.*) {0,1}<(.*)>",fromstring)[0]
        elif ">" in fromstring:
            email=re.findall(r"<(.*)>",fromstring)[0]
            name=None
        elif "(" in fromstring:
            email,name=re.findall(r"(.*) \((.*)\)",fromstring)[0]
        elif " " in fromstring:
            raise ValueError("new author field pattern")
        else:
            email=fromstring
            name=None
        assert validate_email(email.replace("..","."))
        userid=self.snapshotid+"-"+email
        return email,userid,name
    def makeId(self,gmaneid):
        if isinstance(gmaneid,mailbox.email.header.Header):
            gmaneid=str(gmaneid)
        if not gmaneid or gmaneid.count(">")>1:
            return None
        if gmaneid: gmaneid=re.findall(r"<(.*)>",gmaneid)
        if not gmaneid:
            return None
        assert len(gmaneid)==1
        gmaneid_=urllib.parse.quote(gmaneid[0].replace(" ",""))
        id_=self.snapshotid+"-"+gmaneid_
        if not id_:
            raise ValueError("Strange id!")
        return gmaneid[0],id_
def getText(message):
    while message.is_multipart():
        message=message.get_payload()[0]
    charsets=message.get_charsets()
    try:
        text=message.get_payload(decode=True)
    except AssertionError:
        text=""
    if len(charsets)==1 and text:
        charset=charsets[0]
        if charset:
            try:
                text=text.decode(charset)
            except LookupError:
                c("+++ lookup error in decoding messsage; charset:", charset)
                try:
                    text=text.decode()
                except UnicodeDecodeError:
                    try:
                        text=text.decode("latin1")
                        c("+++ used latin1 (no errors)", charset)
                    except UnicodeDecodeError:
                        text=text.decode(errors="ignore")
                        c("+-- unicode decode error in decoding messsage; used utf8 but charset:", charset)
            except UnicodeDecodeError:
                # c(text,charset)
                c("--- unicode error:",charset)
                try:
                    text=text.decode("latin1")
                    c("--- used latin1 (no errors)", charset)
                except UnicodeDecodeError:
                    try:
                        text=text.decode(charset,errors="ignore")
                        c("--+ removed errors in decoding message; charset:", charset)
                    except LookupError:
                        text=text.decode(errors="ignore")
                        c("-++ lookup error in decoding messsage; used utf8 but charset:", charset)
        else:
#            c("*** charset is empty string or None. Might need encoding.")
            try:
                text=text.decode()
            except UnicodeDecodeError:
                try:
                    text=text.decode("latin1")
                    c("**+ used latin1 (no errors)", charset)
                except UnicodeDecodeError:
                    text=text.decode(errors="ignore")
                    c("*++ decoded with utf8 and removed errors", charset)
    elif len(charsets)==0 and text:
        text=text.decode()
    elif text:
        raise ValueError("more than one charset at the lowest payload leaf")
    elif not text:
        text=""
    assert isinstance(text,str)
    content_type=message.get_content_type()
    if content_type=="text/html":
        text=''.join(bs4.BeautifulSoup(text).findAll(text=True))
    elif content_type=="text/plain":
        pass
    #elif "text/plain" in content_type:
    elif "text" in content_type:
        c("WARNING: admitted text without fully understood content type")
    else:
        text=""
        c("=== Lowest not multipart payload. Should not be translated to rdf")
        c("content_type",content_type)
    return text

def parseDate(datetimestring):
    date=datetimestring
    if date.split(" ")[-1].islower():
        date=date.replace(date.split(" ")[-1],date.split(" ")[-1].upper())
#    if date.split(" ")[-1].isupper() and date.split(" ")[-1].isalpha():
#        date=date.replace(date.split(" ")[-1],"")
    #date=date.replace("GMT","")
    #date=date.replace(" CST","")
    #date=date.replace(" CDT","")
    #date=date.replace("(KST)","")
    #date=date.replace("(METDST)","")
    date=date.replace("Thur","Thu")
    date=date.replace("--","-")
    if "+-" in date:
        date=date.split("+-")[0][:-1]
    if "-" in date and len(date.split("-")[-1])==3:
        date=date+"0"
    if "+" in date and len(date.split("+")[-1])==3:
        date=date+"0"
    if "\n" in date:
        date=date.split("\n")[0]
    if date.startswith("So, "):
        date=date[4:]
    try:
        date=dateutil.parser.parse(date)

    except ValueError:
        usual_pattern=re.findall(r"(.*?) {0,1}\(.*\)$",date)
        if usual_pattern:
            assert len(usual_pattern)==1
            date=usual_pattern[0]
        elif re.findall(r"(.*) [A-Z]{3,4}$",date):
            date=re.findall(r"(.*) [A-Z]{3,4}$",date)[0]
        elif re.findall(r"\d{2} \d{4}$",date):
            date=date.replace(date[-4:],"+"+date[-4:])
        else:
            raise ValueError("New datetime or invalid string pattern")
        if re.findall(r" [+-]\d{3}$",date):
            date=date[:-3]+"0"+date[-3:]
        date=dateutil.parser.parse(date)
    if date.tzinfo==None: # colocando localizador em que não tem, para poder comparar
        date=pytz.UTC.localize(date)
    try:
        foo=date.utcoffset() # test
        foo=date.isoformat() # test
    except:
        date=date.replace(tzinfo=None)
        date=pytz.UTC.localize(date)
    return date

def cleanEmailBody(text):
#    return text
    lines=text.splitlines()
    lines_with_content=[line for line in t if line]
    jump_starts=[">","<","return ", "\./","~/","//"," |","| ","On Mon","On Jan","On Tue","On Wed","On Thu","On Fri","On Sat","On Sun","From:","Subject","To","Reply-To:","WARNING","-----BEGIN","Hash: "]
    jump_ends=["wrote: "]
    jump_present="style=","]$","=","INFO","----"
    jump_present_set="if","while","for",")","(","else" # >=3
    jump_present_combo="FLAGS","="
    relevant_lines=[]
    for line in lines_with_content:
        line=line.strip()
        if sum([line.startswith(i) for i in jump_starts]):
            pass
        elif sum([line.endswith(i) for i in jump_ends]):
            pass
        elif sum([i in line for i in jump_present]):
            pass
        elif sum([i in line for i in jump_present_set])>=3:
            pass
        elif sum([i in line for i in jump_present_combo])==2:
            pass
        elif len(line.split()) == 1 and line[-1]!=".": # often a signature?
            pass
        elif line.istitle():
            pass
        elif line.startswith("--"):
            break
        elif line[:4].count("-")>=3:
            break
        else:
            relevant_lines+=[line]
    clean_text="\n".join(relevant_lines)
    return clean_text
