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
        nlost_messages=nurls=nreplies=nmessages=nempty=0
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
        self.messages=[]
        for filecount,file_ in enumerate(self.files):
            if filecount%1000==0:
                c(self.snapshoturi,filecount)
            mbox = mailbox.mbox(self.data_path+self.directory+"/"+file_)
            if not mbox.keys() or not mbox[0]["Message-Id"]:
                self.nempty+=1
                mbox.close()
                continue
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
                triples+=[
                         (messageuri,po.subject,subject),
                         ]
            replyid_=message["In-Reply-To"]
            saneids=self.makeId(replyid_)
            if replyid_ or (bool(replyid_) and not bool(saneids)):
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
                triples+=[
                        (messageuri,po.messageText,text)
                        ]
            content_type=message.get_content_type()
            if content_type:
                triples+=[
                        (messageuri,po.contentType,content_type)
                        ]
            if not content_type:
                raise ValueError("/\/\/\/\/\ message without content type")
            mbox.close()
    def makeMetadata(self):
        info="nEmpty: "+str(self.nempty)
        pass
    def writeAllGmane(self):
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
                c("+++ lookup error in decoding messsage:", charset)
                try:
                    text=text.decode()
                except UnicodeDecodeError:
                    c("+-- unicode decode error in decoding messsage:", charset)
                    text=text.decode(errors="ignore")
            except UnicodeDecodeError:
                # c(text,charset)
                c("--- unicode error:",charset)
                try:
                    text=text.decode(charset,errors="ignore")
                except LookupError:
                    c("-++ lookup error in decoding messsage:", charset)
                    text=text.decode(errors="ignore")
    elif len(charsets)==0 and text:
        text=text.decode()
    elif text:
        raise ValueError("more than one charset at the lowest payload leaf")
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

