import mailbox, bs4
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
        self.messages=[]
        reid=re.compile(r"<(.*)>")
        for filecount,file_ in enumerate(self.files):
            if filecount%1000==0:
                c(self.snapshoturi,filecount)
            mbox = mailbox.mbox(self.data_path+self.directory+"/"+file_)
            if not mbox.keys():
                self.nempty+=1
                mbox.close()
                continue
            message=mbox[0]
            self.messages+=[message]
            from_=message["From"]
            messageid_=reid.findall(message["Message-Id"])
            if len(messageid_)!=1:
                raise ValueError("Strange id!")
            messageid=self.snapshotid+"-"+messageid_[0]
            messageuri=P.rdf.ic(po.EmailMessage,messageid,self.translation_graph,self.snapshoturi)
            datetime=parseDate(message["Date"])
            replyto=message["In-Reply-To"]
            subject=message["Subject"]
            triples=[
                    (messageuri,po.gmaneID,messageid_),
                    (messageuri,po.createdAt,datetime),
                    (messageuri,po.replyTo,replyto),
                    (messageuri,po.subject,subject),
                    ]
            references=message["References"]
            # get to? get other references?
            while message.is_multipart():
                message=message.get_payload()[0]
            charsets=message.get_charsets()
            text=message.get_payload(decode=True)
            if len(charsets)==1:
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
#                        c(text,charset)
                        c("--- unicode error:",charset)
                        try:
                            text=text.decode(charset,errors="ignore")
                        except LookupError:
                            c("-++ lookup error in decoding messsage:", charset)
                            text=text.decode(errors="ignore")
            elif len(charsets)==0:
                text=text.decode()
            else:
                raise ValueError("more than one charset at the lowest payload leaf")
            content_type=message.get_content_type()
            if content_type=="text/html":
                text=''.join(bs4.BeautifulSoup(text).findAll(text=True))
            if content_type=="text/plain":
                pass
            #elif "text/plain" in content_type:
            elif "text" in content_type:
                c("WARNING: admitted text without fully understood content type")
            else:
                text=None
                c("=== Lowest not multipart payload. Should not be translated to rdf")
                c("content_type",content_type)
                triples+=[
                        (messageuri,po.contentType,content_type)
                        ]
                #raise ValueError("Lowest not multipart payload should have plain text")
            #text_=message.decode(charset,error="ignore")
            if text:
                triples+=[
                        (messageuri,po.messageText,text)
                        ]
            mbox.close()
    def makeMetadata(self):
        pass
    def writeAllGmane(self):
        pass
redate=re.compile(r"(.*) \(.*\)")
def parseDate(datetimestring):
    date=datetimestring
    if date.split(" ")[-1].islower():
        date=date.replace(date.split(" ")[-1],date.split(" ")[-1].upper())
    if date.split(" ")[-1].isupper() and date.split(" ")[-1].isalpha():
        date=date.replace(date.split(" ")[-1],"")
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
        date=redate.findall(date)
        assert len(date)==1
        date=dateutil.parser.parse(date[0])
    if date.tzinfo==None: # colocando localizador em que não tem, para poder comparar
        date=pytz.UTC.localize(date)
    try:
        foo=date.utcoffset() # test
        foo=date.isoformat() # test
    except:
        date=date.replace(tzinfo=None)
        date=pytz.UTC.localize(date)
    return date

