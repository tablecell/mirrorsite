# -*- encoding=utf8 -*-
import urllib2
import lxml.html,re
import os.path,stat,io,sys,glob,time
import threading,Queue
from bottle import route, run,template,static_file
from peewee import *
db = SqliteDatabase('post.db')

postlist=Queue.Queue(maxsize=200)


class User(Model):
    uid = IntegerField(primary_key=True)
    name = CharField()
    password = FixedCharField()


    class Meta:
        database = db 
User._meta.auto_increment =True

class Post(Model):
    post_id=IntegerField(primary_key=True)
    node = CharField()
    title = CharField()
    content = CharField()
    author =  ForeignKeyField(User, related_name='author')

    class Meta:
        database = db

class Remark(Model):
    remark_id=IntegerField(primary_key=True)
    post_id = IntegerField()
    content = CharField()
    user_id =  ForeignKeyField(User, related_name='poster')

    class Meta:
        database = db
db.connect()

User.create_table()
Post.create_table()
Remark.create_table()

def fetchHtml(url,options):
    headers={'User-Agent':options['user_agent'],"Host":'www.'+options['domain'],'Connection':"keep-alive",'Refer':options['url'],}
    page=''
    retry=0
    req=urllib2.Request(url)
    for header in headers:
        req.add_header(header,headers[header])
    while not page and retry <3:
        try:
            page=urllib2.urlopen(url).read()
        except:
            retry=retry+1
            print retry
            time.sleep(10)
    return page.decode(options['charset'])

class ScrapIndex(threading.Thread):

    def __init__(self,config):
        threading.Thread.__init__(self)
        self.config=config
    def run(self):
        print("\n run....")
        config=self.config
        url=config['url']
        while True:
            page=''
            try:
                page=fetchHtml(url,config)
            except:
                print("error",url)
            if not page:
                continue
            doc = lxml.html.document_fromstring(page)
            for elem in doc.cssselect(config['links_css']):
                id=re.search(config['href_patten'],elem.get("href")).group(1)
                #print(elem)
                filename=config['save_dir']+'//'+id
                #print(filename)
                if not os.path.exists(filename):
                     print(filename)
                     self.touch(filename)
                     postlist.put(id)

            time.sleep(config['refresh_fruiqence'])

    def touch(self,fname, times=None):
        with open(fname, 'a'):
            os.utime(fname, times)



class Refresh(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config=config
    def run(self):

        dir=self.config['save_dir']
        while True:
            now=time.time()
            for path_and_filename in glob.iglob(dir+"/*"):
                ctime=os.stat(path_and_filename)[stat.ST_CTIME]
                elapse=now-ctime
                if elapse > 86400:
                    print("\t"*3,ctime,path_and_filename,elapse)
                    os.remove(path_and_filename)
                elif elapse > 3600:
                    print(postlist.qsize())
                    postlist.put(os.path.basename(path_and_filename))

            time.sleep(20)



class ScrapPage(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config=config

    def run(self):

        config=self.config
        while True:

            print(postlist.qsize())
            id=postlist.get()
            url=config['detail_url'] % id
            print(url)
            filename=config['save_dir']+"//"+ id
            page=''
            try:
                page=fetchHtml(url,config)
            except:
                print("ERROR:",url)
            if not page:
                continue
            doc = lxml.html.document_fromstring(page)
            size=0
            idset=[]
            if os.path.exists(filename):
                size=os.path.getsize(filename)
                print('size=',size)

            if 0 == size:
                header=doc.cssselect("#Main .box .header")
                node=header[0].findall("./a")[1].get("href").replace('/go/','')
                title=header[0].find("./h1").text_content()
                user=header[0].find("./small/a").text_content()
                content=doc.find_class("topic_content")
                if content:
                    content=content[0].text_content()
                try:
                    user = User.get(User.name ==user)
                    user_id=user.uid
                except:
                    created = User.create(name=user,password='xx')
                    user_id=created.uid
                post,created=Post.create_or_get(post_id=int(id), node=node, title=title,author=user_id,content=content)
                #print(post,created)
                with open(filename,'r+') as f:
                    f.write(chr(32))

            idlist=''
            if 0 < size:
                with open(filename,'r+') as f:
                    idlist=f.read().strip()

            uniq=set()
            if 0 != len(idlist)  :
                uniq=set(idlist.split(','))
                print("\n-----------------------------")

            uniqnew=set([])
            for elem in doc.cssselect('#Main div.box:nth-child(4) div[id^="r_"]'):
                user=elem.find(".//strong/a").text_content()
                try:
                    user = User.get(User.name ==user)
                    user_id=user.uid
                except:
                    created = User.create(name=user,password='xx')
                    user_id=created.uid
                #print(user_id)
                rid=elem.get('id').replace('r_','')
                td=elem.find_class("reply_content")
                content=td[0].text_content()
                #print(td[0].text_content())
                if rid not in uniq:
                    uniq.add(rid)
                    Remark.create(content=content,user_id=user_id,post_id=id)

            time.sleep(10)

#t=ScrapIndex(config)

config={'url':'http://v2ex.com/?tab=all',
'domain':'v2ex.com',
'charset':'utf-8',
'user_agent':'Mozilla/5.0 (Windows NT 6.3; rv:38.0) Gecko/20100101 Firefox/38.0',
'links_css':"div.box:nth-child(2) table td:nth-child(3) .item_title a",
'href_patten':r"/t/(\d+)#",
'save_dir':'tmp',
"detail_url":"http://v2ex.com/t/%s",
'refresh_fruiqence':30}

savedir=config['save_dir']
if not os.path.exists(savedir):
    os.mkdir(savedir)
    db.create_tables([User,Post,Remark])

threads=[ScrapIndex(config),Refresh(config),ScrapPage(config)]
for t in threads:
    t.start()


@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='.')
@route('/')
def index():

    posts=Post.select().paginate(0,30)

    return template('index', page=1,posts=posts)

@route('/recent/:page')
def recent(page):
    page = int(page)
    posts=Post.select().paginate((page-1)*30,30)
    page=page+1
    return template('index',page=page, posts=posts)


@route('/t/:id')
def remark(id):
    id = int(id)
    post=Post.get(Post.post_id==id)

    remarks=Remark.select().where(Remark.post_id==id)
    return template('post', post=post,remarks=remarks)
run(host='localhost', port=8080, debug=True)

sys.exit()
