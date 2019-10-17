import xml.sax
import sys
import logging
import time
import ast
import glob
import heapq
import os
import re
import importlib
from operator import itemgetter
from nltk.tokenize import RegexpTokenizer, WhitespaceTokenizer
from nltk.corpus import stopwords
from nltk import PorterStemmer
from string import digits
#

ps = PorterStemmer()
INDEXFILE=None
tokenizer = RegexpTokenizer(r'\w+')
path_to_index = "."

stem_dict = {}

language = "english"
stop_words = set(stopwords.words(language))

def finding_tokens(string):
        global stem_dict
        string = string.lower()
        #print("str",string)
        my_new_string = re.sub('[^a-zA-Z0-9 \n\.]', ' ', string)
        #print("my new",my_new_string)
        re_dig = "".join(filter(lambda x: not x.isdigit(), my_new_string))
        #print("re",re_dig)
        tokens = tokenizer.tokenize(re_dig)
        #print(tokens)
        
        filtered_words = []
        for word in tokens:
            #print word
            if word not in stop_words:
                #print word
                
                token = str(word)
                if token != "":
                  #w_data = ps.stem(token)
                  if token in stem_dict:
                    w_data = stem_dict[token]
                  else:
                    w_data = ps.stem(token)
                    stem_dict[token] = w_data
                    #print data
                filtered_words.append(w_data)
                #print str(res)
                #token = sno.stem(token)
                #print token


        return filtered_words
def extract_key(line):
        
        return line.split(":",1)[0]


def heap_sort(heap_size,path_to_index,twi):
        hs = time.time()

        filenames=glob.glob(path_to_index+"/*")
    #   
        k=heap_size
        
        imo=0
        #mer=0
        count =0
        while(len(filenames)>1):
            imo=0
            while(imo<len(filenames)):
                    #mer=mer+1
                    
                    files=[]
                    for j in range(imo,imo+k):
                        if j < len(filenames):
                            count+=1
                            #print(count)
                            files.append(open(filenames[j]))

                    with open("intermediate_temp_folders"+"/index_"+str(twi), 'w') as destination:
                            decorated = [
                                ((extract_key(line), line) for line in f)
                                for f in files]
                            mergedata = heapq.merge(*decorated)
                            undecorated = map(itemgetter(-1), mergedata)
                            destination.writelines(undecorated)



                    #f.close()
                    for j in range(imo,imo+k):
                        if j < len(filenames):
                            os.remove(filenames[j])
                    imo=imo+k
                    #print "i",i
            filenames=glob.glob(path_to_index+"/*")
        he = time.time()
        print("heap",  he-hs)

def heap_sort1(heap_size,path_to_index):
        hs = time.time()

        filenames=glob.glob(path_to_index+"/*")
    #   
        k=heap_size
        
        imo=0
        mer=0
        count =0
        while(len(filenames)>1):
            imo=0
            while(imo<len(filenames)):
                    mer=mer+1
                    
                    files=[]
                    for j in range(imo,imo+k):
                        if j < len(filenames):
                            count+=1
                            #print(count)
                            files.append(open(filenames[j]))

                    with open("batch1/"+"new_index_"+str(mer), 'w') as destination:
                            decorated = [
                                ((extract_key(line), line) for line in f)
                                for f in files]
                            mergedata = heapq.merge(*decorated)
                            undecorated = map(itemgetter(-1), mergedata)
                            destination.writelines(undecorated)



                    #f.close()
                    for j in range(imo,imo+k):
                        if j < len(filenames):
                            os.remove(filenames[j])
                    imo=imo+k
                    #print "i",i
            filenames=glob.glob("batch1"+"/index_*")
        he = time.time()
        print("heap",  he-hs)

def merge_after_sort(path_to_index,pathtoind,twi):
        filenames=glob.glob(path_to_index+"/*")
        #print(len(filenames),filenames)
        offset_file = open("offset.txt","w")

        indexfile=open(pathtoind+"/index_file.txt","w")
        final_indexfile=indexfile
        f=None
        if len(filenames)==1:
            
            f=open(filenames[0])

        line=f.readline()
        if line :
            line=line.rstrip()
            previous_key=line.split(":",1)[0]
            offset_file.write(str(final_indexfile.tell())+"\n")
            final_indexfile.write(line)
            
        while True:
            line=f.readline()
            if not line:
                break
            line=line.rstrip()
            line_tokens=line.split(":",1)
            new_key=line_tokens[0]
            value=line_tokens[1]
            if(previous_key==new_key):
                final_indexfile.write("|"+value)
                #offset_file.write(str(final_indexfile.tell()))
                #offset_file.write(str(line.tell())+"\n")
            else :
                final_indexfile.write("\n")
                offset_file.write(str(final_indexfile.tell())+"\n")
                final_indexfile.write(line)
                #offset_file.write(str(line.tell())+"\n")

            previous_key=new_key


def merge_after_sort1(path_to_index):
        filenames=glob.glob("batch1"+"/new_index_*")
        #print(len(filenames),filenames)
        indexfile=open(path_to_index+'/index_file.txt',"w")
        final_indexfile=indexfile
        f=None
        if len(filenames)==1:
            
            f=open(filenames[0])

        line=f.readline()
        if line :
            line=line.rstrip()
            previous_key=line.split(":",1)[0]
            final_indexfile.write(line)
        while True:
            line=f.readline()
            if not line:
                break
            line=line.rstrip()
            line_tokens=line.split(":",1)
            new_key=line_tokens[0]
            value=line_tokens[1]
            if(previous_key==new_key):
                final_indexfile.write("|"+value)
            else :
                final_indexfile.write("\n"+line)
            previous_key=new_key




class Page():
  def __init__(self):
        self.title=""
        self.text=""
        self.comment=""
        self.ref=""
        self.id=""
        self.wordlist=[]
        self.text_lines=[]

class WikiXmlHandler(xml.sax.ContentHandler):
  def __init__(self):
      self.Page=""
      self.Pages=[]
      self.page_id=[]
      self.page_id_txt=""
      self.currentdata=""
      self.temp_txt=[]
      self.temp_text2=[]
      self.all_lines=[]
      self.pages_file="documents.txt"
      self.line_tokens=[]
      self.f=None
      self.word_txt=[]
      self.word_list=[]
      self.page_title_text=""
      self.page_title=[]
      self.page_count =0
      #self.f1 = None



  def startElement(self, tag, attrs):
    self.currentdata=tag
    if(tag=="page"):
       # print "-----------New Page--------------"
        self.Page=Page()
#    if(tag=="ref"):
#        print "new ref"

  def endElement(self, tag):
      
      self.page_count+=1

      if(tag=="page"):
          if(len(self.page_id)>0):
              self.Page.id=self.page_id[0]
          page_title="".join(self.page_title)
          page_title=page_title.rstrip()
          page_title.replace("\"", "")
          mylist=[]
          mylist.append(page_title)
          #print
          #self.f1.write('{"id":'+str(self.page_count)+',"title":'+str(mylist)+'}\n')
          #self.f.write("{'id':"+self.Page.id+",'text':"+str(self.Page.text_lines)+",'title':'"+str(page_title)+"'}"+"\n")
          self.f.write('{"id":'+self.Page.id+',"text":'+str(self.Page.text_lines)+',"title":'+str(mylist)+'}'+'\n')
          
          self.page_id=[]
          self.page_title=[]
          


      if(tag=="id" ):
          self.page_id.append(self.page_id_txt)
          self.page_id_txt=""

      #if(tag=="title" ):


  def characters(self, content):
      if self.currentdata=="title":
          self.page_title.append(content)
          #self.Page.title=self.Page.title+content
          #self.Page.title=self.Page.title+content

      if self.currentdata=="text":

          self.temp_text2.append(content.lower())


          if(content=='\n'):
              line="".join(self.temp_text2)
              #print line
              self.Page.text_lines.append(line)
              self.temp_text2=[]


      if self.currentdata=="id" :
          self.page_id_txt=content


class Document():
    def __init__(self):
        self.doc_id={}
        self.title={}
        self.body={}
        self.infobox={}
        self.categories={}
        self.external_links={}
        self.references={}
    

class WordProcess():

    def __init__(self):
        self.title=""
        self.text=""
        self.comment=""
        self.ref=""
        
        self.wikihandler=None
       
        self.DUMP=None
        
        self.total_doc_length=0

    

    def makeindexfordoc(self,doc,pageid,path_to_index,page_count):
        indexfile=open(path_to_index+"/"+str(pageid),"w")
        index={}
        for key in doc.title:
            try:
                index[key]=index[key]+"t"+str(doc.title[key])
            except :
                index[key]="t"+str(doc.title[key])

        for key in doc.body:
            try:
                index[key]=index[key]+"b"+str(doc.body[key])
            except :
                index[key]="b"+str(doc.body[key])

        for key in doc.infobox:
            try:
                index[key]=index[key]+"i"+str(doc.infobox[key])
            except :
                index[key]="i"+str(doc.infobox[key])

        for key in doc.categories:
            try:
                index[key]=index[key]+"c"+str(doc.categories[key])
            except :
                index[key]="c"+str(doc.categories[key])

        for key in doc.external_links:
            try :
                index[key]=index[key]+"e"+str(doc.external_links[key])
            except :
                index[key]="e"+str(doc.external_links[key])
        for key in doc.references:
            try :
                index[key]=index[key]+"r"+str(doc.references[key])
            except :
                index[key]="r"+str(doc.references[key])


        s=sorted(index.keys())
        for k in s:
           v=index[k]
           indexfile.write(k+":"+str(page_count)+"-"+v+"\n")

        indexfile.close()
        

    def add_to_dict(self,tokens,dict_name):
        for token in tokens:
            try :
                dict_name[token]=dict_name[token]+1
            except :
                dict_name[token]=1

    

    
    
    

    def selectingwords(self,lines,doc,pageid):
      i=-1
      lines_len=len(lines)
      no_of_body_tokens = 0
#     
      if(lines_len==1):
          #print 'yes'
          return 0

      while(i<lines_len-1):
            i=i+1
            
            line=lines[i]
            line = line.strip()
          
            if line.startswith("{{infobox") :
                #print 'infobox',line,pageid
                while True :
                    #print 'yes'
                    if ((i+1)>=lines_len or lines[i+1].startswith("}}")):
                        break
                    i=i+1
                    line=lines[i]
                    
                    self.add_to_dict(finding_tokens(line),doc.infobox)


            elif line.startswith("[[category:"):
                #print 'category',pageid
                line=line[11:]
                self.add_to_dict(finding_tokens(line),doc.categories)

            elif line.startswith("=") :
                title_text=line.replace("=","")
                title_text=title_text.strip()

                if title_text=="references":
                    while True :
                        if (i+1)>=lines_len or lines[i+1].startswith("=") or lines[i+1].startswith("[[category:") :
                            #print 'yes'
                            break
                        i=i+1
                        line=lines[i]

                        if (line.startswith("<ref") or line.startswith("")):
                            #print self.tokenise(line)
                            self.add_to_dict(finding_tokens(line),doc.references)
                    continue


                elif title_text=="see also":
                    while True :
                        if (i+1)>=lines_len or lines[i+1].startswith("=") or lines[i+1].startswith("[[category:"):
                            break
                        i=i+1
                    continue

                elif title_text=="further reading":
                    while True :
                        if (i+1)>=lines_len or lines[i+1].startswith("=") or lines[i+1].startswith("[[category:"):
                            break
                        i=i+1
                    continue

                elif (title_text=="external links"):
                    while True:
                        if( i+1>=lines_len or lines[i+1].startswith("[[category:")):
                              break
                        i=i+1
                        line=lines[i]
                        if(line.startswith('*')):
                            self.add_to_dict(finding_tokens(line),doc.external_links)
                    continue

                else :
                    self.add_to_dict(finding_tokens(title_text),doc.title)
            else :
                tokens_in_line=finding_tokens(line)
                no_of_body_tokens=no_of_body_tokens+len(tokens_in_line)
                self.add_to_dict(tokens_in_line,doc.body)

      self.total_doc_length = self.total_doc_length + no_of_body_tokens
      return no_of_body_tokens






    


def main(sourceFileName,path_to_index):
  #files = glob.glob("temp_folder/*")
  #for f in files:
  #  os.remove(f)
  
  source = open(sourceFileName)
  wikihandler=WikiXmlHandler()
  wikihandler.f=open(wikihandler.pages_file,"w+")
  
  xml.sax.parse(source,wikihandler )              #XmlParsing

  
  wikihandler.f.close()
  wp=WordProcess()
  begin=time.time()
  no_of_files = open("no_of_files.txt","w")
  doc_page = open("doc_title_page.txt","w")
  title_offset_file = open("title_offset.txt","w")
  #title_offset = open("./index_folder/"+"title_offset.txt","w")
  title_offset_file.write(str(doc_page.tell())+"\n")
  doc_page.write(str("not found")+'\n')
  
  m=open(wikihandler.pages_file,"r")
  twi =0
  count =0
  file_count =0

  for line in m:
      count +=1

      mylist=ast.literal_eval(line)
      doc=Document()
      page_id=mylist["id"]
    
      #print page_id
      ll=wp.selectingwords(mylist["text"],doc,page_id)
      #if ll > 0:
      wp.makeindexfordoc(doc,page_id,path_to_index,count)
          
      #doc_page.write('{"id":'+str(count)+',"title":'+str(mylist["title"])+'}\n')
      title_offset_file.write(str(doc_page.tell())+"\n")
      doc_page.write(str(mylist["title"][0])+'\n')
      #title_offset_file.write(str(doc_page.tell())+"\n")

          #print("count",count)
      file_count+=1
      #print("file_count ",file_count)
      #print(twi)
      if(file_count==1000):
            #print(count)
          twi+=1
          #print(twi)
          heap_sort(1000,path_to_index,twi)
            #merge_after_sort(path_to_index,twi)
          
          file_count=0      
      #ll=0
  no_of_files.write(str(count))    
  twi+=1

  #print(count)

  print("files indexing time :", time.time()-begin)
  start = time.time()
  heap_sort(1000,path_to_index,twi)
  heap_sort1(1000,'./intermediate_temp_folders')
  merge_after_sort('./batch1',path_to_index,twi)
  #heap_sort1(75,path_to_index)
  #merge_after_sort1(path_to_index)
  print("merging time : ",  str(time.time()-start))

if __name__ == "__main__":
    importlib.reload(sys)
   
    logging.basicConfig()
    
    start_time=time.time()
    path_to_dump =sys.argv[1]
    path_to_index = sys.argv[2]
   
    #main("enwiki-latest-pages-articles26.xml-p42567204p42663461")
    main(path_to_dump,path_to_index)
    end_time=time.time()
    parse_time=end_time-start_time
    print ("parsing time : ",parse_time)
