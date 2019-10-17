import math
import os
import sys
from collections import defaultdict
from nltk.corpus import stopwords
#from nltk import PorterStemmer
import ast
import time

import re

#ps = PorterStemmer()

offset = []
title_offset = []

title_arr = []
titlefile=open("doc_title_page.txt","r")
line = titlefile.readline()
title_arr.append(line)
while line:
    line = titlefile.readline()
    title_arr.append(line)
titlefile.close()



path_to_index = sys.argv[1]

#f = open('no_of_files.txt', 'r')
nfiles = 19567270
#f.close()

def read_file(testfile):
        with open(testfile, 'r') as file:
            queries = file.readlines()
    
        return queries

def write_file(outputs, path_to_output):
        #print(outputs)
        with open(path_to_output, 'w') as file:
            for output in outputs:
            #print("oo",output)-
                count=0
                for word in output:
                    name= ''.join(c for c in word if c not in '[]')
                    file.write(name)
                file.write('\n')
            file.write('\n'*2)

def rank(result, freq,no_ofdoc_words_occur):
    
    #queryIDF = 0
    score = 0
    val =0 
    #queryIDF = math.log((float(nfiles)-float(no_ofdoc_words_occur)+0.5)/ ( float(no_ofdoc_words_occur) + 0.5))
    
    for i in range(6):
        if i==0:
            val = val*10 + freq["t"]
            score = score+(math.log(1+val)*math.log(nfiles/no_ofdoc_words_occur+1))*.25
        if i==1:
            val = val*10 + freq["b"]
            score = score+(math.log(1+val)*math.log(nfiles/no_ofdoc_words_occur+1))*.25
        if i==2:
            val = val*10 + freq["i"]
            score = score+(math.log(1+val)*math.log(nfiles/no_ofdoc_words_occur+1))*.20
        if i==3:
            val = val*10 + freq["c"]
            score = score+(math.log(1+val)*math.log(nfiles/no_ofdoc_words_occur+1))*.1
        if i==4:
            val = val*10 + freq["e"]
            score = score+(math.log(1+val)*math.log(nfiles/no_ofdoc_words_occur+1))*.05
        if i==5:
            val = val*10 + freq["r"]
            score = score+(math.log(1+val)*math.log(nfiles/no_ofdoc_words_occur+1))*.05
    #print(score)
    return score



def check(pointer):
    name_of_file = os.path.join(path_to_index, 'index_file.txt')
    f = open(name_of_file,'r')
    f.seek(pointer)
    #print("pointer",f.seek(pointer))
    line = f.readline()

    #print("line check",line)
    f.close()
    splitline = line.split(':')

    #print("sp",splitline[0])
    return(splitline[0])

def get_index_line(pointer):
    name_of_file = os.path.join(path_to_index, 'index_file.txt')
    f = open(name_of_file,'r')
    f.seek(pointer)
    line = f.readline()
    f.close()
    #print("hh",line)
    return line

def binary_search(token):
    start = 0
    #print(token)
    end = len(offset)-1
    #print(end)
    while(start<=end):
        mid = int(start+(end-start)/2)
        #print("mid",mid)
        offset[mid]=int(offset[mid])
        #print(offset[mid])
        if(check(offset[mid]) >token):
            #print("1",check(offset[mid]))
            end=mid-1
        if(check(offset[mid])<token):
            start=mid+1
            #print("2",check(offset[mid]))
        if(check(offset[mid])==token):
            #print("3",check(offset[mid]))
            line = get_index_line(offset[mid])
            line = line.rstrip()
            #print(line)
            #line_token = line.split(":")
            #i_word = line_token[0]
            #print(line_token[1])
            return line









class d:
    def __init__(self):
        pass
        


    


    
    '''
    def write_file(self,outputs, path_to_output):
            file = open(path_to_output,"w")
            for output in outputs:
                #print("oo",output)
                for line in output:
                    #print("ll",line)
                    file.write(*line)
                    #file.write(line.strip() + '\n')
                file.write('\n')
    '''

    def search(self,queries):
        final_list = []
        doc_id = ""

        titlefile=open("doc_title_page.txt","r")

        for query in queries:
                flag =0
                count =0
                rank_score =0
                 #print("qq",query)
                #print("each q")
                lis_title = []
                query_terms = query.split()
                idd = ""
                rank_list = {}
                rank_listc = {}
                rank_listr = {}
                rank_listi = {}
                rank_listt = {}
                rank_listb = {}
                rank_liste = {}

                for w in query_terms:
                        doclist = []
                        if re.search(r'[title|body|category|external links|infobox]:', w):
                            flag =2
                            _fields = w.split(':')[0] 
                            _words = w.split(':')[1] 
                            #print(_fields,_words)

                            if not _words in stopwords.words('english'):
                            #words = ps.stem(w)
                                #print("yy")
                                _words = _words.lower()

                                entryin_index = binary_search(_words)
                                lock1 = []
                                lock2 = []
                                #freq_termsindoc = entryin_index.split("|")
                                freq_termsindoc = entryin_index.split("|")
                                #print(freq_termsindoc)
                                lock1 = freq_termsindoc[0].split(":")
                                #print("lock1",lock1)
                                (doc_id,frequencies)=self.finding_doc_freq(lock1[1],lock1[2])
                                no_ofdoc_words_occur = len(freq_termsindoc)
                                #print(no_ofdoc_words_occur)
                                for document in range(1,no_ofdoc_words_occur):
                                    lock2 = freq_termsindoc[document].split(":")
                                    #print("lock2",lock2)
                                    (doc_id,frequencies)=self.finding_doc_freq(lock2[0],lock2[1])
                                    #print(doc_id)
                                #print("entryin_index:::::",entryin_index)
                                
                                #if (entryin_index):
                                #    freq_termsindoc = entryin_index.split("|")
                                #no_ofdoc_words_occur = len(freq_termsindoc)
                                #for document in freq_termsindoc:
                                #    (doc_id,frequencies)=self.finding_doc_freq(document)
                                    
                                #print(frequencies)
                                #print(doc_id)


                                    if(_fields=="category" and frequencies["c"]!=0 ):
                                        rank_score = rank(doc_id,frequencies,no_ofdoc_words_occur)
                                        rank_listc[doc_id]=rank_score
                                        
                                                
                                    elif(_fields=="title" and frequencies["t"]!=0 ):
                                        #print("rankkkk")
                                        rank_score = rank(doc_id,frequencies,no_ofdoc_words_occur)
                                        rank_listt[doc_id]=rank_score
                                        #print(rank_listt)
                                        
                                                
                                    elif(_fields=="body" and frequencies["b"]!=0 ):
                                        rank_score = rank(doc_id,frequencies,no_ofdoc_words_occur)
                                        rank_listb[doc_id]=rank_score
                                        
                                                
                                      
                                    elif(_fields=="infobox" and frequencies["i"]!=0 ):
                                        rank_score = rank(doc_id,frequencies,no_ofdoc_words_occur)
                                        rank_listi[doc_id]=rank_score
                                           
                                                                             
                                    elif(_fields=="external links" and frequencies["e"]!=0 ):
                                        rank_score = rank(doc_id,frequencies,no_ofdoc_words_occur)
                                        rank_liste[doc_id]=rank_score

                                    elif(_fields=="references" and frequencies["r"]!=0 ):
                                        rank_score = rank(doc_id,frequencies,no_ofdoc_words_occur)
                                        rank_listr[doc_id]=rank_score
                                       
                                                
                                    count+=1
                                    

                                

                        else:
                            flag =1
                            if not w in stopwords.words('english'):
                            #words = ps.stem(w)
                                w = w.lower()
                                line = binary_search(w)
                                if line==None:
                                    print("NO RESULT")
                                #print("line",line)
                            

                                #entryin_index = self.find_querywords(w)
                                #print("entryin_index:::::",entryin_index)
                                freq_termsindoc = []
                                lock1 = []
                                lock2 = []
                                #freq_termsindoc = entryin_index.split("|")
                                freq_termsindoc = line.split("|")
                                #print(freq_termsindoc)
                                lock1 = freq_termsindoc[0].split(":")
                                #print("lock1",lock1)
                                (doc_id,dict_freq)=self.finding_doc_freq(lock1[1],lock1[2])
                                no_ofdoc_words_occur = len(freq_termsindoc)
                                #print(no_ofdoc_words_occur)
                                for document in range(1,no_ofdoc_words_occur):
                                    lock2 = freq_termsindoc[document].split(":")
                                    #print("lock2",lock2)
                                    (doc_id,dict_freq)=self.finding_doc_freq(lock2[0],lock2[1])
                                    rank_score = rank(doc_id,dict_freq,no_ofdoc_words_occur)
                                    try:
                                        rank_list[doc_id]+= rank_score
                                    except:
                                        rank_list[doc_id] = rank_score
                        #print(rank_list)

                if(flag==2):
                    #print(rank_listt)
                    list11 = {}
                    sr = set(rank_listr)
                    sc = set(rank_listc)
                    se = set(rank_liste)
                    si = set(rank_listi)
                    st = set(rank_listt)
                    sb = set(rank_listb)
                    #print("tt",rank_listt)
                    list11 = {**rank_listc ,**rank_listb, **rank_listt, **rank_listi, **rank_liste, **rank_listr}
                    #print("st",st)
                    #print(list11)
                    
                    set1 = sr.union(sc)
                    set2 = set1.union(se)
                    set3 = set2.union(si)
                    set4 = set3.union(st)
                    set5 = set4.union(sb)
                    #print(set5)
                    #rank_list = list(set5)
                    rank_list = list11
                    #print(list11)
                    #print("listttt",rank_list)
                    #if(bool(set5)):
                    #    rank_list = {element:list11[element]+str for element in set5} 
                          #  print("gg",rank_list)
                    #else:
                    #    rank_list[0] = 0

                    
                if(len(rank_list)>0):
                    #listoftup = rank_list.sort(reverse=True)
                    listoftup = sorted(rank_list.items(), reverse = True ,key = lambda kv:(kv[1], kv[0]))
                    #print(listoftup)
                    if(len(listoftup)>10):
                        for i in range(10):
                            #print(listoftup[i][0])
                            title = self.find_title(listoftup[i][0],titlefile)
                            #print("title",title)
                            lis_title.append(title)
                    else:
                        for i in range(len(listoftup)):
                            #print(listoftup[i][0])
                            title = self.find_title(listoftup[i][0],titlefile)
                            #print("title",title)
                            lis_title.append(title)



                final_list.append(lis_title)


        titlefile.close()

        #print(final_list)
        return(final_list)

                     
    def find_querywords(self,q_word):
        indexfile = open(path_to_index+"/index_file.txt","r")
        for line in indexfile:
            line = line.rstrip()
            line_token = line.split(":")
            i_word = line_token[0]
            if(q_word==i_word):

                #print("mila")
                return line_token[1]
                break

    def finding_doc_freq(self,token1,token2):
        frequencies={"t":0,"i":0,"c":0,"b":0,"r":0,"e":0,"net_total":0}
        #token_terms = freq_termsindoc.split(":")
        term_doc = token1
        frequency_terms=token2
        freq_field=""
        freq_str=[]
        count=0
        total=[]
        for t in frequency_terms:
                #print t
                try:
                    int(t)
                    freq_str.append(t) #freq_field=num
                except:
                    num_str="".join(freq_str)
                    try :
                        count=int(num_str)
                        total.append(count)
                    except:
                        pass
                    if (len(freq_field)>0):
                        #y[freq_field]=sum(total)
                        frequencies[freq_field]=count
                    freq_field=t
                    freq_str=[]
                    count=0 
            #for last term
        num_str="".join(freq_str)
        count=int(num_str)
        total.append(count)
        if (len(freq_field)>0):
                frequencies[freq_field]=count       
        net_total=sum(total)
        frequencies["net_total"]=net_total  
        return (str(term_doc),frequencies)
                     

    def find_title(self,doc_id,titleFileFD):

        
        
        titlefile = titleFileFD

        titlefile.seek(title_offset[int(doc_id)-1])
        title = titlefile.readline()
       
        return title       

def main():
    #path_to_index = sys.argv[1]
    start = time.time()
    dd = d()
    final = []
    testfile = sys.argv[2]
    path_to_output = sys.argv[3]
    #print(path_to_index)
    with open('offset.txt', 'rb') as f:
        for line in f:
            line = line[:-1]
            #print("off",line)
            offset.append(int(line.strip()))
    with open('title_offset.txt', 'rb') as f:
        for line in f:
            line = line[:-1]
            #print("off",line)
            title_offset.append(int(line.strip()))
    queries = read_file(testfile)
    #print(queries)
    final = dd.search(queries)
    write_file(final, path_to_output)
    final = time.time()
    print("time:",final-start) 

if __name__ == '__main__':
    main()
