#-*- coding: utf-8 -*-

import datetime
import time
import sys
import MeCab
import operator
from pymongo import MongoClient
from bson import ObjectId
from itertools import combinations

DBname = "db20121635"
conn = MongoClient('dbpurple.sogang.ac.kr')
db = conn[DBname]
db.authenticate(DBname, DBname)
stop_word = {}

def printMenu():
    print "0. CopyData"
    print "1. Morph"
    print "2. print morphs"
    print "3. print wordset"
    print "4. frequent item set"
    print "5. association rule"

def p0():
    col1 = db['news']
    col2 = db['news_freq']

    col2.drop()

    for doc in col1.find():
        contentDic = {}
        for key in doc.keys():
            if key != "_id":
                contentDic[key] = doc[key]
        col2.insert(contentDic)


def p1():
    for doc in db['news_freq'].find():
        doc['morph'] = morphing(doc['content'])
        db['news_freq'].update( {"_id":doc['_id']},doc)


# HW-start
def p2(url):
    for doc in db['news_freq'].find():
        if doc['url'] == url:
            for morph in doc['morph']:
                print(morph.encode('utf8')) 

#print( len(doc['morph']) )

def p3():
    col1 = db['news_freq']
    col2 = db['news_wordset']
    col2.drop()
    for doc in col1.find():
        new_doc = {}
        new_set = set()
        for w in doc['morph']:
            new_set.add(w.encode('utf-8'))
        new_doc['word_set'] = list(new_set)
        new_doc['url'] = doc['url']
        col2.insert(new_doc)

def p4(url):
    for doc in db['news_wordset'].find():
        if doc['url'] == url:
            for word in doc['word_set']:
                print(word.encode('utf8'))


def p5_3(min_sup):

    col     = db['news_wordset']
    col1    = db['candidate_L1']
    col2    = db['candidate_L2']
    col3    = db['candidate_L3']


    freq1 = {}
    freq2 = {}
    freq3 = {}

    freq2_key = []
    freq3_key = []
    
    col1.drop()
    col2.drop()
    col3.drop()

    for doc in col.find():
        for word in doc['word_set']:
           if word in freq1:
                freq1[word] += 1
           else:
                freq1[word] = 1

    for key, value in freq1.items():
        if value >= min_sup:
            new = {}
            new['item_set'] = key
            new['support']  = value
            col1.insert(new)
        else:
            del freq1[key]

    for d1 in col1.find():
        for d2 in col1.find():
            if d1['item_set'] != d2['item_set'] :
                temp_key = [ d1['item_set'], d2['item_set']]
                temp_key.sort()
                if temp_key not in freq2_key:
                    freq2_key.append(temp_key)

    for doc in col.find():
        for key in freq2_key:
            if key[0] in doc['word_set'] and key[1] in doc['word_set']:

                temp_key = frozenset(key)

                if temp_key not in freq2:
                   freq2[temp_key] = 1 
                else:
                    freq2[temp_key] += 1
            
    for key, value in freq2.items():
        if value >= min_sup:
            new = {}
            new['item_set'] = list(key)
            new['support']  = value
            col2.insert(new)
        else:
            del freq2[key]
    
    for d1 in col2.find():
        for d2 in col2.find():
            if d1 != d2 and d1['item_set'][0] == d2['item_set'][0]:
                target = [d1['item_set'][1], d2['item_set'][1]]
                target.sort()

                for d3 in col2.find():
                    if target == d3['item_set']:
                        temp_key = [ d1['item_set'][0], target[0], target[1]]
                        temp_key.sort()

                        if temp_key not in freq3_key:
                            freq3_key.append(temp_key)

    for doc in col.find():
        for key in freq3_key:
            if key[0] in doc['word_set'] and key[1] in doc['word_set'] and key[2] in doc['word_set']:

                temp_key = frozenset(key)
                if temp_key not in freq3:
                    freq3[temp_key] = 1
                else:
                    freq3[temp_key] += 1
                    
    for key, value in freq3.items():
        if value >= min_sup:
            new = {}
            new['item_set'] = list(key)
            new['support']  = value
            col3.insert(new)
        else:
            del freq3[key]
 



def p5_2(min_sup):
    
    col     = db['news_wordset']
    col1    = db['candidate_L1']
    col2    = db['candidate_L2']

    freq1 = {}
    freq2 = {}

    freq2_key = []

    col1.drop()
    col2.drop()

    for doc in col.find():
        for word in doc['word_set']:
           if word in freq1:
                freq1[word] += 1
           else:
                freq1[word] = 1

    for key, value in freq1.items():
        if value >= min_sup:
            new = {}
            new['item_set'] = key
            new['support']  = value
            col1.insert(new)
        else:
            del freq1[key]

    for d1 in col1.find():
        for d2 in col1.find():
            if d1['item_set'] != d2['item_set'] :
                temp_key = [ d1['item_set'], d2['item_set']]
                temp_key.sort()
                if temp_key not in freq2_key:
                    freq2_key.append(temp_key)

    for doc in col.find():
        for key in freq2_key:
            if key[0] in doc['word_set'] and key[1] in doc['word_set']:

                temp_key = frozenset(key)

                if temp_key not in freq2:
                   freq2[temp_key] = 1 
                else:
                    freq2[temp_key] += 1
            
    for key, value in freq2.items():
        if value >= min_sup:
            new = {}
            new['item_set'] = list(key)
            new['support']  = value
            col2.insert(new)
        else:
            del freq2[key]
 

def p5_1(min_sup):
    col     = db['news_wordset']
    col1    = db['candidate_L1']
    freq1 = {}

    col1.drop()
    
    for doc in col.find():
        for word in doc['word_set']:
           if word in freq1:
                freq1[word] += 1
           else:
                freq1[word] = 1

    for key, value in freq1.items():
        if value >= min_sup:
            new = {}
            new['item_set'] = key
            new['support']  = value
            col1.insert(new)
        else:
            del freq1[key]

                
def p5(length):

    min_sup = db['news_freq'].count() * 0.1

    if length == 1:
        p5_1(min_sup)

    elif length == 2:
        p5_2(min_sup)

    elif length == 3:
        p5_3(min_sup)

    else:
        print "Insert 1,2,3"

def p6_1(min_conf):

    col1    = db['candidate_L1']
    col2    = db['candidate_L2']

    freq1 = {}
    freq2 = {}

    for doc in col1.find():
        freq1[doc['item_set']] = doc['support']
    for doc in col2.find():
        freq2[frozenset(doc['item_set'])] = doc['support']

    for key in freq2.keys():

        A_B = float(freq2[key])
        temp_key = list(key)

        A = float(freq1[temp_key[0]])
        B = float(freq1[temp_key[1]])

        conf = A_B/A
        if conf >= min_conf:
            print_format(3, temp_key[0], temp_key[1], conf, 0)

        conf = A_B/B
        if conf >= min_conf:
            print_format(3, temp_key[1], temp_key[0], conf, 0)

def p6_2(min_conf):

    col1    = db['candidate_L1']
    col2    = db['candidate_L2']
    col3    = db['candidate_L3']

    freq1 = {}
    freq2 = {}
    freq3 = {}

    for doc in col1.find():
        freq1[doc['item_set']] = doc['support']
    for doc in col2.find():
        freq2[frozenset(doc['item_set'])] = doc['support']
    for doc in col3.find():
        freq3[frozenset(doc['item_set'])] = doc['support']

    for key in freq3.keys():
        temp_key    = list(key)
        A_B_C   = float(freq3[key])
        A_B     = float(freq2[frozenset((temp_key[0], temp_key[1]))])
        A_C     = float(freq2[frozenset((temp_key[0], temp_key[2]))])
        B_C     = float(freq2[frozenset((temp_key[1], temp_key[2]))])
        A       = float(freq1[temp_key[0]])
        B       = float(freq1[temp_key[1]])
        C       = float(freq1[temp_key[2]])


        conf = A_B_C / A
        if conf >= min_conf:
            print_format(4, temp_key[0], temp_key[1], temp_key[2], conf)

        conf = A_B_C / A_B
        if conf >= min_conf:
            print_format(4, temp_key[0], temp_key[1], temp_key[2], conf)

        conf = A_B_C / A_C
        if conf >= min_conf:
            print_format(4, temp_key[0], temp_key[2], temp_key[1], conf)
            
        conf = A_B_C / B
        if conf >= min_conf:
            print_format(4, temp_key[1], temp_key[0], temp_key[2], conf)

        conf = A_B_C / B_C
        if conf >= min_conf:
            print_format(4, temp_key[1], temp_key[2], temp_key[0], conf)

        conf = A_B_C / C 
        if conf >= min_conf:
            print_format(4, temp_key[2], temp_key[0], temp_key[1], conf)


def print_format(mode, v1, v2, v3, v4):
    if mode == 3:
        print(" {}, {}  => {}   ". format(v1, v2, v3))

    if mode == 4:
        print(" {}, {}  => {}   {}". format(v1, v2, v3, v4))


def p6(length):

    min_conf = 0.5

    if length == 2:
        p6_1(min_conf)
    elif length == 3:
        p6_2(min_conf)
    else:
        print " Insert [2,3]"


# HW-end

def make_stop_word():
    f = open("wordList.txt",'r')
    while True:
        line = f.readline()
        if not line: break
        stop_word[line.strip('\n')] = line.strip('\n')
    f.close()


def morphing(content):
    t = MeCab.Tagger('-d/usr/local/lib/mecab/dic/mecab-ko-dic')
    nodes = t.parseToNode(content.encode('utf-8'))
    MorpList = []
    while nodes:
        if nodes.feature[0] == 'N' and nodes.feature[1] == 'N':
            w = nodes.surface
            if not w in stop_word:
                try:
                    w = w.encode('utf-8')
                    MorpList.append(w)
                except:
                    pass
        nodes = nodes.next
    return MorpList



if __name__ == "__main__":

    make_stop_word()
    printMenu()
    selector = input()
    if selector == 0:
        p0()
    elif selector == 1:
        p1()
        p3()
    elif selector == 2:
        url = str(raw_input("input news url:"))
        p2(url)
    elif selector == 3:
        url = str(raw_input("input news url:"))
        p4(url)
    elif selector == 4:
        length = int(raw_input("input length of the frequent item:"))
        p5(length)
    elif selector == 5:
        length = int(raw_input("input length of the frequent item:"))
        p6(length)


 

