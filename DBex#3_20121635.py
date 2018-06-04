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
    print("Function p2")
    for doc in db['news_freq'].find():
        print(doc)
        if doc['url'] == url:
            for morph in doc['morph']:
                print(morph)
            print(len(doc['morph']))

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
    wordset = []
    for doc in db['news_wordset'].find():
        if doc['url'] == url:
            for word in doc['word_set']:
                print(word)
            print( len(doc['word_set']) )
                
def p5(length):
    iter_count = length
    total_wordset = {}
    for doc in db['news_wordset'].find():
        for word in doc['word_set']:
            if word not in total_wordset:
                total_wordset[frozenset([word])] = 0

    min_sup = db['news_freq'].count()*(0.1)
    key_size = 1
    while iter_count:
        new_wordset = {}

        if key_size != 1:
            for key1 in total_wordset.keys():
                for key2 in total_wordset.keys():
                    if key1 != key2:
                        set3 = key1 | key2
                        new_wordset[set3] = 0
            total_wordset = new_wordset
            for key, value in total_wordset.items():
                if len(key) < key_size:
                    del total_wordset[key]



        for doc in db['news_freq'].find():
            for key in total_wordset.keys():
                if key.issubset(doc['morph']):
                    total_wordset[key] += 1

        for key, value in total_wordset.items():
            if value < min_sup:
                del total_wordset[key]


        iter_count -= 1
        key_size += 1


        if key_size != length + 1:
            continue
        for key in total_wordset.keys():
            item_set = list(key)
            db['candidate_L' + str(key_size-1)].insert({
                    "item_set"  : item_set,
                    "support"   : total_wordset[key],
            })

def p6(length):

    if length == 1:
        pass

    for doc in db['candidate_L' + str(length)]:
        itemset = doc['item_set']

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


 






