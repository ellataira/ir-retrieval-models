import math
import os
import pickle
import re
import sys
from nltk.stem.porter import *
import preprocess

from elasticsearch7 import Elasticsearch

es = Elasticsearch("http://localhost:9200", timeout=200)
AP89_INDEX = 'ap89_index'
q_data = "/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/query_desc.51-100.short.txt"

VOCAB_SIZE = 288141

infile = open('/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/doc_lens_dict.pkl', 'rb')
DOC_LENS = pickle.load(infile)
infile.close()

######################## PROCESS QUERIES ##################################################################

# modify input queries through stemming, shifting to lowercase, and removing stop words
# stores the modified queries in a dictionary as key-value : (qID, query)
def query_analyzer(query):
    body={
        "tokenizer": "standard",
        "filter": ["porter_stem", "lowercase", "english_stop"],
        "text": query
    }
    res = es.indices.analyze(body=body, index=AP89_INDEX)
    return [list["token"] for list in res["tokens"]]

# anaylzes .txt files of queries and stores them as key-value: qID, query terms (modified)
def process_all_queries(query_file):

    with open(q_data, encoding="ISO-8859-1") as opened:
        lines = opened.readlines()

        query_dict = {}

        for line in lines:
            sects = line.split(".")
            q_id = sects[0].strip()
            mod_q = query_analyzer(sects[1].strip())
            query_dict[q_id] = mod_q

    opened.close()
    return query_dict

######################## HELPER FUNCTIONS / "GET"-[] ##################################################################

# TODO necessary? from the demo vid
def scroll_body(scroll_id):
    body={
        "scroll_id": scroll_id,
        "scroll": "3m"
    }
    return body

# search index for a given query
# @param processed query (stemmed with removed stop words in array format)
# @return list of relevant docs IDs
def query_search(query):

    relevant_doc_ids = []

    res = es.search(
        index=AP89_INDEX,
        body={
            "size": 10000,
            "query": {
                    "match": {"text": " ".join(query)} # convert query array back into string
            }
        },
        scroll="3m"
    )

    sid = res['_scroll_id']
    scroll_size= len(res['hits']['total'])

    for r in res['hits']['hits']:
        relevant_doc_ids.append(r['_id'])

    while (scroll_size > 0):
        res = es.scroll(scroll_id=sid, scroll='3m')
        # update scroll id
        sid = res['_scroll_id']
        # get number of hits from prev scroll
        scroll_size = len(res['hits']['hits'])
        for r in res['hits']['hits']:
            relevant_doc_ids.append(r['_id'])

    return relevant_doc_ids


# gets term vectors for all documents in index
def term_vectors(doc_ids):
    body = {
        "ids": doc_ids,
        "parameters": {
            "fields": [
                'text'
            ],
            "term_statistics": True,
            "offsets": True,
            "payloads": True,
            "positions": True,
            "field_statistics": True
        }
    }
    term_vectors = es.mtermvectors(index=AP89_INDEX, body=body)
    return term_vectors

# returns term frequency value of given term in the index
def get_ttf(term, tv):
    tf = 1
    try :
        tf = tv['term_vectors']['text']['terms'][term]['ttf']
    except KeyError:
        print("key does not exist in the document: " + term)
    return tf

# returns term frequency of given term in a document
# @param single term vector corresponding to a docid
# @param term
# @return the frequency of given term in tv
def get_word_in_doc_frequency(term, tv):
    tf = 1
    try :
        tf = tv['term_vectors']['text']['terms'][term]['term_freq']
    except KeyError:
        print("key does not exist in the document: " + term)
    return tf

# returns term frequency of given term in a given query
def get_word_in_query_frequency(term, query):
    stemmer = PorterStemmer()
    count = 0
    for s in query:
        if stemmer.stem(s) == stemmer.stem(term):
            count += 1
    return count

def get_avg_doc_length(tv):
    num_docs = tv['term_vectors']['text']['field_statistics']['doc_count']
    sum_ttf = tv['term_vectors']['text']['field_statistics']['sum_ttf']
    return float(sum_ttf) / num_docs

# @param term vector corresponding to docid
def get_doc_length(d_id, term):
    # exp = es.explain(index=AP89_INDEX, id=d_id, body={
    #     "query":{
    #         "term": {"text": term}
    #     }})
    # try:
    #     dl = exp['explanation']['details'][0]['details'][2]['details'][3]['value']
    # except: #TODO: BUT WHAT IF THE TERM ISNT IN THE DOC??! BC IT SETS DL=1
    #     dl = 1
    # return dl
    return DOC_LENS[d_id]

# find term frequency in all documents in corpus
def get_doc_frequency_of_word(tv, term):
    df = 1
    try:
        df = tv['term_vectors']['text']['terms'][term]['doc_freq']
    except:
        print("key does not exist in the corpus: " + term)
    return df

def get_vocab_size():
    # vocab = es.search(index=AP89_INDEX, body={
    #     "aggs": {
    #         "vocab": {
    #             "cardinality": {
    #                 "field": "text"
    #             }
    #         }
    #     },
    #     "size": 0
    # })
    # return vocab['aggregations']['vocab']['value']
    return VOCAB_SIZE


######################### OUTPUT  RESULTS / SORT #########################################################
# sorts documents in descending order, so the doc with the highest score is first (most relevant)
# and truncates at k docs
def sort_descending(relevant_docs, k):
    sorted_docs = sorted(relevant_docs.items(), reverse=True)
    del sorted_docs[k:]
    return sorted_docs

# outputs search results to a file
# uses fields specific to ES builtin search
# the ES builtin search already sorts the hits in decresing order, so there is no need to reorder before saving
def save_to_file_for_es_builtin(relevant_docs, doc_name):
    f = '/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /scores' + doc_name + '.txt'

    if os.path.exists(f):
        os.remove(f)

    with open(f, 'w') as f:
        for query_id, docs in relevant_docs.items():
            count = 1
            for d in docs['hits']['hits']:
                f.write(str(query_id) + ' Q0 ' + str(d['_id']) + ' ' + str(count) + ' ' + str(d['_score']) + ' Exp\n')
                count += 1

    f.close()

def save_to_file(relevant_docs, filename):
    f = '/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /scores' + filename + '.txt'
    k = 1000 # want to save the top 1000 files

    if os.path.exists(f):
        os.remove(f)

    with open(f, 'w') as f:
        for query_id, results_dict in relevant_docs.items():
            sorted_dict = sort_descending(results_dict, k)
            count = 1
            for d_id, score in sorted_dict.items():
                f.write(str(query_id) + ' Q0 ' + str(d_id) + ' ' + str(count) + ' ' + str(score) + ' Exp\n')
                count+=1

    f.close()

######################## ES BUILT-IN  ##################################################################

def es_search(queries):
    relevant_docs = {}

    for id, query in queries.items(): # query_list stores (id, query) key value pairs
        body = {
            "size": 1000,
            "query": {
                "match": {"text": " ".join(query)} # convert query array back into string
            }
        }
        res_es_search = es.search(index=AP89_INDEX, body=body)
        relevant_docs[id] = res_es_search

    return relevant_docs

######################## OKAPI TF ##################################################################

def okapi_tf(tf_wd, dl, adl):
    score = tf_wd / (tf_wd + 0.5 + 1.5 * (dl/adl))
    return score

######################## TF IDF ##################################################################

def tf_idf(okapi_score,d,df_w):
    score = okapi_score * math.log(d/df_w)
    return score

######################## Okapi BM25 ##################################################################

def okapi_bm25(tf_wq, tf_wd, df_w, adl, dl,d):
    b = 0.75
    k1 = 1.2
    k2 = 500 # k2 param is typically 0 <= k2 <= 1000
    log = math.log((d+0.5)/(df_w+0.5))
    a = (tf_wd + k1 * tf_wd) / (tf_wd + k1((1-b)+b*(dl / adl)))
    b = (tf_wq + k2 * tf_wq) / (tf_wq+k2)
    return log * a * b


######################## Unigram LM with Laplace smoothing ##################################################

def uni_lm_laplace(tf_wd, dl, v):
    p_laplace = (tf_wd + 1) / (dl + v)
    return math.log(p_laplace)


######################## Unigram LM with Jelinek-Mercer smoothing #########################################
def uni_lm_jm(tf_wd, dl, ttf, v):
    l = 0.7 # a high lambda value prefers docs containing all query words; a low lambda is better for longer queries
    p_jm = l (tf_wd / dl) + (1-l)(ttf / v)
    return math.log(p_jm)

##########################################################################################################
def score():
    queries = process_all_queries(q_data)
    print(queries)
    okapi_scores = {}
    tf_idf_scores = {}
    okapi_bm25_scores = {}
    uni_lm_laplace_scores = {}
    uni_lm_jm_scores = {}

    ## execute ES builtin:
    es_builtin = es_search(queries)

    ## execute ranking models
    for id, query in queries:
        q_id = query['_id']
        doc_ids = query_search(query)
        tvs = term_vectors(doc_ids)
        d = len(tvs)

        for tv in tv['docs']:
            d_id = tv['_id']
            for term in query:
                print(term)
                tf_wd = get_word_in_doc_frequency(term, tv)
                tf_wq = get_word_in_query_frequency(term, query)
                dl = get_doc_length(d_id, term)
                adl = get_avg_doc_length(tv)
                df_w = get_doc_frequency_of_word(tv, term)
                v = get_vocab_size()
                ttf = get_ttf(term, tv)

                ## OkapiTF
                okapi_score = okapi_tf(tf_wd, dl, adl)
                okapi_scores[q_id][d_id] += okapi_score

                ## TF-IDF
                tf_idf_score = tf_idf(okapi_score, d, df_w)
                tf_idf_scores[q_id][d_id] += tf_idf_score

                # Okapi BM25
                okapi_bm25_score = okapi_bm25(tf_wq, tf_wd, df_w, adl, dl, d)
                okapi_bm25_scores[q_id][d_id] += okapi_bm25_score

                # Unigram LM with Laplace smoothing
                uni_lm_laplace_score  = uni_lm_laplace(tf_wd, dl, v)
                uni_lm_laplace_scores[q_id][d_id] += uni_lm_laplace_score

                # Unigram LM with Jelinek-Mercer smoothing
                uni_lm_jm_score = uni_lm_jm(tf_wd, dl, ttf, v)
                uni_lm_jm_scores[q_id][d_id] += uni_lm_jm_score

    # once completed ranking for every query, export results

    save_to_file_for_es_builtin(es_builtin, "es_builtin")
    save_to_file(okapi_scores, "okapi_tf")
    save_to_file(tf_idf_scores, "tf_idf")
    save_to_file(okapi_bm25_scores, "okapi_bm25")
    save_to_file(uni_lm_laplace_scores, "uni_lm_laplace")
    save_to_file(uni_lm_jm_scores, "uni_lm_jm")



queries = process_all_queries(q_data)
# print(queries)
# # es_builtin = ES_Search(queries)
# # save_to_file_for_es_builtin(es_builtin, "es_builtin")
c=0
for id, q in queries.items():
    if c < 4:
         print(q)
         doc_ids= query_search(q)
         tvs = term_vectors(doc_ids)
         for t in q:
            print("query tfrequ: " + str(get_word_in_query_frequency(t, q)))
            # print(get_term_tfrequency(t, tvs))
            for tv in tvs['docs']:
                print(str(tv) + "\n")
                print("term: " + t)
                print("Word in doc: " + str(get_word_in_doc_frequency(t, tv)))
                print("get dl: " + str(get_doc_length(tv["_id"], t)))
                print("avg doc length: " +  str(get_avg_doc_length(tv)))
                print("vocab size: " + str(get_vocab_size()))
         c+=1
#     print(q)

