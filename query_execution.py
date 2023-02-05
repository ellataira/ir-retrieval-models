import math
import os
import pickle
from nltk.stem.porter import *
import preprocess

from elasticsearch7 import Elasticsearch

es = Elasticsearch("http://localhost:9200", timeout=900000000)
AP89_INDEX = 'ap89_index'
q_data = "/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/AP_DATA/new_queries.txt"

VOCAB_SIZE = 288141
TOTAL_DOCS = 84678
SIZE = 1000

infile = open('/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/AP_DATA/doc_lens_dict.pkl', 'rb')
DOC_LENS = pickle.load(infile)
infile.close()

######################## PROCESS QUERIES ##################################################################

# modify input queries through stemming, shifting to lowercase, and removing stop words
# stores the modified queries in a dictionary as key-value : (qID, query)
def query_analyzer(query):
    body = {
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
                "match": {"text": " ".join(query)}  # convert query array back into string
            }
        },
        scroll="3m"
    )

    sid = res['_scroll_id']
    scroll_size = len(res['hits']['total'])

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

# get a term vectors corresponding to a given document ID
def get_term_vector(d_id):
    term_vector = es.termvectors(index=AP89_INDEX, id=d_id, fields='text',
                                 term_statistics=True, offsets=True,
                                 payloads=True, positions=True, field_statistics=True)
    return term_vector


# returns term frequency value of given term in the index
def get_ttf(term, tv):
    try:
        ttf = tv['term_vectors']['text']['terms'][term]['ttf']
    except KeyError:
        ttf = 100
    return ttf


# returns term frequency of given term in a document
# @param single term vector corresponding to a docid
# @param term
# @return the frequency of given term in tv
def get_word_in_doc_frequency(term, tv):
    try:
        tf = tv['term_vectors']['text']['terms'][term]['term_freq']
    except KeyError:
        tf = 0
    return tf


# returns term frequency of given term in a given query
def get_word_in_query_frequency(term, query):
    stemmer = PorterStemmer()
    count = 0
    for s in query:
        if s == stemmer.stem(term):
            count += 1
    return count


# returns the average length (number of words) in the corpus
def get_avg_doc_length(tv):
    num_docs = tv['term_vectors']['text']['field_statistics']['doc_count']
    sum_ttf = tv['term_vectors']['text']['field_statistics']['sum_ttf']
    return float(sum_ttf) / num_docs


# returns the length (number of words) in a specified document
# @param term vector corresponding to docid
def get_doc_length(d_id, term):
    exp = es.explain(index=AP89_INDEX, id=d_id, body={
        "query":{
            "term": {"text": term}
        }})
    try:
        dl = exp['explanation']['details'][0]['details'][2]['details'][3]['value']
    except:
        dl = 100
    return dl


# find term frequency in all documents in corpus
def get_doc_frequency_of_word(tv, term):
    try:
        df = tv['term_vectors']['text']['terms'][term]['doc_freq']
    except:
        df = 1
    return df


# returns the vocab size of the entire corpus
# i.e. the number of unique terms
def get_vocab_size():
    return VOCAB_SIZE

# returns the total number of documents in the index
def get_total_docs():
    return TOTAL_DOCS

######################### OUTPUT  RESULTS / SORT #########################################################

# sorts documents in descending order, so the doc with the highest score is first (most relevant)
# and truncates at k docs
def sort_descending(relevant_docs, k):
    print("pre sort: " + str(relevant_docs))
    sorted_docs = sorted(relevant_docs.items(), key=lambda item: float(item[1]), reverse=True)
    del sorted_docs[k:]
    print("sorted desc: " + str(sorted_docs))
    return sorted_docs


# outputs search results to a file
# uses fields specific to ES builtin search
# the ES builtin search already sorts the hits in decresing order, so there is no need to reorder before saving
def save_to_file_for_es_builtin(relevant_docs, doc_name):
    f = '/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/scores/' + doc_name + '7.txt'

    if os.path.exists(f):
        os.remove(f)

    with open(f, 'w') as f:
        for query_id, docs in relevant_docs.items():
            count = 1
            for d in docs['hits']['hits']:
                f.write(str(query_id) + ' Q0 ' + str(d['_id']) + ' ' + str(count) + ' ' + str(d['_score']) + ' Exp\n')
                count += 1

    f.close()

# saves a list of scored docs to a .txt file
# @param 2-d dictionary of scored documents [query][documents]
def save_to_file(relevant_docs, filename):
    f = '/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/scores/' + filename + '7.txt'
    k = SIZE  # want to save the top 1000 files

    if os.path.exists(f):
        os.remove(f)

    with open(f, 'w') as f:
        for query_id, results_dict in relevant_docs.items():
            sorted_dict = sort_descending(results_dict, k)
            count = 1
            for d_id, score in sorted_dict:
                f.write(str(query_id) + ' Q0 ' + str(d_id) + ' ' + str(count) + ' ' + str(score) + ' Exp\n')
                count += 1

    f.close()


######################## ES BUILT-IN  ##################################################################

# uses built-in elasticsearch method to rank documents
# @param given dictionary of queries
def es_search(queries):
    relevant_docs = {}

    for id, query in queries.items():  # query_list stores (id, query) key value pairs
        body = {
            "size": SIZE,
            "query": {
                "match": {"text": " ".join(query)}  # convert query array back into string
            }
        }
        res_es_search = es.search(index=AP89_INDEX, body=body)
        relevant_docs[id] = res_es_search

    return relevant_docs


######################## OKAPI TF ##################################################################

# calculates okapi tf score of a single document
def okapi_tf(tf_wd, dl, adl):
    score = tf_wd / (tf_wd + 0.5 + (1.5 * (dl / adl)))
    return score


######################## TF IDF ##################################################################

# calculates tf -idf score of a single document
def tf_idf(okapi_score, d, df_w):
    score = okapi_score * math.log(d / df_w)
    return score


######################## Okapi BM25 ##################################################################

# calculates the okapi bm25 score of a single document
def okapi_bm25(tf_wq, tf_wd, df_w, adl, dl, d):
    b = 0.75
    k1 = 1.2
    k2 = 100  # k2 param is typically 0 <= k2 <= 1000
    log = math.log((d + 0.5) / (df_w + 0.5))
    a = (tf_wd + k1 * tf_wd) / (tf_wd + k1 * ((1 - b) + b * (dl / adl)))
    b = (tf_wq + k2 * tf_wq) / (tf_wq + k2)
    return log * a * b


######################## Unigram LM with Laplace smoothing ##################################################

# calculates Unigram LM with Laplace smoothing score of a single document
def uni_lm_laplace(tf_wd, dl, v):
    p_laplace = (tf_wd + 1) / (dl + v)
    return math.log(p_laplace)

# make doc length big if not exist -- log of a larger number is greater , so want bigger frac denom

# tf=0 (not present), dl=100, v= 1000
# lp = -3.04

# tf=0 (not present), dl=1, v= 1000
# lp = -3.00

# want a more negative number if the term is not present, so set DL to a large number

######################## Unigram LM with Jelinek-Mercer smoothing #########################################

# calculates Unigram LM with Jelinek-Mercer smoothing score of a single document
def uni_lm_jm(tf_wd, dl, ttf, v):
    l = 0.9  # a high lambda value prefers docs containing all query words; a low lambda is better for longer queries
    p_jm = l * (tf_wd / dl) + (1 - l) * (ttf / v)
    return math.log(p_jm)

# tf=0 (not present), dl=100, v= 1000, ttf=1
# jm = -4

# tf=0 (not present), dl=1, v= 1000, ttf=1
# tf = -4
# THE DL WILL BE CANCELLED OUT IF TF = 0 , so it doesn't matter what it is set to

# tf=0 (not present), dl=1, v= 1000, ttf=20000
# jm = 0.301

# tf=0 (not present), dl=1, v= 1000, ttf=10
# jm = -3

# want a more negative number if the term is not present, so set TTF to a large number. we don't care about DL, bc it
# will be cancelled out by TF=0

##########################################################################################################

# method that calls all vector ranking models
# will increment a document score only if the term is in the current document
def Vector_Prob_Models(queries):
    # initialize scores dictionaries
    okapi_scores = {}
    tf_idf_scores = {}
    okapi_bm25_scores = {}

    # for each query,
    for id, query in queries.items():
        q_id = id
        print("q_id: " + str(q_id))
        print("query: " + str(query))

        # for each query, instaniate its index in 2-d solution array
        okapi_scores[q_id] = {}
        tf_idf_scores[q_id] = {}
        okapi_bm25_scores[q_id] = {}

        # get relevant documents for the query
        doc_ids = query_search(query)

        d = get_total_docs()
        v = get_vocab_size()

        for d_id in doc_ids:
            tv = get_term_vector(d_id)

            # for each term in query,
            for term in query:
                d_id = tv['_id']

                # only calculate score if the term is in the document
                if term in tv["term_vectors"]["text"]["terms"].keys():
                    tf_wq = get_word_in_query_frequency(term, query)
                    tf_wd = get_word_in_doc_frequency(term, tv)
                    dl = get_doc_length(d_id, term)
                    adl = get_avg_doc_length(tv)
                    df_w = get_doc_frequency_of_word(tv, term)

                    # okapi-tf
                    okapi_score = okapi_tf(tf_wd, dl, adl)
                    try:
                        okapi_scores[q_id][d_id] += okapi_score
                    except (KeyError):
                        okapi_scores[q_id][d_id] = okapi_score

                    # TF-IDF
                    tf_idf_score = tf_idf(okapi_score, d, df_w)
                    try:
                        tf_idf_scores[q_id][d_id] += tf_idf_score
                    except (KeyError):
                        tf_idf_scores[q_id][d_id] = tf_idf_score

                    # Okapi BM25
                    okapi_bm25_score = okapi_bm25(tf_wq, tf_wd, df_w, adl, dl, d)
                    try:
                        okapi_bm25_scores[q_id][d_id] += okapi_bm25_score
                    except (KeyError):
                        okapi_bm25_scores[q_id][d_id] = okapi_bm25_score

    return okapi_scores, tf_idf_scores, okapi_bm25_scores

def Unigram_Models(queries):
    # initialize scores dictionaries
    laplace_scores = {}
    jm_scores = {}

    # for each query,
    for id, query in queries.items():
        q_id = id
        print("q_id: " + str(q_id))
        print("query: " + str(query))

        # for each query, instaniate its index in 2-d solution array
        laplace_scores[q_id] = {}
        jm_scores[q_id] = {}

        # get relevant documents for the query
        doc_ids = query_search(query)

        d = get_total_docs()
        v = get_vocab_size()

        for d_id in doc_ids:
            tv = get_term_vector(d_id)

            # for each term in query,
            for term in query:
                d_id = tv['_id']

                tf_wd = get_word_in_doc_frequency(term, tv)
                dl = get_doc_length(d_id, term)
                ttf = get_ttf(term, tv)

                # if the term is not present in document, then tf_wd = 0
                uni_lm_laplace_score = uni_lm_laplace(tf_wd, dl, v)
                try:
                    laplace_scores[q_id][d_id] += uni_lm_laplace_score
                except (KeyError):
                    laplace_scores[q_id][d_id] = uni_lm_laplace_score

                # Unigram LM with Jelinek-Mercer smoothing
                uni_lm_jm_score = uni_lm_jm(tf_wd, dl, ttf, v)
                try:
                    jm_scores[q_id][d_id] += uni_lm_jm_score
                except (KeyError):
                    jm_scores[q_id][d_id] = uni_lm_jm_score

    return laplace_scores, jm_scores


# runs all ranking models and saves their outputs to txt files
# processes queries, searches for relevant documents, and then scores those documents using the
# different ranking models
def run_all_models():
    # process, stem, remove stop words from queries
    queries = process_all_queries(q_data)
    print(queries)

    # vector / prob models
    okapi_scores, tf_idf_scores, okapi_bm25_scores = Vector_Prob_Models(queries)

    save_to_file(okapi_scores, "okapi_tf")
    print("saved okapi scores")

    save_to_file(tf_idf_scores, "tf_idf")
    print("saved tf idf scores")

    save_to_file(okapi_bm25_scores, "okapi_bm25")
    print("saved okapi bm25 scores")

    # ES builtin:
    es_builtin_scores = es_search(queries)

    save_to_file_for_es_builtin(es_builtin_scores, "es_builtin")
    print("saved built in scores")

    # language models

    uni_lm_laplace_scores, uni_lm_jm_scores = Unigram_Models(queries)

    save_to_file(uni_lm_laplace_scores, "uni_lm_laplace")
    print("saved uni lm laplace scores")

    save_to_file(uni_lm_jm_scores, "uni_lm_jm")
    print("saved uni lm jm scores")

    print("complete!")


if __name__ == '__main__':
    run_all_models()


