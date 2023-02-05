import os
import re
import pickle

from elasticsearch7 import Elasticsearch
from nltk import PorterStemmer, word_tokenize

data = "/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/AP_DATA/ap89_collection"

AP89_INDEX = 'ap89_index'

"""
    regex syntax: 
    '.' operator == matches any character 
    '*' operator == repeat preceding operator (i.e. any char) 0+ times
    '?' operator == repeat preceding char 0 or 1 times 
"""
DOC_REGEX = re.compile("<DOC>.*?</DOC>", re.DOTALL) ## DOTALL allows '.' to equal any character, including \n
DOCNO_REGEX = re.compile("<DOCNO>.*?</DOCNO>")
TEXT_REGEX = re.compile("<TEXT>.*?</TEXT>", re.DOTALL)

# for use in model calculations which require vocab_size
VOCAB_SIZE = 0
VOCAB = []
# for use in the model calculations which require document_length
DOC_LENS = {}

def main() :

    stops = read_stop_words("/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/AP_DATA/stoplist.txt")

    es = Elasticsearch("http://localhost:9200")
    stemmer = PorterStemmer()

    # delete previously made index if it already exists
    es.indices.delete(index=AP89_INDEX, ignore=[404, 400])

    request_body={
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": stops
                    }
                },
                "analyzer": {
                    "stopped": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "porter_stem"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "fielddata": True,
                    "analyzer": "stopped",
                    "index_options": "positions"
                }
            }
        }
    }

    response = es.indices.create(index=AP89_INDEX, body=request_body)
    print(response)

    open_dir(es, stemmer, stops)

    VOCAB_SIZE = len(VOCAB)
    print(VOCAB_SIZE)

    p = open('/Users/ellataira/Desktop/is4200/homework-1-ellataira/IR_data/AP_DATA/doc_lens_dict.pkl','wb')
    pickle.dump(DOC_LENS, p)
    p.close()

    print("completed indexing!")


"""opens file collection and delegates to parse individual files """
def open_dir(es, stemmer, stops) :

    entries = os.listdir(data)
    id = 0

    print(id)
    # for every 'ap....' file in the opened directory, parse it for documents
    for entry in entries:
        if 'ap' in entry: ## excludes the readme file
            filepath = data + "/" + entry
            id = parse(filepath, id, es, stemmer, stops)
            print("parsed: "+ filepath + "\n")



"""parses an individual file from the collection for documents / info """
def parse(filepath, id, es, stemmer, stops):

    with open(filepath, encoding="ISO-8859-1") as opened:

        read_opened = opened.read()
        found_docs = re.findall(DOC_REGEX, read_opened)

        print(id)
        for doc in found_docs:
            id +=1

            found_doc = re.search(DOCNO_REGEX, doc)
            docno = re.sub("(<DOCNO> )|( </DOCNO>)", "", found_doc[0])

            found_text = re.search(TEXT_REGEX, doc)
            text = re.sub("(<TEXT>\n)|(\n</TEXT>)", "", found_text[0])
            text = re.sub("\n", " ", text)

            tokens = word_tokenize(text)
            res = []
            for t in tokens:
                if t not in stops:
                    res.append(stemmer.stem(t))
                if t not in VOCAB:
                    VOCAB.append(t)

            # add stemmed, stop-word-removed text length to list of doc lengths
            DOC_LENS[docno] = len(res)

            text = " ".join(res)

            parsed_doc =  {
                'text': text
            }

            es.index(index=AP89_INDEX, id=docno, body=parsed_doc)

        print("doc index: " + str(id))
        return id

def read_stop_words(filename):
    lines = []
    with open(filename, 'r') as f:
        for line in f:
            lines.append(line.strip())
    return lines


if __name__ == '__main__':
    main()