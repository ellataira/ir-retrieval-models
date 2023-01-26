import os
import re

from elasticsearch7 import Elasticsearch

data = "/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/ap89_collection"

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

def main() :

    es = Elasticsearch("http://localhost:9200")

    # delete previously made index if it already exists
    es.indices.delete(index=AP89_INDEX, ignore=[404, 400])

    request_body={
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "my_analyzer": {
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "porter_stem",
                            "custom_stop_filter"
                        ]
                    }
                },
                "filter": {
                    "custom_stop_filter": {
                        "type": "stop",
                        "ignore_case": True,
                        "stopwords": "IR_data /AP_DATA/stoplist.txt"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "fielddata": True,
                    "analyzer": "my_analyzer",
                    "index_options": "positions"
                }
            }
        }
    }

    response = es.indices.create(index=AP89_INDEX, body=request_body)
    print(response)

    open_dir(es)

    print("completed indexing!")


"""opens file collection and delegates to parse individual files """
def open_dir(es) :

    entries = os.listdir(data)
    id = 0

    print(id)
    # for every 'ap....' file in the opened directory, parse it for documents
    for entry in entries:
        if 'ap' in entry: ## excludes the readme file
            filepath = data + "/" + entry
            id = parse(filepath, id, es)
            print("parsed: "+ filepath + "\n")



"""parses an individual file from the collection for documents / info """
def parse(filepath, id, es):

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

            parsed_doc =  {
                'text': text
            }

            es.index(index=AP89_INDEX, id=docno, body=parsed_doc)

        print("doc index: " + str(id))
        return id


if __name__ == '__main__':
    main()
