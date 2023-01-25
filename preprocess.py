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
            "analysis": {
                "analyzer": {
                    "my_analyzer": {
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "porter_stem",  ## TODO: should i use stem-classes.txt for a custom stemmer instead?
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
        }
    }

    response = es.indices.create(index=AP89_INDEX, body=request_body)
    print(response)

    open_dir(es)

    print("completed indexing!")


"""opens file collection and delegates to parse individual files """
def open_dir(es) :

    entries = os.listdir(data) ##TODO FIX THISJKDKLFJKLAJDFKL
    id = 0

    # for every 'ap....' file in the opened directory, parse it for documents
    for entry in entries:
        if 'ap' in entry: ## excludes the readme file
            filepath = data + "/" + entry
            id = parse(filepath, id, es)



"""parses an individual file from the collection for documents / info """
def parse(filepath, id, es):

    with open(filepath, encoding="ISO-8859-1") as opened:

        read_opened = opened.read()
        found_docs = re.findall(DOC_REGEX, read_opened)

        for doc in found_docs:
            id +=1

            docno = ""
            text = ""

            find_docno = re.search(DOCNO_REGEX, doc)
            if find_docno:
                docno = re.sub(DOCNO_REGEX, "", str(find_docno))  ##TODO: getting warning bc re.search returns String or None, and re.sub doesnt support None
                docno.strip()

            find_text = re.search(TEXT_REGEX, doc)
            if find_text:
                text = re.sub(TEXT_REGEX, "", str(find_text))

            parsed_doc =  {
                'DOCNO': docno,
                'TEXT': text
            }

            es.index(index=AP89_INDEX, id=id, body=parsed_doc)

if __name__ == '__main__':
    main()
