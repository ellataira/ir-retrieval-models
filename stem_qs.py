import os
import re

from elasticsearch7 import Elasticsearch

data = "/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/query_desc.51-100.short.txt"

QS_INDEX = 'query_index'

# indexes, stems, and tokenizes queries in the same format as documents to determine how to reduce
# queries to minimal key words
def main() :

    es = Elasticsearch("http://localhost:9200")

    # delete previously made index if it already exists
    es.indices.delete(index=QS_INDEX, ignore=[404, 400])

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

    response = es.indices.create(index=QS_INDEX, body=request_body)
    print(response)

    parse(data, es)

    print("completed indexing!")

# parse query .txt file, line by line
def parse(filepath, es):

    with open(filepath, encoding="ISO-8859-1") as opened:

        lines = opened.readlines()

        for line in lines:

            text = re.sub("Document will", "", line)

            parsed_doc =  {
                'text': text
            }

            es.index(index=QS_INDEX, body=parsed_doc)


if __name__ == '__main__':
    main()
