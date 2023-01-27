import re
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import sys

data = "/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/query_desc.51-100.short.txt"

QS_INDEX = 'query_index'
STOPS = '/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/stoplist.txt'

# indexes, stems, and tokenizes queries in the same format as documents to determine how to reduce
# queries to minimal key words
def main() :
    f = open('/Users/ellataira/Desktop/cs4200/homework-1-ellataira/IR_data /AP_DATA/stemmed_queries.txt', 'w')
    sys.stdout = f

    with open(data, encoding="ISO-8859-1") as opened:

        stemmer = PorterStemmer()
        lines = opened.readlines()

        for line in lines:
            result = " "
            text = re.sub("(Document will) | (Document must)", "", line)
            text = re.sub(r"(^|\W)\d+", "", text)
            text.strip()


            tokens = word_tokenize(text)

            for t in tokens:
                if t not in STOPS:
                    result += " " + stemmer.stem(t)

            print(result)

    f.close()

if __name__ == '__main__':
    main()
