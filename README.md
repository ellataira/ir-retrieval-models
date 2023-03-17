# Retrieval Models

The objective of this project is to implement and compare various retrieval systems using vector space models and language models. The two main components are a program to parse the corpus of documents and index it with Elasticsearch and a query processor, which runs queries from an input file using a selected retrieval model.

## Task 2: Document Indexing 

I used Elasticsearch7.5.1 order to create an index of the downloaded corpus in `preprocess.py`. 
To do so, I created an index under the name "ap89_index" and parsed using regex expressions to find each document, 
its document ID, and its contents. For each found document, I removed stop words and stemmed each term before adding 
it to the index using Elasticsearch. 

I used the provided list of stop words, "stoplist.txt", and the PorterStemmer. Additionally, while processing the 
tokenized form of each document's text field, 
I stored that document's length (i.e. number of words) in a dictionary for future ranking. I also counted the total
number of unique terms encountered to find the size of the corpus' vocabulary, again for future ranking needs. 

## Task 3: Query Execution 

In the next phase, I implemented 6 different retrieval models, each to find the top 1000 documents for every query in 
the provided query list. The method, `run_all_models()`, generates a .txt file for each ranking model, listing the query 
and its corresponding top 1000 most relevant documents, in order from most relevant to least. 

Before searching for and scoring the documents, I processed the provided queries, again stemming and removing stop words.
I also refined the original queries to have fewer terms and removed any duplicate words. 

I implemented the task of scoring by creating numerous helper functions that retrieve specified values from each document's 
Elasticsearch-generated term vector. These helper functions possess the functionality to return term frequency ( tf_w,d ), 
document length (len(d)), average document length(avg(len(d))), total number of documents (D), document frequency (df_w), and vocabulary size (V). 

I also wrote helper methods to write the ranked documents to their output files. 

When the file `query_execution.py`is run, `run_all_models()` is called. The method searches for the top 1000
documents for eaach query, and, for every term in the query, ranks each document using the six different models. For vector 
and probabilistic models, a document-term combination is only scored if that term is present in the document; in comparison,
in the language models, every document-term combination is given a score, regardless of if the term exists in that document. 
For the language models, when a term is not found in a document, that calculated score is given a strongly negative value in 
order to mark it as not relevant. 

### Retrieval Models 

The six different models are as follows:
* ES Built-in
  * uses Elasticsearch's built-in API to find and score relevant documents 
* Okapi TF
  * vector space model  
  * calculates using term frequency, document length, and average document length
  * modifies the term frequency to account for different length documents and resulting term frequencies 
* TF-IDF
  * vector space model
  * calculates  using the Okapi TF score and inverse document frequency 
  * TF-IDF will increase with term frequency in a document and/or rarity of a term in the collection 
* Okapi BM25
  * language model extending binary independence model 
  * focused on topical relevance 
  * uses constants k1, k2, and b, which are found through experimentation 
* Unigram LM with La Place smoothing
  * language model-- predicts the probability of a word given all words in corpus 
  * La Place smoothing to estimate probabilities of missing or unseen words 
* Unigram LM with Jelinek-Mercer smoothing
  * language model-- predicts the probability of a word given all words in corpus 
  * Jelinek-Mercer smoothing favors longer documents, as it puts more weight on words IN the document compared to those not in the document 

## Task 4: Evaluation 

|                                           | ES Built-in | Okapi TF | TF-IDF | Okapi BM25 | La Place | Jelinek-Mercer|
|-------------------------------------------|---| --- | --- | --- |---| --- |
| **Uninterpolated mean average precision** | 0.3052| 0.2553 | 0.2920 |  0.2957 | 0.2477 | 0.2652 |
| **Precision at 10**                       | 0.4480| 0.4400 | 0.4320| 0.4360 | 0.4600 | 0.4560 |
| **Precision at 30**                       | 0.3893  |0.3573 | 0.3813 | 0.3667 | 0.3640 | 0.3733|

Manual Comparison: 

Query #85: 

| ES Built-in   | TF-IDF          | Okapi BM25 | La Place       |
|---------------|-----------------| --- |----------------| 
 | AP890108-0030 | AP890108-0030   |  AP890108-0030    | AP890108-0030  |
 | AP890107-0129 |   AP890107-0129 |   AP890107-0129 |   AP890704-0150 |
|AP890220-0143   |  AP890518-0050  |   AP890220-0143   |    AP890710-0011|
|AP890516-0158    | AP890220-0143   |  AP890516-0158    |   AP890620-0122 |
|AP890516-0072     |AP890516-0158    | AP890516-0072    |   AP891013-0120|
|AP890518-0050    | AP890516-0072    | AP890518-0050    |   AP890518-0050|
|AP890517-0182    | AP891111-0095   |  AP890517-0182    |   AP890622-0096|
|AP890125-0007    | AP890517-0182    | AP890125-0007    |   AP890417-0030|
|AP890122-0062    | AP890125-0007    | AP890122-0062    |   AP890105-0199|
|AP891111-0095    | AP890112-0180    | AP891111-0095    |   AP890125-0007|

