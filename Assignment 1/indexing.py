#-------------------------------------------------------------------------
# AUTHOR: Ryan Andrada
# FILENAME: indexing.py
# SPECIFICATION: Derives tf-idf matrix from a given .csv file
# FOR: CS 4250- Assignment #1
# TIME SPENT: 1 Hour
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays

#Importing some Python libraries
import csv
import math

documents = []

#Reading the data in a csv file
with open('collection.csv', 'r') as csvfile:
  reader = csv.reader(csvfile)
  for i, row in enumerate(reader):
         if i > 0:  # skipping the header
            documents.append (row[0])

#Conducting stopword removal. Hint: use a set to define your stopwords.
#--> add your Python code here
stopWords = {"i", "she", "they", "and", "their", "her"}
documents_without_stopWords = []

for document in documents:
    non_stopWords = []
    content = document.split()

    #Remove stopwords
    for word in content:
        if word.lower() not in stopWords:
            non_stopWords.append(word)

    # Reconstruct documents without stopwords
    cleaned_document = " ".join(non_stopWords)
    documents_without_stopWords.append(cleaned_document)

#Conducting stemming. Hint: use a dictionary to map word variations to their stem.
#--> add your Python code here
stemming = {
    "loves" : "love",
    "cats" : "cat",
    "dogs" : "dog"
}
stemmed_documents = []

for document in documents_without_stopWords:
    stemmed_words = []
    content = document.split()

    for word in content:
        # Get the root word from the stemming dictionary, or keep the original word
        root_word = stemming.get(word, word)
        stemmed_words.append(root_word)

    # Reconstruct documents with stemmed words
    stemmed_document = " ".join(stemmed_words)
    stemmed_documents.append(stemmed_document)

#Identifying the index terms.
#--> add your Python code here
terms = []

for document in stemmed_documents:
    content = document.split()
    for word in content:
        if word not in terms:
            terms.append(word)


#Building the document-term matrix by using the tf-idf weights.
#--> add your Python code here
docTermMatrix = []

for document in stemmed_documents:
    # Split the document into words
    words = document.split()

    # Calculate tf for the current document
    tf_dict = {}
    for word in words:
        tf_dict[word] = tf_dict.get(word, 0) + 1
    for word, count in tf_dict.items():
        tf_dict[word] = count / len(words)

    # Calculate df for each term
    term_df = {}
    for term in terms:
        df = sum(1 for doc in stemmed_documents if term in doc)
        term_df[term] = df

    # Calculate idf for each term
    term_idf = {}
    for term in terms:
        term_idf[term] = math.log(abs(len(stemmed_documents) / term_df[term]), 10)

    # Build and append row to document term matrix
    docTermMatrixRow = []
    for term in terms:
        tf_idf = round(tf_dict.get(term, 0) * term_idf.get(term, 0), 2)
        docTermMatrixRow.append(tf_idf)

    docTermMatrix.append(docTermMatrixRow)

#Printing the document-term matrix.
#--> add your Python code here
for row in docTermMatrix:
    print(row)