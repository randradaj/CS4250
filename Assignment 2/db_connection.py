#-------------------------------------------------------------------------
# AUTHOR: Ryan Andrada
# FILENAME: db_connection.py
# SPECIFICATION: Connects to databse using psycopg2 and defines/performs database operations
# FOR: CS 4250- Assignment #2
# TIME SPENT: 4 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays
#importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():

    DB_NAME = "CS4250 Assignment 2"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database connection unsuccessful")

def createCategory(cur, catId, catName):
    sql = '''CREATE TABLE IF NOT EXISTS Categories
    (
        catId integer NOT NULL,
        catName character varying,
        PRIMARY KEY (catId)
    );'''
    cur.execute(sql)

    # Insert a category in the database
    sql = "Insert into Categories (catId, catName) Values (%s, %s)"
    recset = [catId, catName]

    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    sql = '''CREATE TABLE IF NOT EXISTS Documents
    (
        docId integer NOT NULL,
        text character varying,
        title character varying,
        numChars integer,
        date date,
        category integer,
        PRIMARY KEY (docId),
        CONSTRAINT categoryId FOREIGN KEY (category) REFERENCES Categories (catId)
    );'''
    cur.execute(sql)

    # 1 Get the category id based on the informed category name
    cur.execute("SELECT catId FROM Categories WHERE catName = %s", (docCat,))
    categoryId = int(cur.fetchone()['catid'])

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    filteredText = ''
    for char in docText:
        if char.isalnum():
            filteredText += char

    numChars = len(filteredText)

    sql = "INSERT INTO Documents (docId, text, title, numChars, date, category) VALUES (%s, %s, %s, %s, %s, %s)"
    recset = [docId, docText, docTitle, numChars, docDate, categoryId]
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    sql = '''CREATE TABLE IF NOT EXISTS Terms
    (
        term character varying NOT NULL,
        numChars integer NOT NULL,
        PRIMARY KEY (term)
    );'''
    cur.execute(sql)

    filteredText = ''
    for char in docText:
        if char.isalnum() or char.isspace():
            filteredText += char

    filteredText = filteredText.lower()
    terms = filteredText.split()

    newTerms = []
    for term in terms:
        cur.execute("SELECT COUNT(*) From Terms WHERE term=%s", (term,))
        if cur.fetchone()['count'] == 0 and term not in newTerms:
            newTerms.append(term)

    if (len(newTerms) > 0):
        sql = "INSERT INTO Terms (term, numChars) VALUES (%s, %s)"
        for term in newTerms:
            numChars = len(term)
            recset = [term, numChars]
            cur.execute(sql, recset)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    sql = '''CREATE TABLE IF NOT EXISTS Index
    (
        docId integer NOT NULL,
        term character varying NOT NULL,
        termCount integer NOT NULL,
        PRIMARY KEY (docId,term),
        CONSTRAINT docId FOREIGN KEY (docId) REFERENCES Documents (docId),
        CONSTRAINT term FOREIGN KEY (term) REFERENCES Terms (term)
    );'''
    cur.execute(sql)

    termCounts = {}
    for term in terms:
        if term in termCounts:
            termCounts[term] += 1
        else:
            termCounts[term] = 1

    for term, termCount in termCounts.items():
        sql = "INSERT INTO Index (docId, term, termCount) VALUES (%s, %s, %s)"
        recset = [docId, term, termCount]
        cur.execute(sql, recset)

def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    cur.execute("SELECT term FROM Index WHERE docId=%s", (docId,))
    indexRows = cur.fetchall()

    for row in indexRows:
        term = row['term']
        cur.execute("DELETE FROM Index WHERE docId=%s AND term=%s", (docId, term))

        cur.execute("SELECT COUNT(*) FROM Index WHERE term=%s", (term,))
        termCount = dict(cur.fetchone())
        if termCount['count'] == 0:
            cur.execute("DELETE FROM Terms WHERE term=%s", (term,))

    # 2 Delete the document from the database
    cur.execute("DELETE FROM Documents WHERE docId=%s", (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    index = {}

    cur.execute(
        "SELECT Index.term, Documents.title, Index.termCount FROM Index INNER JOIN Documents ON Index.docId = Documents.docId")
    indexRows = cur.fetchall()

    for row in indexRows:
        term = row['term']
        title = row['title']
        termCount = row['termcount']

        if term not in index:
            index[term] = {}

        if title not in index[term]:
            index[term][title] = termCount
        else:
            index[term][title] += termCount

    index_formatted = {}
    for term, docCounts in index.items():
        doc_count_str = ", ".join([f"{doc}:{count}" for doc, count in docCounts.items()])
        index_formatted[term] = doc_count_str

    return index_formatted
