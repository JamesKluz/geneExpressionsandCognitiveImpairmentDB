# CSCI49371 - Big Data
# Project 1
#
# Written by Gil Dekel
# Last updated: March 21st, 2017
#
# All MongoDB functions to parse input files
# and build the two tables: Patient info & ROSMAP

import os                           # file stats
import csv
import sys                          # argv
import pandas as pd

from lxml import etree              # for xml->json parsing
from xml.etree.ElementTree import fromstring  # parse json objects from strings
from xmljson import yahoo           # for yahoo style
from json import dumps              # pretty json formatting
import re, collections              # regext and deque
import json                         # more json formatting stuff
import time                         # for the benchmark class

from pymongo import MongoClient

# A benchmark class for timing algorithms
# Used mostly for timing our load/remove database functions
class Timer(object):
    def __init__(self): pass

    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        seconds = time.time() - self.tstart
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        print ("Elapsed: %d:%02d:%02d" % (h, m, s))
    

# This parser class allows us to parse the xml file incrementally
# EXPECTS:
#   @filename: filename of the file to parse
#   @reTerminator: the text terminating regular expression condition.
#   @blockSize: the size of data blocks to bring to memory (optional)
class EntryParser(object):
    
    def __init__(self, filename, reTerminator, blockSize=-1):
        self.f = open(filename)
        self.sentence_terminator = re.compile(reTerminator)
        self.blockSize = blockSize

        # a deque to incrementally collect a string between
        # regex delimiters
        self.buffer = collections.deque([''])

        # ignore the first two lines
        for _ in range(2):
            next(self.f)
    
    # Fetch the next slice of txt from the file        
    def next(self):
        while len(self.buffer) < 2:
            data = self.f.read(self.blockSize)
            if not data:
                return -1
            
            self.buffer += self.sentence_terminator.split(self.buffer.pop()+data)
        
        return self.buffer.popleft()
    
    def __iter__(self):
        return self

    def __exit__(self):
        self.f.close()


#############################################################################################
#                                    UNIPROT FUNCTIONS                                      #
#############################################################################################

# This function adds to - or build, if the collection is empty -
# the uniprot.human MongoDB database, which represents the uniprot-human.xml
# data file.
# EXPECTS:
#   @xmlUniport_f: The filepath/filename of the uniprot data file
def buildUniportDB(xmlUniport_f):
    # Open and connect to the mongodb database and collection
    # Documents will be added to the unipurt_human.uniport collection
    client = MongoClient()
    db = client.uniprot
    coll = db.human

    # Begin benchmark. Do everything while timer is running
    print('Parsing: %s' % xmlUniport_f)
    
    # Instantiate a new parser with the file input and a regex to
    # identify end events for entries in the xml, i.e. </entry>
    parser = EntryParser(xmlUniport_f, r'(?<=</entry>)\n')

    # setup progress bar
    toolbar_width = 40
    progress = 0
    filesize = os.stat(xmlUniport_f).st_size
    bucketsize = int(filesize/40)
    buckets = 0
    currentEntriesCount = coll.count()

    sys.stdout.write("|%s|0%%" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\r")

    #grab the first slice of the txt file
    elem = parser.next()
    while elem != -1:
        # update progress and buckets for the progress bar
        progress += sys.getsizeof(elem)
        buckets = int(progress/bucketsize)

        # parse the xml txt from the file into a json object (dictionary)
        # and insert it to the mongodb collection
        jsonObj = yahoo.data(etree.fromstring(elem))
        coll.insert_one(json.loads(dumps(jsonObj)))
        
        # redraw the bar with updated info
        sys.stdout.write("|" + chr(0x2586)*buckets \
            + " "*(toolbar_width-buckets)+ " |%d%%" % int(progress/filesize*100))
        sys.stdout.flush()
        sys.stdout.write("\r")

        # Keep getting the next slice of txt
        elem = parser.next()

    # draw final completed bar
    sys.stdout.write("|" + chr(0x2586)*toolbar_width + " |100%\n")
    sys.stdout.flush()

    # print summary
    print("[SUCCESS] " + xmlUniport_f + " was parsed and loaded successfully!")
    print("[STATS] %d new entries were added to uniprot.human." % (coll.count()-currentEntriesCount))

    # disconnect from mongod
    client.close()    

# This function expects a uniprot IDs list, iterates over it
# and uses each uniprot_id to query the uniprot.human database
# for assosiated gene information.
# EXPECTS:
#   @uniprots: a list of uniprot IDs.
# RETURN: a list of pandas DataFrames for ease of presentation.
#   NOTE: if nothing is return, an empty list it returned.
def getGeneInfo(uniprots):
    client = MongoClient()
    db = client.uniprot
    coll = db.human

    data = []

    for uni in uniprots:
        info = coll.find( { "entry.name": uni }, {'_id':0} )
        if info.count():
            data.append(pd.DataFrame(info[0]))

    client.close()
    return data

# This function expects a list of pandas DataFrame produced by
# getGeneInfo() and prints it in a controlled way.
# EXPECTS:
#   @entries: a list of pandas DataFrames that store <entry> objects
def printGeneInfo(entries):
    for e in entries:
        input("---- %s ----" % (e['entry']['name']))

        for c in list(e.index):
            input(c.upper() + ':')
            print(e['entry'][c], '\n')



#############################################################################################
#                                 PATIENT/ROSMAP FUNCTIONS                                  #
#############################################################################################


# This function checks if a given diagnosis query input
# matches one of the pre-defined classes: AD/MCI/NCI.
# For example, if diagnosisStr = AD/Ad/aD/ad, the function
# returns a list = ['4', '5']
# EXPECTS:
#   @diagnosisStr: a variable holding a diagnosis query
# RETURNS:
#   If a specific list is diagnosisStr is a valid matching str
#   else None if string doesnt match,
#   or the untouched diagnosisStr if it's not of type str
def getQueryFromTag(diagnosisStr):
    if isinstance(diagnosisStr, str):
        diagnosisStr = diagnosisStr.lower()
        if diagnosisStr == "ad":
            return ['4', '5']
        elif diagnosisStr == "nci":
            return ['1']
        elif diagnosisStr == "mci":
            return ['2', '3']
        elif diagnosisStr == "other":
            return ['6']
        else:
            return None
    else:
        return diagnosisStr

# This is a helper function for the getStatisticsByCustomQuery().
# Given a diagnosis query, if it's a string, append it directly to
# a list of dictionaries that will be used in the MongoDB query later on.
# else, if it is a list of diagnosis classes (e.g. [1,4,2]), iterate over it
# and construct a nested list of dictionaries.
# EXPECTS:
#   @queryList: a single string query, or a list of queries
# RETURNS:
#   @query: a list of dictionaries that will be used as a query value
#           in a MongoDB query in getStatisticsByCustomQuery()
def queryMaker(queryList):
    queryList = getQueryFromTag(queryList)
    query = []
    if isinstance(queryList, list):
        for q in queryList:
            query.append({'diagnosis': { '$eq': str(q)}})
    else:
        query.append({'diagnosis': { '$eq': str(queryList)}})

    return query


# This function takes in a custom query from the user (e.g. [1,4,2])
# and an entrez id, and prints a short statistics report which includes
# number of patients in the slice, the gene's mean value, and std.
# EXPECTS:
#   @diagnosis: custom query as a string or list of
#               diagnoses types (e.g. "AD" or [1,4,2])
#   @entrez_id: gene name (e.g. 6416)
# RETURNS:
#   @df: a pandas DataFrame() for ease of presentation
def getStatisticsByCustomQuery(diagnosis, entrez_id):
    client = MongoClient()
    db = client.rosmap
    coll = db.RNASeq

    query = queryMaker(getQueryFromTag(diagnosis))
    entrez_id = str(entrez_id)

    doc = coll.aggregate(
        [ 
            { '$match': { '$or': query } },
            { '$group': { '_id': 'null', 
                          'stdDev': { '$stdDevPop': "$"+entrez_id },
                          'mean': { '$avg': "$"+entrez_id},
                          '#patients': { '$sum':1 }
                        }
            } 
        ] 
    )
    df = pd.DataFrame(list(doc))
    if df.empty:
        return None
    else:
        del df['_id']
        df.index.name = entrez_id
#         df.insert(0, 'entrez_id',entrez_id)        

    client.close()
    return df

# This function is just a package that uses getStatisticsByCustomQuery()
# to get statistics for AD/MCI/NCI in a short and comfortable report.
# EXPECTS:
#   @entrez_id: the gene for which the user wants these statistics for
# RETURNS:
#   @df: a pandas DataFram() for ease of presentation
#        if information is successfully retrived
#   None: if nothing is found in the query
def getAD_MCI_NCI(entrez_id):
    dfAD = getStatisticsByCustomQuery("ad", entrez_id)
    dfMCI = getStatisticsByCustomQuery("mci",entrez_id)
    dfNCI = getStatisticsByCustomQuery("nci",entrez_id)

    if dfAD is not None:
        if dfMCI is not None:
            if dfNCI is not None:
                df = pd.DataFrame(columns=dfAD.columns ,index=["AD","MCI","NCI"])
                df.index.name = dfAD.index.name
                df.loc["AD"] = dfAD.loc[0]
                df.loc["MCI"] = dfMCI.loc[0]
                df.loc["NCI"] = dfNCI.loc[0]
            
                return df

    return None

# A simple function to retrive patient information from the
# rosmap.RNASeq MongoDB database.
# We ommit all entrez id fields from the returned report.
# EXPECTS:
#   @patient_id: a patient's id (e.g.: X448_120507)
# RETURN:
#   None: if patient's ID is not the in the table
#   @df: a pandas DataFrame() for ease or presentation
def getPatientInfo(patient_id):
    client = MongoClient()
    db = client.rosmap
    coll = db.RNASeq

    doc = coll.find({"_id" : patient_id}, {"_id":1, "age":1, "gender":1, "education":1, "diagnosis":1})
    client.close()

    if doc.count() == 0:
        return None
        # print("Patient id: %s not found" % patient_id)
    else:
        df = pd.DataFrame(doc[0], index=[''])
        cols = df.columns.tolist()
        cols = [cols[0], cols[1], cols[-1], cols[3], cols[2]]
        df = df[cols]
        return df


# A function to manually insert a single patient to the rosmap.RNASeq collection.
# EXPECTS:
#   @patient_id: the new patient's ID. Should be
#                unique, otherwise the insertion is rejected
#   @age: new patient's age
#   @gender: new patient's gender
#   @education: new patient's education
#   @diagnosis: new patient's diagnosis
# RETURNS:
#   True if patient added successfully, False otherwise
def inserNewPatientFromUI(patient_id, age, gender, education, diagnosis):
    client = MongoClient()
    db = client.rosmap
    coll = db.RNASeq

    newPatient = {'diagnosis': diagnosis,
                  'education': education,
                  '_id': patient_id,
                  'gender': gender,
                  'age': age
                 }

    print(newPatient)

    try:
        coll.insert(newPatient)
        return True
    except:
        return False

    client.close()

# This function adds to - or build, if the collection is empty -
# the rosmap.RNASeq MongoDB database, which represents a join of
# the patients.csv and ROSMAP_RNASeq_entrez.csv data files.
# EXPECTS:
#   @patients_f: the patients.csv filepath string
#   @rosmap_f: the ROSMAP_RNASeq_entrez.csv filepath string
#   @overwrite: A flag to deny/allow overwriting existing patients
def insertNewPatientsFromFile(patient_f, rosmap_f, overwrite=False):
    # Open the patients file, get the number of rows
    # and open it again to reset the file pointer
    print("Opening: %s ......" % patient_f)
    try:
        patients = open(patient_f, 'r')
        numOfPatients = len(list(patients))-1
        patients.close()
        patients = open(patient_f, 'r')
    except IOError:
        print("[ERROR] FileNotFoundError: No such file or directory: %s" % patient_f)
        return None

    # Open the ROSMAP file
    try:
        print("Opening: %s ......" % rosmap_f)
        rosmap = open(rosmap_f, 'r')
    except IOError:
        print("[ERROR] FileNotFoundError: No such file or directory: %s" % rosmap_f)
        return None

    pReader = csv.DictReader(patients, restkey=None, restval=None, dialect='excel')
    rReader = csv.DictReader(rosmap, restkey=None, restval=None, dialect='excel')
    pReader.fieldnames.extend(["diagnosis"])
    pReader.fieldnames.insert(0,"_id")
    pReader.fieldnames.remove('patient_id')
    pReader.fieldnames.extend(rReader.fieldnames[2:])


    # Open and connect to the mongodb database and collection
    # Documents will be added to the rosmap.RNASeq collection
    client = MongoClient()
    db = client.rosmap
    coll = db.RNASeq

    print('Parsing: %s and %s' % (patient_f, rosmap_f))
    toolbar_width = 40
    progress = 0
    bucketsize = int(numOfPatients/40)
    buckets = 0
    currentEntriesCount = coll.count()

    # setup progress bar
    sys.stdout.write("|%s|0%%" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\r")

    fails = []
    writes = 0
    for r1,r2 in zip(pReader, rReader):
        progress += 1
        buckets = int(progress/bucketsize)

        r1['diagnosis'] = r2['DIAGNOSIS']

        for f in pReader.fieldnames[5:]:
            if r2[f] == '':
                r1[f] = 0
            else:
                r1[f] = float(r2[f])
        
        try:
            coll.insert_one(json.loads(dumps(r1)))
            writes += 1
        except:
            if overwrite:
                coll.remove({'_id': r1['_id']})
                coll.insert_one(json.loads(dumps(r1)))
                writes += 1

            else:
                fails.append(r1['_id'])  

        # redraw the bar with updated info
        sys.stdout.write("|" + chr(0x2586)*buckets \
            + " "*(toolbar_width-buckets)+ " |%d%%" % int(progress/numOfPatients*100))
        sys.stdout.flush()
        sys.stdout.write("\r")

    # draw final completed bar
    sys.stdout.write("|" + chr(0x2586)*toolbar_width + " |100%\n")
    sys.stdout.flush()

    # print summary
    print("[SUCCESS] " + patient_f + " and " + rosmap_f + " were joined and loaded successfully!")
    print("[STATS] %d new entries were written to rosmap.RNASeq." % writes) 
    
    # if not overwrite:
    #     print("[FAILS]:\n",fails)   

    patients.close()
    rosmap.close()
    client.close()

# A function to remove a single patient by patient ID
# EXPECTS:
#   @patient_id: a patient's id (e.g.: X448_120507)
# RETURNS:
#   positive number if a patient was removed
#   zero if none were removed
def removePatient(patient_id):
    client = MongoClient()
    db = client.rosmap
    coll = db.RNASeq

    result = coll.delete_one({'_id': patient_id})

    client.close()
    return result.deleted_count

# Drop (remove) the rosmap.RNASeq db
def dropRosmapDB():
    client = MongoClient()
    client.drop_database('rosmap')
    client.close()

# Drop (remove) the uniprot.human db
def dropUniprotDB():
    client = MongoClient()
    client.drop_database('uniprot')
    client.close()

