import MySQLdb
import sys

#These parameters can be changed to match the user's settings
db_path_sql = "localhost"
db_user_sql = "root"
db_pass_sql = "52637dualred"
db_name = "BIG_DATA_PROJECT1"

#This function loads the database from scratch from a file that is
#expected to be tab de-limited
def loadSql(filename, number_of_header_lines):
  # Open database connection
  db = MySQLdb.connect("localhost","root","52637dualred","BIG_DATA_PROJECT1" )

  # prepare a cursor object using cursor() method
  cursor = db.cursor()

  # Drop table if it already exist using execute() method.
  try:
    cursor.execute("DROP TABLE IF EXISTS ENTREZ_UNIPROT")
  except:
    pass
  # Create table as per requirement
  sql = """CREATE TABLE ENTREZ_UNIPROT (
           ENTREZ_ID  CHAR(20) NOT NULL,
           UNIPROT_ID CHAR(30) NOT NULL,  
           GENE_NAME CHAR(150) )"""

  cursor.execute(sql)
  #count the number of entries
  num_rows = 0
  with open(filename) as source_file:
    token = source_file.readline()
    while token:
      num_rows+=1
      token = source_file.readline()
  with open(filename) as source_file:
    counter = 0
    while counter < number_of_header_lines:
      token = source_file.readline()
      counter += 1
    counter = 1
    token = source_file.readline()
    toolbar_width = 40
    progress = 0
    bucketsize = int(num_rows/40)
    buckets = 0
    # setup progress bar
    sys.stdout.write("|%s|0%%" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\r")

    while token:
      progress += 1
      buckets = int(progress/bucketsize)
      #split line into list 
      token_list = token.split()
      token = source_file.readline()
      gene_name = token_list[2]
      i = 3
      while i < len(token_list):
        gene_name = gene_name + " " + token_list[i]
        i+=1
      gene_name = gene_name.replace("'", "\'\'")
      sql = "INSERT INTO ENTREZ_UNIPROT(ENTREZ_ID, \
               UNIPROT_ID, GENE_NAME) \
               VALUES (\'%s\', \'%s\', \'%s\')" % \
               (token_list[0], token_list[1], gene_name)
      try:
        # Execute the SQL command
        cursor.execute(sql)
        # Commit your changes in the database
        db.commit()
      except:
        # Rollback in case there is any error
        db.rollback()
        print("error loading: " + " " + token_list[0] + " " + token_list[1] + " +++ " + gene_name)  
      # redraw the bar with updated info
      sys.stdout.write("|" + chr(0x2586)*buckets \
          + " "*(toolbar_width-buckets)+ " |%d%%" % int(progress/num_rows*100))
      sys.stdout.flush()
      sys.stdout.write("\r")
  # draw final completed bar
  sys.stdout.write("|" + chr(0x2586)*toolbar_width + " |100%\n")
  sys.stdout.flush()
  # disconnect from server
  db.close()

#This function loads a single entry to the database
def loadSingleConnection(entrez_id, uniprot_id, gene_name):
  db = MySQLdb.connect(db_path_sql, db_user_sql, db_pass_sql, db_name)
  # prepare a cursor object using cursor() method
  cursor = db.cursor()
  sql = "SELECT * FROM ENTREZ_UNIPROT WHERE ENTREZ_ID = \'%s\' AND UNIPROT_ID = \'%s\';" % (entrez_id, uniprot_id)
  cursor.execute(sql)
  rows = cursor.fetchall()
  if rows:
    db.close()
    return False
  else:
    gene_name = gene_name.replace("'", "\'\'")
    sql = "INSERT INTO ENTREZ_UNIPROT(ENTREZ_ID, \
              UNIPROT_ID, GENE_NAME) \
              VALUES (\'%s\', \'%s\', \'%s\')" % \
              (entrez_id, uniprot_id, gene_name)
    try:
      # Execute the SQL command
      cursor.execute(sql)
      # Commit your changes in the database
      db.commit()
      db.close()
      return True
    except:
      # Rollback in case there is any error
      db.rollback()
      db.close()
      return False 
    # disconnect from server
  db.close()

#This function loads the database from scratch from a file that is
#expected to be tab de-limited
def loadAdditionalFile(filename, number_of_header_lines, overwrite = False):
  # Open database connection
  try:
    db = MySQLdb.connect("localhost","root","52637dualred","BIG_DATA_PROJECT1" )
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    with open(filename) as source_file:
      counter = 0
      while counter < number_of_header_lines:
        token = source_file.readline()
        counter += 1
      counter = 1
      token = source_file.readline()
      while token:
        #split line into list 
        token_list = token.split()
        token = source_file.readline()
        gene_name = token_list[2]
        i = 3
        while i < len(token_list):
          gene_name = gene_name + " " + token_list[i]
          i+=1
        gene_name = gene_name.replace("'", "\'\'")
        sql = "SELECT * FROM ENTREZ_UNIPROT WHERE ENTREZ_ID = \'%s\' AND UNIPROT_ID = \'%s\';" % (token_list[0], token_list[1])
        cursor.execute(sql)
        rows = cursor.fetchall()
        write_to_db = True
        if rows:
          if overwrite:
            delstatmt = "DELETE FROM ENTREZ_UNIPROT WHERE entrez_id = \'%s\' AND uniprot_id = \'%s\'" % (token_list[0], token_list[1])
            try:
              # Execute the SQL command
              cursor.execute(delstatmt)
              # Commit your changes in the database
              db.commit()
              print("overwriting entry with key:", token_list[0], token_list[1])
            except:
              # Rollback in case there is any error
              db.rollback()
              print("error deleting entry with key:", token_list[0], token_list[1])
          else:      
            print("Entry with primary key", token_list[0], token_list[1], "already exists!")
            write_to_db = False

        if write_to_db:
          sql = "INSERT INTO ENTREZ_UNIPROT(ENTREZ_ID, \
                   UNIPROT_ID, GENE_NAME) \
                   VALUES (\'%s\', \'%s\', \'%s\')" % \
                   (token_list[0], token_list[1], gene_name)
          try:
            # Execute the SQL command
            cursor.execute(sql)
            # Commit your changes in the database
            db.commit()
          except:
            # Rollback in case there is any error
            db.rollback()
            print("error loading: " + " " + token_list[0] + " " + token_list[1] + ": " + gene_name)  
    # disconnect from server
    db.close()
    return True
  except: 
    print("File does not exist or is formatted incorrectly")
    return False

#Delete row associated to the key entrez_id uniprot_id
def deleteRow(entrez_id, uniprot_id):
  db = MySQLdb.connect(db_path_sql, db_user_sql, db_pass_sql, db_name)
  # prepare a cursor object using cursor() method
  cursor = db.cursor()
  sql = "SELECT * FROM ENTREZ_UNIPROT WHERE ENTREZ_ID = \'%s\' AND UNIPROT_ID = \'%s\';" % (entrez_id, uniprot_id)
  cursor.execute(sql)
  rows = cursor.fetchall()
  deleted = False
  if rows:
    delstatmt = "DELETE FROM ENTREZ_UNIPROT WHERE entrez_id = \'%s\' AND uniprot_id = \'%s\'" % (entrez_id, uniprot_id)
    try:
      # Execute the SQL command
      cursor.execute(delstatmt)
      # Commit your changes in the database
      db.commit()
      deleted = True
    except:
      # Rollback in case there is any error
      db.rollback()
      print("error deleting entry with key: " + " " + entrez_id + " " + uniprot_id)
  else:
    print("Entry with primary key", entrez_id, uniprot_id, "doesn't exists!")
  db.close()
  return deleted
  
#returns a list of uniprot id's associated to the entered entrez_id
def getUniprot(entrez_id):
  db = MySQLdb.connect(db_path_sql, db_user_sql, db_pass_sql, db_name)
  # prepare a cursor object using cursor() method
  cursor = db.cursor()
  sql = "SELECT * FROM ENTREZ_UNIPROT WHERE ENTREZ_ID = \'%s\';" % entrez_id
  results = []
  try:
    # Execute the SQL command
    cursor.execute(sql)
    # Fetch all the rows in a list of lists.
    rows = cursor.fetchall()
    if rows:
      for r in rows:
        results.append(r[1])
  except:
    print("Query failed")
  return results

#Drops the table from the database
def dropTable():
  db = MySQLdb.connect("localhost","root","52637dualred","BIG_DATA_PROJECT1" )
  # prepare a cursor object using cursor() method
  cursor = db.cursor()
  # Drop table if it already exist using execute() method.
  print("Dropping entrez_id -> uniprot_id table...")
  try:
    cursor.execute("DROP TABLE IF EXISTS ENTREZ_UNIPROT")
  except:
    print("Entrez_uniprot table does not exist")


