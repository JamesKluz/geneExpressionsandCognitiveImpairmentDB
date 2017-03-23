import sys
import mongo_queries as mq
import input_validators as validators
import neo4j as neo
import entrez_uniprot_sql as eu

#variables for input file paths
interaction_file = "datafiles/BIOGRID-MV-Physical-3.4.144.tab2"
entrez_uniprot_file = "datafiles/entrez_ids_uniprot.txt"
patient_file = "datafiles/patients.csv"
rosmap_file = "datafiles/ROSMAP_RNASeq_entrez.csv"
uniprot_xml_file = "datafiles/uniprot-human.xml"

#This function displays the entrance screen
def genesis():
  user_input = ''
  while user_input != 'q':
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) to load all tables and graphs from scratch. 
    (2) to start working. 
    (3) drop all tables
    (q) to quit.
    >> """)
    if user_input == '1':
      user_input = input("Are you sure? This will delete and reload the entire database (y/n) >> ")
      user_input = validators.isYorN(user_input)
      if user_input == 'y':
        clearAll()
        loadAll()
    elif user_input == '2':
      workingMenu()
    elif user_input == '3':
      clearAll()

#This function displays the working menu
def workingMenu():
  user_input = ''
  while user_input != 'b' and user_input != 'q': 
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) Query database. 
    (2) Add/remove data from database
    (q) to quit.
    (b) go back.
    >> """)
    if user_input == '1':
      queryMenu()
    elif user_input == '2':
      addRemoveMenu()
  if user_input == 'q':
    sys.exit()

#This function brings up the query menu
#here the user chooses which type of query they would like to make
def queryMenu():
  user_input = ''
  while user_input != 'b' and user_input != 'q':
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) Find a gene's N-order interacting genes. 
    (2) Get mean/std of gene expression for AD/MCI/NCI.
    (3) Get all information associated with a gene.
    (4) Find a patient's information.
    (q) to quit.
    (b) go back.
    >> """)
    if user_input == '1':
      nOrderMenu()
    elif user_input == '2':
      statisticsMenu()
    elif user_input == '3':
      geneInfoMenu()
    elif user_input == '4':
      patientInfoMenu()
  if user_input == 'q':
    sys.exit()

#This function brings up a list of options for adding/removing data from the database
def addRemoveMenu():
  user_input = ''
  while user_input != 'b' and user_input != 'q':
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) add/remove from gene interaction graph database. 
    (2) add/remove from entrez->uniprot table.
    (3) add/remove from Patient-ROSMAP table.
    (q) to quit.
    (b) go back.
    >> """)
    if user_input == '1':
      editInteractionGraph()
    elif user_input == '2':
      editEntrezUniprotTable()
    elif user_input == '3':
      editPatientTable()
  if user_input == 'q':
    sys.exit()

#This function brings up the menu that guides the user through querying n-order 
#gene interactins
def nOrderMenu():
  user_input = ''
  while user_input != 'b' and user_input != 'q':
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) Get all n-order interacting genes for a gene.
    (2) Get all genes reachable in n interactions or less.
    (q) to quit.
    (b) to go back. 
    >> """)
    if user_input == '1' or user_input == '2':
      gene_name = input("Enter entrez id: ")
      n = input("Enter an n: ")
      n = validators.isInt(n)
      if n > 3:
        print("\n---------------------------------------------------------------------")
        print("For n > 3 the results can be quite large, are you sure?")
        user_sure = input("(y/n) >> ")
        user_sure = validators.isYorN(user_sure)
        if user_sure == 'n':
          continue
      if user_input == '1':
        interacting_nodes = neo.nOrderInteractors(gene_name, n, True)
      else:
        interacting_nodes = neo.nOrderInteractors(gene_name, n, False)
      if interacting_nodes:
        print("Results:")
        for g in interacting_nodes:
          print(g, end=' ')
        print('')

  if user_input == 'q':
    sys.exit()

#This function brings up the menu to guide the user through getting
#gene expression statistics
def statisticsMenu():
  user_input = ''
  while user_input != 'b':
    user_input = input("""Choose from the following:
    (1) get mean and std by gene for AD/MCI/NCI. 
    (2) get mean and std by custom query.
    (b) go back.
    >> """)
    if user_input == '1':     
      entrez_id = input("Enter the gene name (i.e. entrez id): ")
      result = mq.getAD_MCI_NCI(entrez_id)

      
      if result is not None:
        print("--------------------------------")
        print(result)
        print("--------------------------------\n\n") 
      else:
        print("----------------------------------------------------")
        print('[ERROR] Could not retrive data. Tables may be empty.')          
        print("----------------------------------------------------\n\n") 

    elif user_input == '2':
      print("""Enter diagnosis clases. For example: 1,3,6:""")
      c = input(">> ")
      c = c.replace(" ", "")
      c = c.split(',')

      entrez_id = input("Enter the gene name (i.e. entrez id): ")
      print("--------------------------------")
      print(mq.getStatisticsByCustomQuery(c, entrez_id))
      print("--------------------------------\n\n") 

#This function brings up the menu to guide the user through getting
#gene information
def geneInfoMenu():
  entrez_id = input("Please enter the gene name (i.e. entrez id):\n>> ")
  uniprots = eu.getUniprot(entrez_id)

  if uniprots:
    print("-------------------------------------")
    print('Gene info report for entrez id: %s' % entrez_id)
    print("-------------------------------------")
    input("Press Enter(s) to continue...")
    print('\n')
    mq.printGeneInfo(mq.getGeneInfo(uniprots))
  else:
    print("-----------------------------")
    print('No mapping for gene: %s' % entrez_id)
    print("-----------------------------\n\n")

#This function brings up the menu to guide the user through getting
#patient information
def patientInfoMenu():
    patient_id = input("Enter the patient's ID: ")

    print("--------------------------------------------")
    print(mq.getPatientInfo(patient_id))
    print("--------------------------------------------\n\n")

#This function brings up the menu to guide the user through adding
#or removing entries from the interaction graph
def editInteractionGraph():
  user_input = ''
  while user_input != 'b' and user_input != 'q':
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) Add interactions
    (2) Remove interactions
    (3) Remove genes
    (q) to quit
    (b) to go back. 
    >> """)
    if user_input == '1':
      print("\n---------------------------------------------------------------------")
      user_input = input("""Choose from the following:
    (1) Add interactions from a file
    (2) Add single interaction manually
    (q) to quit
    (b) go back
    >> """)
      if user_input == '1':
        file_name = input("enter file path: ")
        header_size = input("enter length of file header in lines: ")
        header_size = validators.isInt(header_size)
        if neo.loadAdditionalFile(file_name, header_size):
          print("interactions added succesfully.")
      elif user_input == '2':
        node_1 = input("Enter the entrez_id for the first gene: ")
        node_2 = input("Enter the entrez_id for the second gene: ")
        if neo.addConnection(node_1, node_2):
          print("interaction added successfully.")
        else:
          print("interaction already exists.")
    elif user_input == '2':
      node_1 = input("Enter the entrez_id for the first gene: ")
      node_2 = input("Enter the entrez_id for the second gene: ")
      if neo.removeConnection(node_1, node_2):
        print("interaction removed successfully.")
      else:
        print("interaction does not exist.")
    elif user_input == '3':
      node_1 = input("Enter the entrez_id for the gene to be removed: ")
      if neo.removeNode(node_1):
        print("gene removed successfully.")
      else:
        print("gene does not exist.")
  if user_input == 'q':
    sys.exit()

#This function brings up the menu to guide the user through adding
#or removing entries from the entrez->uniprot mapping table
def editEntrezUniprotTable():
  user_input = ''
  while user_input != 'b' and user_input != 'q':
    print("\n---------------------------------------------------------------------")
    user_input = input("""Choose from the following:
    (1) Add mappings (entrez -> uniprot)
    (2) Remove mappings (entrez -> uniprot)
    (q) to quit
    (b) to go back. 
    >> """)
    if user_input == '1':
      print("\n---------------------------------------------------------------------")
      user_input = input("""Choose from the following:
    (1) Add mappings from a file
    (2) Add single mapping manually
    (q) to quit
    (b) go back
    >> """)
      if user_input == '1':
        file_name = input("enter file path: ")
        header_size = input("enter length of file header in lines: ")
        header_size = validators.isInt(header_size)
        over_write = input("over write existing mappings? (y/n) >> ")
        over_write = validators.isYorN(over_write)
        if over_write =='y':
          over_write = True
        else:
          over_write = False
        if eu.loadAdditionalFile(file_name, header_size, over_write):
          print("Connections added succesfully.")
      elif user_input == '2':
        entrez_id = input("Enter the entrez_id: ")
        uniprot_id = input("Enter the uniprot_id: ")
        gene_name = input("Enter the gene name: ")
        if eu.loadSingleConnection(entrez_id, uniprot_id, gene_name):
          print("interaction added successfully.")
        else:
          print("interaction already exists.")
    elif user_input == '2':
      entrez_id = input("Enter the entrez_id: ")
      uniprot_id = input("Enter the uniprot_id: ")
      if eu.deleteRow(entrez_id, uniprot_id):
        print("Row with key", entrez_id, uniprot_id, "was removed successfully")
      else:
        print("deletion not successful")
  if user_input == 'q':
    sys.exit()

#This function brings up the menu to guide the user through adding
#or removing entries from the patient info table
def editPatientTable():
  user_input = ''
  while user_input != 'b':
    user_input = input("""Add/Remove from Patient-ROSMAP:
    (1) ADD new patient manually.
    (2) ADD patients from file.
    (3) REMOVE patient by patient's ID.
    (b) go back.
    >> """)
    if user_input == '1':
      print("Please enter the patient's:")
      patient_id = input("ID: ")
      age = input("Age: ")
      gender = input("Gender: ")
      education = input("Education: ")
      diagnosis = input("Diagnosis: ")

      print("--------------------------------------------------")
      if mq.inserNewPatientFromUI(patient_id, age, gender, education, diagnosis):
        print('[SUCCESS] Patient %s was added successfully to the database!' % patient_id)  
      else:
        print('[ERROR] Patient %s is already in the database.' % patient_id)
      print("--------------------------------------------------\n\n")

    elif user_input == '2':
      patient_f = input("Please enter the new patients' filename:\n>> ")
      rosmap_f = input("Please enter the new ROSMAP filename:\n>> ")
      overwrite = input("Overwrite existing patients? [y\\n] ")
      overwrite = overwrite.lower()
      if overwrite == "y":
        overwrite = True
      else:
        overwrite = False

      mq.insertNewPatientsFromFile(patient_f, rosmap_f, overwrite)
      print('\n')

    elif user_input == '3':
      patient_id = input("Please enter the patient's ID:\n>> ")

      print("----------------------------------------------------------------")
      if mq.removePatient(patient_id):
        print('[SUCCESS] Patient %s was REMOVED from the database successfully!' % patient_id)  
      else:
        print('[ERROR] Patient %s is NOT in the database.' % patient_id)  
      print("----------------------------------------------------------------\n\n")

#This function clears all tables and graphs in the database
def clearAll():
  neo.dropGraph()
  eu.dropTable()
  print("Dropping Paient-ROSMAP table...")
  mq.dropRosmapDB()
  print("Dropping uniprot_human table...")
  mq.dropUniprotDB()

#This function loads the entire database. It expects that clearAll() has been
#run first
def loadAll():
  with mq.Timer():
    with mq.Timer():
      neo.loadNeo4j(interaction_file, 1)
    with mq.Timer():
      eu.loadSql(entrez_uniprot_file, 1)
    with mq.Timer():
      mq.insertNewPatientsFromFile(patient_file, rosmap_file)
    with mq.Timer():
      mq.buildUniportDB(uniprot_xml_file)

#Start the entrance screen
genesis()
