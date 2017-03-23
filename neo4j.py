from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase
import sys

#These parameters can be changed to match the user's settings
db_path = "http://localhost:7474"
db_username = "neo4j"
db_password = "jamesandgil"
db = GraphDatabase(db_path, username=db_username, password=db_password)

#This function is for creating a new database from a file.
#The file is expected to be in the BIOGRID format:
def loadNeo4j(filename, number_of_header_lines):
  #A dictionary from a gene to a list of genes it interacts with
  gene_dict = dict()
  dict_of_connections = dict()
  with open(filename) as neoFile:
    #kill header and give token a non-null value
    counter = 0;
    while counter < number_of_header_lines:
      token = neoFile.readline()
      counter+=1
    #load entries
    counter = 0
    token = neoFile.readline()
    print("Loading gene interaction graph:")
    while token:
      #split line into list 
      token_list = token.split()
      inter_A = token_list[1]
      inter_B = token_list[2]
      #connections don't have direction so this step
      #helps us avoid extraneous connections  
      if inter_A < inter_B:
        temp = inter_A
        inter_A = inter_B
        inter_B = temp
      connection = inter_A + " " + inter_B
      if connection not in dict_of_connections:
        dict_of_connections[connection] = True
        #check to see if 'inter_A' is in dictionary if not add new entry
        if inter_A in gene_dict:
          gene_dict[inter_A].append(inter_B)
        else:
          counter +=1
          gene_dict[inter_A] = [inter_B] 
      #get next line
      token = neoFile.readline()
    toolbar_width = 40
    progress = 0
    bucketsize = int(counter/40)
    buckets = 0

    # setup progress bar
    sys.stdout.write("|%s|0%%" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\r")

  #define a class Gene
  genes = db.labels.create("Gene")
  dict_of_genes_in_db = dict()
  #iterate through gene_dict and add genes and connections to DB
  for inter_A in gene_dict:
    progress += 1
    buckets = int(progress/bucketsize)
    #Get list of "interacts with" genes
    inter_B_list = gene_dict[inter_A]
    if inter_A not in dict_of_genes_in_db:
      g1 = db.nodes.create(name=inter_A)
      genes.add(g1)  
      #dict_of_genes_in_db[inter_A] = g1.id
      dict_of_genes_in_db[inter_A] = g1
    else:
      #g1 = db.nodes.get(dict_of_genes_in_db[inter_A])
      g1 = dict_of_genes_in_db[inter_A]
    for inter_B in inter_B_list:
      #Key for this connection
      if inter_A == inter_B:
        g1.relationships.create("interacts_with", g1)  
      else:
        if inter_B not in dict_of_genes_in_db:
          g2 = db.nodes.create(name=inter_B)
          genes.add(g2)  
          #dict_of_genes_in_db[inter_B] = g2.id
          dict_of_genes_in_db[inter_B] = g2
        else:
          #g2 = db.nodes.get(dict_of_genes_in_db[inter_B])
          g2 = dict_of_genes_in_db[inter_B]
        g1.relationships.create("interacts_with", g2)
        g2.relationships.create("interacts_with", g1)
    # redraw the bar with updated info
    sys.stdout.write("|" + chr(0x2586)*buckets \
        + " "*(toolbar_width-buckets)+ " |%d%%" % int(progress/counter*100))
    sys.stdout.flush()
    sys.stdout.write("\r")
  # draw final completed bar
  sys.stdout.write("|" + chr(0x2586)*toolbar_width + " |100%\n")
  sys.stdout.flush()
#returns true if node named "gene" is in the database
def isNodeInDatabase(gene):
  q = 'MATCH (g:Gene) WHERE g.name="' + gene + '" RETURN g'
  results = db.query(q, returns=client.Node)
  #this node exists
  if results:
    return True
  else:
    return False

#Checks to see if nodes are connected
def isConnected(gene_1, gene_2):
  q = 'MATCH p = (g:Gene)-[r:interacts_with]->(m:Gene) WHERE g.name="' + gene_1 + '" AND m.name = "' + gene_2 + '" RETURN p'
  results = db.query(q, returns=str)
  if results:
    return True
  else:
    return False

#adds a connection between nodes, if nodes don't exist in database, they are added, if already connected noting happens
def addConnection(gene_1, gene_2):
  if isConnected(gene_1, gene_2):
    return False
  else:
    genes = db.labels.create("Gene")
    q = 'MATCH (g:Gene) WHERE g.name="' + gene_1 + '" RETURN g'
    results = db.query(q, returns=client.Node)
    if results:
      g1 = results[0][0]
    else:
      g1 = db.nodes.create(name=gene_1)
      genes.add(g1)  
    q = 'MATCH (g:Gene) WHERE g.name="' + gene_2 + '" RETURN g'
    results = db.query(q, returns=client.Node)
    if results:
      g2 = results[0][0]
    else:
      g2 = db.nodes.create(name=gene_2)
      genes.add(g2)  
    g1.relationships.create("interacts_with", g2)
    g2.relationships.create("interacts_with", g1)
    return True

#This function is for adding extra nodes and connections from a file
#Since it checks to see if nodes / connections are already present
#it is much slower than 
def loadAdditionalFile(filename, number_of_header_lines):
  try:
    with open(filename) as neoFile:
      #kill header and give token a non-null value
      counter = 0;
      while counter < number_of_header_lines:
        token = neoFile.readline()
        counter+=1
      #load entries
      token = neoFile.readline()
      while token:
        #split line into list 
        token_list = token.split()
        gene_1 = token_list[1]
        gene_2 = token_list[2]
        addConnection(gene_1, gene_2)
        #get next line
        token = neoFile.readline()
      return True
  except:
    print("File does not exist or is formatted incorrectly")
    return False

#Removes a connection between 2 nodes, returns False if nodes already not connected
#This needs to be called twice (in reverse) to remove bi-connected nodes 
def removeConnection(gene_1, gene_2):
  q = 'MATCH (g:Gene)-[r:interacts_with]->(m:Gene) WHERE g.name="' + gene_1 + '" AND m.name ="' + gene_2 + '" RETURN g, r, m'
  results = db.query(q, returns=(client.Node, client.Relationship, client.Node))
  if results:
    r1 = results[0][1]
    r1.delete() 
    return True
  else:
    return False
    
#removes node and all of the relationships associated with this node
def removeNode(gene):
  q = 'MATCH (g:Gene) WHERE g.name="' + gene + '" RETURN g'
  results = db.query(q, returns=client.Node)
  #this node exists
  if results:
    g1 = results[0][0]
    rels = g1.relationships.all()
    for r in rels:
      r.delete()
    g1.delete()
    return True
  else:
    return False

#This function takes a string node_name, int distance, a boolean show_only_n
#and returns a list of the names of  
#of all distance-order interacting nodes with the node that has name "node_name".
#If show_only_n is false then the list will contain all m-order (where m < distance) 
#interacting node's as well 
def nOrderInteractors(gene, distance, show_only_n):
  if distance < 0:
    print("n must be a positive integer")
    return None
  nodes_seen = dict()
  node_list = []
  if show_only_n:
    q = 'MATCH p = (g:Gene)-[r:interacts_with *'+ str(distance) +']->(m:Gene) WHERE g.name="' + gene + '" RETURN m'
    #results = db.query(q, returns=client.Node)
    results = db.query(q, returns = str)
    if results:
      for r in results:
        temp = r[0].split("'name': '")[1].split("'")[0]
        if temp not in nodes_seen:
          nodes_seen[temp] = 0
          node_list.append(temp)
    else:
      print("No such ID")

  #need to return all nodes reachable in n edges or less
  else:
    q = 'MATCH (g:Gene) WHERE g.name="' + gene + '" RETURN g'
    results = db.query(q, returns=str)
    #this node exists
    if results:
      node_queue = [gene]
      i = 0
      while i <= distance:
        j = 0
        s = len(node_queue)
        while j < s:
          temp_node = node_queue.pop(0)
          node_list.append(temp_node)
          if i != distance:
            q = 'MATCH p = (g:Gene)-[r:interacts_with]->(m:Gene) WHERE g.name="' + temp_node + '" RETURN m'
            results = db.query(q, returns=str)
            if results:
              for r in results:
                new_node = r[0].split("'name': '")[1].split("'")[0]
                if new_node not in nodes_seen:
                  nodes_seen[new_node] = 0
                  node_queue.append(new_node)
          j+=1        
        i+=1
    else:
      print("No such entrez_id")
  return node_list

#removes node and all of the relationships associated with this node
#This takes quite awhile. Our research shows that this is expected
def dropGraph():
  print("Dropping gene interaction graph...")
  q = 'MATCH (g:Gene) DETACH DELETE g'
  results = db.query(q, returns=str)
  # q = 'MATCH (g:Gene) RETURN g'
  # results = db.query(q, returns=client.Node)
  # i = 0
  # print("Dropping gene interaction graph:")
  # num_nodes = len(results)
  # if num_nodes == 0:
  #   return
  # toolbar_width = 40
  # progress = 0
  # bucketsize = int(num_nodes/40)
  # buckets = 0
  # # setup progress bar
  # sys.stdout.write("|%s|0%%" % (" " * toolbar_width))
  # sys.stdout.flush()
  # sys.stdout.write("\r")
  # for r in results:
  #   progress += 1
  #   buckets = int(progress/bucketsize)
  #   g1 = r[0]
  #   rels = g1.relationships.all()
  #   for r in rels:
  #     r.delete()
  #   g1.delete()
  #   # redraw the bar with updated info
  #   sys.stdout.write("|" + chr(0x2586)*buckets \
  #       + " "*(toolbar_width-buckets)+ " |%d%%" % int(progress/num_nodes*100))
  #   sys.stdout.flush()
  #   sys.stdout.write("\r")
  # # draw final completed bar
  # sys.stdout.write("|" + chr(0x2586)*toolbar_width + " |100%\n")
  # sys.stdout.flush()