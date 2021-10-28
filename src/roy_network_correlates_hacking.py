import os
import networkx as nx
import community

my_io_folder ='/home/alexei/Dropbox/alexei_roy_shared_folder/rest_api_data/MENA_gender_arabic'
my_graph_file = my_io_folder + os.sep + 'influencers.graphml'

###############################################################################
###############################################################################

my_graph = nx.read_graphml(my_graph_file) # load graph

# tutorial / documentation: https://networkx.github.io/documentation/stable/tutorial.html