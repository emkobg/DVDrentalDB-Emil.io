from datetime import datetime
import networkx as nx
from matplotlib import pyplot as plt

#creating dependencies   
graph = nx.DiGraph()
graph.add_edges_from([("Connect", "Store"),("Connect", "Film"), ("Connect", "Customer"), ("Connect", "Date"), ("Connect", "Staff"), ("Staff", "Fact_Rentals"),("Date", "Fact_Rentals"), ("Store", "Fact_Rentals"),("Customer", "Fact_Rentals"),("Film", "Fact_Rentals"),("Staff", "Fact_Rentals"),("Store", "Fact_Rentals"),("Fact_Rentals","Demolition"),("Demolition", "Disconnect")])

#creating a graphical representation of my dependencies 
plt.figure()
nx.draw_networkx(graph, arrows = True, with_labels = True)
plt.savefig('firstdag.png', format="png")
plt.clf()
    

