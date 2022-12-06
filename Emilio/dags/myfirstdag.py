from datetime import datetime
import networkx as nx
from matplotlib import pyplot as plt

#creating dependencies   
graph = nx.DiGraph()
graph.add_edges_from([("connectiontask", "storetask"),("connectiontask", "filmtask"), ("connectiontask", "customertask"), ("connectiontask", "datetask"), ("connectiontask", "stafftask"), ("stafftask", "factrentalstask"),("datetask", "factrentalstask"), ("storetask", "factrentalstask"),("customertask", "factrentalstask"),("filmtask", "factrentalstask"),("stafftask", "factrentalstask"),("storetask", "factrentalstask"),("factrentalstask","demolitiontask"),("demolitiontask", "disconnectstask")])

#creating a graphical representation of my dependencies 
plt.figure()
nx.draw_networkx(graph, arrows = True, with_labels = True)
plt.savefig('firstdag.png', format="png")
plt.clf()
    

