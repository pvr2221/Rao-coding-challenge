from dateutil.parser import parse
import datetime as dt
import json
from numpy import median

def relevant(time_payment, running_max_time):
# checks whether a payment is relevant according to running maximum timestamp

    difference = running_max_time - time_payment
    if difference.total_seconds() >= 60:
        return False
    else:
        return True

def generate_graph(new_payment, graph,running_max_time):
# returns an updated graph based on a new_payment, previous graph and running maximum timestamp

    new_payment_actor = str(new_payment['actor'])
    new_payment_target = str(new_payment['target'])
    new_payment_time = parse(str(new_payment['created_time']))

    # consider cases 1) actor already active 2) new actor
    if new_payment_actor in graph:

        #check for duplicate transactions with different times, and delete
        duplicates = []
        for i in range(len(graph[new_payment_actor])):
            if graph[new_payment_actor][i][0] == new_payment_target:
                duplicates.append(graph[new_payment_actor][i])
        for duplicate in duplicates:
            graph[new_payment_actor].remove(duplicate)

        # add new payment
        graph[new_payment_actor].append((new_payment_target,new_payment_time))


    else:
        # add actor to graph and add payment
        graph[new_payment_actor]= [(new_payment_target,new_payment_time)]

    if new_payment_target in graph:

        #check for duplicate transactions with different times, and delete
        duplicates = []
        for i in range(len(graph[new_payment_target])):
            if graph[new_payment_target][i][0] == new_payment_actor:
                duplicates.append(graph[new_payment_target][i])
        for duplicate in duplicates:
            graph[new_payment_target].remove(duplicate)

         # add other end of payment
        graph[new_payment_target].append((new_payment_actor, new_payment_time))

    else:
        # add target to graph and add payment
        graph[new_payment_target] = [((new_payment_actor, new_payment_time))]

    # check for irrelevant connections (out of 60-second interval, duplicate & disconnected nodes)

    irrelevant = []
    disconnected = []
    for actor in graph:
        for i in range(len(graph[actor])):
            payment_time = graph[actor][i][1]
            if relevant(payment_time,running_max_time) != True:
                irrelevant.append(graph[actor][i])
        for tuple in irrelevant:
            # delete irrelevant transactions
            graph[actor].remove(tuple)
        irrelevant = []
        if graph[actor] == []:
            disconnected.append(actor)
    # delete disconnected nodes
    for disconnected_actor in disconnected:
        graph.pop(disconnected_actor,None)

    return graph

def findMedianDegree(graph):
# returns the median degree of the graph if non-empty, and zero otherwise

    degrees = []
    for actor in graph:
        degrees.append(len(graph[actor]))
    if len(degrees)!=0:
        return median(degrees)
    else:
        return 0

#open files in current directory (compatible with run.sh)
file_in = open("./venmo_input/venmo-trans.txt",'r')
file_out = open("./venmo_output/output.txt",'w')

#define graph as a Python dict, flag = 0 (first line of file), 1 (otherwise)
graph = {}
flag = 0

for payment in file_in.readlines():

    # convert json to python dict
    payment = json.loads(payment)

    # ignore empty target/actor fields
    if payment['actor']!="" and payment['target']!="":

        time_payment = parse(str(payment['created_time']))


        if flag == 0:

            # generate ground level of undirected graph

            running_max_time = time_payment
            actor = str(payment['actor'])
            target = str(payment['target'])

            graph = {actor:[(target,time_payment)],target:[(actor,time_payment)]}

            flag = 1

        else:

            # update running_max_time

            difference = time_payment - running_max_time

            if difference.total_seconds() > 0:
                running_max_time = time_payment

            if relevant(time_payment, running_max_time):
            # generate graph based on previous graph and updated max time
                 graph = generate_graph(payment,graph,running_max_time)

    median_degree = format(findMedianDegree(graph), '.2f')
    file_out.write(str(median_degree)+'\n')

file_in.close()
file_out.close()