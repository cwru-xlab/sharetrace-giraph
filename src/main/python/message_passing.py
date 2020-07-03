import numpy as np
import math
import networkx as nx
from networkx.algorithms import bipartite
import datetime
import random
from scipy.stats import powerlaw
import time
#seed for random numbers
random.seed( 20 )

#parameters
d_p = 15 * 60
#dates
trans_rate = 0.7
max_num_itr = 4
curr_date = '30/06/2020' #datetime.date.today()
tol_val = 0.00001


def merge_dicts(dict1, dict2):
    for key in dict1:
        if key not in dict2 or dict1[key] > dict2[key]:
            dict2[key] = dict1[key]
    return {**dict1, **dict2}
    
#compute the message with respect to each contact between the two users and pick the maximum value
def compute_msg(G, variable_node, factor_node, date, risk, stm):
    max_msg = 0
    for i in range(len(G.nodes[factor_node]['cont_inf'])):
        text = G.nodes[factor_node]['cont_inf'][i]
        val = text.split(';')
        t = val[0]
        d = int(val[1])
        if datetime.datetime.strptime(date,'%d/%m/%Y').date() <= datetime.datetime.strptime(t, '%d/%m/%Y').date():
            stm = True
            msg = risk * trans_rate
            if msg > max_msg:
                max_msg = msg
    G.add_edges_from([(variable_node, factor_node, {'m_fv' : {t: max_msg}})])
    return stm

def compute_message_factor_to_variable(G, factor_node, variable_node, neigh):
    for i in range(len(neigh)):
        if neigh[i] != variable_node:
            #local risk scores of the variable node
            loc_risks = G.nodes[neigh[i]]['local_risks']
            #messages the variable node revieved from the other factor nodes (each message contains a time and a risk score)
            mvf = G[factor_node][neigh[i]]['m_vf']
            comb_dict = merge_dicts(loc_risks, mvf)
            #sort the risk scores according to time
            sorted_risk = {k: v for k, v in sorted(comb_dict.items(), key=lambda item: datetime.datetime.strptime(item[0],'%d/%m/%Y'), reverse=True)}
            sorted_risks = list(sorted_risk.items())
            ind = 0
            stm = False
            while stm == False and ind < len(sorted_risks):
                val = sorted_risks[ind]
                stm = compute_msg(G, variable_node, factor_node, val[0], val[1], stm)
                ind = ind + 1
            if stm == False:
                #if there is no local risk computed before the time users had a contact, then send a risk score of 0 for the current date
                G.add_edges_from([(variable_node, factor_node, {'m_fv' : {curr_date: 0}})]) 
    

def compute_max_local_risk(G, bottom_nodes, file_out):
    old_risk = []
    for node in bottom_nodes:
        #compute max inital local risk (time: local_risk) - it can be updated in each iteration
        sorted_risk = {k: v for k, v in sorted(G.nodes[node]['local_risks'].items(), key=lambda item: item[1], reverse=True)}
        max_risk = next(iter(sorted_risk.items()))
        G.nodes[node]['max_risk'] = {max_risk[0] : max_risk[1]}
        #stored the inital risk to compute the average risk score increase in the end (not sure if you need it)
        G.nodes[node]['init_risk'] = max_risk[1]
        old_risk.append(max_risk[1])
        file_out.write('U_' + str(node) + ' -' + str(G.nodes[node]['max_risk']) + '\n')
        #initialize m_vf (messages from variable nodes to factor nodes)
        neigh = list(G.neighbors(node))
        for factor_node in neigh:
            G.add_edges_from([(node, factor_node, {'m_vf' : {}})])
    return old_risk


def run_bp(G, top_nodes, bottom_nodes, file_path):
    outf = open(file_path, 'w')
    start_time = time.time()
    outf.write('--------initial risks---------------\n')
    old_risk = compute_max_local_risk(G, bottom_nodes, outf)

    for itr in range(max_num_itr):
        curr_risk = [] 
        #message from factor nodes to variable nodes
        for factor_node in top_nodes:
            neigh = list(G.neighbors(factor_node))
            for variable_node in neigh:
                compute_message_factor_to_variable(G, factor_node, variable_node, neigh)
                                        
        #message from variable nodes to factor nodes
        for variable_node in bottom_nodes:
            neigh = list(G.neighbors(variable_node))
            for factor_node in neigh:
                #initialize m_vf (messages from variable nodes to factor nodes)
                G.add_edges_from([(variable_node, factor_node, {'m_vf' : {}})])
                #store in m_vf all the incoming message to variable node v except for the one it recieved from factor node f to avoid self-bias
                for i in range(len(neigh)):
                    if neigh[i] != factor_node:
                        G[variable_node][factor_node]['m_vf'].update(G[variable_node][neigh[i]]['m_fv'])
        
        #compute final risk by getting the maximum value of the local risks and received messages in each variable node
        for variable_node in bottom_nodes:
            neigh = list(G.neighbors(variable_node))
            for factor_node in neigh:
                val = next(iter(G[variable_node][factor_node]['m_fv'].items()))
                if val[1] > next(iter(G.nodes[variable_node]['max_risk'].items()))[1]:
                    G.nodes[variable_node]['max_risk'] = G[variable_node][factor_node]['m_fv']        
            risk = [v for k,v in G.nodes[variable_node]['max_risk'].items()]
            curr_risk.append(risk[0])
            
        #check if the risk scores have converged (we might change the tolerance value)
        if sum(np.array(curr_risk) - np.array(old_risk)) < tol_val:
            break
        old_risk = curr_risk[:]
        
    #final risks
    risk_incr = []
    outf.write('--------final risks---------------\n')
    for variable_node in bottom_nodes:
        outf.write('U_' + str(variable_node) + ' -' + str(G.nodes[variable_node]['max_risk']) + '\n')
        final_risk = next(iter(G.nodes[variable_node]['max_risk'].items()))[1]
        risk_incr.append(final_risk - G.nodes[variable_node]['init_risk']) 

    run_time = time.time() - start_time
    return np.mean(risk_incr), itr, run_time
