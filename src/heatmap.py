import os
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
import pandas as pd
import numpy as np

base_dir = "C:/Users/wb256280/Dropbox/alexei_roy_shared_folder/yemen_famine_jan2021/yemen_grouped_results"
input_dir = os.path.join(base_dir, "yemen_crisis_eng")
output_dir = os.path.join(base_dir, "images")
my_graph_path = os.path.join(input_dir, "all_users_designation_position.graphml")
my_filter = (0, 1) # an ordered list of group designations on which the heatmap will be based
my_var = 'designation_position'

########################################################
########################################################
########################################################

K = nx.read_graphml(my_graph_path)
rows = [(node, K.nodes()[node][my_var]) for node in K.nodes() if my_var in K.nodes()[node].keys()]
node_df = pd.DataFrame(rows, columns=['node', my_var])
node_df[my_var] = node_df[my_var].apply(lambda x: round(x))
node_df['node'] = node_df['node'].astype(str)
nx.set_node_attributes(K, dict(zip(node_df['node'], node_df[my_var])), my_var)
print(f"loaded graph with {len(K.nodes())} nodes and {len(K.edges())}")

def user_var_dataframe(K, my_var = 'designation_position', filter=(0, 1)):
    rows = [(node, K.nodes()[node][my_var]) for node in K.nodes() if my_var in K.nodes()[node].keys()]
    node_df = pd.DataFrame(rows, columns=['node', my_var])
    node_df = node_df.loc[node_df[my_var].isin(filter), :]
    return node_df
def merge_var_data_on_engagers_and_engagees(node_df, edges_df, my_var='designation_position'):
    engager_var = f'engager_{my_var}'
    engagee_var = f'engagee_{my_var}'

    num_edges = edges_df.shape[0]
    x = edges_df.merge(node_df, left_on='engager', right_on='node', how='left', indicator=True)
    x['_merge'].value_counts()
    print(f"losing {x.loc[x['_merge']=='left_only', :].shape[0]} edges of {x.shape[0]}")
    x = x.loc[x['_merge']=='both', :]
    x.drop(columns=['node', '_merge'], inplace=True)
    x.rename(columns={my_var: engager_var}, inplace=True)

    x = x.merge(node_df, left_on='engagee', right_on='node', how='left', indicator=True)
    x['_merge'].value_counts()
    print(f"losing {x.loc[x['_merge']=='left_only', :].shape[0]} edges of {x.shape[0]}")
    x = x.loc[x['_merge']=='both', :]
    x.drop(columns=['node', '_merge'], inplace=True)
    x.rename(columns={my_var: engagee_var}, inplace=True)
    num_edges_left = x.shape[0]
    perc_edges_labeled = 100.0*float(num_edges_left)/num_edges
    print(f"labeled {perc_edges_labeled}% of edges")
    return x, engager_var, engagee_var

def get_heatmap_data(df, engager_var, engagee_var, my_filter):
    temp_df = df.groupby([engager_var, engagee_var])[(engager_var, engagee_var)].count()
    temp_df = temp_df[[engager_var]]
    temp_df.rename(columns={engager_var: 'count'}, inplace=True)
    temp_df.reset_index(inplace=True, drop=False)

    data_rows = []
    for engager in my_filter:
        data_row = []
        for engagee in my_filter:
            count = temp_df.loc[np.logical_and(temp_df[engager_var]==engager,
                                       temp_df[engagee_var]==engagee), 'count'].values[0]
            data_row.append(count)
        data_rows.append(data_row)
    return data_rows

def draw_heat_map_and_save(data_rows, my_xticks, my_yticks, output_path):
    sns.set(rc={'figure.figsize': (8, 6)})
    sns.heatmap(data_rows, cmap="YlGnBu",
                xticklabels=my_xticks,
                yticklabels=my_yticks, annot=True)  # cmap="YlGnBu",, annot = True
    plt.savefig(output_path)

    plt.show()
    plt.close()

# create a dataframe of users and their designation positions
node_df = user_var_dataframe(K, my_var=my_var, filter=my_filter)
# create edges dataframe:
edges_df = pd.DataFrame(list(K.edges()), columns=['engager', 'engagee'])

# merge first on engager, then on engagee:
df, engager_var, engagee_var = merge_var_data_on_engagers_and_engagees(node_df, edges_df, my_var=my_var)

# collapse by filter:
data_rows = get_heatmap_data(df, engager_var, engagee_var, my_filter)

# get shares:
data_rows_shares = [[val/float(sum(data_row)) for val in data_row] for data_row in data_rows]

# draw heatmap:
draw_heat_map_and_save(data_rows, my_xticks=my_filter,
                       my_yticks=my_filter,
                       output_path=output_dir + os.sep + "heatmap.png")

# draw heatmap shares version:
draw_heat_map_and_save(data_rows_shares, my_xticks=my_filter,
                       my_yticks=my_filter,
                       output_path=output_dir + os.sep + "heatmap_shares.png")