import os
import pandas as pd
import networkx as nx
import community
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns

imputation_var = 'vax_position'
#base_dir = "C:/Users\wb256280/Dropbox/alexei_roy_shared_folder/yemen_famine_jan2021"
base_dir = "C:/Users\wb256280/Dropbox/alexei_roy_shared_folder/arabic_covid_vaccines_mar2021"
#input_dir = os.path.join(base_dir, "yemen_grouped_results")
input_dir = os.path.join(base_dir, "covid_vax_indiv_hashtag_results")
#labeled_csv = os.path.join(base_dir, "master_yemen_designation_v2_corrected.csv")
labeled_csv = os.path.join(base_dir, "vaxpos_master_v4.csv")

output_dir = os.path.join(input_dir, "images")
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

# bring in the labeled seed accounts:
def load_seed_labeled_accounts():
    df = pd.read_csv(labeled_csv)
    df['user_id_str'] = df['user_id'].astype(str)
    return df
seed_df = load_seed_labeled_accounts()

folders = [x for x in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, x)) and x!='images']
print(f"found {len(folders)} folders:")
print(f"{folders}")

def find_seed_users_in_graph():
    df = seed_df.copy()
    df['keep'] = False
    for i in range(0, df.shape[0]):
        user_id_str = df.loc[i, 'user_id_str']
        if user_id_str in K.nodes():
            df.loc[i, 'keep'] = True
    print(f"seed users found in this graph:")
    print(df['keep'].value_counts())
    df = df.loc[df['keep']==True, :]
    df = df[['user_id_str', imputation_var]]
    df.reset_index(inplace=True, drop=True)
    print(f"df.shape={df.shape}")
    return df

def get_csv_from_graph(K):
    rows = []
    for node in K.nodes():
        # print(node, K.nodes()[node])
        user_id = K.nodes()[node]['user_id']
        user_screen_name = K.nodes()[node]['user_screen_name']
        user_followers_count = K.nodes()[node]['user_followers_count']
        if imputation_var in K.nodes()[node].keys():
            user_imputation_var = K.nodes()[node][imputation_var]
        else:
            user_imputation_var = None
        row = [user_id, user_screen_name, user_followers_count, user_imputation_var]
        rows.append(row)
    df = pd.DataFrame(rows, columns=['user_id', 'user_screen_name', 'user_followers_count', imputation_var])
    return df

def histogram_of_alignment(df):
    """
    display histogram of alignment
    """
    sns.set(rc={'figure.figsize': (8, 6)})
    df[imputation_var].hist(bins=21)
    plt.savefig(os.path.join(output_path, f"{my_folder}_imputation_var_histogram_before_iteration{iteration}.png"))
    plt.show()
    plt.close()

# for node in K.nodes():
#     if imputation_var in K.nodes()[node].keys():
#         print(K.nodes()[node])
#         break


def update_files_and_create_graphs(K, labeled_df,
                                       missing_labeled_influencers_df):
    # add alignments to graph:
    nx.set_node_attributes(K, dict(zip(labeled_df['user_id_str'], labeled_df[imputation_var])), imputation_var)
    nx.write_graphml(K, my_output_graph_path)

    # create csv from graphml:
    df2 = get_csv_from_graph(K)
    # load influencer csv:
    df = pd.read_csv(my_influencer_csv_path)

    # append missing influencers to df2, then export
    df2['appended'] = False
    if missing_labeled_influencers_df.shape[0] > 0:
        missing_labeled_influencers_df['appended'] = True
        df2 = df2.append(missing_labeled_influencers_df, sort=True)
    df2.reset_index(inplace=True, drop=True)
    print(df2['appended'].value_counts())
    df2.to_csv(os.path.join(my_folder_path, f"all_users_{imputation_var}.csv"), index=False, encoding='utf-8-sig')

    # merge in the position information from all_users into the influencer df:
    df2 = df2[['user_id', imputation_var, 'appended']]
    x = df.merge(df2, on='user_id', how='left')
    print(x['appended'].value_counts())
    x.to_csv(my_influencer_csv_path[:-4] + f'_{imputation_var}.csv', index=False, encoding='utf-8-sig')
    x['user_id_str'] = x['user_id'].astype(str)
    G = nx.read_graphml(my_influencer_graph_path)
    nx.set_node_attributes(G, dict(zip(x['user_id_str'], x[imputation_var])), imputation_var)
    nx.write_graphml(G, my_influencer_output_graph_path)

    histogram_of_alignment(labeled_df)

    print(f"total users labeled now: {labeled_df.shape[0]}")
    print(f"total users in all-users graph: {len(K.nodes())}")
    print(f"% labeled: {100.0*labeled_df.shape[0]/float(len(K.nodes()))}")

    return K

def impute_alignments(K, labeled_df):
    G = K.to_undirected()

    # retweeter_alignments = {}

    neighbors_list = []
    labeled_users = list(labeled_df['user_id_str'])
    print(f"building list of imputees")
    for user_id in tqdm(labeled_users):
        neighbors = G.neighbors(user_id)
        for neighbor in neighbors:
            # if neighbor not in neighbors_list:
            neighbors_list.append(neighbor)
    from timeit import default_timer as timer
    print(f"de-duplicating neighbors_list")
    start = timer()
    neighbors_list = list(set(neighbors_list)) # de-duplicate
    elapsed_time = timer() - start
    print(elapsed_time)

    # for each neighbor, go ahead and find out average ideology:
    rows = []
    skip_count = 0
    for node in tqdm(neighbors_list):
        if imputation_var not in G.nodes()[node].keys(): # so we need to label this one
            neighbors = G.neighbors(node)
            count = 0
            net_position = 0
            for neighbor in neighbors:
                if imputation_var in K.nodes()[neighbor].keys():
                # if neighbor in labeled_users: # this is a labeled neighbor
                    count += 1
                    net_position += K.nodes()[neighbor][imputation_var]
            net_position = float(net_position)/count
            rows.append((node, net_position))
        else:
            skip_count += 1
    print(f"skipped: {skip_count} out of {len(neighbors_list)}")
    temp_df = pd.DataFrame(rows, columns=['user_id_str', imputation_var])

    old_num_users = labeled_df.shape[0]
    additional_users = temp_df.shape[0]
    perc_increase = 100.0 * additional_users / float(old_num_users)
    print(f"appending {additional_users} new users at the end of iteration {iteration}")
    labeled_df = labeled_df.append(temp_df, sort=True)
    labeled_df.reset_index(inplace=True, drop=True)
    print(f"labeled_df.shape[0]={labeled_df.shape[0]}"
          f"which is a {perc_increase}% increase")

    return labeled_df, perc_increase

# fix later:
meta_csv = os.path.join(output_dir, 'alignment_imputation_results.csv')
if os.path.exists(meta_csv):
    meta_df = pd.read_csv(meta_csv)
else:
    meta_df = pd.DataFrame()

my_folder = folders[0]
for my_folder in tqdm(folders):
    print(f"analyzing {my_folder}...")
    # my_folder = "vax_lebanon_1"
    my_folder_path = os.path.join(input_dir, my_folder)
    output_path = os.path.join(output_dir, my_folder)
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    my_graph_path = os.path.join(my_folder_path, "all_users.graphml")
    my_output_graph_path = my_graph_path.rstrip('.graphml') + f'_{imputation_var}.graphml'
    my_influencer_csv_path = os.path.join(my_folder_path, "influencers.csv")
    my_influencer_graph_path = os.path.join(my_folder_path, "influencers.graphml")
    my_influencer_output_graph_path = my_influencer_graph_path.rstrip('.graphml') + f'_{imputation_var}.graphml'

    # if os.path.exists(my_output_graph_path):
    #     print(f"all_users output graph already exists! skipping...")
    #     continue

    influencer_csv = os.path.join(my_folder_path, "influencers.csv")
    influencer_df = pd.read_csv(influencer_csv, usecols=['user_id', 'user_followers_count'])
    labeled_influencers_df = influencer_df.merge(seed_df, on='user_id', how='left', indicator=True)
    labeled_influencers_df = labeled_influencers_df.loc[labeled_influencers_df['_merge']=='both', :]
    labeled_influencers_df.drop(columns=['_merge'], inplace=True)

    if os.path.exists(my_graph_path):
        K = nx.read_graphml(my_graph_path)
        #K = nx.read_graphml(test)
        print(f"loaded graph with {len(K.nodes())} nodes and {len(K.edges())} edges")
    else:
        print(f"no all_users.graphml found in {my_folder_path}")
        continue

    labeled_df = find_seed_users_in_graph()
    num_seeds_found = labeled_df.shape[0]

    # identify influencers for whom we know vaxpos but aren't in labeled_df:
    missing_labeled_influencers_df = labeled_influencers_df.loc[~labeled_influencers_df['user_id_str'].isin(list(labeled_df['user_id_str']))]
    missing_labeled_influencers_df.drop(columns=['user_id_str'], inplace=True)
    print(f"there are {missing_labeled_influencers_df.shape[0]} influencers for whom we know position but aren't in all_users.graphml")
    ####################################################

    # start iterating:
    iteration = 0
    print(f"starting iteration {iteration}")

    """
    For each user whose ideological position we already know,
    check who retweeted them, and award those users
    """

    # this loop adds alignments, retweetee counts, to each retweeter
    # of a labeled account that it encounters in the graph

    perc_increase = 100.0
    while perc_increase > 0.0:
        print("=================================================================")
        K = update_files_and_create_graphs(K, labeled_df, missing_labeled_influencers_df)
        labeled_df, perc_increase = impute_alignments(K, labeled_df)
        iteration += 1

    update_files_and_create_graphs(K, labeled_df,
                                   missing_labeled_influencers_df)

    meta_stats = [(my_folder, 100.0*labeled_df.shape[0]/float(len(K.nodes())), num_seeds_found, len(K.nodes()))]
    meta_temp_df = pd.DataFrame(meta_stats, columns=['group', 'perc_labeled', 'num_seeds_found', 'num_users'])
    meta_df = meta_df.append(meta_temp_df, sort=True)
    meta_df.to_csv(meta_csv, index=False, encoding='utf-8-sig')