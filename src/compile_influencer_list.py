import os
import pandas as pd
import new_mylib
import tweet_write_to_db_library
import tweepy

header = ['user_id', 'user_screen_name', 'group']

home_dir = '/home/alexei/Dropbox/alexei_roy_shared_folder'
input_dir = home_dir + os.sep + 'twitter_handles_to_track'
output_dir = home_dir + os.sep + 'raw_data'
log_dir = home_dir + os.sep + 'scripts' + os.sep + 'logs'
master_csv = home_dir + os.sep + 'scripts' + os.sep + 'influencers_list_compiled_from_source_files.csv'

logger = tweet_write_to_db_library.define_logger('compile_influencer_list', log_dir)
######################################################################
######################################################################
######################################################################
######################################################################
######################################################################
######################################################################
######################################################################
######################################################################
# GROUPS WITH USER_ID ALREADY KNOWN:
group = 'congress'
df = pd.read_csv(input_dir + os.sep + 'cspan_116th_congress.csv', usecols=['user_id', 'user_screen_name'])
df['user_screen_name'] = df['user_screen_name'].str.lower()
df.drop_duplicates(subset='user_screen_name', inplace=True)
print('{} members of {}'.format(df.shape[0], group))
df['group'] = group
print('df.shape = {} after adding {}'.format(df.shape, group))

group = 'senate'
temp_df = pd.read_csv(input_dir + os.sep + 'cspan_116th_senators.csv', usecols=['user_id', 'user_screen_name'])
temp_df['user_screen_name'] = temp_df['user_screen_name'].str.lower()
temp_df.drop_duplicates(subset='user_screen_name', inplace=True)
print('{} members of {}'.format(temp_df.shape[0], group))
temp_df['group'] = group
df = df.append(temp_df)
print('df.shape = {} after adding {}'.format(df.shape, group))

group = 'political_scientist'
temp_df = pd.read_csv(input_dir + os.sep + group + 's.csv', usecols=['user_id', 'user_screen_name'])
temp_df['user_screen_name'] = temp_df['user_screen_name'].str.lower()
temp_df.drop_duplicates(subset='user_screen_name', inplace=True)
print('{} members of {}'.format(temp_df.shape[0], group))
temp_df['group'] = group
df = df.append(temp_df)
print('df.shape = {} after adding {}'.format(df.shape, group))
############################################################
############################################################
# THINK TANKS:
group = 'think_tank'
temp_df = pd.read_csv(input_dir + os.sep + 'think_tanks_twitter_handles.csv', usecols=['twitter_handle'])
my_screen_names = list(temp_df['twitter_handle'])
my_screen_names = [screen_name for screen_name in my_screen_names if screen_name.find('@') != -1] # only all strings with @
my_screen_names = [screen_name.replace('@', '') for screen_name in my_screen_names] # remove @
my_screen_names = [screen_name.replace(' ', '') for screen_name in my_screen_names] # remove whitespace
# https://cis.org/ says their twitter handle is CIS_org, not wwwcisorg
# ctr_sais no longer exists on twitter at least
problem_screen_names = ['ctr_sais', 'wwwcisorg', 'alaska_csc']
my_screen_names = [x for x in my_screen_names if x.lower() not in [y.lower() for y in problem_screen_names]]
my_screen_names += ['CIS_org', 'alaska_casc']
rows = []
for my_screen_name in my_screen_names:
    rows.append([None, my_screen_name, group])
temp_df = pd.DataFrame(rows, columns=['user_id', 'user_screen_name', 'group'])
temp_df['user_screen_name'] = temp_df['user_screen_name'].str.lower()
temp_df.drop_duplicates(subset='user_screen_name', inplace=True)
print('{} members of {}'.format(temp_df.shape[0], group))
df = df.append(temp_df)
print('df.shape = {} after adding {}'.format(df.shape, group))
############################################################
############################################################
# TWIPLOMACY, NEWS OUTLETS, JOURNALISTS, AND ECONOMISTS:
for group in ['international_organization', 'twiplomacy_50_most_active_world_leader',
              'twiplomacy_50_most_connected_world_leader',
              'twiplomacy_50_most_followed_world_leader',
              'twiplomacy_50_most_influential_world_leader',
              'twiplomacy_50_most_interactions_world_leader',
              'news_outlet', 'journalist', 'economist']:
    temp_df = pd.read_csv(input_dir + os.sep + group + 's.csv')
    my_screen_names = list(temp_df['user_screen_name'])
    my_screen_names = [x.replace('@', '') for x in my_screen_names]
    my_screen_names = [x.replace(' ', '') for x in my_screen_names]
    rows = []
    for my_screen_name in my_screen_names:
        rows.append([None, my_screen_name, group])
    temp_df = pd.DataFrame(rows, columns=['user_id', 'user_screen_name', 'group'])
    temp_df['user_screen_name'] = temp_df['user_screen_name'].str.lower()
    temp_df.drop_duplicates(subset='user_screen_name', inplace=True)
    print('{} members of {}'.format(temp_df.shape[0], group))
    df = df.append(temp_df)
    print('df.shape = {} after adding {}s'.format(df.shape, group))

# convert all user_screen_names to lowercase:
df['user_screen_name'] = df['user_screen_name'].str.lower()
print('{} before dropping duplicates'.format(df.shape[0]))
# df.drop_duplicates(subset='user_screen_name', inplace=True)
# print('{} after dropping duplicates'.format(df.shape[0]))
df.reset_index(inplace=True, drop=True)
############################################################
############################################################
############################################################

print('{} user_screen_names with unknown user_ids'.format(df.loc[df['user_id'].isna(), :].shape[0]))

# EXPORT RESULTS TO CSV:
df.to_csv(master_csv, index=False)