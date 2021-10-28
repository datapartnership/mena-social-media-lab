import psycopg2

my_db = "alexei_roy_db"
my_user = "roy"
my_password = "d74eafdaa428b02ece89381c9816c10c"
my_host = "ec2-3-19-59-227.us-east-2.compute.amazonaws.com"

# my_db = "new_db"
# my_user = "roy"
# my_password = "weide"
# my_host = "128.100.127.52"

aws_conn = psycopg2.connect(dbname=my_db, user=my_user, password=my_password,
                            host=my_host, port='5432',
                            sslmode='require')
aws_conn.set_session(autocommit=False)
aws_cur = aws_conn.cursor()
aws_cur.execute('select * FROM inequality LIMIT 10000')
tweets = aws_cur.fetchall()

aws_cur.execute('select * FROM inequality LIMIT 0')
column_metadata = aws_cur.description
aws_conn.close()

########################################################################################

import numpy as mp
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

column_names = [x.name for x in column_metadata]  # extracts a list of 61 variable names
for k in column_names:
    print(k, ":", len(k))

df = pd.DataFrame(tweets, columns=column_names)
print(df.shape)
list(df)

print([df[var].dtype for var in list(df)])


df["ln_retweet"]=mp.log(1+df["tweet_retweet_count"])
df["ln_retweet"].hist()
plt.show()

print(df["user_location"].value_counts().sort_values(ascending=False)[:10])

pd.crosstab(index=df["user_followers_count"], columns="count")

#df_user=df.groupby(["user_id"]).sum()[["tweet_retweet_count", "tweet_favorite_count"]]
groupby_user=df.groupby(["user_id"])
df_user_mean=groupby_user["user_followers_count"].mean().reset_index()
df_user_sum=groupby_user[["tweet_retweet_count", "tweet_favorite_count"]].sum().reset_index()
df_user=pd.merge(left=df_user_mean, right=df_user_sum, on="user_id", how="left")

list(df_user)
df_user.count()
df_user.describe()

#compute Lorenz curves
vars=['user_followers_count', 'tweet_retweet_count', 'tweet_favorite_count']
for y in vars:
    #print(df_user[y].describe())
    #df_user["p_" + y]=[i for i in range(1, df_user.shape[0] + 1, 1)]
    #print(df_user["p_" + y].describe())
    df_user.sort_values(by=[y], inplace=True)
    df_user["n"] = [i for i in range(1, df_user.shape[0] + 1, 1)]
    df_user["p_"+y] = df_user["n"] / df_user["n"].max()
    df_user["s_"+y] = df_user[y].cumsum() / df_user[y].sum()
    plt.plot("p_" + y, "s_" + y, data=df_user, marker="", color="blue", linewidth=1)

plt.show()

#plot Lorenz curves
for y in vars:
    plt.plot("p_"+y, "s_"+y, data=df_user, marker="o", color="blue", linewidth=1)

#plt.plot("p_", "p_", data=df_user, marker="", color="black", linewidth=1)
plt.show()