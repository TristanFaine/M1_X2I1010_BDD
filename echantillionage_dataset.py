from operator import concat
from pickle import TRUE
from numpy import concatenate
import pandas as pd
import os
from sklearn.utils import shuffle

for top, dirs, files in os.walk(os.getcwd(), topdown=False):
    for i, file in enumerate(files):
        globals()['df_'+str(i)] = pd.read_csv(top+'/'+file)


df = pd.DataFrame()
for i in range(10):
    df_temp = globals()['df_'+str(i)][globals()
                                      ['df_'+str(i)]['CANCELLED'] == 1]
    df_temp = shuffle(df_temp)
    df = pd.concat([df, df_temp.head(30000)])


df = df.drop(labels='Unnamed: 27', axis=1)
df.describe()
df.head()


df2 = pd.DataFrame()
for i in range(10):
    df_temp = globals()['df_'+str(i)][globals()
                                      ['df_'+str(i)]['CANCELLED'] == 0]
    df_temp = df_temp[df_temp['DEP_DELAY'] > 0]
    df_temp['CANCELLATION_CODE'] = "notCancelled"
    df_temp = df_temp.drop(labels='Unnamed: 27', axis=1)
    df_temp = df_temp.dropna()
    df_temp = shuffle(df_temp)
    df2 = pd.concat([df2, df_temp.head(70000)])

df2.describe()


df = pd.concat([df, df2])


df.to_csv(r'D:\\Univ Nantes\\Cours\\M1\\S2\\BDD evo\\flights\\final.csv', index=False)