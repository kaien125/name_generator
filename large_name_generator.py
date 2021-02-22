import pandas as pd
import numpy as np
import random
import time
import string

# load data 
# data source: https://github.com/philipperemy/name-dataset
first_name = pd.read_csv('first_names.csv',header = None,names= ['first_name'])
first_name['first_name'] = first_name['first_name'].apply(lambda x: str(x).capitalize())
last_name = pd.read_csv('last_names.csv',header = None,names= ['last_name'])
last_name['last_name'] = last_name['last_name'].apply(lambda x: str(x).capitalize())

def name_expender(num, names):
    names_large = pd.DataFrame()
    start = time.time()
    for i in range(num):    
        names_large = names_large.append(names, ignore_index=True)
        print(i)
    print('shuffle')    
    names_large = names_large.sample(frac=1).reset_index(drop=True)
    end = time.time()
    time_taken = end - start
    print(time_taken)
    return names_large
    
print('last_name processing:')
last_name_large = name_expender(204, last_name)
print('first_name processing:')
first_name_large = name_expender(122, first_name)
print('middle_name_1 processing:')
middle_name_1_large = name_expender(40,first_name)
print('middle_name_2 processing:')
middle_name_2_large = name_expender(12,first_name)

print('concate names')
full_name_large = pd.concat([first_name_large.reset_index(drop=True),middle_name_1_large.reset_index(drop=True),middle_name_2_large.reset_index(drop=True),last_name_large.reset_index(drop=True)], axis=1)
full_name_large.columns = ['last_name', 'middle_name_1','middle_name_2','first_name']

print('drop redundant rows')
full_name_large = full_name_large[full_name_large['first_name'].notna()]
full_name_large = full_name_large[full_name_large['last_name'].notna()]

print('count last_name:')
print(full_name_large['last_name'].count())

print('count middle_name_1:')
print(full_name_large['middle_name_1'].count())

print('count middle_name_2:')
print(full_name_large['middle_name_2'].count())

print('count first_name:')
print(full_name_large['first_name'].count())

# fill na with whitespace for people have no middle name 
print('Replace missing values')
full_name_large['middle_name_1'] = full_name_large['middle_name_1'].fillna('')

full_name_large['middle_name_2'] = full_name_large['middle_name_2'].fillna('')

# concate names into full name
print('concate names into full name')
full_name_large['full_name'] = full_name_large['first_name'].map(str) + ' ' + full_name_large['middle_name_1'].map(str) + ' ' + full_name_large['middle_name_2'].map(str)+ ' ' + full_name_large['last_name'].map(str)

#remove extra whitespace
print('remove extra whitespace')
full_name_large['full_name']=full_name_large['full_name'].replace({' +':' '},regex=True)

# save to csv
print('save to csv')
full_name_large['full_name'].to_csv('output_20m.csv', index=False)

print('done')