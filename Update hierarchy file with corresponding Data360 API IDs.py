
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import datetime
import requests
import json
import ast

get_ipython().magic('matplotlib inline')


# In[45]:


# set working folder for hierarchy files
working_folder = '2018-02-16-hierarchy-files'
update_date = datetime.datetime.today().strftime('%m.%d.%y')


# In[49]:


# download latest indicators dataset in Backend PROD
response = requests.get('http://tcdata360-backend.worldbank.org/api/v1/indicators/')
tc_indicators = pd.read_json(response.text)

# set key columns for hierarchy file
key_columns = list(df_ind.columns)
key_columns.append('id')

# get latest hierarchy.indicators file shared with Vendor
df_ind_filename = glob.glob("%s/hierarchy.indicators *.csv" % working_folder)[0]
df_ind = pd.read_csv(df_ind_filename)

# merge indicators dataset API ID against latest hierarchy file
df_tc_hier_id_mapping = df_ind.merge(tc_indicators,
                                     how='left',
                                   left_on=['Display Name', 'Dataset','Value Type Slug', 'Value Type Descriptor', 'Units'],
                                   right_on=['name', 'dataset','valueType', 'subindicatorType','units'])[key_columns]

# check if pulled IDs are unique.
if df_tc_hier_id_mapping['id'][df_tc_hier_id_mapping['id'].notnull()].is_unique:
    print("IDs are unique.")
    
current_new = df_tc_hier_id_mapping[(pd.to_numeric(df_tc_hier_id_mapping['id'], errors='coerce') != pd.to_numeric(df_tc_hier_id_mapping['Indicator ID'], errors='coerce')) & (df_tc_hier_id_mapping['id'].notnull())]['Indicator ID'].value_counts().sum()
print("There are currently %d indicators labeled as 'New' or 'new' in the latest hierarchy file." % current_new)

# update all new indicators with latest API ID.
df_tc_hier_id_mapping.loc[df_tc_hier_id_mapping['Indicator ID'].isin(['new', 'New']), 'Indicator ID'] = np.nan
df_tc_hier_id_mapping['Indicator ID_updated'] = df_tc_hier_id_mapping['Indicator ID'].combine_first(df_tc_hier_id_mapping['id'])

# all uningested indicators will have ID = "new"
uningested_indicators = df_tc_hier_id_mapping.loc[df_tc_hier_id_mapping['Indicator ID_updated'].isnull(), 'Indicator ID_updated'].size
df_tc_hier_id_mapping.loc[df_tc_hier_id_mapping['Indicator ID_updated'].isnull(), 'Indicator ID_updated'] = 'new'
print("Finished updating indicator IDs for %d indicators in the hierarchy file." % (current_new - uningested_indicators))
print("There are %d uningested indicators in the hierarchy file labeled as 'new'." % uningested_indicators)

# update Indicator ID column in hierarchy file
df_tc_hier_id_mapping['Indicator ID'] = df_tc_hier_id_mapping['Indicator ID_updated']
df_tc_hier_id_mapping = df_tc_hier_id_mapping[df_ind.columns]

df_tc_hier_id_mapping.to_csv("%s/hierarchy.indicators %s.csv" % (working_folder, update_date), index=False, line_terminator='\r')

