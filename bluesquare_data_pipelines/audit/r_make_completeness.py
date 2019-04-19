"""Routine to check data requirements and build observed data completeness"""

import completeness

import importlib
importlib.reload(completeness)

data_dir = "/Users/grlurton/data/dhis/rdc/snis/pdss/"
data = readRDS_to_pandas(data_dir+"data.rds")

metadata_dir = "/Users/grlurton/data/dhis/rdc/snis/"
OU_metadata_DSinfo = readRDS_to_pandas(metadata_dir + "OU_metadata_DSinfo.rds")
M_category_combos = readRDS_to_pandas(metadata_dir + "CC_metadata.rds")
M_data_sets = readRDS_to_pandas(metadata_dir + "DS_metadata.rds")
M_org_units = readRDS_to_pandas(metadata_dir + 'OU_metadata_DSinfo.rds')


cc_values = M_data_sets.groupby('categoryCombo.id').apply(n_cc_values, M_category_combos).reset_index()
cc_values.columns = ['categoryCombo.id', 'n_values']

count_data_sets = cc_values.merge(M_data_sets, on='categoryCombo.id')

data_sets_size = count_data_sets.groupby('DS_name')['n_values'].sum().reset_index()
data_sets_size.columns = ['DS_name', 'n_values']

# Export data needs store in analysis space.


full_value = M_org_units.merge(count_data_sets, on=['DS_name', 'DS_id'])


org_unit_size = full_value.groupby(['DS_name', 'parent.parent.name'])['n_values'].sum()


fac_de_period_value = data.groupby(['orgUnit', 'period', 'dataElement']).apply(n_rep_values)

fac_de_period_value.head()

data.head()
