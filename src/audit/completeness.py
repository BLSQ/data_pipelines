"""Functions to compute completeness of the data at hand."""

import pandas as pd
from rpy2.robjects import r, pandas2ri
pandas2ri.activate()
readRDS = r['readRDS']


def readRDS_to_pandas(path):
    """Read RDS data into a pandas DataFrame."""
    df = readRDS(path)
    df = pandas2ri.ri2py(df)
    return df


def n_cc_values(data, M_category_combos):
    """Compute the number of data values expected the CCs of some data."""
    cc_id = data['categoryCombo.id'].unique()[0]
    out = len(M_category_combos[M_category_combos.CatCombo_id == cc_id])
    return out


def n_rep_values(data):
    """Get the number of values that have been reported for a data set/orgunit."""
    return len(data)


def aggr_reported(reported_values, time, space):
    aggr_rep = reported_values.groupby(time + space + ['uid_data_element', 'name_data_element'])['count'].sum().reset_index()
    return aggr_rep


def make_full_data_expectations(data_elements_sets, org_units_sets, full_de_cc, org_units_structure):
    expectation_full = data_elements_sets.merge(full_de_cc, left_on='dataElement_id',
                                                right_on='uid_de').groupby(['dataSet_id', 'dataElement_id']).apply(len).reset_index()
    expectation_full.columns = ['dataSet_id' ,'dataElement_id', 'n_values']
    expectation_full = expectation_full.merge(org_units_sets).merge(org_units_structure, left_on='OrgUnit_id', right_on='organisationunituid')
    return expectation_full


def aggr_expectation(full_expectation, space):
    if type(space) == str:
        space = [space]
    de_expectation = expectation_full.groupby(space + ['dataElement_id'])['n_values'].sum().reset_index()
    de_expectation.columns = space + ['dataElement_id', 'n_expected']
    return de_expectation


def make_availability(aggr_reported_values, expectations, space, fac=1):
    de_availability = aggr_reported_values.merge(expectations,
                                                 left_on=[space, 'uid_data_element'],
                                                 right_on=[space, 'dataElement_id'])
    de_availability['value'] = de_availability['count'] / (fac * de_availability.n_expected)
    return de_availability
