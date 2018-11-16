"""Builds a preferred serie on a given thematics from multiple series."""

import pandas as pd
from scipy.interpolate import CubicSpline
import numpy as np


class measured_serie(object):
    """A serie as it is measured in one or multiple data sources.

    A given serie is being measured from different data sources. We want to
    keep a unique serie of values for this serie.

    Parameters:
    -----------
    data_list: A dictionnary of data sources on this serie
    data_type: A description of the type of data at hand
    preferred_source: An indication of a preferred data source for the serie in
    case there is one

    Attributes:
    -----------

    """

    def __init__(self, data_list, data_type, preferred_source = ""):
        # TODO Check if preferred source is in the data list
        self.data_list = data_list
        self.data_type = data_type
        self.preferred_source = preferred_source

    def reconcile_series(self, prefer_threshold=2, fill_gaps=True):
        # Question1 : we may not want to fill the gaps with not preferred values. leave it as a parameter.
        # Question2 : have benchmarking metrics
        """Build a unique data series from multiple data sources."""
        sources = list(self.data_list.keys())
        full_time = []
        for source in sources:
            full_time = full_time + self.data_list[source]['monthly'].tolist()
            if len(sources) > prefer_threshold * len(self.data_list[self.preferred_source]):
                self.preferred_source = source
        full_time = sorted(list(set(full_time)))
        if len(sources) == 1:
            out = self.data_list[sources[0]]
            out['source'] = sources[0]
        if (self.preferred_source is not "") and (self.preferred_source in sources):
            out = self.data_list[self.preferred_source]
            out['source'] = self.preferred_source
        if (len(full_time) > len(out)) & (fill_gaps is True):
            sources.remove(self.preferred_source)
            for source in sources:
                remaining = [x for x in full_time if x not in list(out.monthly)]
                add_dat = self.data_list[source][self.data_list[source].monthly.isin(remaining)]
                add_dat['source'] = source
                out = out.append(add_dat)
        self.preferred_serie = out

    def missingness_imputation(self, data, full_range):
        """Imputes the number of patients for missing monthes of data."""
        allmonths = pd.DataFrame(pd.date_range(full_range[0], full_range[1],
                                 freq='MS'), columns=['all_months_id'])
        allmonths['month_order'] = allmonths.all_months_id.rank()
        allmonths['month'] = allmonths.all_months_id.dt.strftime("%Y-%b")
        data['month'] = pd.to_datetime(data.monthly, format="%Y%m").dt.strftime("%Y-%b")
        all_data = pd.merge(data, allmonths, how='outer', left_on='month', right_on='month')
        all_data = all_data.sort_values('month_order')
        fit_data = all_data.dropna()
        spliner = CubicSpline(fit_data['month_order'], fit_data['value'],
                              bc_type = 'natural',
                              extrapolate=True)
        add_data = all_data[pd.isna(all_data.monthly)]
        add_data.value = spliner(add_data.month_order, extrapolate=True)
        add_data.source = 'imputation'
        out = fit_data.append(add_data)
        return out

    def format_monthly(self, monthly):
        monthly = monthly[0:4] + '-' + monthly[4:6]
        return monthly

    def impute_missing(self, full_range):
        data = self.preferred_serie
        self.imputed_serie = self.missingness_imputation(data, full_range)

    def benchmark_serie(self, train_perc=.75):
        data = self.preferred_serie.sample(frac=train_perc)
        full_range = [self.format_monthly(min(data.monthly)),
                      self.format_monthly(max(data.monthly))]
        imputed_serie = self.missingness_imputation(data, full_range)
        benchmark_data = self.preferred_serie.merge(imputed_serie, on = 'month', suffixes = ['' , '_imputed'])
        benchmark_data.value = pd.to_numeric(benchmark_data.value)
        benchmark_data.value_imputed = pd.to_numeric(benchmark_data.value_imputed)
        #rmse = benchmark_data.value - benchmark_data.value_imputed
        name = np.mean(abs(benchmark_data.value - benchmark_data.value_imputed) / np.mean(benchmark_data.value))
        return benchmark_data
