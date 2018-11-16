"""Builds a preferred serie on a given thematics from multiple series."""

import pandas as pd
from scipy.interpolate import CubicSpline


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

    def impute_missing(self, full_range):
        """Imputes the number of patients for missing monthes of data."""
        allmonths = pd.DataFrame(pd.date_range(full_range[0], full_range[1],
                                 freq='MS'), columns=['all_months_id'])
        allmonths['month_order'] = allmonths.all_months_id.rank()
        allmonths['all_months'] = allmonths.all_months_id.dt.strftime("%Y-%b")
        self.preferred_serie['month'] = pd.to_datetime(self.preferred_serie.monthly, format="%Y%m").dt.strftime("%Y-%b")
        all_data = pd.merge(self.preferred_serie, allmonths, how='outer', left_on='month', right_on='all_months')
        all_data = all_data.sort_values('month_order')

        #y_to_spline = all_data['value']
        #spliner = CubicSpline(all_data['month_order'], y_to_spline, extrapolate = True)
        #self.preferred_serie.month =

        return all_data

    def benchmark_serie(self):
        return self
