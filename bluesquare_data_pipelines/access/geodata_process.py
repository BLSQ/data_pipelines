"""Read a DHIS database and prepare its content for analysis."""
import geopandas as gpd
import numpy as np

def dhis_to_GeoDataFrame(dhis2_orgunits_table, level, type):
        level_data = dhis2_orgunits_table[dhis2_orgunits_table.level == level]
        if type == "point":
            level_data.loc[:,"coordinates"] = level_data.coordinates.astype(str)
            level_data.loc[:,"lat"] = level_data.loc[:,"coordinates"].str.split("\[|\]|,").apply(lambda x: float(x[1]) if len(x) == 4 else np.nan)
            level_data.loc[:,"lon"] = level_data.loc[:,"coordinates"].str.split("\[|\]|,").apply(lambda x: float(x[2]) if len(x) == 4 else np.nan)
        data = level_data.drop(["lat","lon","coordinates"], axis=1)
        geodataframe = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(level_data.lat, level_data.lon) ,crs={'init':'epsg:4326'})
        return geodataframe