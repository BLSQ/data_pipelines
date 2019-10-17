"""Read a DHIS database and prepare its content for analysis."""
import geopandas as gpd
import numpy as np

def dhis_to_GeoDataFrame(dhis2_orgunits_table, level, type_shape):
        level_data = dhis2_orgunits_table[dhis2_orgunits_table.level == level]
        level_data.loc[:,"coordinates"] = level_data.coordinates.astype(str)
        if type_shape == "point":
            get_latlon = lambda x: Point(float(x[1]), float(x[2])) if len(x) == 4 else Point(np.nan, np.nan)
            level_data.loc[:, "coordinates"] = level_data.loc[:,"coordinates"].str.split("\[|\]|,").apply(get_latlon)
        if type_shape == "polygon":
            coordinates = level_data.loc[:, "coordinates"].str.replace("\[\[\[|\]\]\]","").str[1:-1].str.split("\],\[")
            coordinates = coordinates.apply(lambda x : [i.split(",") for i in x] if len(x) > 1 else np.nan)
            coordinates = coordinates.apply(lambda x: [(float(i[0].replace("[","")), float(i[1].replace("]",""))) for i in x if (("E" in i[1]) | ("E" in i[0])) == False] if type(x) is list else np.nan)
            level_data.loc[:, "coordinates"] = coordinates.apply(lambda x : Polygon(x) if type(x) is list else np.nan)
            level_data = level_data[~pd.isnull(level_data.coordinates)]
        geodataframe = gpd.GeoDataFrame(level_data, geometry="coordinates", crs={'init':'epsg:4326'})
        return geodataframe