"""Read a DHIS database and prepare its content for analysis."""
import pandas as pd
import psycopg2 as pypg
from pathlib import Path
from dotenv import dotenv_values
import datetime as dt
import csv as csv
import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, Polygon, Point

class dhis_instance(object):
    """Information and metadata about a given DHIS instance.
    Parameters
    ----------
    dbname: The name of the database
    user: The user name used to access the database
    host: The server on which the database is hosted
    Attributes
    ----------
    organisationunit: DataFrame
        The organisation units in the DHIS
    dataelement: DataFrame
        Names and IDs of data elements in the DHIS
    orgunitstructure: DataFrame
        The hierarchical structure of the DHIS organisation units
    """

    def __init__(self, credentials):
        """Create a dhis instance."""
        self.dhis_connect(credentials)
        print("Getting Orgunits")
        self.organisationunit = pd.read_sql_query("SELECT organisationunitid, uid, name, path, coordinates FROM organisationunit;",
                                                  self.connexion)
        print("Getting Data Elements")
        self.dataelement = pd.read_sql_query("SELECT uid, name, dataelementid, categorycomboid FROM dataelement;", self.connexion)
        self.dataelement.name = self.dataelement.name.str.replace("\n|\r", " ")
        print("Getting Data Element Groups")
        self.dataelementgroup = pd.read_sql_query("SELECT uid, name, dataelementgroupid FROM dataelementgroup;", self.connexion)
        self.dataelementgroupmembers = pd.read_sql_query("SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;", self.connexion)
        print("Getting Orgunits Pyramid")
        self.orgunitstructure = pd.read_sql_query("SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;", self.connexion)
        self.label_org_unit_structure()
        print("Getting Category Option Combos")
        self.categoryoptioncombo = pd.read_sql_query("SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;", self.connexion)
        self.categorycombos_optioncombos = pd.read_sql_query("SELECT *  FROM categorycombos_optioncombos;", self.connexion)
        print("Getting Periods")
        self.periods = pd.read_sql_query("SELECT *  FROM _periodstructure;",
                                         self.connexion)

    def dhis_connect(self, credentials):
        fromPath = Path.home() / '.credentials'
        env_path = (fromPath / (credentials)).resolve()
        loaded = dotenv_values(dotenv_path=str(env_path))
        connecting = "dbname='" + loaded["dbname"] + "' user='" + loaded["user"] + "' host='" + loaded["host"] + "' password='" + loaded["password"] + "'"
        try:
            self.connexion = pypg.connect(connecting)
            print("Connected we are")
        except:
            print("Failed connection")

    def sql_read(self, sql_script, how="file"):
        if how == "file":
            query = open(sql_script, 'r')
            out = pd.read_sql_query(query.read(), self.connexion)
        if how == "str":
            out = pd.read_sql_query(sql_script, self.connexion)
        return out

    def build_de_cc_table(self):
        """Build table in which category combos are linked to data elements."""
        # First associate data elements to their category combos
        de_catecombos_options = self.dataelement.merge(self.categorycombos_optioncombos, on='categorycomboid')
        # Then associate data elements to category options combos
        de_catecombos_options_full = de_catecombos_options.merge(self.categoryoptioncombo, on='categoryoptioncomboid', suffixes=['_de', '_cc'])
        return de_catecombos_options_full

    def get_reported_de(self):
        # TODO : allow tailored reported values extraction
        """Get the amount of data reported for each data elements, aggregated at Level 3 level."""
        reported_de = pd.read_sql_query("SELECT datavalue.periodid, datavalue.dataelementid, _orgunitstructure.uidlevel3, count(datavalue) FROM datavalue JOIN _orgunitstructure ON _orgunitstructure.organisationunitid = datavalue.sourceid GROUP BY _orgunitstructure.uidlevel3, datavalue.periodid, datavalue.dataelementid;",
                                        self.connexion)
        reported_de = reported_de.merge(self.organisationunit,
                                        left_on='uidlevel3', right_on='uid',
                                        how='inner')
        reported_de = reported_de.merge(self.dataelement, 
                                        left_on='dataelementid',
                                        right_on='dataelementid',
                                        suffixes=['_orgUnit', '_data_element'])
        reported_de = reported_de.merge(self.periods)
        reported_de = reported_de.merge(self.orgunitstructure,
                                        left_on='uidlevel3',
                                        right_on='organisationunituid')
        reported_de = reported_de[['quarterly', 'monthly',
                                   'uidlevel2', 'namelevel2',
                                   'uidlevel3_x', 'namelevel3',
                                   'count',
                                   'uid_data_element', 'name_data_element']]
        reported_de.columns = ['quarterly', 'monthly',
                               'uidlevel2', 'namelevel2',
                               'uidlevel3', 'namelevel3',
                               'count',
                               'uid_data_element', 'name_data_element']
        return reported_de
        
    def get_data(self, de_ids, ou_ids, yearly=None, comment = ""):
        # TODO : allow tailored reported values extraction
        """Extract data reported for each data elements."""
        today = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        periods = []
        des = "('" + "','".join(de_ids) + "')"
        ous = "('" + "','".join(ou_ids) + "')"
        query = "SELECT datavalue.value, _orgunitstructure.uidlevel3, _orgunitstructure.uidlevel2, _periodstructure.enddate, _periodstructure.monthly, _periodstructure.quarterly, dataelement.uid AS dataelementid, dataelement.name AS dataelementname, categoryoptioncombo.uid AS CatComboID , categoryoptioncombo.name AS CatComboName,dataelement.created, organisationunit.uid as uidorgunit FROM datavalue JOIN _orgunitstructure ON _orgunitstructure.organisationunitid = datavalue.sourceid JOIN _periodstructure ON _periodstructure.periodid = datavalue.periodid JOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid JOIN organisationunit ON organisationunit.organisationunitid = datavalue.sourceid WHERE dataelement.uid IN " + des + " AND organisationunit.uid IN " + ous
        if yearly is not None :
            periods = self.periods.periodid[self.periods.yearly == str(yearly)].tolist()
            periods = [str(x) for x in periods]
            period_query =  " AND datavalue.periodid IN " + "('" + "','".join(periods) + "')"
            query = query + period_query
        data = pd.read_sql_query(query + ";", self.connexion)
        with open('data/logs/extraction_log.csv', 'a+', newline='') as extraction_log:
                fieldnames = ['date','de_ids', 'ou_ids', 'periods', 'comment']
                writer = csv.DictWriter(extraction_log, fieldnames=fieldnames)
                writer.writerow({'date':today, 'de_ids':de_ids, 'ou_ids':ou_ids, 'periods':periods, 'comment':comment})     
        return data

    def label_org_unit_structure(self):
        """Label the Organisation Units Structure table."""
        variables = self.orgunitstructure.columns
        uids = [x for x in variables if x.startswith('uid')]
        tomerge = self.organisationunit[['uid', 'name']]
        self.orgunitstructure = self.orgunitstructure.merge(tomerge,
                                                            left_on='organisationunituid',
                                                            right_on='uid')
        for uid in uids:
            tomerge.columns = ['uid', 'namelevel'+uid[-1]]
    # works as long as structure is less than 10 depth. update to regex ?
            self.orgunitstructure = self.orgunitstructure.merge(tomerge,
                                                                how='left',
                                                                left_on=uid,
                                                                right_on='uid')
        self.orgunitstructure = self.orgunitstructure[['organisationunituid', 'level'] + uids + ['namelevel'+x[-1] for x in uids]]
    
    def get_geodataframe(self, level, type_shape, structure=True):
        level_fosa = self.orgunitstructure.loc[self.orgunitstructure.level == level, "organisationunituid"]
        level_data = self.organisationunit[self.organisationunit.uid.isin(level_fosa)]
        if type_shape == "point":
            level_data.loc[:,"coordinates"] = level_data.coordinates.astype(str)
            level_data.loc[:,"lat"] = level_data.loc[:,"coordinates"].str.split("\[|\]|,").apply(lambda x: float(x[1]) if len(x) == 4 else np.nan)
            level_data.loc[:,"lon"] = level_data.loc[:,"coordinates"].str.split("\[|\]|,").apply(lambda x: float(x[2]) if len(x) == 4 else np.nan)
        if type_shape == "polygons":
            coordinates = level_data.loc[:, "coordinates"].str.replace("\[\[\[|\]\]\]","").str[1:-1].str.split("\],\[")
            coordinates = coordinates[~pd.isnull(coordinates)]
            coordinates = coordinates.apply(lambda x : [i.split(",") for i in x] if len(x) > 1 else np.nan)
            print(coordinates)
            coordinates = coordinates.apply(lambda x: [(float(i[0].replace("[","")), float(i[1].replace("]",""))) for i in x if (("E" in i[1]) | ("E" in i[0])) == False] if type(x) is list else np.nan)
            level_data.loc[:, "coordinates"] = coordinates.apply(lambda x : Polygon(x) if type(x) is list else np.nan)
            level_data = level_data[~pd.isnull(level_data.coordinates)]
        if structure == True :
            hierarchy = self.orgunitstructure[self.orgunitstructure.level == level]
            level_data = level_data.merge(hierarchy, left_on="uid", right_on="organisationunituid")
        geodataframe = gpd.GeoDataFrame(level_data, geometry="coordinates", crs={'init':'epsg:4326'})
        return geodataframe
