"""Read a DHIS database and prepare its content for analysis."""
import pandas as pd
import psycopg2 as pypg
from pathlib import Path
from dotenv import dotenv_values
import datetime as dt
import csv as csv

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
        self.organisationunit = pd.read_sql_query("SELECT organisationunitid, uid, name, path FROM organisationunit;",
                                                  self.connexion)
        self.dataelement = pd.read_sql_query("SELECT uid, name, dataelementid, categorycomboid FROM dataelement;", self.connexion)
        self.dataelement.name = self.dataelement.name.str.replace("\n|\r", " ")
        self.dataelementgroup = pd.read_sql_query("SELECT uid, name, dataelementgroupid FROM dataelementgroup;", self.connexion)
        self.dataelementgroupmembers = pd.read_sql_query("SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;", self.connexion)
        self.orgunitstructure = pd.read_sql_query("SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;", self.connexion)
        self.categoryoptioncombo = pd.read_sql_query("SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;", self.connexion)
        self.categorycombos_optioncombos = pd.read_sql_query("SELECT *  FROM categorycombos_optioncombos;", self.connexion)
        self.periods = pd.read_sql_query("SELECT *  FROM _periodstructure;",
                                         self.connexion)
        self.label_org_unit_structure()

    def dhis_connect(self, credentials):
        fromPath = Path.home() / '.credentials'
        loaded = dotenv_values(dotenv_path=str(fromPath.resolve()))
        connecting = "dbname='" + loaded["RDS_DB_NAME"] + "' user='" + loaded["RDS_USERNAME"]  + "' password='" + loaded["RDS_PASSWORD"] + "'"
        try:
            self.connexion = pypg.connect(connecting)
        except:
            print("Failed connection")

    def build_de_cc_table(self):
        """Build table in which category combos are linked to data elements."""
        # First associate data elements to their category combos
        de_catecombos_options = self.dataelement.merge(self.categorycombos_optioncombos, on='categorycomboid')
        # Then associate data elements to category options combos
        de_catecombos_options_full = de_catecombos_options.merge(self.categoryoptioncombo, on='categoryoptioncomboid', suffixes=['_de', '_cc'])
        return de_catecombos_options_full

    def get_reported_de(self, level = 'uidlevel3', period1 = 'quarterly', period2='monthly'):
             """Get the amount of data reported for each data elements, aggregated at Level of choice -
            default is level 3, and periods of choice - defaults are quarterly and monthly."""
            hierachical_level = (level) # this has to be a tuple

            query = f"""
                SELECT datavalue.periodid, datavalue.dataelementid, _orgunitstructure.{hierachical_level}, 
                count(datavalue) FROM datavalue JOIN _orgunitstructure ON _orgunitstructure.organisationunitid = datavalue.sourceid
                GROUP BY _orgunitstructure.uid{hierachical_level}, datavalue.periodid, datavalue.dataelementid;
            """

            reported_de = pd.read_sql_query(query,
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
            reported_de = reported_de[[period1, period2,
                                       'uidlevel2', 'namelevel2',
                                       'uidlevel3_x', 'namelevel3',
                                       'count',
                                       'uid_data_element', 'name_data_element']]
            reported_de.columns = [period1, period2,
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
        log = [today, de_ids, ou_ids, periods]
       # with open('data/logs/extraction_log.csv', 'a+', newline='') as extraction_log:
               # fieldnames = ['date','de_ids', 'ou_ids', 'periods', 'comment']
               # writer = csv.DictWriter(extraction_log, fieldnames=fieldnames)
               # writer.writerow({'date':today, 'de_ids':de_ids, 'ou_ids':ou_ids, 'periods':periods, 'comment':comment})
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

    def impute_zero_dataelementname(df):
        
        """ This function is used to populate a pandas dfs with rows for each data element and zero imputed value
        where at least one value for fosa x period x data element exists in the data.
        
        :param df: This is a pandas df that has rows only for manually inputed data element.
        
        :return: out: This function returns a pandas df augmented with extra rows for every existing data element.
        """
    
        import pandas as pd
        
        out = []
        
        list_fosa = df.fosa.unique()
        
        for fosa in list_fosa:
            sub_df = df[df.fosa == fosa]
            # first, drop duplicates in month x data element, then create a contigency table month x dataelement
            sub_df_piv = sub_df.drop_duplicates(subset=["monthly","dataelementname"], keep='last').pivot(index="monthly", columns="dataelementname", values=["value"])
            # second, stacked the values to go back to a df format with one row per month and data element
            # we replace NAs by 0, as we think that if a report was filled out, the missing values are really zeros
            sub_df_piv_stacked = sub_df_piv.fillna(0).stack()
            sub_df_piv_stacked = pd.DataFrame(sub_df_piv_stacked)
            sub_df_piv_stacked = sub_df_piv_stacked.reset_index()
            # finally, we merged it back with the original data frame
            sub_df_col = sub_df[["province","fosa","monthly","quarterly"]]
            new_df = sub_df_col.merge(sub_df_piv_stacked, 'outer')
            
            out.append(new_df)
        # return the transformed data frame
        df_augmented = pd.concat(out)
        return(df_augmented)
