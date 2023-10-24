import pandas as pd
import numpy as np
import logging
import os

logger = logging.getLogger('get_tables')
logger.setLevel('INFO')
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


#############################################################################
# DEFINE TABLES AND COLUMN NAMES
#############################################################################

# Name, T90

# Main summary table.
summary_path = 'https://swift.gsfc.nasa.gov/results/batgrbcat/summary_cflux/summary_general_info/summary_general.txt'
summary_cols = ['GRBname', 'Trig_ID', 'T90', 'T90_err']

# Redshift table.
redshift_path = 'https://swift.gsfc.nasa.gov/results/batgrbcat/summary_cflux/summary_general_info/GRBlist_redshift_BAT.txt'
redshift_cols = ['GRBname', 'z', 'Uncertainty']

all_grbs_filepath = 'grb_tables/all_grb.csv'
redshift_grbs_filepath = 'grb_tables/redshift_grb.csv'

#############################################################################
# SWIFT BAT CATALOG
#############################################################################

def updateTable() -> pd.DataFrame:
    """
    Download the latest table versions from Swift databases and format them.

    Inputs:
        tab_summary     globally defined url to summary table.
           _redshift    "                     " redshift table.
           
    Returns:
        table           formatted and fully merged table of GRB parameters.
    """
    logger.debug("updateTable() called.")

    # Download required tables and format them.
    logger.debug("Downloading tables.")
    tab_summary   = formatTable(pd.read_table(summary_path,  sep='|', header=22), summary_cols)
    tab_redshift  = formatTable(pd.read_table(redshift_path, sep='|', header=18), redshift_cols)

    logger.debug("Merging tables.")
    merge_par = {'on':'GRBname', 'how':'left'}
    table = tab_summary.merge(tab_redshift, **merge_par)
    
    logger.debug("Rename columns.")
    table.rename(columns={'z': 'redshift', 'Uncertainty':'z_err'}, inplace=True)

    # Check and remove any columns with name or ID as N/A -- these rows are never useful.
    logger.debug("Remove row with empty GRB name.")
    table = table[~table['GRBname'].isnull()]

    # Set column data types.
    logger.debug("Set column datatypes.")
    table.T90         = table.T90.astype(float)
    table.T90_err     = table.T90_err.astype(float)
    table.redshift    = table.redshift.astype(str)
    table.z_err       = table.z_err.astype(str)

    logger.debug("Adding 'A' suffix in the case of bursts that don't have one.")
    for _, row in table.iterrows():
        if row.GRBname[-1] not in ('A', 'B', 'C', 'D'):
            row.GRBname = row.GRBname+'A' # If GRB doesn't end in a letter, assume it's meant to be A.

        print(row.redshift, type(row.redshift))
        print(row.z_err, type(row.z_err))

    logger.debug("Complete; returning table.")
    print(table)
    return table

def formatTable(data: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Take in an imported Swift table and return the formatted table.

    Inputs:
        table           Variable pointing to imported table.s
        cols            Array of col names to keep.

    Returns:
        data            Pandas data table after formatting.
    """

    logger.debug("Starting formatTable")
    logger.debug("Format column names.")
    data.columns = data.columns.str.replace(' ', '', regex=True)
    data.columns = data.columns.str.replace('#', '', regex=True)
    data.columns = data.columns.str.replace(';', '', regex=True)
    data.columns = data.columns.str.replace('-', '', regex=True)
    data.columns = data.columns.str.replace('.', '', regex=False)
    data.columns = data.columns.str.replace('\([^)]*\)', '', regex=True) # Remove brackets and everything in them.
    # There's a better way to do this, I'm sure.

    # Remove all spaces from table values.
    logger.debug("Format table values.")
    for column in data:
        if data[column].dtypes is not np.float64: # Check the column isn't automatically imported as floats (for once a properly formatted BAT catalog table...)
            data[column] = data[column].str.replace(" ", "", regex=True)
            data[column] = data[column].str.replace("?", "", regex=False)
            data[column] = data[column].replace("N/A", np.NaN) # read_table NaNs are not yet stripped so N/A is missed at first sweep.
            data[column] = data[column].str.replace('\([^)]*\)', '', regex=True)

    # Only keep the required columns.
    data = data[cols]

    logger.debug("Completed format.")
    return data

# def onlyRedshift(data):

#     # Remove rows without any redshifts.
#     newtable = data[~data['z'].isnull()]
    
#     # Only bursts with CPL best fit.
#     # newtable = newtable[newtable['reduced_chi2'] > 0.5]
#     # newtable = newtable[newtable['reduced_chi2'] < 1.5]

#     # Seems there's a significant population with Epeak ~10000 keV
#     # newtable = newtable[newtable['Epeak'] < 9000]

#     lg.debug("Producing data table of bursts only with redshifts.")
#     lg.debug("NOTE: for now, only values that can be parsed as floats are available.")
#     for idx, row in newtable.iterrows():
#         # Test if redshift can be made into a float, else remove that row.
#         try:
#             newtable.at[idx, ['z']] = float(row.z)
#             # Test if redshift err can be made into a float, else set that redshift to 0.
#             try:
#                 newtable.at[idx, 'z_err'] = float(row.z_err)
#             except:
#                 lg.debug(f"Setting row {idx}: {row.z_err} to {0}.")
#                 newtable.at[idx, 'z_err'] = 0
#         except:
#             lg.debug(f"Dropping row {idx} for value '{row.z}'")
#             newtable.drop(index=idx, axis=0, inplace=True)

#     lg.debug("Setting column data types to floats.")
#     newtable.z     = newtable.z.astype(float)
#     newtable.z_err = newtable.z_err.astype(float)

#     return newtable


if __name__ == '__main__':

    logger.info("Starting update process.")

    # Obtain old tables if they exist.
    if os.path.exists(all_grbs_filepath):
        old_all = pd.read_csv(all_grbs_filepath)
        # old_redshift = pd.read_csv(redshift_grbs_filepath)
        OldTables = True
        os.remove(all_grbs_filepath)
        # os.remove(redshift_grbs_filepath)
        logger.debug("Found current existing tables.")
    else:
        OldTables = False
        logger.debug("Table does not exist yet.")

    # Updating tables.
    logger.info("Downloading update tables (this may take a few moments)...")
    new_table = updateTable()

    # Compare old to new; report how many new burst there are.
    if OldTables:
        logger.debug("Compare old/new tables.")
        comparison_table = new_table.merge(old_all.drop_duplicates(), on=['GRBname'], how='left', indicator=True)
        new_bursts = comparison_table.query("_merge == 'left_only'")

        if len(new_bursts) > 0:
            logger.info(f"{len(new_bursts)} new bursts found since last update.")
        else:
            logger.info(f"No new burst data recorded since last update.")

        
    # Output to file.
    logger.info(f"Saving table to {all_grbs_filepath}.")
    
    new_table.to_csv(all_grbs_filepath, index=False)