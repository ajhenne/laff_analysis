import laff
import glob
from csv import writer
import os

####################################################################################

def export_fit(name, grb):

    with open('runs/continuum.csv', 'a') as file:
        writer_object = writer(file)
        
        par_names = laff.utility.PAR_NAMES_CONTINUUM
        stat_names = laff.utility.STAT_NAMES_CONTINUUM
        
        continuum_par = list([grb['continuum']['parameters'][x] for x in par_names])
        continuum_fit_stats = [grb['continuum']['fit_statistics'][x] for x in stat_names]
        continuum_fluence = grb['continuum']['fluence']

        continuum_output = (name, *continuum_par, *continuum_fit_stats, continuum_fluence)
        
        writer_object.writerow(continuum_output)

    with open('runs/flares.csv', 'a') as file:
        writer_object = writer(file)

        if grb['flares'] is not False:
            for count, flare in enumerate(grb['flares']):

                print('fluences for this flare: ', *flare['fluence'])
                flare_output = (name, count, *flare['times'], *flare['par'], *flare['par_err'], *flare['fluence'], *flare['peak_flux'])
                writer_object.writerow(flare_output)

    return



def write_error(name, message):
    with open('runs/error.csv', 'a') as file:
        writer(file).writerow([name + ' | ' + message])
    return


if __name__ == '__main__':

    print('[[ starting run_yi ]]')
    data_files = glob.glob('/Users/ah724/Desktop/grb_lc_files/yi_sample/*.qdp')

    for filepath in data_files:
        name = os.path.basename(filepath)
        print(name, 'started')

        # Import data.
        try: data = laff.lcimport(filepath, format='python_query')
        except: write_error(name, 'data import failed'); continue

        # Fit data.
        try: grb = laff.fitGRB(data)
        except: write_error(name, 'data fit failed'); continue

        # Export to CSV.
        export_fit(name, grb)

        print(f'Finished {name}')

    print('[[ run_yi complete ]]')