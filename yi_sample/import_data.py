import swifttools.ukssdc.data.GRB as udg
import swifttools.ukssdc.query as uq
import pandas as pd
from astropy.table import Table

saveToDir = True
saveDir = './data/'

# Setup query object.
q = uq.GRBQuery(cat='SDC_GRB')
q.addCol('Name')
q.addCol('TriggerNumber')

# Filter GRBs.
filter1 = ('Name', '>', 'GRB 050401A')
filter2 = ('Name', '<', 'GRB 150330A')
# filter2 = ('BAT_T90', '>', '2')
q.addFilter(filter1)
q.addFilter(filter2)

# Results.
q.submit()
grb_names, grb_triggers = list(q.results['Name']), list(q.results['TriggerNumber'])

# for name, id in zip(grb_names, grb_triggers):
#     print(name, id)

# Save lightcurves to variable.
fullList = udg.getLightCurves(targetID=grb_triggers,
                   destDir=saveDir,
                   incbad='yes',
                   saveData=False,
                   returnData = True,
                   silent=False)

for name, id, burst in zip(grb_names, grb_triggers, fullList):

    print('Name:', name, id, burst)
    print('Datasets:', fullList[burst]['Datasets'])

    try:    # Look for slew mode data.
        fullList[burst]['WTSLEW_incbad']
        sl = True
    except:
        sl = False

    try:    # Look for WT mode data.
        fullList[burst]['WT_incbad']
        wt = True
    except:
        wt = False

    try:    # Look for PC mode data.
        fullList[burst]['PC_incbad']
        pc = True
    except:
        pc = False

    print(sl, wt, pc)

    columns = ['Time','TimePos','TimeNeg','Rate','RatePos','RateNeg']

    table = []
    if sl:
        table.append(fullList[burst]['WTSLEW_incbad'][columns])
    if wt:
        table.append(fullList[burst]['WT_incbad'][columns])
    if pc:
        table.append(fullList[burst]['PC_incbad'][columns])

    if sl or wt or pc:
        fulltable = pd.concat([mode for mode in table])
        fulltable = fulltable.sort_values(by=['Time'])
        fulltable = fulltable.reset_index(drop=True)

        test = Table.from_pandas(fulltable)
        test.write(f"{saveDir}/{name.replace(' ','')}.qdp", err_specs={'terr': [1, 2]})

    else:
        print('NO DATA AVAILABLE FOR THIS BURST.')
    # print(fulltable)

    print('---')