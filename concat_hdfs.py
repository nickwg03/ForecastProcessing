import h5py as h5
import os
import pandas as pd
from contextlib import closing
from multiprocessing.pool import ThreadPool as Pool
from multiprocessing import cpu_count
import numpy as np


os.chdir("/scratch/ngrue/india")
height = '100'
hub = height + 'm'
outfile = "forecast.%s.trial.h5" % (hub,)
processes = cpu_count()


def processer(i):
    if len(str(i)) < 2:
        i = str('0' + str(i))
    else:
        i = str(i)
    fin = h5.File("stats_data.%s.forecasts.all.%s.h5" % (i, hub,))
    dset = fin[f][:]
    print "returning", i
    return dset, i

with h5.File(outfile, "a") as fout:
    month_range = range(1,13)
    for f in ["day","minute","hour","month","year"]:
        dset_out = pd.DataFrame()
        for i in month_range:
            print f, i
            if len(str(i)) < 2:
                i = str('0' + str(i))
            else:
                i = str(i)
            fin = h5.File("stats_data.%s.forecasts.all.%s.h5" % (i, hub,))
            dset = pd.DataFrame(fin[f][:])
            dset_out = pd.concat([dset_out, dset])
        fout[f] = dset_out.values
    fout["lat"] = fin["lat"][:]
    fout["long"] = fin["long"][:]
    results = []
    for f in ["wspd_%s" % (height,), "tk_%s" % (height,), "rh_%s" % (height,), "pres_%s" % (height,)]:
        print "Creating dataset"
        try:
            del fout[f]
        except:
            pass
        out_dset = fout.create_dataset(f, (90889,28785), chunks=(90889, 500), dtype = np.float32)
        start = 0
        pool = Pool(processes) 
        results = list(pool.map(processer, month_range))
        pool.close()
        pool.join()
        for dset, i in results:
            end = start + dset.shape[1]
            out_dset[:,start:end] = dset
            start = end
            print "finished writing", f, i

print "Completed"
