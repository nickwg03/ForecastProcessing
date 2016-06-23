import pandas as pd
import h5py
import os
import numpy as np
from multiprocessing.pool import ThreadPool as Pool
from multiprocessing import cpu_count
from contextlib import closing
import time as t


def main_worker(file, item):
    print "\topening file"
    global src1
    global indset
    src1 = h5py.File(file,'r')  # open file for each worker
    indset = src1[item]

def process_chunk(chunk):
    df2 = pd.DataFrame(indset[chunk[0]:chunk[1]].transpose(), index=index)  # transpose data from row to column
    res = df2.reset_index().drop_duplicates(subset='index', keep="first").set_index("index")  # Keep first record of duplicates. Except for very first run, will want first as that timestamp occurred later into the simulation than the next duplicate, which occurs at the beginning of the next simulation.
    res = res.resample("15min").sum()  # resample dataframe to 15 minutes
    res = res.interpolate(method="linear")  # linear interpolation
    res = res.transpose()  # transpose back to original format (column to row)
    return res, chunk



if __name__=='__main__':
    os.chdir("/scratch/ngrue/india")  # location of hdf file
    processes = cpu_count()  # number of cores to use

    file = 'forecast_80m_testing.h5'  # name of hdf
    wspd_column = 'wspd_80'  # name of dataset in hdf
    output_file = "results_80m_multi.h5"  # output hdf


    time_cols = ["minute","hour","day","month","year"]
    t1 = t.time()

    with h5py.File(file) as src:
        d = dict({
            "year": src["year"][:].flatten(),
            "month": src["month"][:].flatten(),
            "day": src["day"][:].flatten(),
            "hour": src["hour"][:].flatten(),
            "minute": src["minute"][:].flatten()
        })  # collects up all time datasets for combination to generate index

        df = pd.DataFrame(d, index = range(0, len(src["month"])))
        date = df["day"].astype("str") + "-" + df["month"].astype("str") + "-" + df["year"].astype("str")
        time = df["hour"].astype("str") + ":" + df["minute"].astype("str")
        index = pd.to_datetime(date + " " + time, format="%d-%m-%Y %H:%M")  # convert individual time columns to timestamp index
        chunks = []
        for i in range(0,src[wspd_column].shape[0],1000):  # generate chunks for processing (1000 pixels per chunk)
            chunks.append([i, i + 1000])

    with h5py.File(output_file) as out_src:
        for item in [wspd_column]:
            with closing(Pool(processes, main_worker, [file, item])) as pool:  # open pool of workers
                try:
                    del out_src[item + "_15min"]  # deletes output hdf dataset in case it exists (from previous runs). If does not exists, just continues.
                except:
                    print "Cannot delete item or item does not exist"
                out_src.require_dataset(item + "_15min", (90889,35015), dtype = np.float32, chunks=(1000,35015), fillvalue = 0)  # generate empty dataset with desired shape and datatype
                dset = out_src[item + "_15min"]
                for res, chunk in pool.imap_unordered(process_chunk, chunks):  # run multiprocessing function using process_chunk function, passing in chunks of the hdf to process
                    print chunk, item, t.time() - t1
                    dset[chunk[0]:chunk[1],:] = res  # set chunks for output hdf as returned by multiprocessing function above

    print (t.time() - t1) / 60


