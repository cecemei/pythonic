#The most recent LEHD Origin-Destination Employment Statistics available for most states is for year 2015.

from os.path import join, isdir
from os import getcwd, mkdir, listdir
from shutil import copyfileobj, rmtree
import gzip
import requests
import io
import pdb

year = 2016
st_list = ["al", "ar", "az", "ca", "co", "ct", "dc", "de", "fl", "ga", "hi", "ia", "id", "il",
           "in", "ks", "ky", "la", "ma", "md", "me", "mi", "mn", "mo", "ms", "mt", "nc", "nd",
           "ne", "nh", "nj", "nm", "nv", "ny", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn",
           "tx", "ut", "va", "vt", "wa", "wi", "wv", "wy"]
#no lodes data for puerta rico

zips_dir = join(getcwd(),'zips')
csvs_dir = join(getcwd(),'csvs')
national_csv = join(getcwd(),"national_lodes_"+str(year)+".csv")

def pre_clean():
    def create_emptydir(idir):
        if not isdir(idir):
            mkdir(idir)
        else:
            rmtree(idir)
            mkdir(idir)

    create_emptydir(zips_dir)
    create_emptydir(csvs_dir)

def main(mergeNational=False):
    #pre_clean()
    #downloading and extracting lodes data by state
    for st in st_list:
        searchyear = year
        while searchyear>(year-5):
            try:
                download(st, searchyear)
                break
            except requests.exceptions.HTTPError as e:
                print("No Data in %s for year of %d" % (st, searchyear))
                searchyear -= 1

    #merging into one national file
    if mergeNational:
        try:
            national_file = open(national_csv,"wb")
            write_header = False
            for file_name in listdir(csvs_dir):
                st_file = join(csvs_dir, file_name)
                f = open(st_file, 'rb')
                if write_header:
                    f.readline()
                else:
                    national_file.write(f.readline())
                    write_header = True
                copyfileobj(f, national_file)
                f.close()
        except Exception as e:
            raise
        finally:
            national_file.close()



def download(st, year):
    rooturl = "http://lehd.ces.census.gov/data/lodes/LODES7/"+st+"/od/"
    url1 = rooturl +st+"_od_main_JT01_"+str(year)+".csv.gz"
    url2 = rooturl +st+"_od_aux_JT01_"+str(year)+".csv.gz"

    def filestream_io(url):
        file_name = join(zips_dir, url.split('/')[-1])
        file_name_csv = join(csvs_dir, url.split('/')[-1][:-3])
        response = requests.get(url, stream=True)
        if(response.status_code == 200):
            file_size = response.headers['Content-Length']
            print("Downloading and extracting: %s  %s KB" % (file_name, file_size))
            with open(file_name, 'wb') as f, open(file_name_csv, 'wb') as f_csv:
                copyfileobj(io.BytesIO(response.content), f)
                copyfileobj(gzip.open(io.BytesIO(response.content)), f_csv)

        else:
            response.raise_for_status()

    filestream_io(url1)
    filestream_io(url2)






if __name__ == "__main__":
    try:
        import cProfile
        #cProfile.run('main()')
        pr = cProfile.Profile()
        pr.enable()
        main()
        pr.disable()
        pr.print_stats(sort='time')
    except KeyboardInterrupt:
        pass
