#The most recent LEHD Origin-Destination Employment Statistics available for most states is for year 2014.
import os
import shutil
import urllib2
import gzip

year = 2014
st_list = ["al", "ar", "az", "ca", "co", "ct", "dc", "de", "fl", "ga", "hi", "ia", "id", "il", 
           "in", "ks", "ky", "la", "ma", "md", "me", "mi", "mn", "mo", "ms", "mt", "nc", "nd",
           "ne", "nh", "nj", "nm", "nv", "ny", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn",
           "tx", "ut", "va", "vt", "wa", "wi", "wv", "wy"]
#no lodes data for puerta rico

zips_dir = os.path.join(os.getcwd(),'zips')
csvs_dir = os.path.join(os.getcwd(),'csvs')
national_csv = os.path.join(os.getcwd(),"national_lodes_"+str(year)+".csv")


def main():        
        if not os.path.isdir(zips_dir):
                os.mkdir(zips_dir)
        else:
                shutil.rmtree(zips_dir)
                os.mkdir(zips_dir)
        if not os.path.isdir(csvs_dir):
                os.mkdir(csvs_dir)
        else:
                shutil.rmtree(csvs_dir)
                os.mkdir(csvs_dir)
        #downloading and extracting lodes data by state
        for st in st_list:
                try:
                        download(st,year)               
                except urllib2.HTTPError, e:
                        try:
                                download(st, year-1)
                        except urllib2.HTTPError, e:
                                print "No Data in %s for year of %d and %d" % (st, year, year-1)
        #merging inton one national file
        national_file = open(national_csv,"wb")
        write_header = False
        for file_name in os.listdir(csvs_dir):
                st_file = os.path.join(csvs_dir, file_name)
                f = open(st_file, 'rb')
                if write_header:
                        f.readline()
                else:
                        national_file.write(f.readline())
                        write_header = True
                shutil.copyfileobj(f, national_file)
                f.close()

        national_file.close()


                
def download(st, year):
        dir = "http://lehd.ces.census.gov/data/lodes/LODES7/"+st+"/od/"
        url1 = dir +st+"_od_main_JT01_"+str(year)+".csv.gz"
        file_name = os.path.join(zips_dir, url1.split('/')[-1])
        u = urllib2.urlopen(url1)
        if(u.getcode() == 200):   
                f = open(file_name, 'wb')
                meta = u.info()
                file_size = int(meta.getheaders("Content-Length")[0])/1000
                print "Downloading and extracting: %s  %s KB" % (file_name, file_size)
                f.write(u.read())
                f.close()
        my_zip = gzip.open(file_name, 'rb')
        my_csv = open(os.path.join(csvs_dir, url1.split('/')[-1][:-3]),'wb')
        shutil.copyfileobj(my_zip, my_csv)
        my_zip.close()
        my_csv.close()

        
        url2 = dir +st+"_od_aux_JT01_"+str(year)+".csv.gz"
        file_name = os.path.join(zips_dir, url2.split('/')[-1])
        u = urllib2.urlopen(url2)
        if(u.getcode() == 200):   
                f = open(file_name, 'wb')
                meta = u.info()
                file_size = int(meta.getheaders("Content-Length")[0])/1000
                print "Downloading and extracting: %s  %s KB" % (file_name, file_size)
                f.write(u.read())
                f.close()
        my_zip = gzip.open(file_name, 'rb')
        my_csv = open(os.path.join(csvs_dir, url2.split('/')[-1][:-3]),'wb')
        shutil.copyfileobj(my_zip, my_csv)
        my_zip.close()
        my_csv.close()





                
if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    pass
