from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:Datam1ner@mydb.cnqujrmnprhe.us-west-1.rds.amazonaws.com:5432/tdm', echo=False)

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from os.path import dirname, join, isfile
from os import getcwd, mkdir, listdir
csvs_dir = join(join(dirname(getcwd()), 'lehd'),'csvs')

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)

from sqlalchemy import Column, Integer, String
from sqlalchemy import Sequence
class trip(Base):
    __tablename__ = 'trip'
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    w_geocode = Column(String)
    h_geocode = Column(String)
    S000 = Column(Integer)
    createdate = Column(String)

try:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    session = Session()
    for i, ifile in enumerate(listdir(csvs_dir)):
        file_name = join(csvs_dir, ifile)
        #if i!=0:
        #    continue

        try:
            f = open(file_name, 'r')
            readHeader = True
            for line in f:
                if readHeader:
                    readHeader = False
                    continue
                data = line.strip('\n').split(',')
                i_trip = trip(w_geocode=data[0], h_geocode=data[1], S000=int(data[2]), createdate=data[-1])
                session.add(i_trip)
            print("SUCCESS: migrated %s to postgres" % file_name)
        except Exception:
            print("Failed: migrating %s to postgres" % file_name)
            raise
        finally:
            f.close()
            session.commit()
except Exception:
    raise
finally:
    session.close()
