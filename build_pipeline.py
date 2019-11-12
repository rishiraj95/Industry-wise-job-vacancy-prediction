#Setting up Luigi Tasks

import luigi
import requests
import zipfile
import psycopg2
import os
import re

#First task to download the datasets. Create a Luigi task that will download the datasets and put them into the required database.


class downloadQCEWdata(luigi.Task):
# This class downloads the QCEW data for a year     
    year=luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('{}_quarterly.zip'.format(self.year),luigi.format.Nop)



    def run(self):
# Download the data        
        url = 'https://data.bls.gov/cew/data/files/{}/csv/{}_qtrly_by_area.zip'.format(self.year, self.year)
        r=requests.get(url)
        with self.output().open('wb') as f:
            f.write(r.content)

class unzipQCEWdata(luigi.Task):
# This class unzips a downloaded zip file            
    year=luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('./{}_quarterly'.format(self.year))
    
    def requires(self):
        return downloadQCEWdata(self.year)


    def run(self):
        with zipfile.ZipFile('{}_quarterly.zip'.format(self.year),'r') as zip_ref:
            zip_ref.extractall('./{}_quarterly'.format(self.year))
        zip_ref.close()

#Put files into PostgreSql
class putQCEWpostgres(luigi.Task):
    def run(self):
        data_dir="./1990_quarterly/1990.q1-q4.by_area"
        conn_cred="host=54.87.35.15 dbname=qcew user=postgres password=happy@1234"
        conn=psycopg2.connect(conn_cred)
        cur=conn.cursor()
        for filename in os.listdir(data_dir): 
            tablename=filename[17:-3]+filename[:16]
            tablename=''.join(tablename.split())
            tablename=tablename.replace(',', '_')
            tablename=tablename.replace('.','_')
            tablename=tablename.replace('-','_')
            tablename=re.sub("[$&+,:;=?@#|'<>.^*()%!-]",'',tablename)
            print(tablename)
            cur.execute("""CREATE TABLE {}(area_fips text, own_code text, industry_code text, agglvl_code text, size_code text, year integer, qtr integer, disclosure_code text, area_title text, own_title text,
       industry_title text, agglvl_title text, size_title text, qtrly_estabs_count text,
       month1_emplvl text, month2_emplvl text, month3_emplvl text, total_qtrly_wages text,
       taxable_qtrly_wages text, qtrly_contributions text, avg_wkly_wage text,
       lq_disclosure_code text, lq_qtrly_estabs_count text, lq_month1_emplvl text,
       lq_month2_emplvl text, lq_month3_emplvl text, lq_total_qtrly_wages text,
       lq_taxable_qtrly_wages text, lq_qtrly_contributions text, lq_avg_wkly_wage text,
       oty_disclosure_code text, oty_qtrly_estabs_count_chg text,
       oty_qtrly_estabs_count_pct_chg text, oty_month1_emplvl_chg text,
       oty_month1_emplvl_pct text, oty_month2_emplvl_chg text,
       oty_moanth2_emplvl_pct text, oty_month3_emplvl_chg text,
       oty_month3_emplvl_pct text, oty_total_qtrly_wages_chg text,
       oty_total_qtrly_wages_pct text, oty_taxable_qtrly_wages_chg text,
       oty_taxable_qtrly_wages_chg1 text, oty_qtrly_contributions_chg text,
       oty_qtrly_contributions_pct text, oty_avg_wkly_wage_chg text,
       oty_avg_wkly_wage_pct text)""".format(tablename))

            cur.execute("truncate " + tablename)
#Commit th transaction to begin a new transaction. This avoids too many locks per transaction problem.
            conn.commit()
        conn.close()

"""
                
            with open(os.path.join(data_dir,filename)) as f:
                next(f)
                cur.copy_from(f, tablename, sep=',')
                conn.commit()

"""
# Delete tables from qcew db

class delete_tables_qcew(luigi.Task):
    def run(self):
        data_dir="./1990_quarterly/1990.q1-q4.by_area"
        conn_cred="host=54.87.35.15 dbname=qcew user=postgres password=happy@1234"
        conn=psycopg2.connect(conn_cred)
        cur=conn.cursor()
        for filename in os.listdir(data_dir): 
            tablename=filename[17:-3]+filename[:16]
            tablename=''.join(tablename.split())
            tablename=tablename.replace(',', '_')
            tablename=tablename.replace('.','_')
            tablename=tablename.replace('-','_')
            tablename=re.sub("[$&+,:;=?@#|'<>.^*()%!-]",'',tablename)
            cur.execute("drop table {}".format(tablename))
            conn.commit()
        conn.close()

if __name__ == '__main__':
    luigi.run()





        






