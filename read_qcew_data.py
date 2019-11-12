import urllib.request
import urllib
import csv


def createcsv(csv, f):
    try: dataLines = csv.decode().split('\r\n')
    except er: dataLines = csv.split('\r\n')
    with f:
        writer=csv.writer(f)
        
        for row in dataLines:
            writer.writerow(row)


def qcewGetAreaData(year,qtr,area):
    urlPath = "http://data.bls.gov/cew/data/api/[YEAR]/[QTR]/area/[AREA].csv"
    urlPath = urlPath.replace("[YEAR]",year)
    urlPath = urlPath.replace("[QTR]",qtr.lower())
    urlPath = urlPath.replace("[AREA]",area.upper())
    httpStream = urllib.request.urlopen(urlPath)
    csv = httpStream.read()
    httpStream.close()
    return csv


#Area Michigan
year="2015"
quarter="1"
AreaCode="26000"

rawdata=qcewGetAreaData(year,quarter,AreaCode)
f= open('Michigan_y{}_q{}_a{}.csv'.format(year,quarter,AreaCode),'w')
dataLines=rawdata.decode().split('\r\n')
with f:
    writer=csv.writer(f)
    for row in dataLines:
        writer.writerow(row)


#createcsv(rawdata,f)






