import requests
year="1990"
url = 'https://data.bls.gov/cew/data/files/1990/csv/{}_qtrly_by_area.zip'.format(year)
r=requests.get(url)


filename='try.zip'

with open (filename,'wb') as f:
    f.write(r.content)


