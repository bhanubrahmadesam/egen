#############################################################################################################
# READ THE API AND WRITE IT TO A TEMP JSON
# NEW JSON FILE CREATED EACH DAY
#############################################################################################################
def readAPI():
    import requests
    import json
    a = requests.get('https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD')
    data = a.json()

    from datetime import datetime
    filename = datetime.now().strftime('%m%d%Y')+"_API.json"
    with open(filename, 'w') as f:
        json.dump(data, f)
    return filename
