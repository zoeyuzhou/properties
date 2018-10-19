import requests
import sys
import pandas as pd
import json
from pandas.io.json import json_normalize


def main():
    # test = True
    test = False

    # for test:
    if test:
        file1 = './sampleJson/propertySample.json'
        file2 = './sampleJson/propertySample2.json'
        df1 = get_property_for_test(file1)
        df2 = get_property_for_test(file2)
        orig_df = df1.append(df2)
        orig_df.to_csv('../data/propertiesDetails.csv')
    else:
        # for run

        # token = get_token()
        token = '5a970a23cf4d25179986fae107992f48'
        properties = get_property_id_from_file('../data/property_id.txt')

        orig_df = pd.DataFrame()
        for property_id in properties:
            df = get_property(property_id, token)
            orig_df = orig_df.append(df)
            print(orig_df.head())

        orig_df.to_csv('../data/propertiesDetails.csv')


def get_token():
    payload = 'grant_type=client_credentials&scope=api_agencies_read%20api_listings_read'
    r = requests.post('https://auth.domain.com.au/v1/connect/token',
                      auth=('client_TODO_CHANGE', 'secret_TODO_CHANGE'),
                      data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})

    token = r.json().get('access_token')
    print(token)
    return token


def get_property(property_id, token):
    url = 'https://api.domain.com.au/v1/listings/' + property_id
    headers = {'Authorization':  'Bearer ' + token}
    print(headers)
    r = requests.get(url, headers=headers, verify=False)
    print(r.text)
    d = json.loads(r.text)

    new_df = None
    if d.get('inspectionDetails', None) is not None:
        if d.get('inspectionDetails', None).get('inspections', None) is not None:
            if len(d.get('inspectionDetails', None).get('inspections', None)) > 0:
                new_df = json_normalize(d.get('inspectionDetails', None).get('inspections', None)).add_prefix('inspections.')

    df = json_normalize(d)
    df = df.drop(axis=1, columns=['media', 'advertiserIdentifiers.contactIds',
                                  'advertiserIdentifiers.advertiserId'], errors='ignore')
    if new_df is not None:
        result_df = pd.concat([df, new_df], axis=1)
    else:
        result_df = df
    return result_df


def get_property_for_test(filename):
    with open(filename) as f:
        d = json.load(f)
    new_df = None
    if len(d.get('inspectionDetails', None).get('inspections', None)) > 0:
        new_df = json_normalize(d.get('inspectionDetails', None).get('inspections', None)).add_prefix('inspections.')

    df = json_normalize(d)
    df = df.drop(axis=1, columns=['inspectionDetails.inspections', 'media', 'advertiserIdentifiers.contactIds',
                                  'advertiserIdentifiers.advertiserId'], errors='ignore')
    if new_df is not None:
        result_df = pd.concat([df, new_df], axis=1)
    else:
        result_df = df
    return result_df


def get_property_id_from_file(fname):
    with open(fname) as f:
        content = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in content]


if __name__ == "__main__":
    sys.exit(main())