import pandas as pd
import datetime
import pytz
import sys

pst = pytz.UTC
timezone = pytz.timezone('Australia/Sydney')


def main():
    details_df = pd.read_csv("../data/propertiesDetails.csv")
    schools_df = pd.read_csv("../data/property_school.csv", header=None, usecols=[0, 2, 3, 4, 5, 6, 7])
    schools_df = schools_df.rename(
        columns={0: 'id', 2: 'school_1', 3: 'school_distance_1', 4: 'school_2', 5: 'school_distance_2', 6: 'school_3',
                 7: 'school_distance_3'})
    empty_df = pd.DataFrame([[0, '', '', '', '', '', '']],
                            columns=['id', 'school_1', 'school_distance_1', 'school_2', 'school_distance_2', 'school_3',
                                     'school_distance_3'])
    schools_df = pd.concat([schools_df, empty_df])
    details_df['id'] = details_df['id'].fillna(value=0)
    details_df['id'].astype('int64')
    properties_df = details_df.merge(details_df.merge(schools_df, how='left', on='id', sort=False))

    properties_df['inspections.openingDateTime'] = properties_df['inspections.openingDateTime'].apply(utc_local_time)
    properties_df['inspections.closingDateTime'] = properties_df['inspections.closingDateTime'].apply(utc_local_time)
    properties_df['saleDetails.auctionDetails.auctionSchedule.openingDateTime'] = \
        properties_df['saleDetails.auctionDetails.auctionSchedule.openingDateTime'].apply(utc_local_time)
    properties_df.to_csv('../data/orig_result.csv')

    properties_df = properties_df.rename(
        columns={'addressParts.displayAddress': 'Address', 'addressParts.suburb': 'Suburb',
                 'priceDetails.displayPrice': 'priceDetails', 'inspections.closingDateTime': 'InspectionCloseTime',
                 'inspections.openingDateTime': 'InspectionOpenTime',
                 'saleDetails.auctionDetails.auctionSchedule.locationDescription': 'AuctionLocation',
                 'saleDetails.auctionDetails.auctionSchedule.openingDateTime': 'AuctionOpenTime',
                 'saleDetails.saleMethod': 'saleMethod', 'inspectionDetails.inspections': 'inspections'})

    properties_df = properties_df.drop(axis=1, columns=['addressParts.displayType', 'addressParts.stateAbbreviation',
                                                        'addressParts.street', 'addressParts.streetNumber',
                                                        'addressParts.suburbId', 'addressParts.postcode', 'media',
                                                        'advertiserIdentifiers.contactIds', 'addressParts.unitNumber',
                                                        'advertiserIdentifiers.advertiserType', 'channel', 'dateUpdated',
                                                        'energyEfficiencyRating', 'id', 'inspectionDetails.inspections',
                                                        'inspectionDetails.pastInspections', 'inspections.description',
                                                        'inspections.recurrence', 'isNewDevelopment', 'objective',
                                                        'priceDetails.canDisplayPrice', 'priceDetails.price',
                                                        'priceDetails.priceFrom', 'priceDetails.pricePrefix',
                                                        'priceDetails.priceTo', 'saleDetails.soldDetails.source',
                                                        'saleDetails.tenderDetails.tenderAddress',
                                                        'saleDetails.tenderDetails.tenderRecipientName',
                                                        'saleMode', 'status', 'virtualTourUrl'], errors='ignore')
    # properties_df.to_csv('../data/after_drop.csv')

    properties_df = properties_df[['Address', 'Suburb', 'bedrooms', 'bathrooms', 'carspaces', 'landAreaSqm',
                                   'buildingAreaSqm', 'propertyTypes', 'priceDetails', 'InspectionOpenTime',
                                   'InspectionCloseTime', 'headline', 'saleMethod', 'AuctionLocation', 'AuctionOpenTime',
                                   'school_1', 'school_distance_1', 'school_2', 'school_distance_2', 'school_3',
                                   'school_distance_3', 'features', 'description', 'seoUrl']]

    properties_df.to_csv('../data/result.csv')


def utc_local_time(time_str_utc):
    if isinstance(time_str_utc, str):
        d = datetime.datetime.strptime(time_str_utc, '%Y-%m-%dT%H:%M:%SZ')
        d = pst.localize(d)
        d = d.astimezone(timezone)
        return d.strftime("%Y-%m-%d  %H:%M")
    else:
        return None


if __name__ == "__main__":
    sys.exit(main())