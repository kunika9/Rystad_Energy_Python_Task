import pandas as pd
import requests


def get_dataset(padd_no):
    # personal api key
    api_key = "4915003e3c8e7a7db077368f7f7a4792"
    url = "http://api.eia.gov/series/?api_key=" + api_key + "&series_id=PET.MCRRIP" + str(padd_no) + "2.M"
    response = requests.get(url)
    response_data = response.json().get('series')[0]
    df_name = response_data.get('name')
    frequency = 'M'
    units = 'Thousand Barrels per Day'
    data = response_data.get('data')
    time_period = []
    quantity = []
    for i in data:
        time_period.append(i[0])
        quantity.append(i[1])
    df = pd.DataFrame({
        'Period': time_period,
        'Value': quantity,
    })
    df['Series_Name'] = df_name
    df['Frequency'] = frequency
    df['Units'] = units
    df['year'] = df.Period.apply(lambda x: int(x[:4]))
    df['month'] = df.Period.apply(lambda x: int(x[4:]))
    df['quarter'] = df.apply(lambda x: x['month'] // 3 if x['month'] % 3 == 0 else x['month'] // 3 + 1, axis=1)
    # df.to_csv("padd1_data.csv", index=False)
    return df


def data_processing():
    # fetching dataframe from link part(a)
    padd1_df = get_dataset(1)
    padd2_df = get_dataset(2)
    padd3_df = get_dataset(3)
    padd4_df = get_dataset(4)
    padd5_df = get_dataset(5)

    # taking only entries greater than or equal to 2016 part(b)
    padd1_df_2016_onwards = padd1_df[padd1_df['year'] >= 2016][['year', 'month', 'quarter', 'Value']]
    padd2_df_2016_onwards = padd2_df[padd2_df['year'] >= 2016][['year', 'month', 'quarter', 'Value']]
    padd3_df_2016_onwards = padd3_df[padd3_df['year'] >= 2016][['year', 'month', 'quarter', 'Value']]
    padd4_df_2016_onwards = padd4_df[padd4_df['year'] >= 2016][['year', 'month', 'quarter', 'Value']]
    padd5_df_2016_onwards = padd5_df[padd5_df['year'] >= 2016][['year', 'month', 'quarter', 'Value']]

    # merging data to get all padd's data in one dataframe part(c)
    padd1_padd2_merged_df = padd1_df_2016_onwards.merge(padd2_df_2016_onwards, on=['year', 'quarter', 'month'],
                                                        how='left',
                                                        suffixes=('_padd1', '_padd2'))
    padd3_padd4_merged_df = padd3_df_2016_onwards.merge(padd4_df_2016_onwards, on=['year', 'quarter', 'month'],
                                                        how='left',
                                                        suffixes=('_padd3', '_padd4'))
    padd3_padd4_padd5_merged_df = padd3_padd4_merged_df.merge(padd5_df_2016_onwards, on=['year', 'quarter', 'month'],
                                                              how='left')
    merged_df = padd1_padd2_merged_df.merge(padd3_padd4_padd5_merged_df, on=['year', 'quarter', 'month'], how='left')

    # renaming the fifth padd column to 'Value_padd5'
    merged_df = merged_df.rename(columns={"Value": "Value_padd5"})

    # suming up total crude oil input of all padd's part(d)
    merged_df['total_us_refinery_net_input'] = merged_df.apply(
        lambda x: x['Value_padd1'] + x['Value_padd2'] + x['Value_padd3'] + x['Value_padd4'] + x['Value_padd5'], axis=1)
    total_us_refinery_net_input_of_crude_oil_df = merged_df[['year', 'quarter', 'month', 'total_us_refinery_net_input']]

    # summarizing monthly data by 'quarter' part(e)
    total_us_refinery_net_input_of_crude_oil_quarterly_df = merged_df.groupby(['year', 'quarter']).agg({
        "Value_padd1": sum,
        "Value_padd2": sum,
        "Value_padd3": sum,
        "Value_padd4": sum,
        "Value_padd5": sum,
        "total_us_refinery_net_input": sum,
    }).reset_index()

    # summarizing monthly data by 'year' part(f)
    total_us_refinery_net_input_of_crude_oil_yearly_df = merged_df.groupby(['year']).agg({
        "Value_padd1": sum,
        "Value_padd2": sum,
        "Value_padd3": sum,
        "Value_padd4": sum,
        "Value_padd5": sum,
        "total_us_refinery_net_input": sum,
    }).reset_index()

    total_us_refinery_net_input_of_crude_oil_monthly_df = merged_df.groupby(['year', 'quarter', 'month']).agg({
        "Value_padd1": sum,
        "Value_padd2": sum,
        "Value_padd3": sum,
        "Value_padd4": sum,
        "Value_padd5": sum,
        "total_us_refinery_net_input": sum,
    }).reset_index()

    # total_us_refinery_net_input_of_crude_oil_monthly_df.to_csv(
    #     "monthly_summarized_us_refinery_net_input_of_crude_oil.csv", index=False)
    # total_us_refinery_net_input_of_crude_oil_yearly_df.to_csv(
    #     "yearly_summarized_us_refinery_net_input_of_crude_oil.csv", index=False)
    # total_us_refinery_net_input_of_crude_oil_quarterly_df.to_csv(
    #     "quarterly_summarized_us_refinery_net_input_of_crude_oil.csv", index=False)

    # print(total_us_refinery_net_input_of_crude_oil_quarterly_df)
    # print(total_us_refinery_net_input_of_crude_oil_yearly_df)
    # print(len(merged_df))
