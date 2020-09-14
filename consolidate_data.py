from datetime import datetime
from datetime import timedelta
import os
import pandas as pd

def main():
    """
    Consolidates the data from the data and market-data directories into a single csv file relating
    the daily opening prices of popular securities to the concatenated corpus of comments posted on
    Reddit the day before.
    """

    dates_comments = {
        'date': [],
        'concat_comments_prev_day': []
    }

    for submission_filename in os.listdir('data'):
        if submission_filename.endswith('.csv'):
            submission_date = datetime.strptime(submission_filename, '%Y-%m-%d.csv')
            # Use the post on the day BEFORE a market date to predict the market
            market_date = submission_date + timedelta(days=1)
            dates_comments['date'].append(market_date)
            comments_df = pd.read_csv(f'data/{submission_filename}')
            dates_comments['concat_comments_prev_day'].append(comments_df['body'].str.cat(sep=' '))

    # Sort dataframe indices by date
    df = pd.DataFrame(dates_comments).sort_values(by='date').reset_index(drop=True)

    for security_filename in os.listdir('market-data'):
        if security_filename.endswith('.csv'):
            security_df = pd.read_csv(f'market-data/{security_filename}')

            # Only track opening prices for dates we have comment data for
            opening_prices = security_df[
                pd.to_datetime(security_df['Date'], format='%Y-%m-%d').isin(df['date'])
            ].sort_values(by='Date')['Open'].reset_index(drop=True)

            # Insert opening prices into dataframe
            df.insert(
                loc=1,
                column=f'{security_filename[:-4]}_open',
                value=opening_prices
            )

    # Write dataframe to csv
    df.to_csv('consolidated_data.csv', index=False)

if __name__ == '__main__':
    main()
