import pandas as pd
import re
from timeit import default_timer as timer
from sqlalchemy.sql import text

# Convert timestamp to date (yyyy-mm-dd)
def ts_to_date(df):
    date_col = pd.to_datetime(df['timestamp'], unit='s').apply(lambda x: x.date())
    df.insert(0, 'date', date_col)

# De-duplicate posts and format date before returning
def dedup_and_date_format(df):
    df.drop_duplicates(subset=['timestamp', 'user_id', 'url', 'poster', 'post_text',
                                ], inplace=True)
    
    ts_to_date(df)
    return df


# Filter out test users (short usernames)
def filter_test_users(df):
    clean_df = df.drop(df[df.user_id.apply(lambda x: len(x)<8)].index)
    return clean_df

def get_demographics(db, start_ts: int, end_ts: int) -> pd.DataFrame:
    # Get overall demographics for a given time period
    # (original query by Micha)
    q = text(
        '''WITH demo_filtered AS (
                SELECT DISTINCT ON (user_id)
                    demo.*
                FROM "facebook-timeline" ft
                INNER JOIN demographics as demo
                    ON demo.user_id = ft.user_id
                WHERE (ft.timestamp < :end)
                AND (ft.timestamp > :start)
            )
                SELECT 
                    race,
                    count(*)
                FROM demo_filtered
                GROUP BY race'''
    )

    demo = pd.read_sql_query(q, con=db.engine, params={"start": start_ts, "end": end_ts})
    return demo.append({'race': 'TOTAL',
                'count': demo.sum().item()}, 
                ignore_index=True)
    
    


# Take a post dataframe and demographic dataframe as input; 
# Calculate percentage of racial demographic group that saw a type of post
# as a proportion of the demographic in demographic_df
def demo_to_percent(dataframe, demographic_df, characteristic):
    grouped_df = dataframe.groupby(characteristic)['user_id'].nunique().sort_values(ascending=False).reset_index()
    grouped_df.columns = [characteristic,'user_count']
    # Rename columns & merge
    grouped_demo = grouped_df[[characteristic,'user_count']].merge(demographic_df[[characteristic,'count']],on=characteristic,how='left')

    # Rename columns & calculate percentage
    grouped_demo.columns = [characteristic,'user_view_count','panel_count']

    grouped_demo['percent_demographic'] = (grouped_demo['user_view_count'] / grouped_demo['panel_count'])*100

    grouped_demo['percent_demographic'] = grouped_demo['percent_demographic'].apply(lambda x: round(x,1))
    
    return grouped_demo.sort_values(by='percent_demographic',ascending=False)