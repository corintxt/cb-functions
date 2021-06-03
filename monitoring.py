import pandas as pd
from pandas._libs.tslibs import Timestamp
from sqlalchemy.sql import text
from timeit import default_timer as timer
from datetime import datetime, timedelta
from cf_helper_functions import dedup_and_date_format

def is_it_working():
    print("Yes it is")

## SEARCH FOR URL DOMAIN
def search_for_url_domain(db, url: str) -> pd.DataFrame:
    func_start = timer()
    q = text(
        f'''
        SELECT DISTINCT ON (ft.user_id, ft.post_text, ftl.url) 
            ft.user_id,
            ft.timestamp,
            ft.poster,
            ft.post_text,
            ftl.url,
            ftl.url_domain,
            ftl.og_title, 
            ftl.og_image,
            ftl.og_site_name,
            ftl.og_locale,
            ftl.og_type,
            ftl.og_description,
            ftl.title
        FROM "facebook-timeline" ft
        INNER JOIN "facebook-timeline_attachments:links" ftl
        ON ft."attachments:links" = ftl.key
        WHERE ftl.url_domain = '{url}'
        AND ft."attachments:links" != 'null'
        '''
        )

    res = pd.read_sql_query(q, con=db.engine)

    # Remove test users (short usernames)
    res = res[res['user_id'].apply(lambda x: len(x)>5)]
    df_clean = dedup_and_date_format(res)
    
    func_end = timer()
    func_time = func_end - func_start

    print(f'Search returned {len(df_clean)} rows, completed in {round(func_time, 2)} seconds.')
    return df_clean

## FULL TEXT SEARCH
# DB query: searches all relevant post fields for a keyword
def full_text_search(db, keystring: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    func_start = timer()
    q = text(
    f"""
    /* 
    Get all posts mentioning keyword string
     */

    SELECT 
        ft.user_id, ft.timestamp, ft.is_public, ft.post_text, ft.poster, ft.poster_url, 
        ft."group_metadata:group_name", ft."attachments:image_alts",
        ftl.url, ftl.url_domain, ftl.description,
        ftl.og_title, ftl.og_description, ftl.og_site_name,
        dem.race, dem.gender, dem.vote_2020, dem.state, dem.birth_year
    FROM "facebook-timeline" AS ft
    LEFT JOIN "facebook-timeline_attachments:links" AS ftl
    ON ft."attachments:links" = ftl.key
    LEFT JOIN "demographics" AS dem
    ON dem.user_id = ft.user_id
    WHERE (ft.timestamp BETWEEN :start AND :end)
    AND (ft.post_text ~* :keywords_str
       OR ft."attachments:image_alts" ~* :keywords_str
       OR ftl.url ~* :keywords_str
       OR ftl.description ~* :keywords_str
       OR ftl.og_title ~* :keywords_str
       OR ftl.og_description ~* :keywords_str
       OR ft."group_metadata:group_name" ~* :keywords_str)
    """
    )

    res = pd.read_sql_query(q, params={"keywords_str": keystring, "start": start_ts, "end": end_ts}, 
                        con=db.engine)

    df_clean = dedup_and_date_format(res)

    func_end = timer()
    func_time = func_end - func_start

    print(f'Search returned {len(df_clean)} rows, completed in {round(func_time, 2)} seconds.')

    return df_clean.sort_values(by='date').reset_index().drop('index',axis=1)

##### NEWSLETTER MONITORING FUNCTIONS #####

# Set a timeframe - number of weeks to search w/ week offset from present
def set_timeframe(n_weeks, wk_offset):
    today_date = datetime.today()
    print(f"Current date: {today_date.strftime('%x')}")
    print(f"Query duration: {n_weeks} weeks, offset {wk_offset} week(s) from today.")
    end_date = today_date - timedelta(weeks = wk_offset)
    
    start_date = end_date - timedelta(weeks = n_weeks)
    
    print(f"Query start date: {start_date.strftime('%x')}")
    print(f"Query end date: {end_date.strftime('%x')}")
    
    return start_date.timestamp(), end_date.timestamp()

### GROUPS ## 
## Get a list of the top groups in a timeframe
# Optionally: that contain a certain keyword(s) - use regex
def get_top_groups(db, start_ts, end_ts, keywords='.'):
    print("Starting query...")
    func_start = timer()
    q = text("""
    SELECT 
        MIN ( groups.group_name) as group_name,
        COALESCE( count(DISTINCT groups.user_id), 0 ) as n_user_recommend,
        MIN( groups.timestamp ) as first_sighted,
        MAX( groups.timestamp ) as last_sighted,
        MIN( groups.group_slug ) as group_slug,
        MAX( groups.n_members ) as n_members

    FROM "facebook-groups" as groups
    --JOIN "demographics" demo
    --    ON demo.user_id = groups.user_id

    WHERE groups.timestamp >= :start_date
    AND groups.timestamp <= :end_date
    AND groups.group_name ~* :keywords
    GROUP BY groups.group_slug
    ORDER BY n_user_recommend DESC
    """)
    df = pd.read_sql_query(sql=q, 
                       params={"start_date": start_ts, "end_date": end_ts, "keywords": keywords},
                       con=db.engine)
    
    df['first_sighted'] = pd.to_datetime(df['first_sighted'], unit='s').apply(lambda x: x.date())
    df['last_sighted'] = pd.to_datetime(df['last_sighted'], unit='s').apply(lambda x: x.date())
    
    func_end = timer()
    func_time = func_end - func_start

    print(f'Identified {len(df)} groups, completed in {round(func_time, 2)} seconds.')


    return df

### POLITICAL ADS ##
## Get all political ads in a timeframe
# Optionally: containing a keyword
def get_political_ads(db, start_ts, end_ts, keywords='.'):
    q = text(
        '''
        SELECT
            ft.timestamp,
            ft.poster,
            ft.post_text,
            ft.sponsored_by,
            ft.poster_url,
            ftl.url_domain,
            ftl.url,
            demo.race,
            demo.gender,
            demo.education,
            demo.birth_year,
            demo.state,
            demo.zipcode,
            demo.vote_2020,
            ft.user_id
        FROM "facebook-timeline" ft
        JOIN "demographics" demo
            ON ft.user_id = demo.user_id
        JOIN "facebook-timeline_attachments:links" ftl
            ON ft."attachments:links" = ftl.key
        WHERE sponsored_by is not NULL
        AND ft.timestamp >= :start
        AND ft.timestamp <= :end
        AND ft.post_text ~* :keywords
        order by timestamp ASC
        '''
        )
    
    df = pd.read_sql_query(q, con=db.engine, 
                       params={"start": start_ts, 
                               "end": end_ts, 
                               "keywords": keywords},
                          )
    df = dedup_and_date_format(df)
    df.drop('timestamp', axis=1, inplace=True)
    print(f"Found {len(df)} ads")
    
    return df

## TOP SPONSORED + NON-SPONSORED POSTS
## Get top posts in a timeframe
# Optionally: served to users who voted in a certain way
def get_top_posts(db, start_ts, end_ts, keywords='.', is_sponsored=False, vote=None):
    # Comment out vote_2020 matching based on condition
    if vote:
        vote_switch = ''
    else:
        vote_switch = '--'
    
    q = text(
        f'''
        SELECT 
            COUNT(DISTINCT ft.user_id),
            ft.poster, 
            ft.post_text,
            ftl.url,
            ft."attachments:image_alts" AS image_alt
        FROM "facebook-timeline" ft
        JOIN "facebook-timeline_attachments:links" ftl
            ON ft."attachments:links" = ftl.key
        JOIN "demographics" demo
            ON ft.user_id = demo.user_id
        WHERE ft.timestamp BETWEEN :start AND :end
        AND ft.is_facebook = False
        AND ft.is_sponsored = :is_sponsored
        AND ft.post_text ~* :keywords
        {vote_switch}AND demo.vote_2020 = :vote 
        GROUP BY ft.poster, ft.post_text, ftl.url, image_alt
        ORDER BY count DESC
        LIMIT 1000
        '''
        )
    
    res = pd.read_sql_query(q, con=db.engine, 
                            params = {"start": start_ts, "end": end_ts, 
                                      "is_sponsored": is_sponsored, 
                                      "keywords": keywords,
                                     "vote": vote})
    return res

## Get top posters
# (Same as top posts but aggregates all posts from same source)
def get_top_posters(db, start_ts, end_ts, keywords='.', is_sponsored=False, vote=None):
    if vote:
        vote_switch = ''
    else:
        vote_switch = '--'
    
    q = text(
        f'''
        SELECT 
            COUNT(DISTINCT ft.user_id),
            ft.poster
        FROM "facebook-timeline" ft
        JOIN "demographics" demo
            ON ft.user_id = demo.user_id
        WHERE ft.timestamp BETWEEN :start AND :end
        AND ft.is_facebook = False
        AND ft.is_sponsored = :is_sponsored
        AND ft.post_text ~* :keywords
        AND ft.poster IS NOT NULL
        {vote_switch}AND demo.vote_2020 = :vote 
        GROUP BY ft.poster
        ORDER BY count DESC
        LIMIT 100
        '''
        )
    
    res = pd.read_sql_query(q, con=db.engine, 
                            params = {"start": start_ts, "end": end_ts, 
                                      "is_sponsored": is_sponsored, 
                                      "keywords": keywords,
                                     "vote": vote})
    return res

### TOP POSTS AND POSTERS:
## Call both of the above functions + return dict of 2 dataframes
def get_top_posts_and_posters(db, start_ts: int, end_ts: int, keywords='.', is_sponsored=False, vote=None) -> dict:
    res = dict()    
    res['description'] = {"keywords": keywords, "is_sponsored": is_sponsored, "vote": vote}
    res['top_posters'] = get_top_posters(db, start_ts, end_ts, keywords=keywords, is_sponsored=is_sponsored, vote=vote)
    res['top_posts'] = get_top_posts(db, start_ts, end_ts, keywords=keywords, is_sponsored=is_sponsored, vote=vote)
    return res

## Pull individual posts in time period
# (Joined to demographics - for more precise filtering in pandas)
def get_posts_in_time_period(db, start_ts, end_ts):
    func_start = timer()
    q = text(
        '''
        SELECT
            ft.timestamp,
            ft.poster,
            ft.post_text,
            ftl.url,
            demo.race,
            demo.gender,
            demo.vote_2020,
            ft.user_id
        FROM "facebook-timeline" as ft 
        LEFT JOIN "demographics" demo
            ON demo.user_id = ft.user_id
        LEFT JOIN "facebook-timeline_attachments:links" ftl
            ON ft."attachments:links" = ftl.key
        WHERE ftl.url_domain IS NOT NULL
        AND ft.poster IS NOT NULL
        AND ft.is_sponsored = False
        AND ft.timestamp BETWEEN :start AND :end
        '''
    )
    res = pd.read_sql_query(q, con=db.engine, 
                        params = {"start": start_ts, "end": end_ts})
    
    df_clean = dedup_and_date_format(res)
    
    func_end = timer()
    func_time = func_end - func_start

    print(f'Search returned {len(df_clean)} rows, completed in {round(func_time, 2)} seconds.')
    print("Use `df.groupby('url').user_id.nunique().sort_values(ascending=False)` to see top posts.")
    
    return df_clean


### MISINFORMATION + OTHER 
## Get all flagged posts
# Option: filter out Covid-related flags which are the most common
# leaving mostly misinformation and graphic content
def get_flagged_posts(db, start_ts, end_ts, filter_covid=False) -> pd.DataFrame:
    q = text(
        '''
        SELECT DISTINCT ON (ft.poster, ft.post_text, ft.user_id, ft.timestamp)
            to_timestamp(ft.timestamp)::date AS date,
            ft.poster, 
            ft.post_text, 
            ft."facebook_flags:message" AS flag,
            ft."attachments:image_alts" AS image_alt,
            ftl.url,
            demo.race,
            demo.gender,
            demo.birth_year,
            demo.vote_2020
        FROM "facebook-timeline" ft
        JOIN "facebook-timeline_attachments:links" ftl
            ON ft."attachments:links" = ftl.key
        JOIN "demographics" demo
            ON ft.user_id = demo.user_id
        WHERE ft."facebook_flags:message" IS NOT null
        AND ft.poster IS NOT null
        AND ft.timestamp BETWEEN :start AND :end
        '''
    )
    res = pd.read_sql_query(q, con=db.engine, 
                            params = {"start": start_ts, "end": end_ts})
    
    if filter_covid:
        res = res[~res.flag.str.contains('covid', case=False).fillna(False)]
    
    return res

### AD TARGETING
## Get most-used ad interests in a time bracket, by poster
def get_ad_interests_ranked(db, start_ts, end_ts) -> pd.DataFrame:
    q = text(
        '''
        SELECT
            ft.poster,
            COUNT(*) as ad_count,
            interest_dict ->> 'name' as interest,
            ftl.url
        FROM 
            "facebook-timeline" as ft 
        LEFT JOIN "facebook-timeline_attachments:links" ftl
            ON ft."attachments:links" = ftl.key
        LEFT JOIN "facebook-timeline_targeting" as ftt
            ON ft.targeting = ftt.key
        CROSS JOIN jsonb_array_elements(ftt.interests) as interest_dict
        WHERE ft.targeting IS NOT NULL 
        AND ft.poster IS NOT NULL
        AND ftt.type = 'INTERESTS' 
        AND ft.timestamp BETWEEN :start AND :end
        GROUP BY (poster, url, interest)
        ORDER BY ad_count DESC
        '''
    )
    res = pd.read_sql_query(q, con=db.engine, 
                        params = {"start": start_ts, "end": end_ts})
    return res    