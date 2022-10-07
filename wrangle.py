import pandas as pd
import numpy as np
import os
import env

def pull_data():
    
    '''
    
    '''
    
    if os.path.isfile('other-curriculum-access.csv'):
        
        df = pd.read_csv('other-curriculum-access.csv')
    
    else:
        
        url = f'mysql+pymysql://{env.username}:{env.password}@{env.host}/curriculum_logs'

        query = '''
        SELECT 
        logs.date,
        logs.time,
        logs.path,
        logs.cohort_id,
        logs.user_id,
        logs.ip,
        cohorts.name,
        cohorts.start_date,
        cohorts.end_date,
        cohorts.program_id
        FROM logs
        JOIN cohorts ON logs.cohort_id = cohorts.id
        '''
    
        df0 = pd.read_sql(query, url)
    
        df = df0.copy()
        
        df.to_csv('other-curriculum-access.csv')
    
    
    return df


def prep():
    
    #acquire data
    df = pull_data()
    
    # removes null from dataframe (no path recorded)
    df = df.drop(df.index[478630])
    
    # removes unnecessary column that mimics index
    df = df.iloc[:,1:]
    
    # removes erroneous unknown program type
    df = df[df.program_id != 4]
    
    # convert id #'s into program names 
    df['program_id'] = np.where(df.program_id == 3, 'data_science', 'web_development')
    
    # renaming column
    df = df.rename(columns={'program_id': 'program', 'cohort_id': 'cohort', 'user_id': 'user'})
    
    df['date'] = df['date'] + ' ' + df['time']
    
    df = df.drop(columns = ['time'])
    
    df = lesson_sep(df)
    
    df['unit'] = np.where(df.unit == '', 'table_of_contents', df.unit)
    
    df = df.fillna('')
    
    df = df[df.unit != 'table_of_contents']
    
    df = df[df.name != 'Staff']
    
    df = df[(df.lesson != '') & (df.lesson != 'introduction')]
    
    df = df[(df.lesson != 'overview') & (df.lesson != 'search_index.json')]

    return df
    

def convert_date(df, col):
    
    '''
    
    '''
    
    df[col] = df[col].astype('datetime64')
    
    return df

def lesson_sep(df):
    
    '''
    
    '''
    
    unit_lesson = df.path.str.split('/',1,expand=True)
    
    unit_lesson.columns = ['unit','lesson']
    
    df = pd.concat([df, unit_lesson], axis=1)
    
    df = df.drop(columns= ['path'])
  
    return df


def get_lower_and_upper_bounds(col, mult=1.5):
    '''
    positional arguments:
    col: a pandas Series
    keyword arguments:
    mult: default 1.5, the magnutude specified for the IQR rule
    
    returns:
    lower_bound, upper_bound: two float values representing the fence values requested
    '''
    q1 = col.quantile(0.25)
    q3 = col.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - mult * iqr
    upper_bound = q3 + mult * iqr
    return lower_bound,upper_bound

def max_df(df):
    
    df = df[(df.lesson != '') & (df.lesson != 'introduction')]
    cohort_lessons = df.groupby('name')['lesson'].value_counts()

    cohort_lessons = pd.DataFrame(cohort_lessons)
    cohort_lessons = cohort_lessons.rename(columns = {'lesson' : 'number_of_hits'})
    
    cohort_lessons = cohort_lessons.reset_index()
    
    max_lessons = cohort_lessons.groupby('name').max()
    
    cohort_lessons = cohort_lessons.set_index('name')
    
    final = pd.DataFrame()
    for i in max_lessons.index:
        new = cohort_lessons.loc[i][cohort_lessons.loc[i]['number_of_hits'] == max_lessons.loc[i]['number_of_hits']]
        final = pd.concat([final, new])
    
    return final

def min_df(df):
    
    df = df[df.lesson != '']
    cohort_lessons = df.groupby('name')['lesson'].value_counts()

    cohort_lessons = pd.DataFrame(cohort_lessons)
    cohort_lessons = cohort_lessons.rename(columns = {'lesson' : 'number_of_hits'})
    
    cohort_lessons = cohort_lessons.reset_index()
    
    min_lessons = cohort_lessons.groupby('name').min()
    
    cohort_lessons = cohort_lessons.set_index('name')
    
    final = pd.DataFrame()
    for i in min_lessons.index:
        new = cohort_lessons.loc[i][cohort_lessons.loc[i]['number_of_hits'] == min_lessons.loc[i]['number_of_hits']]
        final = pd.concat([final, new])
    
    return final