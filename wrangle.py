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


def prep(df):
    
    df = df.drop(df.index[478630])
                 
    return df
    

def convert_date(df, col):
    
    '''
    
    '''
    
    df[col] = df[col].astype('datetime64')
    
    return df

def lesson_sep(df, col):
    
    '''
    
    '''
    
    lessons = []
    non_lessons = []
    for i in df[col]:
        if df[col][i].find('/') < 0:
            non_lessons.append(df[col][i])
        else:
            lessons.append(df[col][i])
  
    return lessons, non_lessons
    