'''
@author: Alexander Bendeck

'''

import pandas as pd
import numpy as np
import os

def formatDate(date):
    '''
    Re-formats dates from daily activity
    file in format M/D/YYYY to be the same as
    YYYY/MM/DD to match surveys and SMS data
    '''
    m, day, yr = date.split("/")
    if len(m) < 2:
        m = '0'+m
    if len(day) < 2:
        day = '0'+day
    return "-".join([yr, m, day])

def dateToUnix(date):
    '''
    Converts date to Unix timestamp
    '''
    if date == "NA":
        return date
    else:
        return (pd.to_datetime([date]).astype(int) / 10**9)[0].astype(int)

def mergeFilesForUser(uid, write_csv=False):
    '''
    Merge the following: 
       * daily activity FitBit data (one file per subject)
       * daily survey data (one combined file)
       * fMRI data (one file per run, per subject)
       * SMS data (one file per subject)
    
    All input data should be in a directory named:
        data_raw
    which is in the same directory as this script.
    
    All ouput data will be placed in a directory named:
        data_clean
    which should be created beforehand in the same 
    directory as this script.
    '''
    path_to_data = os.path.join("data_raw", "")
    
    # Find activity file for user, load dataframe, and re-format date
    activityFile = [f for f in os.listdir(path_to_data) if f.startswith(uid)][0]
    userActivity = pd.read_csv(path_to_data + activityFile)
    userActivity['ActivityDate'] = userActivity['ActivityDate'].apply(formatDate)

    # Find SMS data file, load dataframe, clean up subject day numbers,
    # and throw out survey timestamp (keep date)
    smsData = pd.read_csv(path_to_data + "sub-" + uid + "_sms-times.csv") 
    smsData.rename(columns={'Unnamed: 0':'subj_day_num'}, inplace=True)
    smsData['subj_day_num'] = smsData['subj_day_num'].apply(lambda x: x+1)
    smsData['SmsDate'] = smsData['timestamp'].apply(lambda x: x.split()[0])
    
    # Merge survey and SMS rows on date
    act_SMS = pd.merge(userActivity, smsData, left_on='ActivityDate', right_on='SmsDate')
    
    # Re-order columns of final_merged dataframe
    cols = act_SMS.columns.tolist()
    cols.remove('subj_day_num')
    act_SMS = act_SMS[['subj_day_num']+cols]
    
    # Find combined survey data file, load dataframe, and throw out survey timestamp (keep date)
    surveyData = pd.read_csv(path_to_data + "DailySurveys_DATA_2019-09-11_1204.csv") 
    surveyData['SurveyDate'] = surveyData['daily_survey_timestamp'].apply(lambda x: x.split()[0])
    
    # Find this user's rows in the survey dataframe and store in new dataframe
    surveyDataForUser = surveyData.loc[surveyData['subject_id'] == int(uid)]

    # Merge activity/SMS and survey rows on date, fill survey cols with NA if missing surveys
    act_SMS_surveys = pd.merge(act_SMS, surveyDataForUser, how='left', left_on='ActivityDate', right_on='SurveyDate')
    act_SMS_surveys = act_SMS_surveys.fillna("NA")
    
    # Create combined message ID column (currenly not used in output)
    valence = act_SMS_surveys['valence']
    valence_short = valence.apply(lambda x: x[0:3])
    s_ns = act_SMS_surveys['s_ns']
    s_ns_short = s_ns.apply(lambda x: x[0:3] if x[0] == 's' else x[0:6])
    msg_num = act_SMS_surveys['id'].astype(str)
    act_SMS_surveys['msg_id'] = valence_short + "_" + s_ns_short + "_" + msg_num
    
    # Read in the subject's two fMRI runs as dataframes and label rows with run number
    run1 = pd.read_csv(path_to_data + "sub-" + uid + "_task-HealthMessage_run-01_events.tsv", sep='\t')
    run1['run'] = '01'
    run2 = pd.read_csv(path_to_data + "sub-" + uid + "_task-HealthMessage_run-02_events.tsv", sep='\t')
    run2['run'] = '02'
    
    # Concatenate dataframes for different runs
    runs = pd.concat([run1, run2])
    
    # Create final merged dataframe
    final_merged = pd.merge(act_SMS_surveys, runs, how='inner', left_on=['msg_id'], right_on=['id'])
    
    # Normalize TotalSteps and RestingHeartRate
    totSteps = final_merged['TotalSteps'].apply(lambda x: int(x) if x != "NA" else np.NaN)
    restingHR = final_merged['RestingHeartRate'].apply(lambda x: int(x) if x != "NA" else np.NaN)
    final_merged['TotalSteps_norm'] = (final_merged['TotalSteps'] - totSteps.mean()) / totSteps.std()
    final_merged['RestingHeartRate_norm'] = (restingHR - restingHR.mean()) / restingHR.std()
    
    # Re-format column names and null values
    final_merged.rename(columns={'msg_id':'msg_id_combined'}, inplace=True)
    final_merged.rename(columns={'unix_timestamp':'sms_timestamp',
                            'valence_x':'valence',
                            's_ns_x':'s_ns',
                            'id_x':'msg_id',
                            'daily_survey_timestamp':'survey_complete_timestamp'}, inplace=True)
    final_merged = final_merged.fillna('NA')
    finalCols = ['sub', 'run', 'onset', 'duration', 'trial', 'trial_type', 'rating', 'resp_time',
                 'subj_day_num', 'sms_timestamp',  'TotalSteps', 'TotalSteps_norm', 'TotalDistance', 'TrackerDistance', 'LoggedActivitiesDistance',
                 'VeryActiveDistance', 'ModeratelyActiveDistance', 'LightActiveDistance', 'SedentaryActiveDistance',
                 'VeryActiveMinutes', 'FairlyActiveMinutes', 'LightlyActiveMinutes', 'SedentaryMinutes',
                 'Calories', 'Floors', 'CaloriesBMR', 'MarginalCalories', 'RestingHeartRate', 'RestingHeartRate_norm',
                 'valence', 's_ns', 'msg_id', 'message',
                 'survey_complete_timestamp', 'location', 'lap', 'hap', 'han', 'lan', 'la', 'p', 'n', 'ha', 'self_efficacy_daily']
    final_merged = final_merged[finalCols]
    final_merged['survey_complete_timestamp'] = final_merged['survey_complete_timestamp'].apply(dateToUnix)
    
    # Write CSV files (one per fMRI run) for this subject, if desired
    if write_csv:
        run01 = final_merged.loc[final_merged['run'] == '01'].sort_values(by=['trial', 'onset'])
        fname01 = "sub-" + uid + "_task-HealthMessage_run-01_events_all_vars"
        pd.DataFrame.to_csv(run01, os.path.join("data_clean" , fname01) + ".csv", index=False)
        run02 = final_merged.loc[final_merged['run'] == '02'].sort_values(by=['trial', 'onset'])
        fname02 = "sub-" + uid + "_task-HealthMessage_run-02_events_all_vars"
        pd.DataFrame.to_csv(run02, os.path.join("data_clean" , fname02) + ".csv", index=False)
    
    # Return the final merged dataframe
    return final_merged

def getCombinedFile(uids):
    '''
    Create one ouput file with
    all runs of all subjects
    '''
    dataframes = []
    for uid in uids:
        dataframes.append(mergeFilesForUser(uid, write_csv = False))
    allInOne = pd.concat(dataframes)
    pd.DataFrame.to_csv(allInOne, os.path.join("data_clean" ,"final_merged_data_all_norm.csv"), index=False)

def getSeparateFiles(uids):
    '''
    Create two ouput files per subject,
    one for each fMRI run
    '''
    for uid in uids:
        mergeFilesForUser(uid, write_csv = True)

if __name__ == "__main__":
    path_to_data = os.path.join("data_raw", "")
    
    # Only run for subjects where we have both SMS and fMRI data
    uids_sms = [f[4:8] for f in os.listdir(path_to_data) if 'sms-times' in f]
    uids_fmri = [f[4:8] for f in os.listdir(path_to_data) if 'HealthMessage' in f]
    uids = sorted(set(uids_sms) & set(uids_fmri), key = lambda x: int(x))
    uids = ['1071']
    # Output separate files for these subjects
    getSeparateFiles(uids)
