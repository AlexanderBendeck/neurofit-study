'''
@author: Alexander Bendeck


REQUIREMENTS
    There should be a folder called 'data_raw' in the
    same directory as this file (mergeData.py).
    
    There should be a folder called 'data_clean' in the
    same directory as this file (mergeData.py).


HOW TO RUN
    Put all of the following files in the 'data_raw' folder,
    with the default file name from when they were acquired:
        1. Daily activity logs for each individual - Fitabase
        2. Sleep stages ("day") logs for each individual - Fitabase
        3. SMS data logs for each individual - TextMagic
        4. fMRI data (2 files) for each individual - Server/Cluster
        5. Daily surveys (1 combined file with all participants) - Redcap
    
    The script calls mergeData with a list of participant id numbers
    as strings and returns a large combined file for all participants.
    By default, we specify individual_files=True to indicate that we
    also want to get files for each participant separated by fMRI run
    (i.e. 2 files per person). Both the large combined file and the
    individual files will be created in the 'data_clean' folder.
    
'''

import pandas as pd
import numpy as np
import os


def safeDateConvert(val, verbose=False):
    '''
    Takes dates/times from surveys, SMS, & sleep data
    (in the format 2020-04-08 11:32:50)
    and returns just the date (i.e. 2020-04-08);
    returns N/A if missing or invalid date
    '''
    if isinstance(val, str):
        if ":" in val: ## If a valid date/time
            return val.split()[0]
    else:
        if verbose:
            print(val)  ## What is val if it's not a string?
    return "N/A"  ## If not a valid date (not string, or string but not date)


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
       * Daily activity FitBit data (one file per subject)
       * Sleep stages "day" logs (one file per subject)
       * Daily survey data (one combined file)
       * fMRI data (one file per run, per subject)
       * SMS data (one file per subject)
    
    All input data should be in a directory named:
        'data_raw'
    which is in the same directory as this script.
    
    All ouput data will be placed in a directory named:
        'data_clean'
    which should be created beforehand in the same 
    directory as this script.
    '''
    path_to_data = os.path.join("data_raw", "")
    
    # Find fitabase files for user
    fitabase_files = [f for f in os.listdir(path_to_data) if f.startswith(uid)]
    
    # Load activity file for user, load dataframe, and re-format date
    try:
        activityFile = [f for f in fitabase_files if "Activity" in f][0]
        userActivity = pd.read_csv(path_to_data + activityFile)
        userActivity['ActivityDate'] = userActivity['ActivityDate'].apply(formatDate)
    except:
        print("No activity data for uid " + uid + "; aborting for this participant")
        return

    # Load sleep file for user, load dataframe, and re-format date
    try:
        sleepFile = [f for f in fitabase_files if "sleep" in f][0]
        sleepLog = pd.read_csv(path_to_data + sleepFile)
        sleepLog['DateToFormat'] = sleepLog['SleepDay'].apply(safeDateConvert)
        sleepLog['DateToMerge'] = sleepLog['DateToFormat'].apply(formatDate)
        # Merge with activity data using date
        act_sleep = pd.merge(userActivity, sleepLog, how="left", left_on='ActivityDate', right_on='DateToMerge')
    except:
        print("No sleep data for uid " + uid)
        act_sleep = userActivity
    
    # Find SMS data file, load dataframe, clean up subject day numbers,
    # and re-format the survey timestamp (to keep only the date)
    try:
        smsData = pd.read_csv(path_to_data + "sub-" + uid + "_sms-times.csv") 
        smsData.rename(columns={'Unnamed: 0':'subj_day_num'}, inplace=True)
        smsData['subj_day_num'] = smsData['subj_day_num'].apply(lambda x: x+1)
    
        smsDates = smsData['timestamp'].apply(safeDateConvert)
        smsDatesClean = smsDates[smsDates != "N/A"]
        smsData['SmsDate'] = smsDatesClean
        
        msgStartDate = pd.Timestamp(smsData['SmsDate'][0])
    
        # Merge survey and SMS rows using date
        act_SMS = pd.merge(act_sleep, smsData, how='left', left_on='ActivityDate', right_on='SmsDate')
        act_SMS['msg_start'] = act_SMS['ActivityDate'].apply(lambda x: 0 if pd.Timestamp(x) < msgStartDate else 1)
    
        # Re-order columns of merged dataframe
        cols = act_SMS.columns.tolist()
        cols.remove('subj_day_num')
        act_SMS = act_SMS[['subj_day_num']+cols]
        smsPresent = True
    except:
        print("Missing SMS data for uid " + uid)
        act_SMS = act_sleep
        smsPresent = False
        
    # Find combined survey data file, load dataframe
    # and re-format survey timestamp (keep only the date)
    surveyFiles = [f for f in os.listdir(path_to_data) if f.startswith("DailySurveys")]
    if len(surveyFiles) > 0:
        # Notify if there are multiple survey files
        if len(surveyFiles) > 1:
            print(str(len(surveyFiles)) + " DailySurveys files found. Using file: " + surveyFiles[0] + "\n")
        
        # Load surveys dataframe and clean data
        surveyData = pd.read_csv(path_to_data + surveyFiles[0]) 
        surveyDates = surveyData['daily_survey_timestamp'].apply(safeDateConvert)
        surveyDatesClean = surveyDates[surveyDates != "N/A"]
        surveyData['SurveyDate'] = surveyDatesClean
        
        # Find this user's rows in the survey dataframe and store in new dataframe
        surveyDataForUser = surveyData.loc[surveyData['subject_id'] == int(uid)]
        if surveyDataForUser.size == 0: # Sometimes these are stored as strings instead
            surveyDataForUser = surveyData.loc[surveyData['subject_id'] == uid]
        
        # Merge activity/SMS and survey rows using date 
        # and fill survey cols with 'NA' if surveys are missing
        act_SMS_surveys = pd.merge(act_SMS, surveyDataForUser, how='left', left_on='ActivityDate', right_on='SurveyDate')
        act_SMS_surveys['daily_survey_timestamp'] = act_SMS_surveys['daily_survey_timestamp'].apply(dateToUnix)
    else:  ## Missing survey file
        act_SMS_surveys = act_SMS
    
    # Format null values as desired
    act_SMS_surveys = act_SMS_surveys.fillna("NA")
    
    # Create combined message ID column using SMS columns
    # (currenly used to merge dataframes, but not used in output file)
    if smsPresent:
        valence = act_SMS_surveys['valence']
        valence_short = valence.apply(lambda x: x[0:3])
        s_ns = act_SMS_surveys['s_ns']
        s_ns_short = s_ns.apply(lambda x: x[0:3] if x[0] == 's' else x[0:6])
        msg_num = act_SMS_surveys['id'].astype(str).apply(lambda x: x[:-2])
        act_SMS_surveys['msg_id'] = valence_short + "_" + s_ns_short + "_" + msg_num
        act_SMS_surveys.rename(columns={'msg_id':'msg_id_combined'}, inplace=True)
    
    # Read in the subject's two fMRI runs as dataframes and label rows with run number
    try:
        run1 = pd.read_csv(path_to_data + "sub-" + uid + "_task-HealthMessage_run-01_events.tsv", sep='\t')
        run1['run'] = '01'
    except:
        print("Missing fmri run 01 for uid " + uid)
        run1 = pd.DataFrame()
    try:
        run2 = pd.read_csv(path_to_data + "sub-" + uid + "_task-HealthMessage_run-02_events.tsv", sep='\t')
        run2['run'] = '02'
    except:
        print("Missing fmri run 02 for uid " + uid)
        run2 = pd.DataFrame()
    
    # Concatenate dataframes for different runs
    runs = pd.concat([run1, run2])
    
    # Create final merged dataframe
    if smsPresent and not runs.empty:
        final_merged = pd.merge(act_SMS_surveys, runs, how='left', left_on=['msg_id_combined'], right_on=['id'])
    else:
        final_merged = act_SMS_surveys

    # Normalize TotalSteps and RestingHeartRate from daily activity
    totSteps = final_merged['TotalSteps'].apply(lambda x: int(x) if x != "NA" else np.NaN)
    restingHR = final_merged['RestingHeartRate'].apply(lambda x: int(x) if x != "NA" else np.NaN)
    final_merged['TotalSteps_norm'] = (final_merged['TotalSteps'] - totSteps.mean()) / totSteps.std()
    final_merged['RestingHeartRate_norm'] = (restingHR - restingHR.mean()) / restingHR.std()
    
    # Re-format column names and null values
    final_merged.rename(columns={'daily_survey_timestamp':'survey_complete_timestamp',
                                 'unix_timestamp':'sms_timestamp',
                                 'valence_x':'valence',
                                 's_ns_x':'s_ns',
                                 'id_x':'msg_id'}, inplace=True)
    final_merged = final_merged.fillna('NA')
    final_merged['sub'] = int(uid)
    
    # All desired columns if no data is missing
    finalCols = ['sub', 'run', 'onset', 'duration', 'trial', 'trial_type', 'rating', 'resp_time', 'msg_start',
                 'subj_day_num', 'sms_timestamp', 'ActivityDate',  'TotalSteps', 'TotalSteps_norm', 'TotalDistance',
                 'VeryActiveDistance', 'ModeratelyActiveDistance', 'LightActiveDistance', 'SedentaryActiveDistance',
                 'VeryActiveMinutes', 'FairlyActiveMinutes', 'LightlyActiveMinutes', 'SedentaryMinutes',
                 'Calories', 'Floors', 'CaloriesBMR', 'MarginalCalories', 'RestingHeartRate', 'RestingHeartRate_norm',
                 'valence', 's_ns', 'msg_id', 'message',
                 'survey_complete_timestamp', 'location', 'lap', 'hap', 'han', 'lan', 'la', 'p', 'n', 'ha', 'self_efficacy_daily',
                 'TotalSleepRecords', 'TotalMinutesAsleep', 'TotalMinutesLight', 'TotalMinutesDeep', 'TotalMinutesREM']
    
    # Fill columns with NA if data was missing (column doesn't exist)
    finalColsDiff = set(finalCols) - set(final_merged.columns)
    for col in finalColsDiff:
        final_merged[col] = "NA"
    
    final_ret = final_merged.copy()[finalCols]  ## To be used in combined file
    
    finalCols.remove('ActivityDate')  ## For individual files, don't include activity date
    finalCols.remove('msg_start')
    final_merged = final_merged[finalCols]
    
    # Write CSV files (one per fMRI run) for this subject, if desired
    if write_csv:
        run01 = final_merged.loc[final_merged['run'] == '01']
        if not run01.empty:
            run01 = run01.sort_values(by=['trial', 'onset'])
            fname01 = "sub-" + uid + "_task-HealthMessage_run-01_events_all_vars"
            pd.DataFrame.to_csv(run01[finalCols[2:]], os.path.join("data_clean" , fname01) + ".csv", index=False)
        
        run02 = final_merged.loc[final_merged['run'] == '02']
        if not run02.empty:
            run02 = run02.sort_values(by=['trial', 'onset'])
            fname02 = "sub-" + uid + "_task-HealthMessage_run-02_events_all_vars"
            pd.DataFrame.to_csv(run02[finalCols[2:]], os.path.join("data_clean" , fname02) + ".csv", index=False)
        
        #both_runs = final_merged.sort_values(by=['run', 'trial', 'onset'])
        #fname03 = "sub-" + uid + "_task-HealthMessage_events_all_vars"
        #pd.DataFrame.to_csv(both_runs, os.path.join("data_clean" , fname03) + ".csv", index=False)
        
    # Return the final merged dataframe for the combined file
    return final_ret


def mergeData(uids, individual_files=True):
    '''
    Create one ouput file with
    all runs of all subjects;
    
    if individual_files is True,
    additionally create two ouput
    files per subject, one for each fMRI run
    '''
    # Notify if there is no combined survey file
    surveyFiles = [f for f in os.listdir(path_to_data) if f.startswith("DailySurveys")]
    if len(surveyFiles) == 0:
        print("DailySurveys file not found. Make sure the file name starts with: 'DailySurveys'" + "\n")
    
    # Get individual dataframes for each subject number
    dataframes = []
    for uid in uids:
        df = mergeFilesForUser(uid, write_csv = individual_files)
        if df is not None:
            dataframes.append(df)
        else: # Something went wrong
            print("uid " + uid + " will not be in combined file")
    
    # Concatenate individual dataframes together
    # and sort rows by subject number and activity date
    if len(dataframes) > 0:
        allInOne = pd.concat(dataframes)
        allInOne = allInOne.sort_values(by=['sub', 'ActivityDate'])
        pd.DataFrame.to_csv(allInOne, os.path.join("data_clean" ,"final_merged_data_all_norm.csv"), index=False)
    else:
        print("No valid participant IDs; no combined file written")


if __name__ == "__main__":
    path_to_data = os.path.join("data_raw", "")
    
    # Only run for subjects where we at least have activity data
    uids_activity = [f[0:4] for f in os.listdir(path_to_data) if 'Activity' in f]
    
    #uids_sms = [f[4:8] for f in os.listdir(path_to_data) if 'sms-times' in f]

    #uids_fmri_1 = [f[4:8] for f in os.listdir(path_to_data) if 'HealthMessage_run-01' in f]
    #uids_fmri_2 = [f[4:8] for f in os.listdir(path_to_data) if 'HealthMessage_run-02' in f]
    #uids_fmri = set(uids_fmri_1) & set(uids_fmri_2)

    uids = uids_activity
    #uids = ['1011', '1105']
    
    # Output files for these subjects
    mergeData(uids, individual_files=True)
    