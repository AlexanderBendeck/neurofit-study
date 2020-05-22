from __future__ import absolute_import, division, print_function

# Import modules
from psychopy import visual, core, event, gui, data, logging
import csv, datetime
from numpy import random
from random import shuffle
from CustomRatingScale import CustomRatingScale
import os.path


#------------------------------------------------------------
# HEALTH MESSAGE TASK
#------------------------------------------------------------
## ESCAPE key at any time to abort
## Timings except iti are set in getDurations() function below
##    Message   8 sec
##    Relevance rating   5 sec
## Fixation (iti) timings set in getFixations(),
##    with a uniform distribution between 2 sec and 6 sec
## Number of trials:   40 per run


def checkID(subj_id):
    while len(subj_id) < 3:  ## At least 3 digits desired
        subj_id = '0' + subj_id  ## Add leading zeros
    return subj_id


def getDurations(frame_rate = 1):
    '''
    message_dur = 8 * frame_rate
    rating_dur = 4 * frame_rate
    stabilize_dur = 10 * frame_rate
    instruct_dur = 2 * frame_rate
    '''
    durations = { 'message': 8 * frame_rate,
                    'rating': 5 * frame_rate,
                    'stabilize': 10 * frame_rate,
                    'instruct': 2 * frame_rate }

    return durations


def drawReady():
    ready_screen = visual.TextStim(win, text="Ready.....", height=1.5, color="#FFFFFF")
    ready_screen.draw()


def drawCross():
    fixation = visual.TextStim(win,text='+', height=3, color="#FFFFFF")  ## Fixation cross
    fixation.draw()


def drawStabilizeScreen():
    stabilize1 = visual.TextStim(win,text='Please', height=3, color="#FFFFFF", pos=(0.0, 1.5) ) # 10-sec stabilization
    stabilize2 = visual.TextStim(win,text=' wait...', height=3, color="#FFFFFF", pos=(0.0, -1.5) ) # 10-sec stabilization

    stabilize1.draw()
    stabilize2.draw()


def drawThanks():
    thanks1 = visual.TextStim(win,text='Thank', height=3, color="#FFFFFF", pos=(0.0, 1.5) ) # thank-you ending screen
    thanks2 = visual.TextStim(win,text=' you!', height=3, color="#FFFFFF", pos=(0.0, -1.5) ) # thank-you ending screen

    thanks1.draw()
    thanks2.draw()


def getScale():  ## CURRENTLY UNUSED
    # Built-in scale
    scale = visual.RatingScale(win, low = 0, high = 10, markerStart = 5, size = 2, acceptPreText = "5",
                                textColor = 'White', scale = None, noMouse = True, acceptKeys = None, skipKeys = None)

    return scale


def getCustomScale():
    # Customized scale (no blinking answer box)
    ## First, set keys based on handedness
    if r_handed:
        lKey = '1'
        rKey = '2'
    else:
        lKey = '7'
        rKey = '6'
    
    ## Instantiate and return scale
    scale = CustomRatingScale(win, low = 0, high = 10, markerStart = 5, size = 2, acceptPreText = "5",
                                textColor = 'White', scale = None, noMouse = True, acceptKeys = None, skipKeys = None,
                                leftKeys = lKey, rightKeys = rKey)

    return scale


def drawAnchors():
    # Draw "Not motivating" and "Very motivating" anchor labels
    firstAnchor = visual.TextStim(win, text='Not motivating', color="#FFFFFF", pos=(-8.5,-7))
    lastAnchor = visual.TextStim(win, text='Very motivating', color="#FFFFFF", pos=(8.5,-7))

    firstAnchor.draw()
    lastAnchor.draw()


def drawInstructions():
    # Instrcution screen
    scale = getCustomScale()
    scale.draw()

    instruction_text = visual.TextStim(win, height=1.3,color="#FFFFFF", 
            text="Use the scale to indicate how motivating each statement is to you", 
            pos=(0,+5))

    drawAnchors()
    instruction_text.draw()


def getRuns(run_number):
    # Load messages from the CSV file into a list of dictonaries:
    ## One for each msg, keys match column headers...
    ## [ {'s_ns': social/nonsoc, 
    ##    'valence': positive/negative, 
    ##    'id': msg ID#, 
    ##    'message': 'You are more likely...', etc.} ]

    stimFile = "stimuli_%s.csv" % (run_number)
    stimuli  = [i for i in csv.DictReader(open(stimFile,'rU'))]

    # Get runs to set up trial handler
    runs = []

    for i in range(len(stimuli)):
        runs.append(stimuli[i])

    return runs


def getFixations(run_num):
    '''
    itiTimes = []
    for n in range(40):
        itiTimes.append(random.random_integers(2,6))
    '''
    ## Used the above code to generate a uniform distribution
    if run_num == 1:
        itiTimes = [2, 6, 5, 3, 6, 3, 6, 3, 5, 6, 4, 2, 4, 2, 2, 5, 4, 6, 4, 4, 6, 3, 4, 4, 6, 3, 5, 3, 3, 3, 2, 5, 5, 3, 3, 5, 2, 5, 4, 6]
    else: # run_num == 2
        itiTimes = [4, 4, 6, 4, 4, 6, 3, 5, 5, 4, 6, 6, 5, 2, 6, 2, 5, 2, 2, 2, 4, 3, 6, 3, 6, 4, 4, 3, 5, 4, 5, 3, 5, 6, 4, 3, 3, 4, 3, 2]
    shuffle(itiTimes)
    return itiTimes


# Do run
def do_run(run_number, trials):
    # Set up CSV data file
    runNumStr = str(run_number)
    if len(runNumStr) < 2:
        runNumStr = '0' + runNumStr

    csvName_noPath = "sub-%s_task-HealthMessageTask_run-%s_events.tsv" % (subj_id, runNumStr)
    csvName = os.path.join("logs", csvName_noPath)
    csvFile = open(csvName, 'w')
    csvWriter = csv.writer(csvFile, delimiter='\t')

    csvWriter.writerow(['onset', 'duration', 'trial', 'trial_type', 'rating', 'resp_time', 'valence', 's_ns', 'id'])
    indDic = {'onset' : 0, 'duration' : 1, 'trial_num': 2, 'trial_type' : 3, 'rating': 4, 'resp_time': 5, 'valence': 6, 's_ns': 7, 'id': 8}
    dataListLength = len( indDic.keys() )

    # Set up log file
    saveLog = False

    if saveLog:
        logName = "%s.log" % (subj_id)
        log_file = logging.LogFile(os.path.join('logs', logName), level=logging.DATA, filemode="w")

    globalClock = core.Clock()

    logging.setDefaultClock(globalClock)

    # Set up button variables
    ##button_labels = { 'b': 0, 'y': 1, 'g': 2, 'r': 3 }
    button_labels = { '1': 0, '2': 1, '3': 2, '4': 3 }
    buttons = button_labels.keys()

    # Set up dictionary of duration values
    durations = getDurations(frame_rate = 1)  ## Default: frame_rate = 1

    # --------- Instructions begin ---------

    # Show instructions
    timer = core.Clock()
    timer.reset()

    while timer.getTime() < durations['instruct']:  ##for frame in range(durations['instruct']):
        drawInstructions()
        win.flip()

    event.waitKeys(keyList=('space'))

    # Display "ready" screen and wait for 'T' to be sent to indicate scanner trigger
    drawReady()
    win.flip()

    event.waitKeys(keyList='t')

    # Reset globalClock and show stabilization screen;
    # time starts when stabilizing screen shows
    
    globalClock.reset()
    
    while globalClock.getTime() < durations['stabilize']:
        drawStabilizeScreen()
        win.flip()

    # Send START log event
    logging.log(level=logging.DATA, msg='******* START (trigger from scanner) - run %s *******' % run_number)

    # Set up message screen (image and message will change repeatedly):
    ##             IMAGE
    ##
    ##            message

    ##pictureStim = visual.ImageStim(win, pos=(0,6.5), size=(12.6,9.2) )
    messageStim = visual.TextStim(win, text='', pos=(0,5.5), color="#FFFFFF", wrapWidth=20, alignHoriz = 'center')
    messageStim.height = 1.25
    questionStim = visual.TextStim(win, text='How motivating is this statement to you?', pos=(0,-0.5), color="#FFFFFF", wrapWidth=20)

    ##itiTimes = getFixations(len(trials.trialList))  ## to have number of ITIs not pre-determined
    itiTimes = getFixations(run_number)

    # --------- MAIN LOOP - present trials ---------

    for tidx, trial in enumerate(trials):
        # Prepare to write CSV data for this trial
        dataList = [""] * dataListLength
        dataList[indDic['trial_num']] = tidx + 1

        # Get stimuli for this trial
        valence = trial['valence']  ## "positive" or "negative"
        s_ns = trial['s_ns']  ## "social" or "nonsocial"
        id = valence[0:3] + "_" + s_ns[:-3] + "_" + trial['id']
        dataList[indDic['valence']] = valence
        dataList[indDic['s_ns']] = s_ns
        dataList[indDic['id']] = id


        ##image_noPath = "%s_%s.png" % (theme, trial_type)
        ##image = os.path.join('images', cond, image_noPath)
        ##audio_noPath = "%s_%s_%s.wav" % (theme, trial_type, cond)
        ##audio = os.path.join('audio', audio_noPath)

        ##message = trial['message']  ## If quotes desired
        message = trial['message'].strip('"')  ## If quotes not desired

        ##pictureStim.setImage(image)
        messageStim.setText(message)

        # send FIXATION log event
        logging.log(level=logging.DATA, msg='FIXATION')
        iti_onset = globalClock.getTime()

        # show fixation
        timer.reset()

        this_iti = itiTimes.pop()
        while timer.getTime() < this_iti:  ##while timer.getTime() < fixation_for_trial:  ###for frame in range(durations['fixation']):
            drawCross()
            win.flip()

        # send MESSAGE log event
        ##logging.log(level = logging.DATA, msg = "MESSAGE: %s - %s - %s" % (cond, theme, trial_type))

        stim_onset = globalClock.getTime()
        trials.addData('stim_onset', stim_onset)

        # show mesage 
        timer.reset()

        while timer.getTime() < durations['message']:  ##for frame in range(durations['message']):
            ##pictureStim.draw()
            messageStim.draw()
            win.flip()
            if 'escape' in event.getKeys():
                core.quit()

        # send SHOW RATING log event
        logging.log(level = logging.DATA, msg = "SHOW RATING")

        choice_onset = globalClock.getTime()
        trials.addData('resp_onset', choice_onset)

        # clear event buffer and get time
        event.clearEvents()
        resp_onset = globalClock.getTime()

        # show rating and collect response  
        timer.reset()

        scale = getCustomScale()
        while timer.getTime() < durations['rating']:  ##for frame in range(durations['rating']):
            ##pictureStim.draw()
            messageStim.draw()
            questionStim.draw()
            scale.draw()
            drawAnchors()
            win.flip()
        if 'escape' in event.getKeys():
            core.quit()

        # get key response
        allRatings = scale.getHistory()  ## List of tuples
        respTup = allRatings[-1]  ## Tuple: (finalResp, finalPress)
        resp_value = respTup[0]
        rt = respTup[1]

        # add response value to the trial handler logging
        trials.addData('resp',resp_value)
        trials.addData('rt', rt)

        # Write trial data to CSV file and flush
        dataList[indDic['rating']] = resp_value
        dataList[indDic['resp_time']] = rt

        dataList[indDic['trial_type']] = 'iti'
        dataList[indDic['onset']] = iti_onset
        dataList[indDic['duration']] = this_iti
        csvWriter.writerow(dataList)

        dataList[indDic['trial_type']] = 'message'
        dataList[indDic['onset']] = stim_onset
        dataList[indDic['duration']] = durations['message']
        csvWriter.writerow(dataList)

        dataList[indDic['trial_type']] = 'rating'
        dataList[indDic['onset']] = choice_onset
        dataList[indDic['duration']] = durations['rating']
        csvWriter.writerow(dataList)

        csvFile.flush()

    drawThanks()
    win.flip()
    core.wait(3)


    # --------- Write log files ---------

    # Send END log event
    logging.log(level = logging.DATA, msg = '******* END run %s *******' % run_number)

    ##log_file.logger.flush()

    # save the trial infomation from trial handler
    log_filename_noPath = '%s.csv' % subj_id
    log_filename = os.path.join('logs', log_filename_noPath)
    log_filename2 = "%s_%s.csv" % (log_filename[:-4], run_number )

    ##trials.saveAsText(log_filename2, delim=',', dataOut=('n', 'all_raw'))
    ##trials.saveAsText(log_filename2, delim=',', dataOut=['resp_onset_raw', 'resp_raw', 'rt_raw', 'stim_onset_raw', 'order_raw'])

    # Quit
    core.quit()


# ==================================
# MAIN - set up trials and do run(s)
# ==================================
if __name__ == '__main__':
    # Get subject ID number from GUI dialog box
    subjDlg = gui.Dlg(title="Health Message Task")
    subjDlg.addField('Enter Subject ID:')
    subjDlg.addField('Select dominant hand:', choices = ['right', 'left'])
    subjDlg.addField('Select Run Number:', choices = ['01', '02'])
    subjDlg.show()
    if subjDlg.OK: ## If "OK" is pressed
        subj_id_raw = subjDlg.data[0]
        hand_raw = subjDlg.data[1]
        run_num = subjDlg.data[2]
    else: ## If "Cancel" is pressed
        core.quit()

    # Initialize global variables; set Full Screen T/F (win)
    global win, mouse, subj_id, r_handed
    win = visual.Window([1024,768], fullscr = True, monitor='testMonitor', units='deg') 
    mouse = event.Mouse(visible = True)
    subj_id = checkID(subj_id_raw)
    r_handed = ('r' == hand_raw[0])  ## right-handed: True or False

    # Run(s)
    runs = getRuns(run_num)
    trials = data.TrialHandler(runs, nReps = 1, dataTypes = ['stim_onset', 'resp_onset', 'rt', 'resp'], method = "random")
    do_run(run_num, trials)  ## First input argument is run number
