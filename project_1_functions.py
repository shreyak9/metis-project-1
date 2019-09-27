#Functions used in project_1_main:

import pandas as pd
import calendar
import datetime

import matplotlib.pyplot as plt
# You can configure the format of the images: ‘png’, ‘retina’, ‘jpeg’, ‘svg’, ‘pdf’.
# %config InlineBackend.figure_format = 'svg'
# this statement allows the visuals to render within your Jupyter Notebook
# %matplotlib inline 

# Plot configurations
plt.rc('axes', axisbelow=True)


#Import data
data_path = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_"

def import_data(date):
    '''
    import data
    clean column names
    '''
    dfname = pd.read_csv(data_path+date+".txt")
    dfname.columns = dfname.columns.str.strip()
    return dfname

#Set time intervals in data_clean function
def time_interval(x):
    '''
    Create time intervals
    '''
    if x in [0,1,2,3]:
        return "8:00PM-11:59PM"
    elif x in [4,5,6,7]:
        return "0:00AM-3:59AM"
    elif x in [8,9,10,11]:
        return "4:00AM-7:59AM"
    elif x in [12,13,14,15]:
        return "8:00AM-11:59AM"
    elif x in [16,17,18,19]:
        return "12:00PM-3:59PM"
    elif x in [20,21,22,23]:
        return "4:00PM-7:59PM"

#Create formatted variables and perform data cleaning
def data_clean(dfname):
    """
    add and format new variables
    """
    dfname2 = dfname.copy()
    # Create variables:
    # "time_hour" that simplifies the hour for later grouping purposes
    dfname2["time_hour"] = pd.to_numeric(dfname2["TIME"].str[0:2])
    # formatted date variable "DDATE"
    dfname2['DDATE']=[datetime.datetime.strptime(x, '%m/%d/%Y') for x in dfname2['DATE']]
    # formatted time variable "DTIME"
    dfname2['DTIME']=[datetime.datetime.strptime(x, '%H:%M:%S') for x in dfname2['TIME']]
    dfname2['DTIME']=[format(x,"%H:%M:%S") for x in dfname2['DTIME']]
    # formatted combined date and time variable "DATETIME"
    dfname2['DDATETIME'] = pd.to_datetime(dfname2['DATE']+" "+dfname2['TIME'])
    # formatted day of the week variable "DDAY"
    dfname2['DDAY']=[calendar.day_name[datetime.datetime.weekday(x)] for x in dfname2['DDATE']]
    # create time period category "time_cat"
    dfname2["time_cat"] = dfname2["time_hour"].apply(time_interval)
    
    # Sort before grouping for difference calculating
    dfname2.sort_values(["C/A","UNIT","SCP","STATION","LINENAME","DIVISION","DATE","TIME","DESC"], inplace = True)
    # Create difference columns to calculate difference in entries and exits between the row and the row before (aka the time before)
    dfname2["entries_diff"] = dfname2.groupby(["C/A","UNIT","SCP","STATION","LINENAME","DIVISION"]).ENTRIES.diff()
    dfname2["exits_diff"] = dfname2.groupby(["C/A","UNIT","SCP","STATION","LINENAME","DIVISION"]).EXITS.diff()
    dfname2["entries-exits"] = dfname2["entries_diff"] - dfname2["exits_diff"]
    dfname2["entries+exits"] = dfname2["entries_diff"] + dfname2["exits_diff"]
        
    #keep only rows with positive entries_diff, exits_diff, and ENTRIES
    dfname2= dfname2[dfname2.entries_diff > 0]
    dfname2 = dfname2[dfname2.exits_diff > 0]
    dfname2 = dfname2[dfname2.ENTRIES > 0]
    # dropping turnstile 00-04-00 at 23rd st due to data anomaly
    dfname2 = dfname2[(dfname2["STATION"] != "TWENTY THIRD ST") & (dfname2["SCP"] != "00-04-00")]
    # exclude high entries and exits that are likely a result of a terminal reset
    dfname2 = dfname2[dfname2.entries_diff < 100000]
    dfname2 = dfname2[dfname2.exits_diff < 100000].reset_index().drop(["index"],axis=1)
    dfname2.drop([412758,412902,413046,413190,413334], inplace=True)
    return dfname2


# Create sorting variables:

def day_sort(x):
    """
    create ordered variable to sort data for graph axes by day of the week
    """
    if x == "Sunday":
        return 1
    elif x == "Monday":
        return 2
    elif x == "Tuesday":
        return 3
    elif x == "Wednesday":
        return 4
    elif x == "Thursday":
        return 5
    elif x == "Friday":
        return 6
    elif x == "Saturday":
        return 7
    
def time_sort(x):
    """
    create ordered variable to sort data for graph axes by time variable
    """
    if x == "0:00AM-3:59AM":
        return 1
    elif x == "4:00AM-7:59AM":
        return 2
    elif x == "8:00AM-11:59AM":
        return 3
    elif x == "12:00PM-3:59PM":
        return 4
    elif x == "4:00PM-7:59PM":
        return 5
    elif x == "8:00PM-11:59PM":
        return 6
    
# Select data for average volume by time period for individual stations
def select_station(datafile,station):
    """
    select data for each station and calculate average volume by time period across all days of the week
    """
    new_data = datafile[(datafile["STATION"] == station) & (datafile["DDAY"] == "Wednesday")].sort_values(["DDAY","time_cat","entries+exits"])
    new_data["time_sort"] = new_data["time_cat"].apply(time_sort)
    new_data.sort_values(["DDAY","time_sort"],inplace=True)    
    return new_data

# Select data for average volume by time period AND day of the week for individual stations
def select_station2(datafile,station):
    """
    select data for each station and calculate average volume by time interval and day of the week
    """
    new_data_1 = datafile.groupby(['STATION','DDAY','DDATE','time_cat'],as_index=False)['entries+exits'].sum()
    new_data = new_data_1.groupby(['STATION','DDAY','time_cat'],as_index=False)['entries+exits'].mean()
    new_data = new_data.loc[new_data["STATION"] == station]
    new_data["time_sort"] = new_data["time_cat"].apply(time_sort)
    new_data.sort_values(["DDAY","time_sort"],inplace=True)
    return new_data

# shape data for graphs on time period and day of the week
def graph_convert(D1,Station,graphname):
    """
    reshape graph data for each station into format for a grouped bar chart
    plot grouped graph data
    """
    day_dict = {}

    days = D1['DDAY'].unique()
    for day in days:
        day_dict[day] =  D1[D1['DDAY'] == day]['entries+exits'].values

    times = D1['time_cat'].unique()


    Final_df = pd.DataFrame({'time':times, 'Monday':day_dict['Monday'], 
                                'Tuesday':day_dict['Tuesday'], 
                                'Wednesday':day_dict['Wednesday'],'Thursday':day_dict['Thursday'],'Friday':day_dict['Friday'],'Saturday':day_dict['Saturday'],'Sunday':day_dict['Sunday']})
    Final_df
    Final_df[['time', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday',
           'Sunday']].plot(kind='bar', 
                           color= ["#41c38a","#4394b7","#fbbb3a","#c3507f","#d16d3a","#a59f9f","#b49d9d"],
                          figsize = (12,6))
    plt.xlabel('Time Period', fontsize=12);
    plt.ylabel('Average Traffic Volume',fontsize=12)
    plt.grid()
    plt.title("Average Traffic Volume for "+Station, fontsize=16, y = 1.05)
    plt.xticks([i for i in range(Final_df.shape[1]-1)], labels = Final_df['time'], rotation = 0);

    plt.savefig(graphname, bbox_inches = 'tight')
    return 
