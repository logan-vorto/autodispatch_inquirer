def ourTimeToEpoch:
    capture("(?<date>[0-9]{4}-[0-9]{2}-[0-9]{2})") |
    .date |
    tostring |
    strptime("%F") | 
    mktime;

def getHour:
    capture("(?<hour>[0-9]{2}):[0-9]{2}:[0-9]") |
    .hour;

# takes in an epoch time in seconds and returns 24 hours prior to that 
def oneDayBefore:
    . - (60 * 60 * 24);
    
