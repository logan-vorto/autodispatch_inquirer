def ourTimeToEpoch:
    capture("(?<date>[0-9]{4}-[0-9]{2}-[0-9]{2})") |
    .date |
    tostring |
    strptime("%F") | 
    mktime;

def getRoundedMinute:
    capture("[0-9]{2}:(?<minute>[0-9]{2}):(?<second>[0-9]{2})") |
    if .second | tonumber > 30 then
        .minute | tonumber + 1
    else
        .minute | tonumber
    end;

def getRoundedHour:
    capture("(?<hour>[0-9]{2}):(?<minute>[0-9]{2}):[0-9]") |
    if .minute | tonumber > 30 then
        .hour | tonumber + 1
    else
        .hour | tonumber
    end;

# takes in an epoch time in seconds and returns 24 hours prior to that 
def oneDayBefore:
    . - (60 * 60 * 24);
    
