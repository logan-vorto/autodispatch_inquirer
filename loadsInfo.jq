include "./funcs";

def getTodaysDrivers(date): [
    .[] | # for each driver
        . as $driver |
        if .Shifts == null then
            empty
        else
            .
        end |
        .Shifts |
        .[] | # for each shift
            (.StartTime | ourTimeToEpoch) as $shift_start |
            if $shift_start == date then
                $driver
            else
                empty
            end
];

def getDaily:
    group_by(.shortDate) |
    .[] | # for each day
    {
        shortDate: .[0].shortDate,
        allDriverIDs: [ .[] | .driversForToday ] | add | unique,
        allLoadIDs: [ .[] | .unassignedLoadCountForDate ] | add | unique,
        carryoverLoads: [ .[] | .carryOverLoadsForDate ] | add | unique
    }
;

def processRow: 
    (.created_at | ourTimeToEpoch) as $ad_created |
    {
        poolID: .pool_id,
        shortDate: $ad_created | strftime("%F"),
        hour: .created_at | getHour,
        adInputDate: .created_at,
        driversForToday: .input_payload.Drivers | 
            getTodaysDrivers($ad_created) |
            map(.ID),
        driverCountForDate: .input_payload.Drivers | 
            getTodaysDrivers($ad_created) |
            length, # if we want raw id's, just remove this filter
        driverAssignedLoadCount: .input_payload.Drivers | 
            getTodaysDrivers($ad_created) |
            [
                .[] | # for each driver
                    if .AssignedLoads != null then
                        .AssignedLoads | map(.ID)
                    else
                        empty
                    end
            ] | add,
        unassignedLoadCountForDate: 
            .input_payload.UnassignedLoads | 
            [ 
                .[] | # for each load
                (.Stops.[0].ArrivalWindowStartTime | ourTimeToEpoch) as $load_time |
                if $load_time == $ad_created then 
                    .ID
                else 
                    empty 
                end
            ],
        carryOverLoadsForDate:
            .input_payload.UnassignedLoads |
            [ 
                .[] | # for each load
                (.Stops.[0].ArrivalWindowStartTime | ourTimeToEpoch) as $load_time |
                if $load_time < $ad_created then 
                    .ID
                else 
                    empty 
                end
            ]
    }
;

if type == "array" then
    [
    if .[0] | type == "array" then
        # we are in a slurp-combined file, iterate the top level things
        .[] | .[] | processRow
    elif .[0] | type == "object" then
        # we are in a single file, iterate the top level things
        .[] | processRow
    else
        "unable to process input rows that is not either object or array"
    end
    ] |
    sort_by(.adInputDate) |
    getDaily
else
    "unable to process input that is not array"
end
