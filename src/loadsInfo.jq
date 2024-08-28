include "./src/funcs";

def getTodaysDrivers(date): [
    if . == null then
        []
    else
        .[] | # for each driver
            . as $driver |
            if .Shifts == null then
                empty
            else
                .Shifts |
                .[] | # for each shift
                if .StartTime != null then
                    (.StartTime | ourTimeToEpoch) as $shift_start |
                    if $shift_start != null and $shift_start == date then
                        if .Standby and .AlreadyCompletedLoad then
                            $driver
                        elif .Standby == false then
                            $driver
                        else
                            empty
                        end
                    else
                        empty
                    end
                else
                    debug("shift \(.ID) has no start time") |
                    empty
                end
            end
    end
];

# expects a list of drivers as input and returns list of all loads that have a pickup stop on specified date
def getDriverLoadsForToday(date):
    if . == null then
        []
    else
        [
            .[] | # for each driver
                if .AssignedLoads != null then
                    .AssignedLoads.[] |
                        .ID as $load_id |
                        .Stops |
                        map(select(.StopType != null and (.StopType == "inbound" or .StopType == "outbound"))) |
                        if length > 0 then
                            if .[0].ArrivalWindowStartTime != null then
                                if (.[0].ArrivalWindowStartTime | ourTimeToEpoch) == date then
                                    [$load_id]
                                else
                                    empty
                                end
                            else
                                debug("assigned load \($load_id) has no arrival window start time") |
                                empty
                            end
                        else
                            empty
                        end
                else
                    empty
                end
        ] | (add // [])
    end
;

# builds unique list and counts of various fields pulled from the AD input payload
def getDaily:
    group_by(.shortDate) |
    [
        .[] | # for each day
        {
            shortDate: .[0].shortDate,
            # list of unique driver ids for the day
            allDriverIDs: [ .[] | .driversForToday ] | add | unique,
            # list of unique load ids that were seen as unassigned and ready for the day
            allReadiedLoadIDs: [ .[] | .unassignedReadiedLoadCountForDate ] | add | unique,
            # list of unique load ids that were seen as assigned to a driver for the day
            allAssignedLoadIDs: [ .[] | .driverAssignedLoad ] | add | unique,
            # list of unique load ids that were seen as either unassigned+ready and/or assigned to a driver for the day (may have overlap with the other lists)
            allReadyAndAssignedLoadIDs: [ .[] | .unassignedReadiedLoadCountForDate + .driverAssignedLoad ] | add | unique,
            # list of unique load ids that were seen today as either unassigned, unassigned+ready, or assigned to a driver for the day (may have overlap with the other lists)
            allTodayLoadIDs: [ .[] | .unassignedReadiedLoadCountForDate + .driverAssignedLoad + .unassignedLoadsForDate ] | add | unique,
            # list of unique load ids that were seen as unassigned and never ready and never assigned
            allUnassignedNeverReadyNeverAssignedIDs: [ .[] | .unassignedLoadsForDate - .unassignedReadiedLoadCountForDate - .driverAssignedLoad ] | add | unique,
            # list of unique load ids that were seen as unassigned and not ready for the day (may have overlap with the readied list)
            allUnassignedLoadIDs: [ .[] | .unassignedLoadsForDate ] | add | unique,
            # list of unique load ids that are unassigned today with a start window time before today (unbounded carryover)
            carryoverLoads: [ .[] | .carryOverLoadsForDate ] | add | unique,
            # list of unique load ids that are unassigned today with a start window time in the previous day (will have overlap with carryover list)
            priorDayLoads: [ .[] | .carryOverLoadsOnlyPreviousDay ] | add | unique
        } |
        .allDriverCount = (.allDriverIDs | length) |
        .allReadiedLoadCount = (.allReadiedLoadIDs | length) |
        .allAssignedLoadCount = (.allAssignedLoadIDs | length) |
        .allReadyAndAssignedLoadCount = (.allReadyAndAssignedLoadIDs | length) |
        .allTodayLoadCount = (.allTodayLoadIDs | length) |
        .allUnassignedLoadCount = (.allUnassignedLoadIDs | length) |
        .allUnassignedNeverReadyNeverAssignedCount = (.allUnassignedNeverReadyNeverAssignedIDs | length) |
        .allCarryoverLoadCount = (.carryoverLoads | length) |
        .allPriorDayLoadCount = (.priorDayLoads | length)
    ]
;

def processRow: 
    if .input_payload.UnassignedLoads == null then
        debug("no unassigned loads found") |
        empty
    end |

    (.created_at | ourTimeToEpoch) as $ad_created |
    {
        poolID: .pool_id,
        shortDate: $ad_created | strftime("%F"),
        hour: .created_at | getHour,
        adInputDate: .created_at,
        driversForToday: .input_payload.Drivers | 
            getTodaysDrivers($ad_created) |
            map(.ID),
        driverAssignedLoad: .input_payload.Drivers | 
            getTodaysDrivers($ad_created) |
            getDriverLoadsForToday($ad_created),
        unassignedLoadsForDate: 
            .input_payload.UnassignedLoads | 
            [ 
                .[] | # for each load
                if .Stops == null then
                    empty
                elif .Stops.[0].ArrivalWindowStartTime != null then
                    (.Stops.[0].ArrivalWindowStartTime | ourTimeToEpoch) as $load_time |
                    if $load_time != null and $load_time == $ad_created then 
                        .ID
                    else 
                        empty 
                    end
                else
                    debug("load \(.ID) has no start time") |
                    empty
                end
            ],
        unassignedReadiedLoadCountForDate: 
            .input_payload.UnassignedLoads | 
            [ 
                .[] | # for each load
                if .Stops.[0].ArrivalWindowStartTime != null then
                    (.Stops.[0].ArrivalWindowStartTime | ourTimeToEpoch) as $load_time |
                    if $load_time != null and $load_time == $ad_created and .ReadyForDispatch then 
                        .ID
                    else 
                        empty 
                    end
                else
                    debug("load \(.ID) has no start time") |
                    empty
                end
            ],
        carryOverLoadsForDate:
            .input_payload.UnassignedLoads |
            [ 
                .[] | # for each load
                if .Stops.[0].ArrivalWindowStartTime != null then
                    (.Stops.[0].ArrivalWindowStartTime | ourTimeToEpoch) as $load_time |
                    if $load_time < $ad_created then 
                        .ID
                    else 
                        empty 
                    end
                else
                    debug("load \(.ID) has no start time") |
                    empty
                end
            ],
        carryOverLoadsOnlyPreviousDay:
            .input_payload.UnassignedLoads |
            [ 
                .[] | # for each load
                if .Stops.[0].ArrivalWindowStartTime != null then
                    (.Stops.[0].ArrivalWindowStartTime | ourTimeToEpoch) as $load_time |
                    if $load_time == ($ad_created | oneDayBefore) then 
                        .ID
                    else 
                        empty 
                    end
                else
                    debug("load \(.ID) has no start time") |
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
