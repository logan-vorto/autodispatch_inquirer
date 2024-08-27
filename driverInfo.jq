include "./funcs";

.[] | # for each row (autodispatch input)
    (.created_at | ourTimeToEpoch) as $ad_created |
    .input_payload.Drivers | 
    [
        .[] | # for each driver
            .Shifts |
            .[] | # for each shift
                (.StartTime | ourTimeToEpoch) as $shift_start |
                if $shift_start == $ad_created then
                    .ID
                else
                    empty
                end
    ] |
    length # if we want raw id's, just remove this filter
