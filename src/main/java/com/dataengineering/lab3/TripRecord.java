package com.dataengineering.lab3;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
        "trip_id", "start_time", "end_time", "bikeid", "tripduration",
        "from_station_id", "from_station_name", "to_station_id", "to_station_name",
        "usertype", "gender", "birthyear"
})
public record TripRecord(
        String trip_id,
        String start_time,
        String end_time,
        String bikeid,
        String tripduration,
        String from_station_id,
        String from_station_name,
        String to_station_id,
        String to_station_name,
        String usertype,
        String gender,
        String birthyear
) {
}
