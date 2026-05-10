package com.dataengineering.lab4;

public record TripParse(String day, double durationSec, String fromStation, String toStation) {

    public boolean validForDurationAndCount() {
        return day != null && !day.isBlank() && Double.isFinite(durationSec) && durationSec >= 0;
    }

    public boolean validForStartStation() {
        return validForDurationAndCount()
                && fromStation != null
                && !fromStation.isBlank();
    }

    public boolean validForEndpointStations() {
        return validForDurationAndCount()
                && fromStation != null && !fromStation.isBlank()
                && toStation != null && !toStation.isBlank();
    }
}
