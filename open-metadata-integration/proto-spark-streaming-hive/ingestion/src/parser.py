import datetime
from typing import List, Dict, Any
from google.protobuf.message import Message
from com.google.transit.realtime import gtfs_realtime_pb2
from proto import gtfs_realtime_NYCT_pb2 as nyct_pb2


def parse_feed(
    feed: gtfs_realtime_pb2.FeedMessage, ingestion_ts: datetime.datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    header = feed.header
    # Mapping NyctFeedHeader (nyct_subway_version, trip_replacement_period)
    nyct_header = header.Extensions[nyct_pb2.nyct_feed_header]
    feed_ts = datetime.datetime.fromtimestamp(
        header.timestamp, tz=datetime.timezone.utc
    )

    # Replacement Map: route_id -> replacement_period end time
    replacement_map = {
        rp.route_id: datetime.datetime.fromtimestamp(
            rp.replacement_period.end, tz=datetime.timezone.utc
        )
        for rp in nyct_header.trip_replacement_period
    }

    vehicle_map = {}
    alert_map = {}
    trip_mod_map = {}

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            vehicle_map[v.trip.trip_id] = {
                "v_timestamp": datetime.datetime.fromtimestamp(
                    v.timestamp, tz=datetime.timezone.utc
                ),
                "v_stop_id": v.stop_id,
                "v_status": gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                    v.current_status
                ),
                "v_id": v.vehicle.id if v.HasField("vehicle") else None,
                "v_label": v.vehicle.label if v.HasField("vehicle") else None,
                "v_license": v.vehicle.license_plate if v.HasField("vehicle") else None,
                "v_occupancy": gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(
                    v.occupancy_status
                )
                if v.HasField("occupancy_status")
                else None,
            }

        if entity.HasField("alert"):
            a = entity.alert
            for informed in a.informed_entity:
                if informed.HasField("trip"):
                    alert_map[informed.trip.trip_id] = {
                        "text": a.header_text.translation[0].text
                        if a.header_text.translation
                        else "Delayed",
                        "cause": gtfs_realtime_pb2.Alert.Cause.Name(a.cause),
                        "effect": gtfs_realtime_pb2.Alert.Effect.Name(a.effect),
                    }

        if entity.HasField("trip_modifications"):
            tm = entity.trip_modifications
            for selected in tm.selected_trips:
                for tid in selected.trip_ids:
                    trip_mod_map[tid] = selected.shape_id


    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip
        t_id = trip.trip_id

        nyct_trip = trip.Extensions[nyct_pb2.nyct_trip_descriptor]

        v_data = vehicle_map.get(t_id, {})
        a_data = alert_map.get(t_id, {})

        affected_trip_id = (
            trip.modified_trip.affected_trip_id
            if trip.HasField("modified_trip")
            else None
        )

        for stu in tu.stop_time_update:
            nyct_stop = stu.Extensions[nyct_pb2.nyct_stop_time_update]
            rows.append({
                # Header Data
                "ingestion_ts": ingestion_ts,
                "feed_timestamp": feed_ts,
                "nyct_subway_version": nyct_header.nyct_subway_version,
                "route_replacement_until": replacement_map.get(trip.route_id),

                # Trip Descriptor Fields
                "trip_id": t_id,
                "affected_trip_id": affected_trip_id,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,  # Standard GTFS-RT
                "start_date": trip.start_date,
                "start_time": trip.start_time,
                "schedule_relationship": gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                    trip.schedule_relationship) if trip.HasField("schedule_relationship") else None,

                # NYCT Trip Extensions
                "nyct_train_id": nyct_trip.train_id,
                "nyct_is_assigned": nyct_trip.is_assigned,
                "nyct_direction": nyct_pb2.NyctTripDescriptor.Direction.Name(nyct_trip.direction) if nyct_trip.HasField(
                    "direction") else None,

                # Experimental TripMod Fields
                "mod_shape_id": trip_mod_map.get(t_id),

                # StopTimeUpdate Fields
                "stop_id": stu.stop_id,
                "stop_sequence": stu.stop_sequence,
                "arrival_time": datetime.datetime.fromtimestamp(stu.arrival.time,
                                                                tz=datetime.timezone.utc) if stu.HasField(
                    "arrival") and stu.arrival.time > 0 else None,
                "departure_time": datetime.datetime.fromtimestamp(stu.departure.time,
                                                                  tz=datetime.timezone.utc) if stu.HasField(
                    "departure") and stu.departure.time > 0 else None,
                "stu_relationship": gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                    stu.schedule_relationship),

                # NYCT Stop Extensions
                "scheduled_track": nyct_stop.scheduled_track,
                "actual_track": nyct_stop.actual_track,

                # Vehicle Data
                "vehicle_id": v_data.get("v_id"),
                "vehicle_label": v_data.get("v_label"),
                "vehicle_license": v_data.get("v_license"),
                "vehicle_last_movement_ts": v_data.get("v_timestamp"),
                "vehicle_current_status": v_data.get("v_status"),
                "occupancy_status": v_data.get("v_occupancy"),

                # Alert Data
                "is_delayed": t_id in alert_map,
                "alert_text": a_data.get("text"),
                "alert_cause": a_data.get("cause"),
                "alert_effect": a_data.get("effect")
            })

    return rows
