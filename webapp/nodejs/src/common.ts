import type { Connection, RowDataPacket } from "mysql2/promise";
import type { Ride, RideStatus } from "./types/models.js";

export const INITIAL_FARE = 500;
export const FARE_PER_DISTANCE = 100;

// マンハッタン距離を求める
export const calculateDistance = (
  aLatitude: number,
  aLongitude: number,
  bLatitude: number,
  bLongitude: number,
): number => {
  return Math.abs(aLatitude - bLatitude) + Math.abs(aLongitude - bLongitude);
};

export const calculateFare = (
  pickupLatitude: number,
  pickupLongitude: number,
  destLatitude: number,
  destLongitude: number,
): number => {
  const meterdFare =
    FARE_PER_DISTANCE *
    calculateDistance(
      pickupLatitude,
      pickupLongitude,
      destLatitude,
      destLongitude,
    );
  return INITIAL_FARE + meterdFare;
};

export const calculateSale = (ride: Ride): number => {
  return calculateFare(
    ride.pickup_latitude,
    ride.pickup_longitude,
    ride.destination_latitude,
    ride.destination_longitude,
  );
};

export const getLatestRideStatus = async (
  dbConn: Connection,
  rideId: string,
): Promise<string> => {
  const [[{ status }]] = await dbConn.query<
    Array<Pick<RideStatus, "status"> & RowDataPacket>
  >(
    "SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1",
    [rideId],
  );
  return status;
};

export const getLatestRideStatusByIds = async (
  dbConn: Connection,
  rideIds: string[],
): Promise<Map<string, string>> => {
  const ride_status_map: Map<string, string> = new Map(
    await dbConn.query<Array<Pick<RideStatus, "status"> & RowDataPacket>>(
      `WITH latest_ride_statuses AS (
        SELECT ride_id,
          MAX(created_at) AS created_at
        FROM ride_statuses
        WHERE ride_id IN (?)
        GROUP BY ride_id
      )
      SELECT ride_statuses.ride_id as ride_id,
        ride_statuses.status as status
      FROM ride_statuses
      INNER JOIN latest_ride_statuses
        ON ride_statuses.ride_id = latest_ride_statuses.ride_id
        AND ride_statuses.created_at = latest_ride_statuses.created_at`,
      [rideIds],
    ).map((rideStatus: RideStatus & RowDataPacket) => [
      rideStatus.ride_id,
      rideStatus.status,
    ]),
  );

  return ride_status_map;
};

export class ErroredUpstream extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ErroredUpstream";
  }
}
