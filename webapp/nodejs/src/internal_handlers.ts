import type { Context } from "hono";
import type { Environment } from "./types/hono.js";
import type { RowDataPacket, Connection } from "mysql2/promise";
import type { Chair, ChairLocation, ChairModel, Ride } from "./types/models.js";

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
export const internalGetMatching = async (ctx: Context<Environment>) => {
  // 配車できていないrideを取得
  const unmatched_ride_ids = await ctx.var.dbConn.query<Array<Ride & RowDataPacket>>(
    "SELECT id FROM rides WHERE chair_id IS NULL ORDER BY created_at",
  );
  if (!unmatched_ride_ids.length) {
    return ctx.body(null, 204);
  }

  // 配車できていないrideのデータを取得
  const unmatched_rides = await ctx.var.dbConn.query<Array<Ride & RowDataPacket>>(
    "SELECT id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude FROM rides WHERE id IN (?)",
    [unmatched_ride_ids.map((ride: Ride) => ride.id)],
  );

  // 配車可能な椅子を取得
  const available_chairs = await ctx.var.dbConn.query<Array<{ id: string, model: string } & RowDataPacket>>(
    "SELECT id, model FROM chairs WHERE is_active = TRUE",
  );

  // 配車可能な椅子の現在位置を椅子のidをキーにしてmapで保持
  const chair_locations_map = new Map<string, { latitude: number, longitude: number }>(
    (await getChairsLatestLocationsByChairIds(
      ctx.var.dbConn,
      available_chairs.map((chair: { id: string }) => chair.id),
    )).map((location: ChairLocation) => [
      location.chair_id,
      { latitude: location.latitude, longitude: location.longitude },
    ]),
  );

  // 椅子のモデルごとの移動速度を取得(nameをkeyにしてspeedをvalueにしたオブジェクトを返す)
  const chair_models_map = new Map<string, number>(
    (await ctx.var.dbConn.query<Array<ChairModel & RowDataPacket>>(
      "SELECT name, speed FROM chair_models WHERE name IN (?)",
      [available_chairs.map((chair: { model: string }) => chair.model)],
    )).map((chair_model: ChairModel) => [chair_model.name, chair_model.speed])
  );

  /**
   * マッチングスコアの計算
   * 
   * 配車できていないrideと配車可能な椅子のマッチングスコアを計算する
   * マッチングスコアは、椅子の乗車位置までの移動時間と, 乗車位置から目的地までの移動時間の合計に待ち時間を加味して計算する
   */
  const scoring_rides = [] as { ride_id: string, chair_id: string, score: number }[];
  for (const ride of unmatched_rides) {
    for (const chair of available_chairs) {
      const chair_location = chair_locations_map.get(chair.id);
      if (!chair_location) {
        throw new Error(`chair_location is undefined: chair_id: ${chair.id}`);
      }
      const chair_speed = chair_models_map.get(chair.model);
      if (!chair_speed) {
        throw new Error(`chair_speed is undefined: chair_id: ${chair.id}, model: ${chair.model}`);
      }

      // 乗車位置までの移動時間
      const pickup_distance = calculateDistance(ride.pickup_latitude, ride.pickup_longitude, chair_location.latitude, chair_location.longitude);
      const pickup_time = pickup_distance / chair_speed;

      // 目的地までの移動時間
      const destination_distance = calculateDistance(chair_location.latitude, chair_location.longitude, ride.destination_latitude, ride.destination_longitude);
      const destination_time = destination_distance / chair_speed;

      // 合計時間
      const total_time = pickup_time + destination_time;

      // 待ち時間を秒数単位に変換
      // const waiting_seconds = (new Date().getTime() - ride.created_at.getTime()) / 1000;

      // スコアの計算: 移動時間に0.7、待ち時間に0.3の重みを付ける
      // const score = (0.7 * total_time) + (0.3 * waiting_seconds);
      const score = total_time;
      scoring_rides.push({
        ride_id: ride.id,
        chair_id: chair.id,
        score,
      });
    }
  }

  /**
   * 配車決定ロジック
   * 
   * 配車済みの椅子を除外していきながら、その中でいちばんスコアが高い椅子をライドに割り当てていく
   */

  // ride_idでscoring_ridesをグルーピング
  const grouped_scoring_rides = new Map<string, { ride_id: string, chair_id: string, score: number }[]>(
    scoring_rides.map((scoring_ride) => [scoring_ride.ride_id, [scoring_ride]]),
  );

  // 配車済みの椅子を除外していきながら、その中でいちばんスコアが高い椅子をライドに割り当てていく
  const matched_chair_ids = [] as string[];
  const matched_rides = [] as { ride_id: string, chair_id: string }[];
  for (const [ride_id, scoring_rides] of grouped_scoring_rides) {
    // 配車済みの椅子を除外
    const available_scoring_rides = scoring_rides.filter((scoring_ride) => !matched_chair_ids.includes(scoring_ride.chair_id));
    const chair_id = available_scoring_rides.sort((a, b) => a.score - b.score)[0].chair_id;
    // 配車済みの椅子に割り当てる
    matched_chair_ids.push(chair_id);
    // 配車済みの椅子に割り当てたライドを記録
    matched_rides.push({ ride_id, chair_id });
  }

  // rideを更新するbulk update文を作成
  const update_rides_query = matched_rides.map((matched_ride) => {
    return `UPDATE rides SET chair_id = '${matched_ride.chair_id}' WHERE id = '${matched_ride.ride_id}'`;
  }).join(";");

  await ctx.var.dbConn.query(update_rides_query);

  return ctx.body(null, 204);
};

async function getChairsLatestLocationsByChairIds(dbConn: Connection, chair_ids: string[]) {
  const [locations] = await dbConn.query<Array<ChairLocation & RowDataPacket>>(
    `SELECT cl.chair_id,
      cl.latitude,
      cl.longitude
    FROM chair_locations cl
    INNER JOIN (
      SELECT chair_id,
        MAX(created_at) as max_created_at
      FROM chair_locations
      WHERE chair_id IN (?)
      GROUP BY chair_id
    ) latest ON cl.chair_id = latest.chair_id 
      AND cl.created_at = latest.max_created_at;`,
    [chair_ids],
  );
  return locations;
};

function calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number) {
  // マンハッタン距離を計算
  return Math.abs(lat1 - lat2) + Math.abs(lng1 - lng2);
}
