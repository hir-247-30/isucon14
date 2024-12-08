import type { Context } from "hono";
import type { Environment } from "./types/hono.js";
import type { RowDataPacket } from "mysql2";
import type { Chair, ChairModel, Ride, ChairLocation } from "./types/models.js";

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
export const internalGetMatching = async (ctx: Context<Environment>) => {
  // マッチしていないrideを取得
  const non_matched_ride_ids = await ctx.var.dbConn.query<
    Array<{ id: string } & RowDataPacket>
  >("SELECT id FROM rides WHERE chair_id IS NULL");
  if (!non_matched_ride_ids.length) {
    return ctx.body(null, 204);
  }

  // 空いている椅子を取得
  const empty_chair_ids = await ctx.var.dbConn.query<
    Array<{ id: string } & RowDataPacket>
  >("SELECT id FROM chairs WHERE is_active = TRUE");
  if (!empty_chair_ids.length) {
    return ctx.body(null, 204);
  }

  // 椅子のモデル(移動速度)を取得 nameをキーにしてmapに変換
  const chair_models = new Map<string, number>(
    await ctx.var.dbConn.query<Array<ChairModel & RowDataPacket>>(
      "SELECT name, speed FROM chair_models WHERE name IN (?)",
      [empty_chair_ids.map((chair: { id: string }) => chair.id)],
    ).map((chair_model: ChairModel & RowDataPacket) => [
      chair_model.name,
      chair_model.speed,
    ]),
  );

  // 椅子とライドの座標を取得
  const ride_locations = await ctx.var.dbConn.query<Array<Ride & RowDataPacket>>(
    "SELECT id, latitude, longitude, created_at FROM rides WHERE id IN (?)",
    [non_matched_ride_ids.map((ride: { id: string }) => ride.id)],
  );

  // 空いている椅子達の最後の座標を取得
  const chair_locations = await ctx.var.dbConn.query<Array<ChairLocation & RowDataPacket>>(
    `WITH latest_chair_location_ids AS (
      SELECT
        chair_id,
        MAX(created_at) AS created_at 
      FROM chair_locations 
      WHERE chair_id IN (?) 
      GROUP BY chair_id
    )
    SELECT 
      id, 
      chair_id, 
      latitude, 
      longitude,
      created_at
    FROM chair_locations 
    INNER JOIN latest_chair_location_ids
      ON chair_locations.chair_id = latest_chair_location_ids.chair_id
      AND chair_locations.created_at = latest_chair_location_ids.created_at`,
    [empty_chair_ids.map((chair: { id: string }) => chair.id)],
  );

  // chair_idをキーにして、ride_idとscoreを格納する
  const match_scores: Record<string, Array<{ ride_id: string; score: number }>> = {};

  for (const ride_location of ride_locations) {
    for (const chair_location of chair_locations) {
      // 距離を計算
      const distance = calculateDistance(
        ride_location.latitude,
        ride_location.longitude,
        chair_location.latitude,
        chair_location.longitude,
      );

      // 移動距離に対して、椅子の移動速度を加味して移動時間を計算
      const chair_speed = chair_models.get(chair_location.model);
      if (!chair_speed) {
        throw new Error(`Chair model not found: ${chair_location.model}`);
      }
      const time = calculateTime(distance, chair_speed);

      // 待ち時間が長いライドはマッチスコアを高くして、先にマッチさせる
      const wait_time = new Date().getTime() - ride_location.created_at.getTime();

      // scoreの計算, 移動時間が短く、待ち時間が長いほどマッチスコアが高い
      const score = time + wait_time;

      match_scores[chair_location.chair_id] = (match_scores[chair_location.chair_id] || []).concat({
        ride_id: ride_location.id,
        score,
      });
    }
  }

  console.log(match_scores);

  // 椅子ごとに最もスコアが高いride_idを割り当てる
  const update_rides_queries = Object.entries(match_scores).map(([chair_id, scores]) => {
      const best_score = scores.sort((a, b) => b.score - a.score)[0];
      return `UPDATE rides SET chair_id = ${chair_id} WHERE id = ${best_score.ride_id}`;
    })
    .join(";");

  await ctx.var.dbConn.query(update_rides_queries);

  return ctx.body(null, 204);
};

function calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number) {
  // マンハッタン距離を計算
  return Math.abs(lat1 - lat2) + Math.abs(lng1 - lng2);
}

function calculateTime(distance: number, speed: number) {
  return distance / speed;
}
