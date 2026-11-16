import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Shells are derived from this thread payload, not persisted separately.
  yield* sql`
    UPDATE orchestration_v2_projection_threads
    SET payload_json = json_remove(
      CASE WHEN json_type(payload_json, '$.followUp') IS NULL
        THEN json_set(payload_json, '$.followUp',
          CASE WHEN json_type(payload_json, '$.waitingOnOthers') = 'true'
            THEN 'waiting-on-others' ELSE NULL END)
        ELSE payload_json END,
      '$.waitingOnOthers'
    )
    WHERE json_type(payload_json, '$.waitingOnOthers') IS NOT NULL
  `;

  // Normalize thread snapshots in the journal too, so replay cannot resurrect
  // the old boolean.
  yield* sql`
    UPDATE orchestration_events
    SET payload_json = json_remove(
      CASE WHEN json_type(payload_json, '$.followUp') IS NULL
        THEN json_set(payload_json, '$.followUp',
          CASE WHEN json_type(payload_json, '$.waitingOnOthers') = 'true'
            THEN 'waiting-on-others' ELSE NULL END)
        ELSE payload_json END,
      '$.waitingOnOthers'
    )
    WHERE application_event_version = 2 AND aggregate_kind = 'thread'
      AND event_type LIKE 'thread.%'
      AND json_type(payload_json, '$.waitingOnOthers') IS NOT NULL
  `;
});
