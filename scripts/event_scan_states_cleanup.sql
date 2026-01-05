BEGIN;

WITH keep AS (
  SELECT id
  FROM event_scan_states
  ORDER BY last_scanned_block DESC, id DESC
  LIMIT 1
)
DELETE FROM event_scan_states
WHERE id NOT IN (SELECT id FROM keep);

UPDATE event_scan_states
SET id = 1
WHERE id != 1;

COMMIT;
