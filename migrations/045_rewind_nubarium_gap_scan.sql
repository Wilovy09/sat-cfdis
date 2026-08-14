-- H-3 of the 2026-08-14 Nubarium/Axented re-audit: 2024-08-15 and 2024-08-22
-- have real emitidas activity (9 and 23 rows) but zero recibidas, which the
-- old combined-side check in find_activity_gap_days (rfc_emisor = $1 OR
-- rfc_receptor = $1) never flagged as a gap — either side having data was
-- enough to call the day covered. That function was rewritten to check each
-- side independently (services/gap_detector.rs, scan_activity_gaps).
--
-- gap_scan_progress.last_scanned_date only moves forward, so Nubarium's
-- cursor (2026-06-27) has already passed August 2024 and the new logic will
-- never see those two days on its own. Rewind the cursor so the next cycle
-- re-walks that window under the fixed one-sided check. Idempotent either
-- way: a day already fully covered just costs a re-scan, no unnecessary
-- resync gets queued unless the DB itself still shows a real gap.
UPDATE pulso.gap_scan_progress
SET last_scanned_date = '2024-08-01', updated_at = now()
WHERE rfc = 'NUB170623KI3';
