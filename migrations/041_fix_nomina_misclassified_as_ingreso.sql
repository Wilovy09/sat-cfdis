-- Backfill for the accent bug in xml_parser.rs's from_metadata(): SAT sends
-- `efectoComprobante: "Nómina"` (with the accent), but `.to_uppercase()`
-- produces "NÓMINA", which never matched the unaccented "NOMINA" literal in
-- the match arm — every payroll CFDI parsed from metadata only (no XML)
-- silently fell through to the `_ => "I"` default, counting salaries as
-- sales revenue. Confirmed against Axented (ADC101206334): 1,893 rows,
-- ~$1.8-3.6M MXN/year misclassified into Ingresos.
--
-- The correct classification (`efectoComprobante = 'Nómina'`) is still
-- sitting in pulso.job_invoices.metadata, which is never deleted after
-- ingestion — so this is a precise reclassification from data already on
-- hand, not a guess. It does NOT populate pulso.cfdi_nomina (percepciones/
-- deducciones detail): that table is only ever built from real XML, which
-- these rows never had. Recovering the payroll detail itself requires
-- re-downloading the XML for these UUIDs — this migration only stops them
-- from inflating Ingresos.
UPDATE pulso.cfdis c
SET tipo_comprobante = 'N'
WHERE c.xml_available = -1
  AND c.tipo_comprobante = 'I'
  AND EXISTS (
      SELECT 1 FROM pulso.job_invoices ji
      WHERE ji.uuid = c.uuid
        AND (ji.metadata::jsonb ->> 'efectoComprobante') = 'Nómina'
  );
