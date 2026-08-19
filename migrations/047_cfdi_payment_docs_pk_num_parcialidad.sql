-- COB-4 (PULSO_auditoria_y_correcciones_v2.md): the PK on cfdi_payment_docs
-- is (payment_uuid, pago_num, invoice_uuid) — missing num_parcialidad. CFDI
-- 4.0 allows a single Pago node to carry two DoctoRelacionado for the same
-- invoice (different partialities); when that happens the second row
-- silently overwrites the first through the existing PK/ON CONFLICT DO
-- NOTHING, losing a partial payment. 47 same-currency (payment_uuid,
-- pago_num) pairs are already measurably short by 408,592.36 pesos.
--
-- num_parcialidad has 3 NULLs platform-wide, so it can't join a PRIMARY KEY
-- (which forces NOT NULL) — same shape as DAT-1/NOM-5: surrogate BIGSERIAL
-- becomes the real PK, natural key moves to a UNIQUE INDEX, which tolerates
-- those NULLs by treating each as distinct.
ALTER TABLE pulso.cfdi_payment_docs DROP CONSTRAINT cfdi_payment_docs_pkey;
ALTER TABLE pulso.cfdi_payment_docs ADD COLUMN id BIGSERIAL PRIMARY KEY;
CREATE UNIQUE INDEX cfdi_payment_docs_uq
  ON pulso.cfdi_payment_docs (payment_uuid, pago_num, invoice_uuid, num_parcialidad);
