-- PULSO_Lote19_Cierre_L18.md, L19-01 puntos 4 y 5.
--
-- Punto 4 -- se retira el filtro de la vista pulso.cfdis (migracion 087). Una vista no sabe
-- quien consulta, asi que no puede proteger nada: mostraba una factura si CUALQUIERA de sus
-- dos lados tenia dueno verificado, y como los clientes reales lo estan, sus facturas
-- seguian visibles para quien registrara la contraparte. La proteccion queda toda en el
-- codigo, por RFC y por dueno:
--   - db::users::user_can_read_rfc_data (detras de routes::analytics::check_rfc_access, que
--     cubre las 41 rutas de analisis): niega el acceso mientras el dueno activo del RFC no
--     este verificado.
--   - Las tres rutas que leen datos sin esa revision: estado de sincronizacion
--     (routes::users::grid_months_with_data), perfil y listado de RFCs
--     (db::users::get_user_rfcs_with_role) -- un RFC no verificado se ve como uno nuevo.
--   - verified_at lo asigna main.rs (verify_owner_after_sat_login) cuando un job pasa el
--     login del SAT con la CIEC vigente del dueno; nunca con una e.firma (L19-03).
--
-- CREATE OR REPLACE con las mismas columnas en el mismo orden: las vistas que dependen de
-- pulso.cfdis (cfdi_exclusion, cfdis_ajustado, cfdi_cobro_estado, nomina_normalizada)
-- siguen atadas a ella por OID y no se recrean.
CREATE OR REPLACE VIEW pulso.cfdis AS
SELECT c.* FROM pulso.cfdis_raw c;

-- Punto 5 -- los RFC genericos del SAT (publico en general, extranjero generico) no son
-- empresas y no deben ser dueno de nada. routes::users::register_rfc ya los rechaza; esto
-- da de baja los que existan y revoca sus accesos compartidos. Su e.firma, si tuviera, la
-- borra el barrido de e.firmas huerfanas (services::fiel::orphan_sweep_worker) al arrancar.
UPDATE pulso.users
SET deleted_at = NOW(), verified_at = NULL
WHERE rfc IN ('XAXX010101000', 'XEXX010101000') AND deleted_at IS NULL;

UPDATE pulso.rfc_shares
SET revoked_at = NOW()
WHERE rfc IN ('XAXX010101000', 'XEXX010101000') AND revoked_at IS NULL;

-- Las facturas que el filtro ocultaba vuelven a la base de la vista materializada de
-- nomina, y el cache de respuestas se invalida para todos los RFC.
REFRESH MATERIALIZED VIEW pulso.nomina_normalizada;

UPDATE pulso.rfc_data_version SET version = version + 1, updated_at = now();
