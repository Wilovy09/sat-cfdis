-- PULSO_Lote19_Cierre_L18.md, L19-07: nombres de emisor/receptor ya guardados con la
-- codificacion del XML (&quot;, &amp;, &#209;, ...). El lector de XML ya decodifica lo nuevo
-- (L18-13, services::xml_parser::decode_attr: una sola pasada de unescape); esto aplica esa
-- misma decodificacion a lo ya guardado. Las descripciones de concepto no se tocan aqui:
-- las reescribe desde el XML src/bin/reconstruct_concepts.rs (L19-06).
--
-- Las reglas de Normalizacion que agrupan/excluyen por nombre guardan una llave derivada
-- del nombre (source_name_key, ver services::analytics::summary::normalize_name_key). La
-- llave de un nombre codificado conserva el "&QUOT" del entity, asi que cambiar el nombre
-- sin cambiar la llave haria que esas reglas dejaran de encontrar sus facturas. Cada llave
-- vieja se traduce a la llave del nombre decodificado, usando los nombres reales de la
-- base como puente.

-- Una sola pasada, igual que quick_xml::escape::unescape: primero las referencias
-- numericas, luego las nombradas con &amp; al final, para que "&amp;quot;" quede como el
-- texto literal "&quot;" y no se decodifique dos veces. Una referencia numerica invalida se
-- deja tal cual, como decode_attr deja el texto crudo.
CREATE FUNCTION pg_temp.decode_xml_entities(s TEXT) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    ref TEXT;
    code BIGINT;
BEGIN
    IF s IS NULL OR position('&' IN s) = 0 THEN
        RETURN s;
    END IF;
    FOR ref IN SELECT DISTINCT m[1] FROM regexp_matches(s, '&#([xX][0-9A-Fa-f]{1,6}|[0-9]{1,7});', 'g') AS m LOOP
        code := CASE
            WHEN ref ILIKE 'x%' THEN ('x' || lpad(substr(ref, 2), 8, '0'))::bit(32)::bigint
            ELSE ref::bigint
        END;
        IF code BETWEEN 1 AND 1114111 AND code NOT BETWEEN 55296 AND 57343 THEN
            s := replace(s, '&#' || ref || ';', chr(code::int));
        END IF;
    END LOOP;
    s := replace(s, '&quot;', '"');
    s := replace(s, '&apos;', '''');
    s := replace(s, '&lt;', '<');
    s := replace(s, '&gt;', '>');
    s := replace(s, '&amp;', '&');
    RETURN s;
END;
$$;

-- Misma forma que la llave de la vista cfdi_exclusion (migracion 087).
CREATE FUNCTION pg_temp.name_key(s TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT regexp_replace(regexp_replace(TRIM(BOTH FROM upper(COALESCE(s, ''))), '\s+', ' ', 'g'), '[^A-Z0-9 &\-]', '', 'g')
$$;

CREATE TEMP TABLE encoded_names AS
SELECT DISTINCT n AS old_name, pg_temp.decode_xml_entities(n) AS new_name
FROM (
    SELECT nombre_emisor AS n FROM pulso.cfdis_raw
    UNION
    SELECT nombre_receptor FROM pulso.cfdis_raw
) names
WHERE n ~ '&(#[0-9]+|#[xX][0-9A-Fa-f]+|quot|amp|apos|lt|gt);';

-- Reglas: llave vieja -> llave nueva, y el nombre que se muestra.
UPDATE pulso.normalization_rules nr
SET source_name_key = k.new_key
FROM (
    SELECT DISTINCT ON (pg_temp.name_key(old_name))
        pg_temp.name_key(old_name) AS old_key,
        pg_temp.name_key(new_name) AS new_key
    FROM encoded_names
    ORDER BY pg_temp.name_key(old_name), new_name
) k
WHERE nr.source_name_key = k.old_key AND k.old_key <> k.new_key;

UPDATE pulso.normalization_rules
SET source_name = pg_temp.decode_xml_entities(source_name)
WHERE source_name ~ '&(#[0-9]+|#[xX][0-9A-Fa-f]+|quot|amp|apos|lt|gt);';

-- Los nombres mismos.
UPDATE pulso.cfdis_raw c
SET nombre_emisor = e.new_name
FROM encoded_names e
WHERE c.nombre_emisor = e.old_name;

UPDATE pulso.cfdis_raw c
SET nombre_receptor = e.new_name
FROM encoded_names e
WHERE c.nombre_receptor = e.old_name;

-- nomina_normalizada copia los nombres; el cache de respuestas los sirve.
REFRESH MATERIALIZED VIEW pulso.nomina_normalizada;

UPDATE pulso.rfc_data_version SET version = version + 1, updated_at = now();

DROP TABLE encoded_names;
DROP FUNCTION pg_temp.name_key(TEXT);
DROP FUNCTION pg_temp.decode_xml_entities(TEXT);
