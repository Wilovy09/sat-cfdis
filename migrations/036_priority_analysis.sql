-- Onboarding preference: which analysis matters most to the user.
-- 'ingresos_clientes' | 'egresos_proveedores' | 'nomina' — drives which
-- dl_type (emitidos/recibidos) gets fully downloaded first during the
-- initial sync. NULL = not answered, defaults to emitidos-first.
ALTER TABLE pulso.users ADD COLUMN IF NOT EXISTS priority_analysis TEXT;
