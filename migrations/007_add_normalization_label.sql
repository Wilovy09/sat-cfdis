ALTER TABLE pulso.normalization_rules ADD COLUMN IF NOT EXISTS label TEXT;
ALTER TABLE pulso.normalization_rules ADD COLUMN IF NOT EXISTS rule_name TEXT;
ALTER TABLE pulso.payroll_normalization_rules ADD COLUMN IF NOT EXISTS label TEXT;
ALTER TABLE pulso.payroll_normalization_rules ADD COLUMN IF NOT EXISTS rule_name TEXT;
