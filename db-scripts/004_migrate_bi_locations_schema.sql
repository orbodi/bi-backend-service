-- Migration de bi.locations vers le modèle enrichi:
-- - retire center_code_short
-- - renomme location_code -> locality_code
-- - ajoute prefecture_code, commune_code, canton_code

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'bi' AND table_name = 'locations'
  ) THEN
    -- Renommer location_code en locality_code si nécessaire
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'bi' AND table_name = 'locations' AND column_name = 'location_code'
    ) AND NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'bi' AND table_name = 'locations' AND column_name = 'locality_code'
    ) THEN
      ALTER TABLE bi.locations RENAME COLUMN location_code TO locality_code;
    END IF;

    -- Ajouter les codes hiérarchiques manquants
    ALTER TABLE bi.locations
      ADD COLUMN IF NOT EXISTS prefecture_code VARCHAR(64),
      ADD COLUMN IF NOT EXISTS commune_code VARCHAR(64),
      ADD COLUMN IF NOT EXISTS canton_code VARCHAR(64);

    -- Supprimer center_code_short si présent
    ALTER TABLE bi.locations
      DROP COLUMN IF EXISTS center_code_short;

    -- Recréer index sur locality_code
    DROP INDEX IF EXISTS bi.ix_locations_location_code;
    CREATE INDEX IF NOT EXISTS ix_locations_location_code
      ON bi.locations (locality_code);
  END IF;
END $$;

