-- =========================
-- IT Asset Management Schema (PostgreSQL)
-- =========================

CREATE SCHEMA IF NOT EXISTS itam;
SET search_path TO itam, public;

-- Extensions
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- =========================
-- Stammdaten / Orga
-- =========================

CREATE TABLE IF NOT EXISTS org_units (
  org_unit_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name               text NOT NULL,
  cost_center_code   text,
  parent_org_unit_id bigint REFERENCES org_units(org_unit_id) ON UPDATE CASCADE ON DELETE SET NULL,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_org_units_parent ON org_units(parent_org_unit_id);

CREATE TABLE IF NOT EXISTS people (
  person_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  display_name  text NOT NULL,
  email         citext NOT NULL UNIQUE,
  org_unit_id   bigint REFERENCES org_units(org_unit_id) ON UPDATE CASCADE ON DELETE SET NULL,
  cost_center   text,
  active        boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_people_org_unit ON people(org_unit_id);

CREATE TABLE IF NOT EXISTS locations (
  location_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name         text NOT NULL,
  address      text,
  room         text,
  rack         text,
  rack_unit    text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- Asset-Katalog / Status
-- =========================

CREATE TABLE IF NOT EXISTS asset_types (
  type_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       text NOT NULL UNIQUE,
  category   text NOT NULL CHECK (category IN ('hardware','software','cloud','other')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS asset_status (
  status_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       text NOT NULL UNIQUE,
  is_active  boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- Vendors / Lieferanten
-- =========================

CREATE TABLE IF NOT EXISTS vendors (
  vendor_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name           text NOT NULL UNIQUE,
  support_email  citext,
  phone          text,
  account_number text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- Assets
-- =========================

CREATE TABLE IF NOT EXISTS assets (
  asset_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_tag        text NOT NULL UNIQUE,
  type_id          bigint NOT NULL REFERENCES asset_types(type_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  status_id        bigint NOT NULL REFERENCES asset_status(status_id) ON UPDATE CASCADE ON DELETE RESTRICT,

  manufacturer     text,
  model            text,
  serial_number    text UNIQUE,
  description      text,

  purchase_date    date,
  purchase_price   numeric(12,2),
  currency         char(3),
  warranty_end     date,

  owner_org_unit_id bigint REFERENCES org_units(org_unit_id) ON UPDATE CASCADE ON DELETE SET NULL,

  notes            text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT chk_currency_upper
    CHECK (currency IS NULL OR currency = upper(currency))
);

CREATE INDEX IF NOT EXISTS idx_assets_type   ON assets(type_id);
CREATE INDEX IF NOT EXISTS idx_assets_status ON assets(status_id);
CREATE INDEX IF NOT EXISTS idx_assets_owner  ON assets(owner_org_unit_id);

-- =========================
-- Zuweisungen
-- =========================

CREATE TABLE IF NOT EXISTS asset_assignments (
  assignment_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_id       bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  person_id      bigint REFERENCES people(person_id) ON UPDATE CASCADE ON DELETE SET NULL,
  location_id    bigint REFERENCES locations(location_id) ON UPDATE CASCADE ON DELETE SET NULL,
  assigned_from  timestamptz NOT NULL DEFAULT now(),
  assigned_to    timestamptz,
  purpose        text,
  notes          text,
  CONSTRAINT chk_assignment_target_present
    CHECK (person_id IS NOT NULL OR location_id IS NOT NULL),
  CONSTRAINT chk_assignment_range
    CHECK (assigned_to IS NULL OR assigned_to > assigned_from),
  CONSTRAINT ex_asset_assignments_no_overlap
    EXCLUDE USING gist (
      asset_id WITH =,
      tstzrange(assigned_from, COALESCE(assigned_to, 'infinity')) WITH &&
    )
);

CREATE INDEX IF NOT EXISTS idx_asset_assignments_asset ON asset_assignments(asset_id);
CREATE INDEX IF NOT EXISTS idx_asset_assignments_person ON asset_assignments(person_id);
CREATE INDEX IF NOT EXISTS idx_asset_assignments_location ON asset_assignments(location_id);

CREATE INDEX IF NOT EXISTS idx_asset_assignments_current
  ON asset_assignments(asset_id)
  WHERE assigned_to IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_one_open_assignment
  ON asset_assignments(asset_id)
  WHERE assigned_to IS NULL;

-- =========================
-- Asset-Relationen
-- =========================

CREATE TABLE IF NOT EXISTS asset_relations (
  relation_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  parent_asset_id  bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  child_asset_id   bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  relation_type    text NOT NULL CHECK (relation_type IN ('attached_to','installed_in','part_of','depends_on','other')),
  created_at       timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT chk_no_self_relation CHECK (parent_asset_id <> child_asset_id),
  CONSTRAINT uq_asset_relation UNIQUE (parent_asset_id, child_asset_id, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_asset_relations_parent ON asset_relations(parent_asset_id);
CREATE INDEX IF NOT EXISTS idx_asset_relations_child  ON asset_relations(child_asset_id);

-- =========================
-- Einkauf
-- =========================

CREATE TABLE IF NOT EXISTS purchases (
  purchase_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  vendor_id     bigint REFERENCES vendors(vendor_id) ON UPDATE CASCADE ON DELETE SET NULL,
  order_no      text,
  invoice_no    text,
  ordered_at    date,
  received_at   date,
  notes         text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_purchases_order UNIQUE (vendor_id, order_no),
  CONSTRAINT uq_purchases_invoice UNIQUE (vendor_id, invoice_no)
);

CREATE INDEX IF NOT EXISTS idx_purchases_vendor ON purchases(vendor_id);
CREATE INDEX IF NOT EXISTS idx_purchases_order  ON purchases(order_no);
CREATE INDEX IF NOT EXISTS idx_purchases_invoice ON purchases(invoice_no);

CREATE TABLE IF NOT EXISTS purchase_items (
  purchase_item_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  purchase_id      bigint NOT NULL REFERENCES purchases(purchase_id) ON UPDATE CASCADE ON DELETE CASCADE,
  asset_id         bigint REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE SET NULL,
  description      text,
  quantity         integer NOT NULL DEFAULT 1 CHECK (quantity > 0),
  unit_price       numeric(12,2),
  currency         char(3),
  created_at       timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT chk_pi_currency_upper
    CHECK (currency IS NULL OR currency = upper(currency))
);

CREATE INDEX IF NOT EXISTS idx_purchase_items_purchase ON purchase_items(purchase_id);
CREATE INDEX IF NOT EXISTS idx_purchase_items_asset    ON purchase_items(asset_id);

-- =========================
-- Verträge
-- =========================

CREATE TABLE IF NOT EXISTS contracts (
  contract_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  vendor_id            bigint REFERENCES vendors(vendor_id) ON UPDATE CASCADE ON DELETE SET NULL,
  type                 text NOT NULL CHECK (type IN ('maintenance','lease','subscription','support','other')),
  name                 text,
  start_date           date,
  end_date             date,
  renewal_date         date,
  notice_period_days   integer CHECK (notice_period_days IS NULL OR notice_period_days >= 0),
  total_value          numeric(14,2),
  currency             char(3),
  document_ref         text,
  notes                text,
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT chk_contract_dates
    CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date),
  CONSTRAINT chk_contract_currency_upper
    CHECK (currency IS NULL OR currency = upper(currency))
);

CREATE INDEX IF NOT EXISTS idx_contracts_vendor ON contracts(vendor_id);
CREATE INDEX IF NOT EXISTS idx_contracts_type   ON contracts(type);

CREATE TABLE IF NOT EXISTS asset_contracts (
  asset_id    bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  contract_id bigint NOT NULL REFERENCES contracts(contract_id) ON UPDATE CASCADE ON DELETE CASCADE,
  PRIMARY KEY (asset_id, contract_id)
);

CREATE INDEX IF NOT EXISTS idx_asset_contracts_contract ON asset_contracts(contract_id);

-- =========================
-- Wartung
-- =========================

CREATE TABLE IF NOT EXISTS maintenance_tickets (
  ticket_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_id           bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  type               text NOT NULL CHECK (type IN ('repair','incident','preventive','upgrade','other')),
  status             text NOT NULL CHECK (status IN ('open','in_progress','waiting','closed','cancelled')),
  opened_at          timestamptz NOT NULL DEFAULT now(),
  closed_at          timestamptz,
  provider_vendor_id bigint REFERENCES vendors(vendor_id) ON UPDATE CASCADE ON DELETE SET NULL,
  external_ref       text,
  cost               numeric(12,2),
  currency           char(3),
  notes              text,

  CONSTRAINT chk_ticket_dates
    CHECK (closed_at IS NULL OR closed_at > opened_at),
  CONSTRAINT chk_ticket_currency_upper
    CHECK (currency IS NULL OR currency = upper(currency))
);

CREATE INDEX IF NOT EXISTS idx_tickets_asset   ON maintenance_tickets(asset_id);
CREATE INDEX IF NOT EXISTS idx_tickets_vendor  ON maintenance_tickets(provider_vendor_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status  ON maintenance_tickets(status);

-- =========================
-- Software / Lizenzen
-- =========================

CREATE TABLE IF NOT EXISTS software_products (
  product_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name        text NOT NULL,
  publisher   text,
  metric      text NOT NULL CHECK (metric IN ('per_user','per_device','concurrent','core_based','site','other')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (name, publisher)
);

CREATE INDEX IF NOT EXISTS idx_software_products_publisher ON software_products(publisher);

CREATE TABLE IF NOT EXISTS licenses (
  license_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_id         bigint NOT NULL REFERENCES software_products(product_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  vendor_id          bigint REFERENCES vendors(vendor_id) ON UPDATE CASCADE ON DELETE SET NULL,
  contract_id        bigint REFERENCES contracts(contract_id) ON UPDATE CASCADE ON DELETE SET NULL,
  total_entitlements integer NOT NULL CHECK (total_entitlements >= 0),
  valid_from         date,
  valid_to           date,
  license_key        text,
  subscription_id    text,
  cost               numeric(14,2),
  currency           char(3),
  notes              text,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT chk_license_dates
    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
  CONSTRAINT chk_license_currency_upper
    CHECK (currency IS NULL OR currency = upper(currency))
);

CREATE INDEX IF NOT EXISTS idx_licenses_product  ON licenses(product_id);
CREATE INDEX IF NOT EXISTS idx_licenses_vendor   ON licenses(vendor_id);
CREATE INDEX IF NOT EXISTS idx_licenses_contract ON licenses(contract_id);

CREATE TABLE IF NOT EXISTS license_assignments (
  license_assignment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  license_id            bigint NOT NULL REFERENCES licenses(license_id) ON UPDATE CASCADE ON DELETE CASCADE,
  person_id             bigint REFERENCES people(person_id) ON UPDATE CASCADE ON DELETE SET NULL,
  asset_id              bigint REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE SET NULL,
  assigned_from         timestamptz NOT NULL DEFAULT now(),
  assigned_to           timestamptz,
  notes                 text,
  CONSTRAINT chk_la_range CHECK (assigned_to IS NULL OR assigned_to > assigned_from),
  CONSTRAINT chk_la_target_xor CHECK (
    (person_id IS NOT NULL AND asset_id IS NULL) OR
    (person_id IS NULL AND asset_id IS NOT NULL)
  ),
  CONSTRAINT ex_license_person_no_overlap
    EXCLUDE USING gist (
      license_id WITH =,
      person_id WITH =,
      tstzrange(assigned_from, COALESCE(assigned_to, 'infinity')) WITH &&
    )
    WHERE (person_id IS NOT NULL),
  CONSTRAINT ex_license_asset_no_overlap
    EXCLUDE USING gist (
      license_id WITH =,
      asset_id WITH =,
      tstzrange(assigned_from, COALESCE(assigned_to, 'infinity')) WITH &&
    )
    WHERE (asset_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_license_assignments_license ON license_assignments(license_id);
CREATE INDEX IF NOT EXISTS idx_license_assignments_person  ON license_assignments(person_id);
CREATE INDEX IF NOT EXISTS idx_license_assignments_asset   ON license_assignments(asset_id);

CREATE INDEX IF NOT EXISTS idx_license_assignments_current
  ON license_assignments(license_id)
  WHERE assigned_to IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_license_person_open
  ON license_assignments(license_id, person_id)
  WHERE assigned_to IS NULL AND person_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_license_asset_open
  ON license_assignments(license_id, asset_id)
  WHERE assigned_to IS NULL AND asset_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS installations (
  installation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_id        bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  product_id      bigint NOT NULL REFERENCES software_products(product_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  version         text,
  discovered_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at    timestamptz,
  source          text,
  raw_payload     jsonb,

  CONSTRAINT chk_install_last_seen CHECK (last_seen_at IS NULL OR last_seen_at >= discovered_at)
);

CREATE INDEX IF NOT EXISTS idx_installations_asset   ON installations(asset_id);
CREATE INDEX IF NOT EXISTS idx_installations_product ON installations(product_id);
CREATE INDEX IF NOT EXISTS idx_installations_last_seen ON installations(last_seen_at);

-- =========================
-- Cloud
-- =========================

CREATE TABLE IF NOT EXISTS cloud_accounts (
  cloud_account_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  provider         text NOT NULL CHECK (provider IN ('aws','azure','gcp','other')),
  account_name     text NOT NULL,
  account_external_id text,
  owner_person_id  bigint REFERENCES people(person_id) ON UPDATE CASCADE ON DELETE SET NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, account_external_id)
);

CREATE INDEX IF NOT EXISTS idx_cloud_accounts_owner ON cloud_accounts(owner_person_id);

CREATE TABLE IF NOT EXISTS cloud_resources (
  resource_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cloud_account_id   bigint NOT NULL REFERENCES cloud_accounts(cloud_account_id) ON UPDATE CASCADE ON DELETE CASCADE,
  resource_type      text NOT NULL,
  resource_identifier text NOT NULL,
  name               text,
  region             text,
  cost_center        text,
  discovered_at      timestamptz NOT NULL DEFAULT now(),
  last_seen_at       timestamptz,
  tags               jsonb,
  raw_payload        jsonb,

  CONSTRAINT uq_cloud_resource UNIQUE (cloud_account_id, resource_identifier),
  CONSTRAINT chk_cloud_last_seen CHECK (last_seen_at IS NULL OR last_seen_at >= discovered_at)
);

CREATE INDEX IF NOT EXISTS idx_cloud_resources_type ON cloud_resources(resource_type);
CREATE INDEX IF NOT EXISTS idx_cloud_resources_last_seen ON cloud_resources(last_seen_at);

-- =========================
-- Audit
-- =========================

CREATE TABLE IF NOT EXISTS asset_events (
  event_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_id         bigint NOT NULL REFERENCES assets(asset_id) ON UPDATE CASCADE ON DELETE CASCADE,
  event_type       text NOT NULL,
  occurred_at      timestamptz NOT NULL DEFAULT now(),
  actor_person_id  bigint REFERENCES people(person_id) ON UPDATE CASCADE ON DELETE SET NULL,
  payload          jsonb,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_asset_events_asset_time ON asset_events(asset_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_asset_events_type ON asset_events(event_type);
CREATE INDEX IF NOT EXISTS idx_asset_events_actor ON asset_events(actor_person_id);

-- =========================
-- Cycle prevention for org_units
-- =========================

CREATE OR REPLACE FUNCTION itam.fn_org_units_no_cycle()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.parent_org_unit_id IS NULL THEN
    RETURN NEW;
  END IF;

  IF NEW.parent_org_unit_id = NEW.org_unit_id THEN
    RAISE EXCEPTION 'Org unit cannot reference itself as parent';
  END IF;

  IF EXISTS (
    WITH RECURSIVE ancestors AS (
      SELECT parent_org_unit_id
      FROM itam.org_units
      WHERE org_unit_id = NEW.parent_org_unit_id
      UNION ALL
      SELECT ou.parent_org_unit_id
      FROM itam.org_units ou
      JOIN ancestors a ON ou.org_unit_id = a.parent_org_unit_id
    )
    SELECT 1 FROM ancestors WHERE parent_org_unit_id = NEW.org_unit_id
  ) THEN
    RAISE EXCEPTION 'Org unit cycle detected for %', NEW.org_unit_id;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_org_units_no_cycle ON itam.org_units;
CREATE TRIGGER trg_org_units_no_cycle
BEFORE INSERT OR UPDATE ON itam.org_units
FOR EACH ROW EXECUTE FUNCTION itam.fn_org_units_no_cycle();

-- =========================
-- Cycle prevention for asset_relations
-- =========================

CREATE OR REPLACE FUNCTION itam.fn_asset_relations_no_cycle()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.parent_asset_id = NEW.child_asset_id THEN
    RAISE EXCEPTION 'Asset relation cannot be self-referential';
  END IF;

  IF EXISTS (
    WITH RECURSIVE path AS (
      SELECT ar.parent_asset_id, ar.child_asset_id
      FROM itam.asset_relations ar
      WHERE ar.child_asset_id = NEW.parent_asset_id
      UNION ALL
      SELECT ar.parent_asset_id, ar.child_asset_id
      FROM itam.asset_relations ar
      JOIN path p ON ar.child_asset_id = p.parent_asset_id
    )
    SELECT 1 FROM path WHERE parent_asset_id = NEW.child_asset_id
  ) THEN
    RAISE EXCEPTION 'Asset relation cycle detected for parent %, child %', NEW.parent_asset_id, NEW.child_asset_id;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_asset_relations_no_cycle ON itam.asset_relations;
CREATE TRIGGER trg_asset_relations_no_cycle
BEFORE INSERT OR UPDATE ON itam.asset_relations
FOR EACH ROW EXECUTE FUNCTION itam.fn_asset_relations_no_cycle();

-- =========================
-- Trigger updated_at automatisch pflegen
-- =========================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT quote_ident(n.nspname) AS nsp, quote_ident(c.relname) AS tbl
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'updated_at'
    WHERE c.relkind = 'r' AND n.nspname = 'itam'
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_updated_at ON %s.%s;', r.tbl, r.nsp, r.tbl);
    EXECUTE format(
      'CREATE TRIGGER trg_%s_updated_at BEFORE UPDATE ON %s.%s
       FOR EACH ROW EXECUTE FUNCTION set_updated_at();',
      r.tbl, r.nsp, r.tbl
    );
  END LOOP;
END $$;
