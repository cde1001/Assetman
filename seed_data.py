import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote
from datetime import datetime

import pg8000.dbapi as pg


def load_database_url(env_path: Path) -> str:
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
    if not env_path.exists():
        raise FileNotFoundError(".env not found")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip()
    raise ValueError("DATABASE_URL not found in env or .env")


def parse_database_url(url: str):
    p = urlparse(url)
    if not all([p.scheme, p.hostname, p.username, p.path]):
        raise ValueError("Invalid DATABASE_URL format")
    return {
        "user": unquote(p.username),
        "password": unquote(p.password) if p.password else None,
        "host": p.hostname,
        "port": p.port or 5432,
        "database": p.path.lstrip("/"),
    }


def main():
    try:
        db_url = load_database_url(Path(".env"))
        conn_args = parse_database_url(db_url)
    except Exception as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        conn = pg.connect(**conn_args, ssl_context=True)
    except Exception as exc:
        print(f"Connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    cur = conn.cursor()
    conn.autocommit = False
    try:
        # Clean slate
        cur.execute(
            """
            TRUNCATE TABLE
              itam.asset_events,
              itam.cloud_resources,
              itam.cloud_accounts,
              itam.installations,
              itam.license_assignments,
              itam.licenses,
              itam.software_products,
              itam.maintenance_tickets,
              itam.asset_contracts,
              itam.contracts,
              itam.purchase_items,
              itam.purchases,
              itam.asset_relations,
              itam.asset_assignments,
              itam.assets,
              itam.asset_status,
              itam.asset_types,
              itam.vendors,
              itam.locations,
              itam.people,
              itam.org_units
            RESTART IDENTITY CASCADE;
            """
        )

        # Org units
        org_units = [
            ("Headquarters", None),
            ("IT", "Headquarters"),
            ("Finance", "Headquarters"),
            ("Sales", "Headquarters"),
            ("Engineering", "Headquarters"),
        ]
        org_ids = {}
        for name, parent in org_units:
            parent_id = org_ids.get(parent)
            cur.execute(
                "INSERT INTO itam.org_units (name, parent_org_unit_id) VALUES (%s, %s) RETURNING org_unit_id",
                (name, parent_id),
            )
            org_ids[name] = cur.fetchone()[0]

        # People
        people = [
            ("Alice Admin", "alice.admin@example.com", "IT", "CC1001"),
            ("Bob Builder", "bob.builder@example.com", "Engineering", "CC2001"),
            ("Carol Cash", "carol.cash@example.com", "Finance", "CC3001"),
            ("Dave Deals", "dave.deals@example.com", "Sales", "CC4001"),
            ("Erin Engineer", "erin.engineer@example.com", "IT", "CC1001"),
            ("Frank Forge", "frank.forge@example.com", "Engineering", "CC2001"),
            ("Grace Gains", "grace.gains@example.com", "Finance", "CC3001"),
            ("Heidi Help", "heidi.help@example.com", "IT", "CC1001"),
        ]
        person_ids = {}
        for name, email, org_name, cc in people:
            cur.execute(
                """
                INSERT INTO itam.people (display_name, email, org_unit_id, cost_center)
                VALUES (%s, %s, %s, %s) RETURNING person_id
                """,
                (name, email, org_ids[org_name], cc),
            )
            person_ids[email] = cur.fetchone()[0]

        # Locations
        locations = [
            ("HQ Office", "Main St 1, 12345 City", "3F", None, None),
            ("Berlin Office", "Alexanderplatz 5, 10178 Berlin", "5.12", None, None),
            ("Data Center A", "Industrial Park 12, 98765 Town", "Room DC-A", "Rack 12", "U22"),
            ("Remote Warehouse", "Logistics Way 7, 54321 City", "Zone B", None, None),
            ("London Office", "1 Fleet St, London", "2.03", None, None),
        ]
        location_ids = {}
        for name, address, room, rack, rack_unit in locations:
            cur.execute(
                """
                INSERT INTO itam.locations (name, address, room, rack, rack_unit)
                VALUES (%s, %s, %s, %s, %s) RETURNING location_id
                """,
                (name, address, room, rack, rack_unit),
            )
            location_ids[name] = cur.fetchone()[0]

        # Asset catalog/status
        asset_types = [
            ("Laptop", "hardware"),
            ("Phone", "hardware"),
            ("Server", "hardware"),
            ("Network", "hardware"),
            ("Software", "software"),
            ("CloudAccount", "cloud"),
            ("Storage", "hardware"),
        ]
        type_ids = {}
        for name, cat in asset_types:
            cur.execute(
                "INSERT INTO itam.asset_types (name, category) VALUES (%s, %s) RETURNING type_id",
                (name, cat),
            )
            type_ids[name] = cur.fetchone()[0]

        asset_statuses = [
            ("in_use", True),
            ("in_stock", True),
            ("repair", True),
            ("retired", False),
        ]
        status_ids = {}
        for name, active in asset_statuses:
            cur.execute(
                "INSERT INTO itam.asset_status (name, is_active) VALUES (%s, %s) RETURNING status_id",
                (name, active),
            )
            status_ids[name] = cur.fetchone()[0]

        # Vendors
        vendors = [
            ("Dell", "support@dell.com"),
            ("Apple", "support@apple.com"),
            ("Cisco", "support@cisco.com"),
            ("Microsoft", "support@microsoft.com"),
            ("AWS", "support@amazon.com"),
            ("Atlassian", "support@atlassian.com"),
        ]
        vendor_ids = {}
        for name, email in vendors:
            cur.execute(
                "INSERT INTO itam.vendors (name, support_email) VALUES (%s, %s) RETURNING vendor_id",
                (name, email),
            )
            vendor_ids[name] = cur.fetchone()[0]

        # Assets
        assets = [
            {
                "tag": "LPT-001",
                "type": "Laptop",
                "status": "in_use",
                "manuf": "Dell",
                "model": "Latitude 7440",
                "serial": "DL-001-ABC",
                "desc": "IT laptop",
                "purchase_date": "2024-01-05",
                "price": 1450.00,
                "currency": "EUR",
                "warranty_end": "2026-01-05",
                "owner": "IT",
            },
            {
                "tag": "LPT-002",
                "type": "Laptop",
                "status": "in_use",
                "manuf": "Apple",
                "model": "MacBook Pro 14",
                "serial": "AP-002-XYZ",
                "desc": "Engineering laptop",
                "purchase_date": "2024-02-10",
                "price": 1900.00,
                "currency": "EUR",
                "warranty_end": "2026-02-10",
                "owner": "Engineering",
            },
            {
                "tag": "LPT-003",
                "type": "Laptop",
                "status": "in_stock",
                "manuf": "Lenovo",
                "model": "T14",
                "serial": "LV-003-XYZ",
                "desc": "Spare laptop",
                "purchase_date": "2024-03-15",
                "price": 1200.00,
                "currency": "EUR",
                "warranty_end": "2026-03-15",
                "owner": "IT",
            },
            {
                "tag": "SRV-001",
                "type": "Server",
                "status": "in_use",
                "manuf": "Dell",
                "model": "PowerEdge R750",
                "serial": "SRV-001-PE",
                "desc": "Main application server",
                "purchase_date": "2023-11-20",
                "price": 8000.00,
                "currency": "USD",
                "warranty_end": "2026-11-20",
                "owner": "IT",
            },
            {
                "tag": "SRV-002",
                "type": "Server",
                "status": "in_stock",
                "manuf": "Dell",
                "model": "PowerEdge R650",
                "serial": "SRV-002-PE",
                "desc": "Standby server",
                "purchase_date": "2024-04-12",
                "price": 7200.00,
                "currency": "USD",
                "warranty_end": "2027-04-12",
                "owner": "IT",
            },
            {
                "tag": "SWT-001",
                "type": "Network",
                "status": "in_use",
                "manuf": "Cisco",
                "model": "Catalyst 9300",
                "serial": "SW-001-CAT",
                "desc": "Core switch",
                "purchase_date": "2023-10-01",
                "price": 3500.00,
                "currency": "USD",
                "warranty_end": "2026-10-01",
                "owner": "IT",
            },
            {
                "tag": "PHN-001",
                "type": "Phone",
                "status": "in_use",
                "manuf": "Apple",
                "model": "iPhone 15",
                "serial": "IP-001-15",
                "desc": "Sales phone",
                "purchase_date": "2024-01-20",
                "price": 900.00,
                "currency": "EUR",
                "warranty_end": "2025-01-20",
                "owner": "Sales",
            },
            {
                "tag": "PHN-002",
                "type": "Phone",
                "status": "in_use",
                "manuf": "Samsung",
                "model": "Galaxy S24",
                "serial": "SM-002-S24",
                "desc": "Engineering phone",
                "purchase_date": "2024-02-05",
                "price": 800.00,
                "currency": "EUR",
                "warranty_end": "2025-02-05",
                "owner": "Engineering",
            },
            {
                "tag": "SFT-001",
                "type": "Software",
                "status": "in_use",
                "manuf": "Microsoft",
                "model": "Windows Server",
                "serial": "WS-001",
                "desc": "Windows Server Datacenter",
                "purchase_date": None,
                "price": None,
                "currency": None,
                "warranty_end": None,
                "owner": "IT",
            },
            {
                "tag": "CLD-001",
                "type": "CloudAccount",
                "status": "in_use",
                "manuf": "AWS",
                "model": "Account",
                "serial": "AWS-ACC-001",
                "desc": "AWS org account",
                "purchase_date": None,
                "price": None,
                "currency": None,
                "warranty_end": None,
                "owner": "IT",
            },
            {
                "tag": "NAS-001",
                "type": "Storage",
                "status": "in_use",
                "manuf": "Synology",
                "model": "RS3621xs+",
                "serial": "NAS-001-SY",
                "desc": "Shared storage",
                "purchase_date": "2023-12-10",
                "price": 2500.00,
                "currency": "EUR",
                "warranty_end": "2026-12-10",
                "owner": "IT",
            },
            {
                "tag": "LPT-004",
                "type": "Laptop",
                "status": "repair",
                "manuf": "HP",
                "model": "EliteBook 840",
                "serial": "HP-004-EB",
                "desc": "Finance laptop in repair",
                "purchase_date": "2024-05-02",
                "price": 1100.00,
                "currency": "EUR",
                "warranty_end": "2026-05-02",
                "owner": "Finance",
            },
        ]
        asset_ids = {}
        for a in assets:
            cur.execute(
                """
                INSERT INTO itam.assets (
                  asset_tag, type_id, status_id, manufacturer, model, serial_number, description,
                  purchase_date, purchase_price, currency, warranty_end, owner_org_unit_id, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING asset_id
                """,
                (
                    a["tag"],
                    type_ids[a["type"]],
                    status_ids[a["status"]],
                    a["manuf"],
                    a["model"],
                    a["serial"],
                    a["desc"],
                    a["purchase_date"],
                    a["price"],
                    a["currency"],
                    a["warranty_end"],
                    org_ids[a["owner"]],
                    None,
                ),
            )
            asset_ids[a["tag"]] = cur.fetchone()[0]

        # Asset assignments
        assignments = [
            ("LPT-001", "alice.admin@example.com", None, "2024-01-10T09:00:00Z", None, "Primary laptop"),
            ("LPT-002", "frank.forge@example.com", None, "2024-02-15T09:00:00Z", None, "Engineering laptop"),
            ("LPT-003", None, "Remote Warehouse", "2025-01-05T10:00:00Z", None, "Spare stock"),
            ("SRV-001", None, "Data Center A", "2024-03-01T08:00:00Z", None, "Rack mount"),
            ("SRV-002", None, "Data Center A", "2024-12-01T08:00:00Z", None, "Spare rack server"),
            ("SWT-001", None, "Data Center A", "2024-03-02T08:00:00Z", None, "Core switch location"),
            ("PHN-001", "dave.deals@example.com", None, "2024-04-01T09:00:00Z", None, "Sales phone"),
            ("PHN-002", "grace.gains@example.com", None, "2024-04-05T09:00:00Z", None, "Engineering phone"),
            ("SFT-001", "erin.engineer@example.com", None, "2024-05-01T09:00:00Z", None, "Software owner"),
            ("CLD-001", "heidi.help@example.com", None, "2024-05-10T09:00:00Z", None, "Cloud account owner"),
            ("NAS-001", None, "Data Center A", "2024-03-05T08:00:00Z", None, "Storage in rack"),
            ("LPT-004", None, "HQ Office", "2025-01-10T10:00:00Z", None, "In repair at HQ"),
        ]
        for asset_tag, person_email, loc_name, start, end, purpose in assignments:
            cur.execute(
                """
                INSERT INTO itam.asset_assignments
                  (asset_id, person_id, location_id, assigned_from, assigned_to, purpose)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    asset_ids[asset_tag],
                    person_ids.get(person_email),
                    location_ids.get(loc_name),
                    start,
                    end,
                    purpose,
                ),
            )

        # Asset relations
        relations = [
            ("SRV-001", "SWT-001", "depends_on"),
            ("SRV-001", "NAS-001", "attached_to"),
            ("CLD-001", "SRV-001", "depends_on"),
        ]
        for parent_tag, child_tag, rel_type in relations:
            cur.execute(
                """
                INSERT INTO itam.asset_relations (parent_asset_id, child_asset_id, relation_type)
                VALUES (%s, %s, %s)
                """,
                (asset_ids[parent_tag], asset_ids[child_tag], rel_type),
            )

        # Purchases
        purchases = [
            ("Dell", "D-1001", "DI-2001", "2024-01-03", "2024-01-07"),
            ("Apple", "A-1002", "AI-2002", "2024-02-08", "2024-02-12"),
            ("Cisco", "C-1003", "CI-2003", "2023-09-25", "2023-10-02"),
            ("Microsoft", "M-1004", "MI-2004", "2023-11-10", "2023-11-15"),
            ("AWS", "W-1005", "WI-2005", "2024-05-15", "2024-05-16"),
        ]
        purchase_ids = {}
        for vendor_name, order_no, invoice_no, ordered_at, received_at in purchases:
            cur.execute(
                """
                INSERT INTO itam.purchases (vendor_id, order_no, invoice_no, ordered_at, received_at, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING purchase_id
                """,
                (vendor_ids[vendor_name], order_no, invoice_no, ordered_at, received_at, None),
            )
            purchase_ids[order_no] = cur.fetchone()[0]

        # Purchase items
        purchase_items = [
            ("D-1001", "LPT-001", "Dell Latitude 7440", 1, 1450.00, "EUR"),
            ("D-1001", "LPT-003", "Lenovo T14", 1, 1200.00, "EUR"),
            ("A-1002", "LPT-002", "MacBook Pro 14", 1, 1900.00, "EUR"),
            ("C-1003", "SWT-001", "Cisco Catalyst 9300", 1, 3500.00, "USD"),
            ("D-1001", "SRV-001", "Dell PowerEdge R750", 1, 8000.00, "USD"),
            ("D-1001", "SRV-002", "Dell PowerEdge R650", 1, 7200.00, "USD"),
            ("A-1002", "PHN-001", "iPhone 15", 1, 900.00, "EUR"),
            ("A-1002", "PHN-002", "Galaxy S24", 1, 800.00, "EUR"),
            ("M-1004", "SFT-001", "Windows Server Datacenter", 1, 0.00, None),
            ("D-1001", "NAS-001", "Synology RS3621xs+", 1, 2500.00, "EUR"),
        ]
        for order_no, asset_tag, desc, qty, unit_price, currency in purchase_items:
            cur.execute(
                """
                INSERT INTO itam.purchase_items (purchase_id, asset_id, description, quantity, unit_price, currency)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    purchase_ids[order_no],
                    asset_ids[asset_tag],
                    desc,
                    qty,
                    unit_price,
                    currency,
                ),
            )

        # Contracts
        contracts = [
            ("Dell Maintenance", "maintenance", "Dell", "2024-01-01", "2025-12-31", 60, 15000.00, "USD", "DOC-DEL-001"),
            ("Microsoft EA", "subscription", "Microsoft", "2023-12-01", "2026-11-30", 90, 50000.00, "USD", "DOC-MS-001"),
            ("AWS Support", "support", "AWS", "2024-01-01", "2024-12-31", 30, 12000.00, "USD", "DOC-AWS-001"),
        ]
        contract_ids = {}
        for name, ctype, vendor, start, end, notice_days, total_value, currency, doc in contracts:
            cur.execute(
                """
                INSERT INTO itam.contracts (vendor_id, type, name, start_date, end_date, notice_period_days, total_value, currency, document_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING contract_id
                """,
                (vendor_ids[vendor], ctype, name, start, end, notice_days, total_value, currency, doc),
            )
            contract_ids[name] = cur.fetchone()[0]

        # Asset contracts
        asset_contract_links = [
            ("SRV-001", "Dell Maintenance"),
            ("SRV-002", "Dell Maintenance"),
            ("SWT-001", "Dell Maintenance"),
            ("SFT-001", "Microsoft EA"),
            ("CLD-001", "AWS Support"),
        ]
        for asset_tag, contract_name in asset_contract_links:
            cur.execute(
                "INSERT INTO itam.asset_contracts (asset_id, contract_id) VALUES (%s, %s)",
                (asset_ids[asset_tag], contract_ids[contract_name]),
            )

        # Maintenance tickets
        tickets = [
            ("SRV-001", "repair", "open", "2025-01-05T08:00:00Z", None, "Dell", "SRV-REP-001", 500.00, "USD"),
            ("LPT-004", "incident", "in_progress", "2025-01-12T09:00:00Z", None, "Dell", "LPT-INC-004", 0.00, "EUR"),
            ("SWT-001", "upgrade", "closed", "2024-06-10T08:00:00Z", "2024-06-12T18:00:00Z", "Cisco", "SWT-UPG-001", 300.00, "USD"),
            ("PHN-001", "repair", "closed", "2024-08-01T09:00:00Z", "2024-08-05T15:00:00Z", "Apple", "PHN-REP-001", 150.00, "EUR"),
        ]
        for asset_tag, ttype, status, opened, closed, vendor, ext_ref, cost, currency in tickets:
            cur.execute(
                """
                INSERT INTO itam.maintenance_tickets
                  (asset_id, type, status, opened_at, closed_at, provider_vendor_id, external_ref, cost, currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    asset_ids[asset_tag],
                    ttype,
                    status,
                    opened,
                    closed,
                    vendor_ids[vendor],
                    ext_ref,
                    cost,
                    currency,
                ),
            )

        # Software products
        software_products = [
            ("Microsoft 365 E3", "Microsoft", "per_user"),
            ("Atlassian Jira", "Atlassian", "per_user"),
            ("Adobe Creative Cloud", "Adobe", "per_user"),
            ("Windows Server Datacenter", "Microsoft", "per_device"),
        ]
        product_ids = {}
        for name, publisher, metric in software_products:
            cur.execute(
                "INSERT INTO itam.software_products (name, publisher, metric) VALUES (%s, %s, %s) RETURNING product_id",
                (name, publisher, metric),
            )
            product_ids[name] = cur.fetchone()[0]

        # Licenses
        licenses = [
            ("Microsoft 365 E3", "Microsoft", "Microsoft EA", 200, "2024-01-01", "2025-12-31", "E3-KEY-001", None, 24000.00, "USD"),
            ("Atlassian Jira", "Atlassian", None, 50, "2024-01-01", "2024-12-31", "JIRA-KEY-001", None, 6000.00, "USD"),
            ("Adobe Creative Cloud", "Adobe", None, 10, "2024-01-01", "2024-12-31", "ADOBE-KEY-001", None, 7200.00, "USD"),
            ("Windows Server Datacenter", "Microsoft", "Microsoft EA", 10, "2024-01-01", "2026-12-31", "WSDC-KEY-001", None, 15000.00, "USD"),
        ]
        license_ids = {}
        for product, vendor, contract, entitlements, start, end, key, sub_id, cost, currency in licenses:
            cur.execute(
                """
                INSERT INTO itam.licenses
                  (product_id, vendor_id, contract_id, total_entitlements, valid_from, valid_to, license_key, subscription_id, cost, currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING license_id
                """,
                (
                    product_ids[product],
                    vendor_ids.get(vendor),
                    contract_ids.get(contract),
                    entitlements,
                    start,
                    end,
                    key,
                    sub_id,
                    cost,
                    currency,
                ),
            )
            license_ids[product] = cur.fetchone()[0]

        # License assignments
        license_assignments = [
            ("Microsoft 365 E3", "alice.admin@example.com", None, "2024-01-10T09:00:00Z", None),
            ("Microsoft 365 E3", "dave.deals@example.com", None, "2024-02-01T09:00:00Z", None),
            ("Atlassian Jira", "frank.forge@example.com", None, "2024-02-20T09:00:00Z", None),
            ("Atlassian Jira", "erin.engineer@example.com", None, "2024-02-22T09:00:00Z", None),
            ("Adobe Creative Cloud", "grace.gains@example.com", None, "2024-03-01T09:00:00Z", None),
            ("Windows Server Datacenter", None, "SRV-001", "2024-01-15T09:00:00Z", None),
        ]
        for product, person_email, asset_tag, start, end in license_assignments:
            cur.execute(
                """
                INSERT INTO itam.license_assignments
                  (license_id, person_id, asset_id, assigned_from, assigned_to)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    license_ids[product],
                    person_ids.get(person_email),
                    asset_ids.get(asset_tag),
                    start,
                    end,
                ),
            )

        # Installations
        installations = [
            ("SRV-001", "Windows Server Datacenter", "2025.01", "2024-01-15T10:00:00Z"),
            ("LPT-002", "Atlassian Jira", "Cloud Agent", "2024-02-25T10:00:00Z"),
            ("LPT-001", "Microsoft 365 E3", "O365 Desktop", "2024-01-15T10:00:00Z"),
            ("PHN-001", "Microsoft 365 E3", "Mobile", "2024-04-02T10:00:00Z"),
            ("LPT-004", "Adobe Creative Cloud", "2024", "2024-05-10T10:00:00Z"),
            ("SRV-002", "Windows Server Datacenter", "2025.01", "2024-12-05T10:00:00Z"),
        ]
        for asset_tag, product, version, discovered in installations:
            cur.execute(
                """
                INSERT INTO itam.installations
                  (asset_id, product_id, version, discovered_at)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    asset_ids[asset_tag],
                    product_ids[product],
                    version,
                    discovered,
                ),
            )

        # Cloud accounts
        cloud_accounts = [
            ("aws", "AWS Production", "123456789012", "alice.admin@example.com"),
            ("gcp", "GCP Sandbox", "gcp-sbx-001", "heidi.help@example.com"),
        ]
        cloud_account_ids = {}
        for provider, name, ext_id, owner_email in cloud_accounts:
            cur.execute(
                """
                INSERT INTO itam.cloud_accounts (provider, account_name, account_external_id, owner_person_id)
                VALUES (%s, %s, %s, %s) RETURNING cloud_account_id
                """,
                (provider, name, ext_id, person_ids[owner_email]),
            )
            cloud_account_ids[name] = cur.fetchone()[0]

        # Cloud resources
        cloud_resources = [
            ("AWS Production", "ec2", "i-001", "app-server-1", "eu-central-1", "CC1001"),
            ("AWS Production", "rds", "db-001", "itam-db", "eu-central-1", "CC1001"),
            ("AWS Production", "s3", "bucket-logs", "logs-bucket", "eu-central-1", "CC1001"),
            ("GCP Sandbox", "compute", "vm-001", "sbx-vm", "europe-west1", "CC1001"),
            ("GCP Sandbox", "storage", "bucket-sbx", "sbx-bucket", "europe-west1", "CC1001"),
            ("AWS Production", "iam", "role-app", "app-role", "eu-central-1", "CC1001"),
        ]
        for account_name, rtype, identifier, name, region, cost_center in cloud_resources:
            cur.execute(
                """
                INSERT INTO itam.cloud_resources
                  (cloud_account_id, resource_type, resource_identifier, name, region, cost_center)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    cloud_account_ids[account_name],
                    rtype,
                    identifier,
                    name,
                    region,
                    cost_center,
                ),
            )

        # Asset events
        events = [
            ("LPT-001", "provisioned", "2024-01-10T09:05:00Z", "alice.admin@example.com", {"note": "Issued to Alice"}),
            ("SRV-001", "installed", "2024-03-01T09:30:00Z", "erin.engineer@example.com", {"rack": "12", "u": "22"}),
            ("SWT-001", "configured", "2024-03-02T10:00:00Z", "erin.engineer@example.com", {"config": "baseline"}),
            ("CLD-001", "owner_assigned", "2024-05-10T09:10:00Z", "heidi.help@example.com", {"owner": "Heidi"}),
            ("LPT-004", "repair_started", "2025-01-12T09:30:00Z", "carol.cash@example.com", {"vendor_ticket": "LPT-INC-004"}),
        ]
        for asset_tag, event_type, occurred_at, actor_email, payload in events:
            cur.execute(
                """
                INSERT INTO itam.asset_events (asset_id, event_type, occurred_at, actor_person_id, payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    asset_ids[asset_tag],
                    event_type,
                    occurred_at,
                    person_ids.get(actor_email),
                    payload,
                ),
            )

        conn.commit()
        print("Seed data inserted.")
    except Exception as exc:
        conn.rollback()
        print(f"Seed failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
