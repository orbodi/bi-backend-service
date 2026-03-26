import argparse
import csv
import os
import sys
from typing import Dict, List


def load_dotenv_file(dotenv_path: str) -> None:
    if not os.path.isfile(dotenv_path):
        return
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            # Do not override explicit env vars already set.
            os.environ.setdefault(key, value)


def load_dotenv() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    candidates = [
        os.path.join(script_dir, ".env"),
        os.path.join(script_dir, "..", ".env"),
        os.path.join(cwd, ".env"),
    ]
    for path in candidates:
        load_dotenv_file(os.path.abspath(path))


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v


def validate_csv_columns(csv_path: str, required: List[str]) -> None:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header")
        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            raise RuntimeError(
                "CSV missing required columns: " + ", ".join(missing)
            )


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Load locations (region->center) from a CSV into bi.location."
    )
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--lang", default="fra", help="Language code: fra or eng (default: fra)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only validate CSV + DB structure; do not copy/execute loader.",
    )

    # Connection params (prefer env vars)
    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    parser.add_argument("--dbname", default=os.environ.get("PGDATABASE"))
    parser.add_argument("--user", default=os.environ.get("PGUSER"))
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD"))

    args = parser.parse_args()

    if not os.path.isfile(args.csv):
        raise RuntimeError(f"CSV not found: {args.csv}")

    required_csv_cols = [
        "region",
        "code_region",
        "prefecture",
        "commune",
        "canton",
        "localite",
        "localites",
        "locationcode",
        "centres",
        "code_centres",
    ]
    validate_csv_columns(args.csv, required_csv_cols)

    if not args.dbname:
        args.dbname = require_env("PGDATABASE")
    if not args.user:
        args.user = require_env("PGUSER")
    if not args.password:
        # Some environments rely on pg_hba.conf / ident; keep it explicit anyway.
        args.password = require_env("PGPASSWORD")

    try:
        import psycopg2
    except ImportError as e:
        raise RuntimeError(
            "psycopg2 is required. Install with: pip install psycopg2-binary"
        ) from e

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )

    # We'll fail fast and keep the transaction atomic.
    conn.autocommit = False

    with conn:
        with conn.cursor() as cur:
            # 1) Structure checks
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'bi'
                  AND table_name = 'location'
                  AND column_name IN (
                    'code','name','hierarchy_level','hierarchy_level_name','parent_loc_code','lang_code'
                  )
                """
            )
            cols = {r[0] for r in cur.fetchall()}
            expected_cols = {
                "code",
                "name",
                "hierarchy_level",
                "hierarchy_level_name",
                "parent_loc_code",
                "lang_code",
            }
            missing = sorted(expected_cols - cols)
            if missing:
                raise RuntimeError(f"bi.location missing expected columns: {', '.join(missing)}")

            # Make sure hierarchy levels exist for the language.
            cur.execute(
                """
                SELECT hierarchy_level
                FROM bi.loc_hierarchy_list
                WHERE lang_code = %s
                  AND hierarchy_level IN (1,2,3,4,5,6)
                ORDER BY hierarchy_level
                """,
                (args.lang,),
            )
            levels = [r[0] for r in cur.fetchall()]
            if len(levels) != 6:
                raise RuntimeError(
                    f"bi.loc_hierarchy_list does not contain levels 1..6 for lang={args.lang}. Found: {levels}"
                )

            if args.dry_run:
                print("Dry-run OK: CSV columns and bi structure look compatible.")
                return 0

            # 2) Ensure staging exists (same schema as SQL loader)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bi.stg_locations_csv (
                  region        TEXT,
                  code_region   TEXT,
                  prefecture    TEXT,
                  commune       TEXT,
                  canton        TEXT,
                  localite      TEXT,
                  localites     TEXT,
                  locationcode  TEXT,
                  centres       TEXT,
                  code_centres  TEXT
                )
                """
            )
            cur.execute("TRUNCATE TABLE bi.stg_locations_csv")

            # 3) COPY CSV into staging
            copy_sql = """
                COPY bi.stg_locations_csv(
                    region, code_region, prefecture, commune, canton, localite,
                    localites, locationcode, centres, code_centres
                )
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
            """
            with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
                cur.copy_expert(copy_sql, f)

            # 4) Call the SQL loader function
            cur.execute("SELECT * FROM bi.load_locations_from_staging(%s)", (args.lang,))
            loaded = cur.fetchone()

            # 5) Post-load verification
            cur.execute(
                """
                SELECT hierarchy_level, count(*)
                FROM bi.location
                WHERE lang_code = %s
                  AND hierarchy_level BETWEEN 1 AND 6
                GROUP BY hierarchy_level
                ORDER BY hierarchy_level
                """,
                (args.lang,),
            )
            counts_by_level = cur.fetchall()

            def count_bad(pattern: str, level: int) -> int:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM bi.location
                    WHERE lang_code = %s
                      AND hierarchy_level = %s
                      AND code !~ %s
                    """,
                    (args.lang, level, pattern),
                )
                return int(cur.fetchone()[0])

            # Prefecture / Commune / Canton code formats (as requested)
            bad_pref = count_bad(r'^P[0-9]+$', 2)
            bad_comm = count_bad(r'^COM[0-9]+$', 3)
            bad_ctn = count_bad(r'^CTN[0-9]+$', 4)

            # Center codes (should come from CSV code_centres, so only ensure not NULL)
            cur.execute(
                """
                SELECT count(*)
                FROM bi.location
                WHERE lang_code = %s
                  AND hierarchy_level = 6
                  AND (code IS NULL OR trim(code) = '')
                """,
                (args.lang,),
            )
            bad_center = int(cur.fetchone()[0])

            print("Load result (ins_locations_count, upsert_centers_count):", loaded)
            print("Count by hierarchy_level (1..6):", counts_by_level)
            print("Format checks:")
            print("  bad_prefecture_codes (level=2):", bad_pref)
            print("  bad_commune_codes (level=3):", bad_comm)
            print("  bad_canton_codes (level=4):", bad_ctn)
            print("  bad_center_empty_codes (level=6):", bad_center)

            if any(x != 0 for x in [bad_pref, bad_comm, bad_ctn, bad_center]):
                raise RuntimeError("Post-load verification failed. Fix data/logic and re-run.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise

