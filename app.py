import streamlit as st
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime, timedelta, date
import json
from dateutil.relativedelta import relativedelta
import altair as alt
import io
import math
import time
import os
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# Google Drive Config
SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_ID_DEFAULT = "1Y98WYhpaqWoYZ2Y5RRGW-KJPXo1nBtAp"

DB_PATH = "minama.db"
ICON_PATH = os.path.join(os.path.dirname(__file__), "icon.png")

# ---------------------------------
# Configuration Flags
# ---------------------------------
# Dapat diubah jika ingin menonaktifkan pengaruh timeline terhadap skor agregasi
ENABLE_TIMELINE_WEIGHTING = True
# Use absolute path for page icon to ensure it loads even when cwd differs
_icon_arg = ICON_PATH if os.path.exists(ICON_PATH) else "icon.png"
st.set_page_config(layout="wide", page_icon=_icon_arg, page_title="Minama Felonic Solutions")

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    # assign_tracer (for Trace Assigning tab)
    c.execute("""
    CREATE TABLE IF NOT EXISTS assign_tracer (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TRC_Code TEXT,
        Agreement_No TEXT,
        Debtor_Name TEXT,
        NIK_KTP TEXT,
        EMPLOYMENT_UPDATE TEXT,
        EMPLOYER TEXT,
        Debtor_Legal_Name TEXT,
        Employee_Name TEXT,
        Employee_ID_Number TEXT,
        Debtor_Relation_to_Employee TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    # Ensure new column for assigning tracer by name exists
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(assign_tracer)").fetchall()]
        if 'Assigned_To' not in cols:
            c.execute("ALTER TABLE assign_tracer ADD COLUMN Assigned_To TEXT")
    except Exception:
        # Safe to ignore if already exists or PRAGMA failed
        pass
    # Try to enforce unique Agreement_No for tracer assignment (one tracer per loan)
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_assign_tracer_unique_agreement ON assign_tracer(Agreement_No)")
    except Exception:
        # Will fail if duplicates already exist; app-level guards will still apply
        pass
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_assign_tracer_assigned_to ON assign_tracer(Assigned_To)")
    except Exception:
        pass
    # users
    # Fresh schema includes login_id (Id for login) and full_name; keep legacy 'name' for backward-compat.
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        login_id TEXT UNIQUE,
        password_hash TEXT,
        full_name TEXT,
        name TEXT, -- legacy
        email TEXT UNIQUE,
        role TEXT DEFAULT 'Agent', -- Superuser / Supervisor / Tracer / Agent
        approved INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    # Migrate existing tables to ensure columns exist and are populated
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(users)").fetchall()]
        if 'login_id' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN login_id TEXT")
        if 'full_name' not in cols:
            c.execute("ALTER TABLE users ADD COLUMN full_name TEXT")
        # Ensure a unique index for login_id (SQLite cannot alter constraint easily)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_login_id ON users(login_id)")
        # Soft-migrate deprecated 'department' column: keep if exists, but stop using it
        # Soft-migrate old role names to new role set
        try:
            c.execute("UPDATE users SET role='Superuser' WHERE role='admin'")
            c.execute("UPDATE users SET role='Agent' WHERE role='user'")
        except Exception:
            pass
        # Backfill values from legacy columns
        c.execute("""
            UPDATE users
            SET full_name = CASE
                WHEN (full_name IS NULL OR TRIM(full_name)='') THEN COALESCE(name, full_name)
                ELSE full_name
            END
        """)
        c.execute("""
            UPDATE users
            SET login_id = CASE
                WHEN (login_id IS NULL OR TRIM(login_id)='') THEN
                    CASE WHEN (email IS NOT NULL AND TRIM(email)<> '') THEN email ELSE name END
                ELSE login_id
            END
        """)
        conn.commit()
    except Exception:
        pass
    # departments table no longer used; keep existing table if present (no creation needed)
    # app_settings (key-value config)
    c.execute("""
    CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )""")
    # backup_log (log backup DB ke Drive)
    c.execute("""
    CREATE TABLE IF NOT EXISTS backup_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        drive_file_id TEXT,
        status TEXT,
        message TEXT,
        backup_time TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    # audit_logs (log user login events)
    c.execute("""
    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT,
        details TEXT,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)
    # record_notes (catatan manual untuk cek DB restore)
    c.execute("""
    CREATE TABLE IF NOT EXISTS record_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        note TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    # supervisor_data (for Supervisor menu)
    c.execute("""
    CREATE TABLE IF NOT EXISTS supervisor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        DT TEXT,
        Lending_Entity TEXT,
        Date TEXT,
        Case_ID TEXT,
        Task_ID TEXT,
        Customer_name TEXT,
        email TEXT,
        Gender TEXT,
        Customer_Occupation TEXT,
        DPD TEXT,
        Principle_Outstanding TEXT,
        Principal_Overdue_CURR TEXT,
        Interest_Overdue_CURR TEXT,
        Last_Late_Fee TEXT,
        Return_Date TEXT,
        Detail TEXT,
        Loan_Type TEXT,
        Third_Uid TEXT,
        Product TEXT,
        Home_Address TEXT,
        Province TEXT,
        City TEXT,
        Street TEXT,
        RoomNumber TEXT,
        Postcode TEXT,
        Assignment_Date TEXT,
        Withdrawal_Date TEXT,
        Phone_Number_1 TEXT,
        Phone_Number_2 TEXT,
        Contact_Type_1 TEXT,
        Contact_Name_1 TEXT,
        Contact_Phone_1 TEXT,
        Contact_Type_2 TEXT,
        Contact_Name_2 TEXT,
        Contact_Phone_2 TEXT,
        Contact_Type_3 TEXT,
        Contact_Name_3 TEXT,
        Contact_Phone_3 TEXT,
        Contact_Type_4 TEXT,
        Contact_Name_4 TEXT,
        Contact_Phone_4 TEXT,
        Contact_Type_5 TEXT,
        Contact_Name_5 TEXT,
        Contact_Phone_5 TEXT,
        Contact_Type_6 TEXT,
        Contact_Name_6 TEXT,
        Contact_Phone_6 TEXT,
        Contact_Type_7 TEXT,
        Contact_Name_7 TEXT,
        Contact_Phone_7 TEXT,
        Contact_Type_8 TEXT,
        Contact_Name_8 TEXT,
        Contact_Phone_8 TEXT,
        Total_debt_in_third_party TEXT,
        Repayment_on_third_Party TEXT,
        Remaining_Loan_on_third_Party TEXT,
        Virtual_Account_Number TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    # --- New foundational tables ---
    # 1) Agent assignments (one agent per Agreement_No)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT,
            Agent_Assigned_To TEXT,
            assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
            assigned_by TEXT,
            active INTEGER DEFAULT 1
        );
        """
    )
    # Unique per loan for active assignment (soft-enforced via app; hard unique per Agreement_No)
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_assignments_unique ON agent_assignments(Agreement_No)")
    except Exception:
        pass
    # 2) Trace results (touch logs/status)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS trace_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT,
            tracer TEXT,
            status TEXT,
            notes TEXT,
            touch_type TEXT,
            party TEXT,
            touched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        );
        """
    )
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_trace_results_agreement ON trace_results(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_trace_results_touched ON trace_results(touched_at)")
    except Exception:
        pass
    # 3) Masked company dictionary
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS masked_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            masked_name TEXT,
            canonical_name TEXT,
            mapping_notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_masked_companies_masked ON masked_companies(masked_name)")
    except Exception:
        pass
    # 4) Payments recap (daily uploads)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT,
            paid_amount REAL,
            paid_date TEXT,
            status TEXT,
            source_file TEXT,
            uploaded_by TEXT,
            uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_unique ON payments(Agreement_No, paid_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(paid_date)")
    except Exception:
        pass
    # 5) Agent results (handling outcome fields)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT,
            agent TEXT,
            agent_status TEXT,
            agent_ptp_amount REAL,
            agent_ptp_date TEXT,
            agent_notes TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_agent_results_agreement ON agent_results(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_agent_results_agent ON agent_results(agent)")
    except Exception:
        pass
    # ensure assign_tracer has optional masked company name field
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(assign_tracer)").fetchall()]
        if 'Masked_Company_Name' not in cols:
            c.execute("ALTER TABLE assign_tracer ADD COLUMN Masked_Company_Name TEXT")
    except Exception:
        pass
    conn.commit()

    # Seed default settings (idempotent)
    try:
        c.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('auto_restore_enabled','true')")
        # Could add future defaults here
        conn.commit()
    except Exception:
        pass

    # ensure at least one user exists (seed)
    c.execute("SELECT COUNT(*) as cnt FROM users")
    row = c.fetchone()
    if row['cnt'] == 0:
        # Create default users for each role
        users_to_seed = [
            {"login_id": "superuser", "full_name": "Superuser", "email": "superuser", "password": "superuser123", "role": "Superuser", "approved": 1},
            {"login_id": "supervisor", "full_name": "Supervisor", "email": "supervisor", "password": "supervisor123", "role": "Supervisor", "approved": 1},
            {"login_id": "tracer", "full_name": "Tracer", "email": "tracer", "password": "tracer123", "role": "Tracer", "approved": 1},
            {"login_id": "agent", "full_name": "Agent", "email": "agent", "password": "agent123", "role": "Agent", "approved": 1},
        ]
        
        for user in users_to_seed:
            try:
                hashed_pw = hash_password(user['password'])
                # Insert with new schema; also fill legacy 'name' for compatibility
                c.execute(
                    "INSERT INTO users (login_id, full_name, name, email, password_hash, role, approved) VALUES (?,?,?,?,?,?,?)",
                    (user['login_id'], user['full_name'], user['full_name'], user['email'], hashed_pw, user['role'], user['approved'])
                )
            except sqlite3.IntegrityError:
                # User might already exist, skip.
                pass
        
        conn.commit()

    # Always ensure at least one approved user exists for each role (idempotent)
    try:
        ensure_roles = [
            ("Superuser", "superuser", "Superuser", "superuser", "superuser123"),
            ("Supervisor", "supervisor", "Supervisor", "supervisor", "supervisor123"),
            ("Tracer", "tracer", "Tracer", "tracer", "tracer123"),
            ("Agent", "agent", "Agent", "agent", "agent123"),
        ]
        for role_name, login_id_def, full_name_def, email_def, pw_def in ensure_roles:
            r_cnt = c.execute("SELECT COUNT(*) AS c FROM users WHERE role=?", (role_name,)).fetchone()
            cnt_val = (r_cnt[0] if r_cnt and 0 in r_cnt.keys() else r_cnt['c']) if isinstance(r_cnt, sqlite3.Row) else (r_cnt[0] if r_cnt else 0)
            if not cnt_val:
                try:
                    c.execute(
                        "INSERT INTO users (login_id, full_name, name, email, password_hash, role, approved) VALUES (?,?,?,?,?,?,?)",
                        (login_id_def, full_name_def, full_name_def, email_def, hash_password(pw_def), role_name, 1)
                    )
                except sqlite3.IntegrityError:
                    pass
        conn.commit()
    except Exception:
        pass

    conn.close()

# -------------------------
# Helper functions
# -------------------------
def hash_password(pw: str):
    return hashlib.sha256(pw.encode()).hexdigest()

def verify_password(pw: str, h: str):
    return hash_password(pw) == h

def current_user():
    return st.session_state.get("user")

def login_user(user_row):
    st.session_state["user"] = dict(user_row)

def logout_user():
    # Lakukan backup saat logout (jika kredensial tersedia)
    user = current_user()
    try:
        if "service_account" in st.secrets:
            service, _ = build_drive_service()
            ok, msg = perform_backup(service, FOLDER_ID_DEFAULT)
            st.session_state['last_logout_backup'] = {
                'ok': ok,
                'msg': msg,
                'time': datetime.utcnow().isoformat()
            }
    except Exception as e:
        st.session_state['last_logout_backup'] = {
            'ok': False,
            'msg': f'Backup saat logout gagal: {e}',
            'time': datetime.utcnow().isoformat()
        }
    # Catat audit trail logout
    if user:
        try:
            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (user.get('id'), "LOGOUT", f"User {user.get('login_id') or user.get('email') or '-'} logout."))
        except Exception:
            pass
    # Bersihkan sesi user setelah mencoba backup
    if "user" in st.session_state:
        del st.session_state["user"]
    # Reset auto-restore/backup flags on logout
    for k in ["auto_restore_checked", "auto_backup_checked", "auto_restore_attempted"]:
        if k in st.session_state:
            del st.session_state[k]
    st.session_state.page = "Authentication"

def fetchall(query, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def fetchone(query, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    cur = conn.cursor()
    cur.execute(query, params)
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def execute(query, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    last = cur.lastrowid
    conn.close()
    return last

def get_setting(key, default=None):
    row = fetchone("SELECT value FROM app_settings WHERE key=?", (key,))
    if not row:
        return default
    return row.get('value')

def set_setting(key, value):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    conn.commit()
    conn.close()
    
def get_project_capacity_bytes(default_bytes: int = 2 * 1024 * 1024 * 1024) -> int:
    """Ambil kapasitas maksimum proyek (bytes) dari app_settings.
    Jika belum ada, gunakan default 2GB.
    Key: project_capacity_bytes
    """
    val = get_setting('project_capacity_bytes')
    try:
        if val is None:
            return int(default_bytes)
        return int(val)
    except Exception:
        return int(default_bytes)

# -------------------------
# Backup helpers
# -------------------------
def perform_backup(service, folder_id=FOLDER_ID_DEFAULT):
    """Create a timestamped backup of the SQLite DB to Google Drive and record in backup_log.

    Returns (success: bool, info_message: str)
    """
    if not os.path.exists(DB_PATH):
        return False, f"Database '{DB_PATH}' tidak ditemukan." 
    # Nama file backup auto (overwrite, bukan timestamp) agar tidak menumpuk
    base_name = get_setting('auto_backup_filename', 'auto_backup.sqlite') or 'auto_backup.sqlite'
    # Cek kapasitas: jika file belum ada, menambah ukuran; jika sudah ada, overwrite diperbolehkan
    try:
        db_size = os.path.getsize(DB_PATH)
    except Exception:
        db_size = 0
    try:
        usage_now = get_folder_usage_stats(service, folder_id, recursive=True)
        used_bytes_now = int(usage_now.get('total_bytes', 0))
    except Exception:
        used_bytes_now = 0
    capacity = get_project_capacity_bytes()
    # Cek apakah file dengan nama yang sama sudah ada (overwrite diperbolehkan meski full)
    try:
        exists_query = f"name='{base_name}' and '{folder_id}' in parents and trashed=false"
        exists_resp = service.files().list(q=exists_query, spaces='drive', fields='files(id, size)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        existing_files = exists_resp.get('files', [])
    except Exception:
        existing_files = []
    if not existing_files:
        # First time create -> akan menambah ukuran
        if used_bytes_now >= capacity:
            return False, "Gagal backup: kapasitas maksimum tercapai (exceed/max capacity)."
        if used_bytes_now + db_size > capacity:
            return False, "Gagal backup: ukuran backup akan melebihi kapasitas maksimum (exceed)."
    try:
        with open(DB_PATH, 'rb') as f:
            data = f.read()
        fid = upload_or_replace(service, folder_id, base_name, data, mimetype='application/x-sqlite3')
        if fid:
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (base_name, fid, 'SUCCESS', 'overwrite' if existing_files else 'created'))
            return True, f"Backup sukses: {base_name} (ID: {fid})"
        else:
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (base_name, None, 'FAILED', 'Upload gagal'))
            return False, "Upload Drive gagal." 
    except Exception as e:
        execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                (base_name, None, 'FAILED', str(e)))
        return False, f"Gagal backup: {e}" 

def auto_daily_backup(service, folder_id=FOLDER_ID_DEFAULT):
    """Run once per session start (post-login). If last SUCCESS backup is not today -> perform one."""
    # Cek backup sukses terakhir
    row = fetchone("SELECT backup_time FROM backup_log WHERE status='SUCCESS' ORDER BY id DESC LIMIT 1")
    today_str = date.today().isoformat()
    if row:
        try:
            last_date = row['backup_time'][:10]
            if last_date == today_str:
                return False, "Backup harian sudah ada hari ini." 
        except Exception:
            pass
    # Jalankan backup
    ok, msg = perform_backup(service, folder_id)
    return ok, msg


DEFAULT_SCHEDULE_SLOTS = [
    {"start": 6,  "end": 12, "name": "slot_morning"},
    {"start": 12, "end": 18, "name": "slot_afternoon"},
    {"start": 18, "end": 23, "name": "slot_evening"},
    {"start": 23, "end": 6,  "name": "slot_night"},  # wrap
]

def _validate_slot_struct(slots):
    if not isinstance(slots, list) or not slots:
        return False
    names = set()
    for s in slots:
        if not isinstance(s, dict):
            return False
        if 'start' not in s or 'end' not in s or 'name' not in s:
            return False
        try:
            st_h = int(s['start']); en_h = int(s['end'])
        except Exception:
            return False
        if not (0 <= st_h <= 23 and 0 <= en_h <= 23):
            return False
        if st_h == en_h:  # zero-length not allowed
            return False
        nm = str(s['name']).strip()
        if not nm or nm in names:
            return False
        names.add(nm)
    return True

def get_schedule_slots():
    raw = get_setting('scheduled_backup_slots_json')
    if raw:
        try:
            slots = json.loads(raw)
            if _validate_slot_struct(slots):
                # Normalize shape (int casting & strip)
                norm = []
                for s in slots:
                    norm.append({
                        'start': int(s['start']),
                        'end': int(s['end']),
                        'name': str(s['name']).strip()
                    })
                return norm
        except Exception:
            pass
    return DEFAULT_SCHEDULE_SLOTS

def determine_slot(now_local):
    h = now_local.hour
    for s in get_schedule_slots():
        st_h = s['start']; en_h = s['end']
        if st_h < en_h:
            if st_h <= h < en_h:
                return s['name']
        else:  # wrap
            if h >= st_h or h < en_h:
                return s['name']
    return 'slot_unknown'

def check_scheduled_backup(service, folder_id=FOLDER_ID_DEFAULT):
    """If scheduling enabled, ensure one backup per defined slot. Overwrite single file name each time.
    Settings keys used:
      scheduled_backup_enabled: 'true'/'false'
      scheduled_backup_filename: base file name (default 'scheduled_backup.sqlite')
      scheduled_backup_last_slot: last slot string done
    """
    enabled = get_setting('scheduled_backup_enabled', 'false') == 'true'
    if not enabled:
        return False, 'Scheduled backup disabled'
    base_name = get_setting('scheduled_backup_filename', 'scheduled_backup.sqlite') or 'scheduled_backup.sqlite'
    # Determine local time (assume server already GMT+7 or adjust here if needed)
    now_local = datetime.now()  # If server timezone != GMT+7 -> adjust with timedelta(hours=offset)
    slot = determine_slot(now_local)
    if slot == 'slot_unknown':
        return False, 'Outside defined slots'
    last_slot_done = get_setting('scheduled_backup_last_slot')
    today_tag = date.today().isoformat()
    last_slot_date = get_setting('scheduled_backup_last_date')
    composite_last = f"{last_slot_date}:{last_slot_done}" if last_slot_done and last_slot_date else None
    composite_now = f"{today_tag}:{slot}"
    if composite_last == composite_now:
        return False, 'Slot already backed up'
    # Do backup overwrite single file
    if not os.path.exists(DB_PATH):
        return False, 'DB missing'
    try:
        with open(DB_PATH,'rb') as f:
            data = f.read()
        # Catatan: Scheduled backup overwrite (nama tetap) -> tidak menambah jumlah file.
        # Namun tetap pastikan tidak melebihi kapasitas jika file sebelumnya tidak ada (first time).
        try:
            usage_now = get_folder_usage_stats(service, folder_id, recursive=True)
            used_bytes_now = int(usage_now.get('total_bytes', 0))
        except Exception:
            used_bytes_now = 0
        capacity = get_project_capacity_bytes()
        # Cek apakah file dengan nama yang sama sudah ada (overwrite diperbolehkan meski full)
        exists_query = f"name='{base_name}' and '{folder_id}' in parents and trashed=false"
        exists_resp = service.files().list(q=exists_query, spaces='drive', fields='files(id, size)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        existing_files = exists_resp.get('files', [])
        if not existing_files:
            # First time create -> akan menambah ukuran
            if used_bytes_now >= capacity:
                return False, 'Scheduled backup dibatalkan: kapasitas maksimum tercapai.'
            if used_bytes_now + len(data) > capacity:
                return False, 'Scheduled backup dibatalkan: ukuran backup melebihi kapasitas.'
        fid = upload_or_replace(service, folder_id, base_name, data, mimetype='application/x-sqlite3')
        if fid:
            set_setting('scheduled_backup_last_slot', slot)
            set_setting('scheduled_backup_last_date', today_tag)
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (base_name, fid, 'SUCCESS', f'scheduled {slot}'))
            return True, f'Scheduled backup OK ({slot}) -> {base_name}'
        else:
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (base_name, None, 'FAILED', f'scheduled {slot} upload error'))
            return False, 'Upload failed'
    except Exception as e:
        execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                (base_name, None, 'FAILED', f'scheduled {slot} {e}'))
        return False, f'Error {e}'

# -------------------------
# Auto-restore after autosleep reset detection
# -------------------------
def _is_probably_fresh_seed_db():
    """Heuristik baru: anggap DB fresh bila:
    - Jumlah user <= 2 (seed default)
    - backup_log kosong
    - record_notes kosong (opsional penanda manual)
    Tidak lagi bergantung pada tabel 'projects' yang sudah dihapus.
    """
    try:
        user_cnt = fetchone("SELECT COUNT(*) c FROM users")['c']
        if user_cnt > 2:
            return False
        bkup_cnt = fetchone("SELECT COUNT(*) c FROM backup_log")['c']
        if bkup_cnt > 0:
            return False
        try:
            notes_cnt = fetchone("SELECT COUNT(*) c FROM record_notes")['c']
            if notes_cnt > 0:
                return False
        except Exception:
            # Jika tabel belum ada, abaikan
            pass
        return True
    except Exception:
        return False

def _pick_latest_drive_backup_file(service, folder_id):
    try:
        files = list_files_in_folder(service, folder_id)
    except Exception:
        return None
    if not files:
        return None
    candidates = [f for f in files if f.get('name','').endswith('.sqlite') or f.get('name','').endswith('.db')]
    if not candidates:
        return None
    try:
        candidates.sort(key=lambda x: x.get('modifiedTime',''), reverse=True)
    except Exception:
        pass
    return candidates[0]

def attempt_auto_restore_if_seed(service, folder_id=FOLDER_ID_DEFAULT):
    """Jika diaktifkan & terdeteksi DB fresh, restore otomatis dari backup Drive terbaru sekali per sesi."""
    if get_setting('auto_restore_enabled', 'true') != 'true':
        return False, 'Auto-restore disabled'
    if st.session_state.get('auto_restore_attempted'):
        return False, 'Already attempted'
    st.session_state['auto_restore_attempted'] = True
    if not _is_probably_fresh_seed_db():
        return False, 'DB not fresh'
    latest = _pick_latest_drive_backup_file(service, folder_id)
    if not latest:
        return False, 'No backup found'
    fid = latest.get('id'); fname = latest.get('name')
    try:
        data = download_file_bytes(service, fid)
        if not data.startswith(b'SQLite format 3\x00'):
            return False, 'Invalid sqlite header'
        with open(DB_PATH, 'wb') as f:
            f.write(data)
        set_setting('auto_restore_last_file', fname)
        set_setting('auto_restore_last_time', datetime.utcnow().isoformat())
        return True, f'Restored from {fname}'
    except Exception as e:
        return False, f'Restore failed: {e}'

# -------------------------
# Google Drive Helper Functions
# -------------------------
def build_drive_service():
    """Load credentials from Streamlit secrets and build Drive service."""
    try:
        creds_dict = st.secrets["service_account"]
    except Exception:
        st.error("Secrets 'service_account' tidak ditemukan. Tambahkan di Streamlit Cloud.")
        st.stop()
    creds = service_account.Credentials.from_service_account_info(dict(creds_dict), scopes=SCOPES)
    service = build("drive", "v3", credentials=creds)
    return service, creds.service_account_email

def list_files_in_folder(service, folder_id):
    results = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    while True:
        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)",
            pageToken=page_token,
            pageSize=200,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results

def upload_bytes(service, folder_id, name, data_bytes, mimetype="application/octet-stream"):
    media = MediaIoBaseUpload(io.BytesIO(data_bytes), mimetype=mimetype, resumable=True)
    file_metadata = {"name": name, "parents": [folder_id]}
    try:
        created = service.files().create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True).execute()
        return created.get("id")
    except Exception as e:
        err_text = str(e)
        if 'File not found' in err_text:
            st.error("Folder tidak ditemukan atau akses ditolak. Pastikan Folder ID benar dan folder telah dishare ke service account.")
        elif 'storageQuotaExceeded' in err_text:
            st.error("Kuota penyimpanan Google Drive penuh untuk service account ini.")
        else:
            st.error(f"Gagal upload: {err_text}")
        return None

def upload_or_replace(service, folder_id, name, data_bytes, mimetype="application/octet-stream"):
    """Find a file with same name in folder; if exists update, else create. Return file id or None."""
    try:
        query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
        resp = service.files().list(q=query, spaces='drive', fields='files(id, name)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        existing = resp.get('files', [])
        media = MediaIoBaseUpload(io.BytesIO(data_bytes), mimetype=mimetype, resumable=True)
        if existing:
            fid = existing[0]['id']
            service.files().update(fileId=fid, media_body=media, supportsAllDrives=True).execute()
            return fid
        else:
            file_metadata = {"name": name, "parents": [folder_id]}
            created = service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
            return created.get('id')
    except Exception:
        return None

def download_file_bytes(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

def get_folder_metadata(service, folder_id):
    """Return (metadata, error_message)."""
    try:
        meta = service.files().get(fileId=folder_id, fields="id, name, mimeType, owners", supportsAllDrives=True).execute()
        if meta.get('mimeType') != 'application/vnd.google-apps.folder':
            return None, "ID tersebut bukan folder."
        return meta, None
    except Exception as e:
        if 'File not found' in str(e):
            return None, "Folder tidak ditemukan atau belum dibagikan ke service account."
        return None, f"Gagal memeriksa folder: {e}"

def delete_file(service, file_id):
    try:
        service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 404:
            st.error(f"File tidak ditemukan (ID: {file_id})")
        else:
            st.error(f"Gagal menghapus file: {e}")


# -------------------------
# Folder usage (size) helper
# -------------------------
def _format_bytes(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            if u == "B":
                return f"{int(size)} {u}"
            return f"{size:.2f} {u}"
        size /= 1024.0

def get_folder_usage_stats(service, folder_id: str, recursive: bool = True):
    """Hitung total ukuran file dalam folder (opsional termasuk subfolder).
    Mengembalikan dict: { total_bytes, file_count, folder_count, unknown_size_count }
    Catatan: File Google Docs/Sheets bisa tidak memiliki field 'size' sehingga dihitung ke unknown_size_count.
    """
    total_bytes = 0
    file_count = 0
    folder_count = 0
    unknown_size = 0

    page_token = None
    query = f"'{folder_id}' in parents and trashed=false"
    while True:
        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, mimeType, size)",
            pageToken=page_token,
            pageSize=200,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        for f in resp.get("files", []):
            mime = f.get("mimeType", "")
            if mime == 'application/vnd.google-apps.folder':
                folder_count += 1
                if recursive:
                    try:
                        sub = get_folder_usage_stats(service, f["id"], recursive=True)
                        total_bytes += sub["total_bytes"]
                        file_count += sub["file_count"]
                        folder_count += sub["folder_count"]
                        unknown_size += sub["unknown_size_count"]
                    except Exception:
                        # Abaikan error subfolder, lanjutkan
                        pass
            else:
                file_count += 1
                sz = f.get("size")
                if sz is not None:
                    try:
                        total_bytes += int(sz)
                    except Exception:
                        unknown_size += 1
                else:
                    unknown_size += 1
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return {
        "total_bytes": total_bytes,
        "file_count": file_count,
        "folder_count": folder_count,
        "unknown_size_count": unknown_size,
    }


# -------------------------
# Role checks
# -------------------------
def require_login():
    if not current_user():
        st.warning("Silakan login terlebih dahulu.")
        st.session_state.page = "Authentication"
        st.rerun()

def require_admin():
    u = current_user()
    # Backward compatibility: treat 'Superuser' as admin; map old 'admin' to Superuser if still present
    if not u or u.get("role") not in ("Superuser",):
        st.warning("Akses Superuser diperlukan.")
        # Optional: redirect non-admin users to dashboard/login
        if not u:
            st.session_state.page = "Authentication"
        else:
            st.session_state.page = "Dashboard"
        st.rerun()


def get_pending_users_count():
    return fetchone("SELECT COUNT(*) AS count FROM users WHERE approved=0")['count']


# -------------------------
# Centralized Access Control
# -------------------------
# Define roles
ALL_ROLES = ("Superuser", "Supervisor", "Tracer", "Agent")

# Central menu/page configuration and allowed roles
MENU_ITEMS = [
    {"label": "Dashboard",  "page": "Dashboard", "roles": ALL_ROLES, "primary": True},
    {"label": "Supervisor", "page": "Supervisor", "roles": ("Superuser", "Supervisor"), "primary": False},
    {"label": "Tracer",     "page": "Tracer", "roles": ("Superuser", "Supervisor", "Tracer"), "primary": False},
    {"label": "Agent",      "page": "Agent", "roles": ("Superuser", "Supervisor","Agent"), "primary": False},
    {"label": "G Drive",    "page": "G Drive", "roles": ("Superuser", "Supervisor"), "primary": True},
    {"label": "User Setting","page": "User Setting", "roles": ALL_ROLES, "primary": False},
    {"label": "Audit Log",  "page": "Audit Log", "roles": ("Superuser", "Supervisor","Tracer","Agent"), "primary": False},
]

def can_access_page(page_name, user_obj) -> bool:
    if not user_obj:
        return False
    role = user_obj.get('role')
    for item in MENU_ITEMS:
        if item['page'] == page_name:
            return role in item['roles']
    # Default: if page not listed, fall back to logged-in users only
    return True

def first_allowed_page_for_role(role):
    for item in MENU_ITEMS:
        if role in item['roles']:
            return item['page']
    return "User Setting"

def require_roles(allowed_roles):
    u = current_user()
    if not u:
        require_login()
        return
    if u.get('role') not in allowed_roles:
        st.warning("Akses ditolak untuk role Anda.")
        st.session_state.page = first_allowed_page_for_role(u.get('role', ''))
        st.rerun()



# ... (page_auth, page_dashboard, page_resume, page_reporting, page_admin_panel, page_user_guide and main function remain the same) ...
def page_auth():
    # Set layout non-wide khusus halaman login
    try:
        st.set_page_config(layout="centered")
    except Exception:
        pass  # Sudah pernah dipanggil di awal, abaikan error
    # Always use non-wide mode on login/register page
    # Sembunyikan sidebar dengan CSS hack
    st.markdown("""
        <style>
        [data-testid="stSidebar"] {display: none !important;}
        </style>
    """, unsafe_allow_html=True)
    # Tampilkan logo sebagai header
    st.image("logo.png", width=180)
    st.title("Authentication")
    st.markdown("---")
    tab = st.tabs(["Login", "Register"])
    
    if "login_status_message" not in st.session_state:
        st.session_state.login_status_message = {"type": None, "text": ""}

    with tab[0]:
        st.subheader("Login")
        login_id = st.text_input("Id", key="login_id")
        pw = st.text_input("Password", type="password", key="login_pw")
        login_clicked = st.button("Login", use_container_width=True)

        if login_clicked:
            st.session_state.login_status_message = {"type": None, "text": ""}
            # Login by Id (login_id); fallback to email for backward compatibility
            row = fetchone("SELECT * FROM users WHERE login_id=?", (login_id,))
            if not row and login_id:
                row = fetchone("SELECT * FROM users WHERE email=?", (login_id,))
            if not row:
                st.session_state.login_status_message = {"type": "error", "text": "User tidak ditemukan."}
            else:
                if not row['approved']:
                    st.session_state.login_status_message = {"type": "error", "text": "Akun belum disetujui oleh Admin."}
                elif verify_password(pw, row['password_hash']):
                    login_user(row)
                    # Catat audit trail login
                    try:
                        detail_id = row.get('login_id') or row.get('email') or '-'
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (row['id'], "LOGIN", f"User {detail_id} login."))
                    except Exception:
                        pass
                    # Backup on successful login (best-effort)
                    try:
                        if "service_account" in st.secrets:
                            service_b, _ = build_drive_service()
                            ok_b, msg_b = perform_backup(service_b, FOLDER_ID_DEFAULT)
                            st.session_state['last_login_backup'] = {
                                'ok': ok_b,
                                'msg': msg_b,
                                'time': datetime.utcnow().isoformat()
                            }
                            # Tampilkan info singkat tanpa menghalangi redirect
                            if ok_b:
                                st.toast("Backup otomatis saat login berhasil.")
                            else:
                                st.toast("Backup saat login gagal atau dibatalkan.")
                    except Exception as e:
                        st.session_state['last_login_backup'] = {
                            'ok': False,
                            'msg': f'Backup saat login error: {e}',
                            'time': datetime.utcnow().isoformat()
                        }
                    st.session_state.login_status_message = {"type": "success", "text": "Login berhasil. Mengalihkan..."}
                    st.session_state.page = "Dashboard"
                    st.rerun()
                else:
                    st.session_state.login_status_message = {"type": "error", "text": "Password salah."}

        if st.session_state.login_status_message["type"] == "error":
            st.error(st.session_state.login_status_message["text"])
        elif st.session_state.login_status_message["type"] == "success":
            st.success(st.session_state.login_status_message["text"])

    with tab[1]:
        st.subheader("Register")
        reg_id = st.text_input("Id (untuk login)", key="reg_login_id", placeholder="misal: johndoe")
        full_name = st.text_input("Full name", key="reg_full_name")
        email_r = st.text_input("Email", key="reg_email")
        # Department removed
        pw1 = st.text_input("Password", type="password", key="reg_pw1")
        pw2 = st.text_input("Confirm Password", type="password", key="reg_pw2")
        if st.button("Register", use_container_width=True):
            if not reg_id or not full_name or not pw1:
                st.error("Isi semua data.")
            elif pw1 != pw2:
                st.error("Password dan konfirmasi tidak cocok.")
            else:
                try:
                    # Default role for new registration is Agent (awaiting approval)
                    uid = execute(
                        "INSERT INTO users (login_id, full_name, name, email, password_hash, role, approved) VALUES (?,?,?,?,?,?,?)",
                        (reg_id.strip(), full_name.strip(), full_name.strip(), (email_r.strip() or None), hash_password(pw1), "Agent", 0)
                    )
                    # Audit log registration
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (uid, "REGISTER", f"User {reg_id.strip()} registered."))
                    except Exception:
                        pass
                    st.success("Registrasi berhasil. Tunggu approval Admin.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal register: {e}")

def page_gdrive():
    require_roles(ALL_ROLES)
    st.header("📂 Google Drive Files")
    try:
        service, _sa_email = build_drive_service()
    except Exception:
        return
    # Hardcoded folder ID per permintaan user
    folder_id = FOLDER_ID_DEFAULT
    meta, meta_err = get_folder_metadata(service, folder_id)
    if meta_err:
        st.error(meta_err)
        st.info("Pastikan folder dengan ID di-hardcode sudah dishare ke service account sebagai Editor.")
        return
    st.markdown(f"Aktif Folder: **{meta.get('name')}** (`{folder_id}`)")
    
    # Banner kapasitas
    try:
        usage_head = get_folder_usage_stats(service, folder_id, recursive=True)
        used_head = int(usage_head.get('total_bytes', 0))
    except Exception:
        used_head = 0
    capacity = get_project_capacity_bytes()
    if used_head >= capacity:
        st.error("Kapasitas proyek mencapai batas maksimum 2GB (exceed/max capacity). Nonaktifkan upload/backup sampai ada ruang.")
    else:
        remain_head = capacity - used_head
        st.caption(f"Penggunaan: {_format_bytes(used_head)} / {_format_bytes(capacity)} · Sisa: {_format_bytes(remain_head)}")

    tabs = st.tabs(["List", "Upload file", "Download", "Delete", "Sync DB", "Audit Log", "Record", "Drive Usage"])
    # Record Tab
    with tabs[6]:
        st.subheader('📝 Record Catatan Manual')
        user = current_user()
        # Add new note
        with st.form('add_note_form'):
            new_note = st.text_input('Catatan baru', key='new_note_input')
            submitted = st.form_submit_button('Tambah Catatan')
            if submitted and new_note.strip():
                creator = (user.get('login_id') or user.get('email') or '-') if user else '-'
                execute("INSERT INTO record_notes (note, created_by) VALUES (?, ?)", (new_note.strip(), creator))
                st.success('Catatan ditambahkan.')
                st.rerun()
        # List notes
        notes = fetchall("SELECT * FROM record_notes ORDER BY id DESC LIMIT 50")
        if not notes:
            st.info('Belum ada catatan.')
        else:
            df_notes = pd.DataFrame(notes)
            st.dataframe(df_notes[['id','note','created_by','created_at']], use_container_width=True, hide_index=True)
            # Edit/delete per row
            for idx, row in enumerate(notes):
                col1, col2 = st.columns([2,1])
                with col1:
                    edit_val = st.text_input(f"Edit Catatan #{row['id']}", value=row['note'], key=f"edit_note_{row['id']}")
                with col2:
                    if st.button(f"Simpan Edit #{row['id']}", key=f"save_edit_{row['id']}"):
                        execute("UPDATE record_notes SET note=? WHERE id=?", (edit_val.strip(), row['id']))
                        st.success('Catatan diperbarui.')
                        st.rerun()
                    if st.button(f"Hapus #{row['id']}", key=f"delete_note_{row['id']}"):
                        execute("DELETE FROM record_notes WHERE id=?", (row['id'],))
                        st.warning('Catatan dihapus.')
                        st.rerun()

    # List Tab
    with tabs[0]:
        st.subheader("Daftar File")
        # Manual trigger backup (Superuser only)
        u = current_user()
        if u and u.get('role') == 'Superuser':
            if st.button('🚀 Trigger Auto Backup Sekarang'):
                ok, msg = perform_backup(service, folder_id)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
        # Show last 5 backup logs
        logs = fetchall("SELECT * FROM backup_log ORDER BY id DESC LIMIT 5")
        if logs:
            st.markdown("**Riwayat Backup Terbaru:**")
            for lg in logs:
                st.markdown(f"- {lg['backup_time']} | {lg['file_name']} | {lg['status']}")

            st.markdown("---")
            st.markdown("### ⚙️ Pengaturan Scheduled Backup")
            enabled_flag = get_setting('scheduled_backup_enabled', 'false') == 'true'
            col_sb1, col_sb2 = st.columns([1,2])
            with col_sb1:
                enable_toggle = st.checkbox("Aktifkan Jadwal", value=enabled_flag, key='sched_enable')
            default_name = get_setting('scheduled_backup_filename', 'scheduled_backup.sqlite') or 'scheduled_backup.sqlite'
            with col_sb2:
                new_name = st.text_input("Nama File Backup (overwrite)", value=default_name, key='sched_filename')
            if st.button("Simpan Pengaturan Jadwal"):
                set_setting('scheduled_backup_enabled', 'true' if enable_toggle else 'false')
                set_setting('scheduled_backup_filename', new_name.strip() or 'scheduled_backup.sqlite')
                st.success("Pengaturan jadwal disimpan.")
            st.markdown("### ♻️ Auto-Restore Saat Wake (Autosleep)")
            ar_enabled = get_setting('auto_restore_enabled','true') == 'true'
            col_ar1, col_ar2 = st.columns([1,2])
            with col_ar1:
                ar_toggle = st.checkbox('Aktifkan Auto-Restore', value=ar_enabled, key='auto_restore_toggle')
            last_ar_file = get_setting('auto_restore_last_file','-')
            last_ar_time = get_setting('auto_restore_last_time','-')
            with col_ar2:
                st.caption(f"Terakhir restore: {last_ar_file} pada {last_ar_time}")
            if st.button('Simpan Auto-Restore'):
                set_setting('auto_restore_enabled', 'true' if ar_toggle else 'false')
                st.success('Pengaturan auto-restore disimpan.')
            st.caption('Auto-restore akan mencoba mendeteksi DB fresh (reset) dan mengganti otomatis dengan backup Drive terbaru sekali per sesi admin pertama yang login.')
            # --- Dynamic Slot Editor ---
            with st.expander("🕒 Edit Slot Jadwal (Advanced)", expanded=False):
                st.markdown("""
                Atur slot jadwal backup tanpa perlu menulis JSON. Setiap slot menentukan rentang jam lokal (0-23).\
                Jika Start > End maka dianggap melewati tengah malam (wrap). Contoh: 23 -> 6.\
                Tidak boleh ada dua slot yang saling tumpang tindih pada jam yang sama.\
                """)
                hours = list(range(24))
                # Ambil slot saat ini dari setting / default
                if 'slot_editor_state' not in st.session_state:
                    st.session_state.slot_editor_state = get_schedule_slots()
                slots_state = st.session_state.slot_editor_state

                # Tampilkan form per slot
                to_remove_indexes = []
                for idx, slot_obj in enumerate(slots_state):
                    with st.container():
                        c1,c2,c3,c4 = st.columns([1,1,2,0.6])
                        with c1:
                            slots_state[idx]['start'] = c1.selectbox(
                                'Start', hours, index=hours.index(int(slot_obj['start'])), key=f'slot_start_{idx}')
                        with c2:
                            slots_state[idx]['end'] = c2.selectbox(
                                'End', hours, index=hours.index(int(slot_obj['end'])), key=f'slot_end_{idx}')
                        with c3:
                            slots_state[idx]['name'] = c3.text_input('Nama Slot', value=slot_obj['name'], key=f'slot_name_{idx}')
                        with c4:
                            if st.button('🗑️', key=f'del_slot_{idx}'):
                                to_remove_indexes.append(idx)
                    st.markdown("")
                # Hapus slot yang diminta
                if to_remove_indexes:
                    for ridx in sorted(to_remove_indexes, reverse=True):
                        if 0 <= ridx < len(slots_state):
                            slots_state.pop(ridx)
                    st.rerun()

                st.markdown("**Tambah Slot Baru**")
                col_new1, col_new2, col_new3, col_new4 = st.columns([1,1,2,0.8])
                new_start = col_new1.selectbox('Start', hours, key='new_slot_start')
                new_end = col_new2.selectbox('End', hours, index=hours.index((new_start+1) % 24), key='new_slot_end')
                new_name = col_new3.text_input('Nama Slot', key='new_slot_name', placeholder='misal: slot_dawn')
                if col_new4.button('➕ Tambah'):
                    if new_name.strip() == '':
                        st.error('Nama slot tidak boleh kosong.')
                    elif any(s['name'] == new_name.strip() for s in slots_state):
                        st.error('Nama slot harus unik.')
                    elif new_start == new_end:
                        st.error('Start dan End tidak boleh sama (durasi 0).')
                    else:
                        slots_state.append({'start': int(new_start), 'end': int(new_end), 'name': new_name.strip()})
                        st.success('Slot ditambahkan.')
                        st.rerun()

                # Validasi overlap & struktur sebelum simpan
                def _hours_covered(slot):
                    st_h = int(slot['start']); en_h = int(slot['end'])
                    if st_h < en_h:
                        return list(range(st_h, en_h))
                    else:  # wrap
                        return list(range(st_h,24)) + list(range(0,en_h))

                def _check_overlaps(slots):
                    hour_map = {}  # hour -> slot names
                    for s in slots:
                        for h in _hours_covered(s):
                            hour_map.setdefault(h, set()).add(s['name'])
                    conflicts = {h:n for h,n in hour_map.items() if len(n) > 1}
                    return conflicts

                save_col, reset_col, export_col = st.columns([1,1,1])
                with save_col:
                    if st.button('💾 Simpan Slot Jadwal', key='save_slots_btn'):
                        # Basic structure validation
                        if not _validate_slot_struct(slots_state):
                            st.error('Struktur slot tidak valid (nama unik, rentang jam 0-23, start != end).')
                        else:
                            conflicts = _check_overlaps(slots_state)
                            if conflicts:
                                conflict_msgs = []
                                for h, names in sorted(conflicts.items()):
                                    conflict_msgs.append(f"Jam {h}: {' , '.join(sorted(names))}")
                                st.error('Terdapat tumpang tindih slot:\n' + '\n'.join(conflict_msgs))
                            else:
                                set_setting('scheduled_backup_slots_json', json.dumps(slots_state))
                                st.success('Slot jadwal tersimpan ke konfigurasi.')
                with reset_col:
                    if st.button('♻️ Reset Default', key='reset_slots_btn'):
                        st.session_state.slot_editor_state = DEFAULT_SCHEDULE_SLOTS.copy()
                        set_setting('scheduled_backup_slots_json', json.dumps(DEFAULT_SCHEDULE_SLOTS))
                        st.info('Slot dikembalikan ke default.')
                        st.rerun()
                with export_col:
                    if st.button('📄 Lihat JSON', key='export_slots_btn'):
                        st.code(json.dumps(slots_state, indent=2))

                # Preview ringkas
                if slots_state:
                    st.markdown("**Preview Slot Aktif**")
                    prev_df = pd.DataFrame(slots_state)
                    # Durasi jam (approx) hanya untuk info
                    def _dur(srow):
                        st_h=int(srow['start']); en_h=int(srow['end'])
                        return (en_h-st_h) if st_h < en_h else ((24-st_h)+en_h)
                    prev_df['duration_h'] = prev_df.apply(_dur, axis=1)
                    st.dataframe(prev_df[['name','start','end','duration_h']], use_container_width=True, hide_index=True)
                st.caption("Catatan: Backup akan dijalankan sekali per slot saat ada interaksi admin (page refresh / navigasi).")
            last_slot = get_setting('scheduled_backup_last_slot', '-')
            last_date = get_setting('scheduled_backup_last_date', '-')
            st.caption(f"Slot terakhir: {last_slot} pada {last_date}")
            if st.button("Paksa Backup Slot Saat Ini"):
                try:
                    okf, msgf = check_scheduled_backup(service, folder_id)
                    if okf:
                        st.success(msgf)
                    else:
                        st.info(msgf)
                except Exception as e:
                    st.error(f"Gagal paksa backup: {e}")

    # Audit Log Tab
    with tabs[5]:
        st.subheader('📝 Audit Log Login')
        logs = fetchall("SELECT audit_logs.timestamp, COALESCE(users.full_name, users.name) AS full_name, users.login_id, users.email FROM audit_logs JOIN users ON audit_logs.user_id = users.id WHERE audit_logs.action='LOGIN' ORDER BY audit_logs.id DESC LIMIT 50")
        if not logs:
            st.info('Belum ada catatan login.')
        else:
            df = pd.DataFrame(logs)
            # reorder columns if exist
            cols = [c for c in ["timestamp","full_name","login_id","email"] if c in df.columns]
            st.dataframe(df[cols] if cols else df, use_container_width=True, hide_index=True)
        try:
            files = list_files_in_folder(service, folder_id)
        except Exception as e:
            st.error(f"Gagal mengambil daftar file: {e}")
            return
        if not files:
            st.info("Folder kosong.")
        else:
            df = pd.DataFrame(files)
            if 'size' in df.columns:
                def nice_size(s):
                    try:
                        s = int(s)
                    except Exception:
                        return '-'
                    for unit in ['B','KB','MB','GB']:
                        if s < 1024:
                            return f"{s}{unit}"
                        s //= 1024
                    return f"{s}TB"
                df['size'] = df['size'].apply(nice_size)
            st.dataframe(df[['name','id','mimeType','createdTime','modifiedTime'] + ([ 'size'] if 'size' in df.columns else [])], use_container_width=True, hide_index=True)

        st.markdown('---')
        st.subheader('Backup Database ke Drive')
        if st.button('📤 Export Database ke Drive'):
            if os.path.exists(DB_PATH):
                try:
                    with open(DB_PATH,'rb') as f:
                        data = f.read()
                    # Check capacity before creating a new timestamped backup file
                    try:
                        usage_now = get_folder_usage_stats(service, folder_id, recursive=True)
                        used_now = int(usage_now.get('total_bytes', 0))
                    except Exception:
                        used_now = 0
                    cap = get_project_capacity_bytes()
                    if used_now >= cap:
                        st.error("Gagal upload: kapasitas maksimum tercapai (exceed/max capacity).")
                        return
                    if used_now + len(data) > cap:
                        st.error("Gagal upload: ukuran backup akan melebihi kapasitas maksimum.")
                        return
                    backup_name = f"backup_db_{time.strftime('%Y%m%d_%H%M%S')}.sqlite"
                    fid = upload_bytes(service, folder_id, backup_name, data, mimetype='application/x-sqlite3')
                    if fid:
                        st.success(f"Database berhasil diupload sebagai {backup_name} (ID: {fid})")
                    else:
                        st.error("Gagal mengupload database.")
                except Exception as e:
                    st.error(f"Error saat membaca / upload DB: {e}")
            else:
                st.error(f"File database '{DB_PATH}' tidak ditemukan.")

    # Upload Tab
    with tabs[1]:
        st.subheader('Upload File Baru')
        uploaded = st.file_uploader('Pilih file')
        if uploaded and st.button('Upload ke Drive'):
            data = uploaded.read()
            # Capacity guard: adding a new file increases usage
            try:
                usage_now = get_folder_usage_stats(service, folder_id, recursive=True)
                used_now = int(usage_now.get('total_bytes', 0))
            except Exception:
                used_now = 0
            cap = get_project_capacity_bytes()
            user = current_user()
            if used_now >= cap:
                st.error("Upload dibatalkan: kapasitas maksimum tercapai (exceed/max capacity).")
            elif used_now + len(data) > cap:
                st.error("Upload dibatalkan: file ini akan melebihi kapasitas maksimum.")
            else:
                fid = upload_bytes(service, folder_id, uploaded.name, data, mimetype=uploaded.type or 'application/octet-stream')
                if fid:
                    st.success(f"File '{uploaded.name}' terupload (ID: {fid})")
                    # Audit log upload
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (user.get('id') if user else None, "UPLOAD", f"Uploaded file '{uploaded.name}' to Drive (ID: {fid})"))
                    except Exception:
                        pass

    # Download Tab
    with tabs[2]:
        st.subheader('Download File')
        files_all = list_files_in_folder(service, folder_id)
        if not files_all:
            st.info('Folder kosong.')
        else:
            name_to_id = {f['name']: f['id'] for f in files_all}
            sel_name = st.selectbox('Pilih file', list(name_to_id.keys()))
            if st.button('Download file'):
                try:
                    data = download_file_bytes(service, name_to_id[sel_name])
                    st.download_button('Klik untuk download', data=data, file_name=sel_name)
                except Exception as e:
                    st.error(f"Gagal download: {e}")

    # Delete Tab
    with tabs[3]:
        st.subheader('Hapus File')
        files_all = list_files_in_folder(service, folder_id)
        if not files_all:
            st.info('Folder kosong.')
        else:
            name_to_id = {f['name']: f['id'] for f in files_all}
            sel_name = st.selectbox('Pilih file untuk dihapus', list(name_to_id.keys()))
            if st.button('Hapus file'):
                user = current_user()
                try:
                    delete_file(service, name_to_id[sel_name])
                    st.success(f"File '{sel_name}' dihapus.")
                    # Audit log delete
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (user.get('id') if user else None, "DELETE", f"Deleted file '{sel_name}' from Drive."))
                    except Exception:
                        pass
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal hapus: {e}")

    # Sync DB Tab
    with tabs[4]:
        st.subheader('🔄 Sinkronisasi Database')
        st.markdown("Gunakan fitur ini untuk: 1) Mengunggah file database (.sqlite) baru dan menggantikan database lokal. 2) Merestore database lokal dari salinan yang ada di Google Drive.")
        st.warning("Pastikan Anda benar-benar paham dampaknya. Selalu lakukan backup sebelum replace.")

        col_upload, col_restore = st.columns(2)

        # --- Upload & Replace Local DB ---
        with col_upload:
            st.markdown("### ⬆️ Upload & Ganti DB Lokal")
            up_db = st.file_uploader("Pilih file .sqlite", type=["sqlite","db"], key="sync_upload_sqlite")
            auto_push = st.checkbox("Juga upload file ini ke Drive setelah replace", value=True, key="sync_auto_push")
            if up_db and st.button("Replace Database Lokal", type="primary"):
                try:
                    data = up_db.read()
                    # Validasi header sqlite
                    if not data.startswith(b"SQLite format 3\x00"):
                        st.error("File bukan database SQLite yang valid.")
                    else:
                        ts = time.strftime('%Y%m%d_%H%M%S')
                        # Backup lokal lama jika ada
                        if os.path.exists(DB_PATH):
                            backup_local = f"local_backup_before_replace_{ts}.sqlite"
                            try:
                                with open(DB_PATH,'rb') as oldf, open(backup_local,'wb') as newf:
                                    newf.write(oldf.read())
                                st.info(f"Backup lokal lama tersimpan: {backup_local}")
                            except Exception as e:
                                st.error(f"Gagal membuat backup lokal: {e}")
                        # Tulis DB baru
                        with open(DB_PATH,'wb') as fnew:
                            fnew.write(data)
                        st.success("Database lokal berhasil diganti dengan file yang diupload.")
                        # Optional push ke Drive
                        if auto_push:
                            fname_drive = f"uploaded_db_{ts}.sqlite"
                            fid = upload_bytes(service, folder_id, fname_drive, data, mimetype='application/x-sqlite3')
                            if fid:
                                st.success(f"Salinan diupload ke Drive sebagai {fname_drive} (ID: {fid})")
                            else:
                                st.error("Gagal mengupload salinan ke Drive.")
                        st.info("Silakan refresh halaman atau navigasi ulang untuk memastikan app memakai DB baru.")
                except Exception as e:
                    st.error(f"Gagal mengganti database: {e}")

        # --- Restore From Drive ---
        with col_restore:
            st.markdown("### ⬇️ Restore dari Drive")
            try:
                drive_files = list_files_in_folder(service, folder_id)
            except Exception as e:
                drive_files = []
                st.error(f"Tidak bisa mengambil daftar file Drive: {e}")
            # Filter file sqlite/db setelah mencoba mengambil daftar file
            sqlite_files = [
                f for f in drive_files
                if f.get('name','').endswith('.sqlite') or f.get('name','').endswith('.db')
            ]
            if not sqlite_files:
                st.info("Tidak ada file .sqlite / .db di folder Drive.")
            else:
                # Urutkan terbaru berdasarkan modifiedTime
                try:
                    sqlite_files.sort(key=lambda x: x.get('modifiedTime',''), reverse=True)
                except Exception:
                    pass
                name_to_id_restore = {f["name"]: f["id"] for f in sqlite_files}
                sel_restore = st.selectbox("Pilih file DB di Drive", list(name_to_id_restore.keys()), key="restore_sel_db")
                if st.button("Restore Database Lokal dari Drive", type="primary"):
                    try:
                        fid = name_to_id_restore[sel_restore]
                        data = download_file_bytes(service, fid)
                        if not data.startswith(b"SQLite format 3\x00"):
                            st.error("File di Drive bukan database SQLite valid.")
                        else:
                            ts = time.strftime('%Y%m%d_%H%M%S')
                            if os.path.exists(DB_PATH):
                                backup_local = f"local_backup_before_restore_{ts}.sqlite"
                                try:
                                    with open(DB_PATH,'rb') as oldf, open(backup_local,'wb') as newf:
                                        newf.write(oldf.read())
                                    st.info(f"Backup lokal lama tersimpan: {backup_local}")
                                except Exception as e:
                                    st.error(f"Gagal membuat backup lokal: {e}")
                            with open(DB_PATH,'wb') as fnew:
                                fnew.write(data)
                            st.success(f"Database lokal berhasil direstore dari '{sel_restore}'.")
                            st.info("Reload halaman untuk memakai DB baru.")
                    except Exception as e:
                        st.error(f"Gagal restore: {e}")

    # Drive Usage Tab
    with tabs[7]:
        st.subheader('📊 Drive Usage')
        CAPACITY_BYTES = get_project_capacity_bytes()  # default 2 GB
        try:
            usage_du = get_folder_usage_stats(service, folder_id, recursive=True)
            used_bytes = int(usage_du.get('total_bytes', 0))
            unknown_ct = int(usage_du.get('unknown_size_count', 0))
            folder_ct = int(usage_du.get('folder_count', 0))
            file_ct = int(usage_du.get('file_count', 0))
        except Exception as e:
            st.error(f"Tidak bisa menghitung penggunaan folder: {e}")
            used_bytes = 0
            unknown_ct = 0
            folder_ct = 0
            file_ct = 0

        # Metrics summary
        colA, colB, colC = st.columns([1,1,1])
        with colA:
            st.metric(label="Used", value=_format_bytes(used_bytes))
        with colB:
            st.metric(label="Capacity", value=_format_bytes(CAPACITY_BYTES))
        with colC:
            pct = (used_bytes / CAPACITY_BYTES * 100.0) if CAPACITY_BYTES > 0 else 0.0
            st.metric(label="Usage", value=f"{min(pct,100):.1f}%")

        # Progress bar (quick visual)
        st.progress(min(pct/100.0, 1.0))

        # Altair stacked bar used vs free
        used_clamped = min(used_bytes, CAPACITY_BYTES)
        free_bytes = max(CAPACITY_BYTES - used_clamped, 0)
        df_bar = pd.DataFrame([
            {"category": "Used", "bytes": used_clamped},
            {"category": "Free", "bytes": free_bytes},
        ])
        # Blue-brown theme colors
        color_scale = alt.Scale(domain=["Used", "Free"], range=["#8D6E63", "#1E88E5"]) if CAPACITY_BYTES > 0 else alt.Undefined
        bar = (
            alt.Chart(df_bar)
            .mark_bar(height=36)
            .encode(
                x=alt.X('bytes:Q', stack=None, title=None, scale=alt.Scale(domain=[0, CAPACITY_BYTES])),
                color=alt.Color('category:N', scale=color_scale, legend=alt.Legend(orient='bottom')),
                tooltip=[
                    alt.Tooltip('category:N', title='Jenis'),
                    alt.Tooltip('bytes:Q', title='Bytes', format=',')
                ],
            )
            .properties(width=700)
        )
        st.altair_chart(bar, use_container_width=True)

        if used_bytes >= CAPACITY_BYTES:
            over = max(used_bytes - CAPACITY_BYTES, 0)
            if over > 0:
                st.error(f"Penggunaan melebihi kapasitas: kelebihan {_format_bytes(over)} (exceed)")
            else:
                st.error("Penggunaan mencapai batas maksimum (max capacity).")
        else:
            remain = CAPACITY_BYTES - used_bytes
            st.caption(f"Sisa kapasitas: {_format_bytes(remain)}")

        # Extra info
        st.caption(f"Rincian: {file_ct} file · {folder_ct} folder · {unknown_ct} item tanpa ukuran.")
        
        # Contact for capacity increase
        st.markdown(
            "Butuh kapasitas lebih? Hubungi email: "
            "[Primetroyxs@gmail.com](mailto:Primetroyxs@gmail.com) atau WhatsApp: "
            "[+6289524257778](https://wa.me/6289524257778)"
        )
    
def main():
    init_db()

    # Pre-login auto-restore attempt (hanya sekali per sesi sebelum login)
    if "prelogin_auto_restore_done" not in st.session_state:
        # Hanya coba bila auto-restore diaktifkan & DB terindikasi fresh
        if get_setting('auto_restore_enabled', 'true') == 'true' and _is_probably_fresh_seed_db():
            try:
                service_pre, _ = build_drive_service()
                ok_pre, msg_pre = attempt_auto_restore_if_seed(service_pre, FOLDER_ID_DEFAULT)
                st.session_state['prelogin_auto_restore_result'] = {
                    'success': ok_pre,
                    'message': msg_pre,
                    'time': datetime.utcnow().isoformat()
                }
                # Sinkronkan flag lama agar blok admin tidak mencoba ulang
                st.session_state['auto_restore_checked'] = 'restored' if ok_pre else 'checked'
            except Exception as e:
                st.session_state['prelogin_auto_restore_result'] = {
                    'success': False,
                    'message': f'Auto-Restore error: {e}',
                    'time': datetime.utcnow().isoformat()
                }
        else:
            st.session_state['prelogin_auto_restore_result'] = {
                'success': False,
                'message': 'Lewati auto-restore (tidak diaktifkan atau DB tidak fresh)',
                'time': datetime.utcnow().isoformat()
            }
        st.session_state['prelogin_auto_restore_done'] = True
        # Jika benar-benar ada proses restore (berhasil / gagal) tampilkan halaman status.
        # Jika hanya skip (Lewati auto-restore...) langsung ke halaman login.
        msg_prelogin = st.session_state['prelogin_auto_restore_result'].get('message','')
        if msg_prelogin.startswith('Lewati auto-restore'):
            st.session_state.page = 'Authentication'
        else:
            st.session_state.page = 'RestoreStatus'
    
    # Reset flags lama jika user kembali ke halaman login setelah selesai
    if "page" not in st.session_state:
        st.session_state.page = "Authentication"
    if "user" not in st.session_state:
        st.session_state.user = None


    user = current_user()

    # Sidebar minimal: hanya autentikasi & G Drive
    st.sidebar.image("logo.png", use_container_width=True)
    st.sidebar.title("Navigasi")
    # Global sidebar button style: force white buttons for consistency
    st.sidebar.markdown(
        """
        <style>
        /* Base style: white buttons, uniform size */
        div[data-testid="stSidebar"] .stButton { margin-bottom: 6px; }
        div[data-testid="stSidebar"] .stButton > button {
            background-color: #ffffff !important;
            color: #111111 !important;
            border: 1px solid #E0E0E0 !important;
            border-radius: 8px !important;
            padding: 8px 12px !important;
            min-height: 40px !important;
            width: 100% !important;
            box-shadow: none !important;
            text-align: left !important;
        }
        /* Hover */
        div[data-testid="stSidebar"] .stButton > button:hover {
            border-color: #BDBDBD !important;
            background-color: #FAFAFA !important;
        }
        /* Active (use disabled button as current-page highlight) */
        div[data-testid="stSidebar"] .stButton > button:disabled {
            background-color: #E8F0FE !important; /* light blue */
            border-color: #1A73E8 !important;
            color: #1A73E8 !important;
            opacity: 1 !important; /* keep readable */
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if user:
        # Info singkat user
        disp_name = user.get('full_name') or user.get('name') or user.get('login_id')
        st.sidebar.markdown(f"**👤 {disp_name}**")
        if user.get('login_id'):
            st.sidebar.caption(f"Id: {user['login_id']}")
        if user.get('email'):
            st.sidebar.markdown(f"✉️ {user['email']}")
        st.sidebar.markdown(f"**Role:** {user['role'].capitalize()}")
        st.sidebar.markdown("---")
        # Navigasi utama setelah login (centralized) — gunakan button putih seragam; aktif di-highlight
        allowed_items = [it for it in MENU_ITEMS if can_access_page(it['page'], user)]
        for it in allowed_items:
            is_active = (st.session_state.page == it['page'])
            clicked = st.sidebar.button(it['label'], key=f"nav_{it['page']}", use_container_width=True, disabled=is_active)
            if clicked and not is_active:
                st.session_state.page = it['page']
                st.rerun()
        st.sidebar.button("Logout", on_click=logout_user, use_container_width=True)
        st.sidebar.markdown("---")
    elif st.session_state.page != 'RestoreStatus':
        if st.sidebar.button("🔐 Login / Register", use_container_width=True):
            st.session_state.page = "Authentication"


        # --- Improved: Guarantee Auto-Restore before Auto-Backup ---
        # Saat belum login tidak perlu menjalankan logic auto-backup / auto-restore tambahan
        # dan tidak menampilkan tombol G Drive / Logout yang membingungkan.
        # Logic auto restore awal sudah dilakukan sebelum halaman login (RestoreStatus page).
        pass
    
    # Halaman status restore (sebelum login) bila baru saja wake & mencoba restore
    if st.session_state.page == 'RestoreStatus' and not user:
        st.title('⏳ Memeriksa / Memulihkan Database')
        res = st.session_state.get('prelogin_auto_restore_result', {})
        if res.get('success'):
            st.success(f"Berhasil restore otomatis: {res.get('message','')} ")
        else:
            st.info(res.get('message','Tidak ada informasi restore.'))
        st.caption(f"Waktu: {res.get('time','-')}")
        st.markdown('---')
        if st.button('Lanjut ke Login »', type='primary'):
            st.session_state.page = 'Authentication'
            st.rerun()
        return

    if not user:
        page_auth()
        return


    if st.session_state.page == "Supervisor":
        page_supervisor()
        return
    if st.session_state.page == "Dashboard":
        page_dashboard()
        return
    if st.session_state.page == "Tracer":
        page_tracer()
        return
    if st.session_state.page == "Agent":
        page_agent()
        return
    if st.session_state.page == "G Drive":
        page_gdrive()
        return
    if st.session_state.page == "Audit Log":
        page_audit_log()
        return
    if st.session_state.page == "User Setting":
        page_user_setting()
        return
# -------------------------
# Audit Log Page
# -------------------------
def page_audit_log():
    require_roles(("Superuser", "Supervisor"))
    st.title("📋 Audit Log")
    st.caption("Semua aktivitas aplikasi direkam di sini. Waktu: GMT+07:00 (WIB)")
    # Query audit logs with user info
    rows = fetchall("""
        SELECT audit_logs.timestamp, COALESCE(users.full_name, users.name, users.login_id) AS user, audit_logs.action, audit_logs.details
        FROM audit_logs
        LEFT JOIN users ON audit_logs.user_id = users.id
        ORDER BY audit_logs.id DESC LIMIT 200
    """)
    if not rows:
        st.info("Belum ada aktivitas yang tercatat.")
        return
    import pandas as pd
    from datetime import datetime, timedelta
    # Convert UTC to GMT+7
    def to_gmt7(ts):
        try:
            dt = datetime.fromisoformat(ts)
            dt7 = dt + timedelta(hours=7)
            return dt7.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ts
    df = pd.DataFrame([
        {
            "User": r["user"],
            "Date": to_gmt7(r["timestamp"]),
            "Action": r["action"],
            "Detail": r["details"]
        } for r in rows
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)
    # Stay on Audit Log page without redirecting
    return

# -------------------------
# Agent Page (placeholder)
# -------------------------
def page_agent():
    require_roles(("Superuser", "Agent"))
    u = current_user()
    agent_name = (u.get('full_name') or u.get('login_id') or '-') if u else '-'
    st.title("Agent Menu")
    # Simple PTP notif today
    today_str = date.today().isoformat()
    ptp_today = fetchone("SELECT COUNT(*) c FROM agent_results WHERE agent=? AND DATE(agent_ptp_date)=?", (agent_name, today_str))
    count_ptp = ptp_today.get('c') if ptp_today else 0
    if count_ptp and count_ptp > 0:
        st.success(f"Hai {agent_name}, hari ini kamu ada {count_ptp} PTP. Klik di bawah untuk lihat daftar.")

    # Agent's assigned loans
    rows = fetchall("SELECT Agreement_No, assigned_at FROM agent_assignments WHERE Agent_Assigned_To=? ORDER BY assigned_at DESC LIMIT 500", (agent_name,))
    if not rows:
        st.info("Belum ada assignment untuk Anda.")
        return

    # Optional quick search
    q_ag = st.text_input("Cari Agreement_No (Loan Number)", key="ag_q_no")
    filtered = [r for r in rows if (not q_ag or q_ag.strip() in str(r.get('Agreement_No') or ''))]

    st.subheader("Assignments")
    st.dataframe(pd.DataFrame(filtered), use_container_width=True, hide_index=True)

    # Select a loan to open detail
    sel = st.selectbox("Pilih Loan Number", [r['Agreement_No'] for r in filtered], key="ag_sel")
    if not sel:
        return

    st.markdown("---")
    st.subheader(f"Loan Details: {sel}")
    # Fetch minimal debtor info (if present) from assign_tracer and supervisor_data
    info = fetchone("SELECT Debtor_Name, NIK_KTP FROM assign_tracer WHERE Agreement_No=?", (sel,)) or {}
    c1, c2, c3 = st.columns(3)
    with c1:
        st.text_input("Debtor Name", value=info.get('Debtor_Name',''), disabled=True)
    with c2:
        st.text_input("NIK", value=info.get('NIK_KTP',''), disabled=True)
    with c3:
        # Attempt to show phone from supervisor_data (Phone_Number_1)
        sup = fetchone("SELECT Phone_Number_1 FROM supervisor_data WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=? LIMIT 1", (sel, sel, sel))
        phone = (sup.get('Phone_Number_1') if sup else '') or ''
        st.text_input("Phone", value=phone, disabled=True)
    # Click-to-call link (Microsip) if phone exists
    if phone:
        st.markdown(f"[Click to call]({'tel:'+str(phone)})  |  [SIP]({'sip:'+str(phone)})")

    st.markdown("---")
    st.subheader("Hasil Penanganan (D–G)")
    # Store/update agent results
    last = fetchone("SELECT * FROM agent_results WHERE Agreement_No=? AND agent=? ORDER BY id DESC LIMIT 1", (sel, agent_name)) or {}
    with st.form("agent_result_form"):
        ag_status = st.selectbox("Status", ["", "PTP", "NO ANSWER", "RTP", "PAID", "FOLLOW UP", "OTHER"], index=0)
        colx, coly = st.columns(2)
        with colx:
            ptp_amount = st.number_input("PTP Amount", min_value=0.0, value=float(last.get('agent_ptp_amount') or 0.0), step=10000.0)
        with coly:
            ptp_date = st.date_input("PTP Date", value=date.today())
        notes = st.text_area("Catatan", value=last.get('agent_notes') or "")
        sub = st.form_submit_button("Simpan")
        if sub:
            try:
                execute(
                    "INSERT INTO agent_results (Agreement_No, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes) VALUES (?,?,?,?,?,?)",
                    (sel, agent_name, ag_status or None, float(ptp_amount or 0), (ptp_date.isoformat() if ptp_date else None), (notes.strip() if notes else None))
                )
                st.success("Tersimpan.")
                st.rerun()
            except Exception as e:
                st.error(f"Gagal menyimpan: {e}")

    st.markdown("---")
    st.subheader("Email Templates")
    st.caption("Pilih template lalu salin konten untuk dikirim via email/WA.")
    tpl = st.selectbox("Kategori", ["COMPANY", "RELATIVES", "PERSONAL"], index=0)
    debtor = info.get('Debtor_Name','') if isinstance(info, dict) else ''
    nik = info.get('NIK_KTP','') if isinstance(info, dict) else ''
    if tpl == "COMPANY":
        body = f"Yth. HRD,\n\nMohon bantuan verifikasi karyawan atas nama {debtor} (NIK {nik}) terkait kewajiban pembayaran pinjaman. Harap hubungi kami.\n\nTerima kasih."
    elif tpl == "RELATIVES":
        body = f"Halo, kami menghubungi keluarga dari {debtor} (NIK {nik}) untuk menyampaikan informasi penting terkait kewajiban pembayaran. Mohon bantu sampaikan agar yang bersangkutan segera menghubungi kami. Terima kasih."
    else:
        body = f"Halo {debtor},\n\nKami mengingatkan adanya kewajiban pembayaran yang belum diselesaikan. Mohon segera menghubungi kami untuk penyelesaian. Terima kasih."
    st.text_area("Preview", value=body, height=140)

# -------------------------
# Dashboard Page (basic MVP)
# -------------------------
def page_dashboard():
    require_roles(ALL_ROLES)
    st.title("🏠 Dashboard")
    # Period filter (month/week)
    colp1, colp2, colp3 = st.columns([1,1,2])
    with colp1:
        period = st.selectbox("Periode", ["Minggu Ini", "Bulan Ini", "30 Hari Terakhir"]) 
    with colp2:
        today = date.today()
        if period == "Minggu Ini":
            start_date = today - timedelta(days=today.weekday())
        elif period == "Bulan Ini":
            start_date = today.replace(day=1)
        else:
            start_date = today - timedelta(days=30)
    st.caption(f"Rentang: {start_date.isoformat()} s/d {today.isoformat()}")

    # STATUS counts: latest status per Agreement_No within selected period
    rows = fetchall(
        """
        SELECT tr.status FROM trace_results tr
        JOIN (
            SELECT Agreement_No, MAX(touched_at) AS mt
            FROM trace_results
            WHERE DATE(touched_at) >= ?
            GROUP BY Agreement_No
        ) t ON t.Agreement_No = tr.Agreement_No AND t.mt = tr.touched_at
        """,
        (start_date.isoformat(),)
    )
    status_counts = {}
    for r in rows:
        s = (r.get('status') or '').strip().upper()
        if not s:
            continue
        status_counts[s] = status_counts.get(s, 0) + 1
    colA, colB, colC, colD = st.columns(4)
    with colA:
        st.metric("TRACED", status_counts.get("TRACED", 0))
    with colB:
        st.metric("EMAILED", status_counts.get("EMAILED", 0))
    with colC:
        st.metric("RTP", status_counts.get("RTP", 0))
    with colD:
        st.metric("PAYING", status_counts.get("PAYING", 0))

    st.markdown("---")
    # TRC Code (Tracer performance): count touches per tracer in period
    tr_rows = fetchall("SELECT tracer, COUNT(*) as c FROM trace_results WHERE DATE(touched_at) >= ? GROUP BY tracer ORDER BY c DESC LIMIT 20", (start_date.isoformat(),))
    if tr_rows:
        df_tr = pd.DataFrame(tr_rows)
        chart_tr = alt.Chart(df_tr).mark_bar().encode(
            x=alt.X('c:Q', title='Touches'),
            y=alt.Y('tracer:N', sort='-x', title='Tracer')
        ).properties(title='Performa Tracer (Touches)', height=360)
        st.altair_chart(chart_tr, use_container_width=True)
    else:
        st.info("Belum ada data tracer pada periode ini.")

    # Agent Assigned performance: count loans assigned per agent in period
    ag_rows = fetchall("SELECT Agent_Assigned_To as agent, COUNT(*) as c FROM agent_assignments WHERE DATE(assigned_at) >= ? GROUP BY Agent_Assigned_To ORDER BY c DESC LIMIT 20", (start_date.isoformat(),))
    if ag_rows:
        df_ag = pd.DataFrame(ag_rows)
        chart_ag = alt.Chart(df_ag).mark_bar(color='#1E88E5').encode(
            x=alt.X('c:Q', title='Loans Assigned'),
            y=alt.Y('agent:N', sort='-x', title='Agent')
        ).properties(title='Performa Agent (Assignment)', height=360)
        st.altair_chart(chart_ag, use_container_width=True)
    else:
        st.info("Belum ada assignment agent pada periode ini.")

    st.markdown("---")
    # Latest Agent Status per Loan (within period)
    agent_rows = fetchall(
        """
        SELECT ar.agent_status
        FROM agent_results ar
        JOIN (
            SELECT Agreement_No, MAX(updated_at) AS mu
            FROM agent_results
            WHERE DATE(updated_at) >= ?
            GROUP BY Agreement_No
        ) t ON t.Agreement_No = ar.Agreement_No AND t.mu = ar.updated_at
        """,
        (start_date.isoformat(),)
    )
    if agent_rows:
        agg = {}
        for r in agent_rows:
            s = (r.get('agent_status') or '').upper().strip()
            if not s:
                continue
            agg[s] = agg.get(s, 0) + 1
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("PTP (latest)", agg.get('PTP', 0))
        with c2:
            st.metric("PAID (latest)", agg.get('PAID', 0))
        with c3:
            st.metric("RTP (latest)", agg.get('RTP', 0))
        with c4:
            st.metric("FOLLOW UP (latest)", agg.get('FOLLOW UP', 0))
    else:
        st.info("Belum ada status agent pada periode ini.")

    # Status-by-Agent (latest per loan)
    agent_matrix = fetchall(
        """
        SELECT COALESCE(ar.agent,'(Unknown)') AS agent, COALESCE(ar.agent_status,'') AS status, COUNT(*) AS c
        FROM agent_results ar
        JOIN (
            SELECT Agreement_No, MAX(updated_at) AS mu
            FROM agent_results
            WHERE DATE(updated_at) >= ?
            GROUP BY Agreement_No
        ) t ON t.Agreement_No = ar.Agreement_No AND t.mu = ar.updated_at
        GROUP BY COALESCE(ar.agent,'(Unknown)'), COALESCE(ar.agent_status,'')
        ORDER BY c DESC
        LIMIT 200
        """,
        (start_date.isoformat(),)
    )
    if agent_matrix:
        dfm = pd.DataFrame(agent_matrix)
        chart_m = alt.Chart(dfm).mark_bar().encode(
            x=alt.X('c:Q', title='Count'),
            y=alt.Y('agent:N', sort='-x', title='Agent'),
            color=alt.Color('status:N', title='Status'),
            tooltip=['agent:N', 'status:N', 'c:Q']
        ).properties(title='Latest Agent Status by Agent', height=360)
        st.altair_chart(chart_m, use_container_width=True)
    else:
        st.info("Belum ada matriks status agent pada periode ini.")

    st.markdown("---")
    # Paid Amount trend (from payments)
    pay_rows = fetchall("SELECT paid_date, SUM(paid_amount) as amount FROM payments WHERE DATE(paid_date) >= ? GROUP BY paid_date ORDER BY paid_date", (start_date.isoformat(),))
    if pay_rows:
        dfp = pd.DataFrame(pay_rows)
        line = alt.Chart(dfp).mark_line(point=True).encode(
            x=alt.X('paid_date:T', title='Tanggal'),
            y=alt.Y('amount:Q', title='Paid Amount'),
            tooltip=['paid_date:T','amount:Q']
        ).properties(title='Paid Amount Harian')
        st.altair_chart(line, use_container_width=True)
    else:
        st.info("Belum ada Payment Recap pada periode ini.")

    st.markdown("---")
    # Saving by Agent (sum of payments joined to agent assignments)
    sav_rows = fetchall(
        """
        SELECT COALESCE(aa.Agent_Assigned_To, '(Unassigned)') AS agent, COALESCE(SUM(p.paid_amount),0) AS amount
        FROM payments p
        LEFT JOIN agent_assignments aa ON aa.Agreement_No = p.Agreement_No
        WHERE DATE(p.paid_date) >= ?
        GROUP BY COALESCE(aa.Agent_Assigned_To, '(Unassigned)')
        ORDER BY amount DESC
        LIMIT 20
        """,
        (start_date.isoformat(),)
    )
    if sav_rows:
        df_sav = pd.DataFrame(sav_rows)
        bar = alt.Chart(df_sav).mark_bar(color='#43A047').encode(
            x=alt.X('amount:Q', title='Paid Amount'),
            y=alt.Y('agent:N', sort='-x', title='Agent'),
            tooltip=['agent:N','amount:Q']
        ).properties(title='Saving by Agent', height=360)
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("Belum ada saving pada periode ini.")

    st.markdown("---")
    # Saving comparison (MoM to date) — simple total comparison
    this_month_start = today.replace(day=1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    # up to same day-of-month if exists
    day_to = min(today.day, (last_month_end.day if hasattr(last_month_end, 'day') else today.day))
    this_to_date = fetchone("SELECT COALESCE(SUM(paid_amount),0) v FROM payments WHERE DATE(paid_date) BETWEEN ? AND ?", (this_month_start.isoformat(), today.isoformat()))['v']
    last_to_date_end = last_month_start + timedelta(days=day_to-1)
    last_to_date = fetchone("SELECT COALESCE(SUM(paid_amount),0) v FROM payments WHERE DATE(paid_date) BETWEEN ? AND ?", (last_month_start.isoformat(), last_to_date_end.isoformat()))['v']
    colS1, colS2, colS3 = st.columns(3)
    with colS1:
        st.metric("This Month (to-date)", f"{this_to_date:,.0f}")
    with colS2:
        st.metric("Last Month (same days)", f"{last_to_date:,.0f}")
    with colS3:
        delta_val = (this_to_date - last_to_date)
        st.metric("Delta", f"{delta_val:,.0f}", delta=f"{delta_val:,.0f}")

# -------------------------
# User Setting Page
# -------------------------
def page_user_setting():
    require_roles(ALL_ROLES)
    u = current_user()
    st.title("User Setting")
    st.caption("Update your profile information below.")
    user_row = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
    if not user_row:
        st.error("User not found.")
        return
    with st.form("user_setting_form"):
        full_name = st.text_input("Full Name", value=user_row.get('full_name') or "")
        email = st.text_input("Email", value=user_row.get('email') or "")
        pw1 = st.text_input("New Password", type="password", key="user_pw1", placeholder="Leave blank to keep current password")
        pw2 = st.text_input("Confirm New Password", type="password", key="user_pw2", placeholder="Leave blank to keep current password")
        submitted = st.form_submit_button("Update Profile")
        if submitted:
            updates = []
            params = []
            changed = False
            if full_name.strip() != (user_row.get('full_name') or ""):
                updates.append("full_name=?")
                params.append(full_name.strip())
                changed = True
            if email.strip() != (user_row.get('email') or ""):
                updates.append("email=?")
                params.append(email.strip())
                changed = True
            if pw1 or pw2:
                if pw1 != pw2:
                    st.error("Password and confirmation do not match.")
                    return
                if pw1.strip():
                    updates.append("password_hash=?")
                    params.append(hash_password(pw1.strip()))
                    changed = True
            if not changed:
                st.info("No changes to update.")
                return
            params.append(u.get('id'))
            try:
                execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(params))
                updated_user = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
                login_user(updated_user)
                try:
                    detail = []
                    if 'full_name=?' in updates:
                        detail.append(f"Name changed to '{full_name.strip()}'")
                    if 'email=?' in updates:
                        detail.append(f"Email changed to '{email.strip()}'")
                    if 'password_hash=?' in updates:
                        detail.append("Password changed")
                    execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id'), "USER_UPDATE", "; ".join(detail)))
                except Exception:
                    pass
                st.success("Profile updated successfully.")
            except Exception as e:
                st.error(f"Failed to update profile: {e}")

# -------------------------
# Supervisor Page
# -------------------------
def page_supervisor():
    require_roles(("Superuser", "Supervisor"))
    st.title("Supervisor Menu")
    # Monitoring first so it's the default view
    tabs = st.tabs(["Monitoring", "Input", "Trace Assigning", "Agent Assigning", "Trace Results"])

    # --- Monitoring Tab ---
    with tabs[0]:
        # Primary quick search fields
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            q_phone = st.text_input("Phone Number", key="monitor_phone")
        with c2:
            q_case_id = st.text_input("Case ID", key="monitor_case_id")
        with c3:
            q_third_uid = st.text_input("Third Uid", key="monitor_third_uid")
        with c4:
            q_customer = st.text_input("Customer name", key="monitor_customer_name")

        # Advanced filters in expander
        # All additional fields except the four primary ones
        base_filter_fields = [
            "Lending_Entity", "Date", "Task_ID", "email", "Gender", "Customer_Occupation", "DPD",
            "Principle_Outstanding", "Principal_Overdue_CURR", "Interest_Overdue_CURR", "Last_Late_Fee",
            "Return_Date", "Detail", "Loan_Type", "Product", "Home_Address", "Province", "City",
            "Street", "RoomNumber", "Postcode", "Assignment_Date"
        ]
        extra_filters = {}
        with st.expander("Filter lain (opsional)"):
            cols = st.columns(min(4, len(base_filter_fields)))
            for i, f in enumerate(base_filter_fields):
                with cols[i % len(cols)]:
                    extra_filters[f] = st.text_input(f.replace('_',' '), key=f"monitor_extra_{f}")

        # Build query
        query = "SELECT * FROM supervisor_data WHERE 1=1"
        params = []
        # Primary
        if q_phone:
            query += " AND (Phone_Number_1 LIKE ? OR Phone_Number_2 LIKE ?)"
            params.extend([f"%{q_phone}%", f"%{q_phone}%"])
        if q_case_id:
            query += " AND Case_ID LIKE ?"
            params.append(f"%{q_case_id}%")
        if q_third_uid:
            query += " AND Third_Uid LIKE ?"
            params.append(f"%{q_third_uid}%")
        if q_customer:
            query += " AND Customer_name LIKE ?"
            params.append(f"%{q_customer}%")
        # Extras
        for f, v in extra_filters.items():
            if v:
                query += f" AND {f} LIKE ?"
                params.append(f"%{v}%")
        query += " ORDER BY id DESC LIMIT 200"

        rows = fetchall(query, tuple(params))
        if not rows:
            st.info("Tidak ada data supervisor ditemukan.")
        else:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

        with st.expander("Enriched Monitoring (Loan-centric)"):
            st.caption("Gabungan assign_tracer + agent_assignments + latest trace status + payments")
            fcol1, fcol2, fcol3, fcol4 = st.columns(4)
            with fcol1:
                f_ag = st.text_input("Agreement_No contains", key="en_ag")
            with fcol2:
                f_nik = st.text_input("NIK contains", key="en_nik")
            with fcol3:
                tracers = [r['full_name'] for r in fetchall("SELECT COALESCE(full_name,name) AS full_name FROM users WHERE approved=1 ORDER BY 1") if r.get('full_name')]
                f_tracer = st.selectbox("Tracer", options=["(All)"] + tracers, index=0, key="en_tracer")
            with fcol4:
                agents = [r['full_name'] for r in fetchall("SELECT COALESCE(full_name,name) AS full_name FROM users WHERE approved=1 ORDER BY 1") if r.get('full_name')]
                f_agent = st.selectbox("Agent", options=["(All)"] + agents, index=0, key="en_agent")

            fcol5, fcol6, fcol7 = st.columns(3)
            with fcol5:
                f_status = st.multiselect("Latest Status", ["TRACED", "EMAILED", "RTP", "PAYING", "UNREACHABLE", "OTHER"], key="en_status")
            with fcol6:
                f_pay = st.selectbox("Payment", ["All", "With Payment", "Without Payment"], index=0, key="en_pay")
            with fcol7:
                ad_from = st.date_input("Assigned From", value=None, key="en_ad_from")
                ad_to = st.date_input("Assigned To", value=None, key="en_ad_to")

            q_en = (
                "SELECT a.Agreement_No, a.Debtor_Name, a.NIK_KTP, a.Assigned_To AS tracer, "
                "a.Masked_Company_Name, ag.Agent_Assigned_To AS agent, ag.assigned_at, "
                "ts.status AS latest_status, ts.touched_at AS status_time, "
                "COALESCE(p.amount, 0) AS paid_amount_total, p.last_paid_date "
                "FROM assign_tracer a "
                "LEFT JOIN agent_assignments ag ON ag.Agreement_No = a.Agreement_No "
                "LEFT JOIN ( "
                "  SELECT tr1.Agreement_No, tr1.status, tr1.touched_at "
                "  FROM trace_results tr1 "
                "  JOIN (SELECT Agreement_No, MAX(touched_at) mt FROM trace_results GROUP BY Agreement_No) t2 "
                "    ON t2.Agreement_No = tr1.Agreement_No AND t2.mt = tr1.touched_at "
                ") ts ON ts.Agreement_No = a.Agreement_No "
                "LEFT JOIN ( "
                "  SELECT Agreement_No, SUM(paid_amount) AS amount, MAX(paid_date) AS last_paid_date "
                "  FROM payments GROUP BY Agreement_No "
                ") p ON p.Agreement_No = a.Agreement_No "
                "WHERE 1=1"
            )
            p_en = []
            if f_ag:
                q_en += " AND a.Agreement_No LIKE ?"; p_en.append(f"%{f_ag}%")
            if f_nik:
                q_en += " AND COALESCE(a.NIK_KTP,'') LIKE ?"; p_en.append(f"%{f_nik}%")
            if f_tracer and f_tracer != "(All)":
                q_en += " AND COALESCE(a.Assigned_To,'') = ?"; p_en.append(f_tracer)
            if f_agent and f_agent != "(All)":
                q_en += " AND COALESCE(ag.Agent_Assigned_To,'') = ?"; p_en.append(f_agent)
            if f_status:
                placeholders = ",".join(["?"] * len(f_status))
                q_en += f" AND COALESCE(ts.status,'') IN ({placeholders})"; p_en.extend(f_status)
            if f_pay == "With Payment":
                q_en += " AND COALESCE(p.amount,0) > 0"
            elif f_pay == "Without Payment":
                q_en += " AND COALESCE(p.amount,0) = 0"
            if ad_from:
                q_en += " AND DATE(ag.assigned_at) >= DATE(?)"; p_en.append(str(ad_from))
            if ad_to:
                q_en += " AND DATE(ag.assigned_at) <= DATE(?)"; p_en.append(str(ad_to))
            q_en += " ORDER BY ag.assigned_at DESC, a.id DESC LIMIT 500"

            rows_en = fetchall(q_en, tuple(p_en))
            if rows_en:
                st.dataframe(pd.DataFrame(rows_en), use_container_width=True, hide_index=True)
            else:
                st.info("Tidak ada data sesuai filter.")
        st.markdown("---")
        st.subheader("🔎 Lookup NIK Across Loans")
        nik_q = st.text_input("Cari NIK (global)", key="monitor_nik_lookup")
        if nik_q:
            nik_rows = fetchall(
                "SELECT Agreement_No, Debtor_Name, NIK_KTP, Assigned_To FROM assign_tracer WHERE NIK_KTP LIKE ? ORDER BY id DESC LIMIT 200",
                (f"%{nik_q}%",)
            )
            if nik_rows:
                df_n = pd.DataFrame(nik_rows)
                st.caption(f"Ditemukan {len(df_n)} loan untuk NIK mengandung '{nik_q}'")
                st.dataframe(df_n, use_container_width=True, hide_index=True)
            else:
                st.info("Tidak ditemukan loan untuk NIK tersebut.")

    # --- Input Tab ---
    with tabs[1]:
        st.subheader("Upload Excel/CSV Supervisor Data")
        field_names = [
            "DT", "Lending_Entity", "Date", "Case_ID", "Task_ID", "Customer_name", "email", "Gender", "Customer_Occupation", "DPD", "Principle_Outstanding", "Principal_Overdue_CURR", "Interest_Overdue_CURR", "Last_Late_Fee", "Return_Date", "Detail", "Loan_Type", "Third_Uid", "Product", "Home_Address", "Province", "City", "Street", "RoomNumber", "Postcode", "Assignment_Date", "Withdrawal_Date", "Phone_Number_1", "Phone_Number_2", "Contact_Type_1", "Contact_Name_1", "Contact_Phone_1", "Contact_Type_2", "Contact_Name_2", "Contact_Phone_2", "Contact_Type_3", "Contact_Name_3", "Contact_Phone_3", "Contact_Type_4", "Contact_Name_4", "Contact_Phone_4", "Contact_Type_5", "Contact_Name_5", "Contact_Phone_5", "Contact_Type_6", "Contact_Name_6", "Contact_Phone_6", "Contact_Type_7", "Contact_Name_7", "Contact_Phone_7", "Contact_Type_8", "Contact_Name_8", "Contact_Phone_8", "Total_debt_in_third_party", "Repayment_on_third_Party", "Remaining_Loan_on_third_Party", "Virtual_Account_Number"
        ]
        uploaded = st.file_uploader("Upload file Excel/CSV", type=["csv", "xlsx"])
        if uploaded:
            user = current_user()
            try:
                if uploaded.name.endswith(".csv"):
                    df = pd.read_csv(uploaded)
                else:
                    df = pd.read_excel(uploaded)
                # --- Normalize header names to match expected fields ---
                def _norm_col(s: str) -> str:
                    if s is None:
                        return ""
                    s = str(s).replace("\ufeff", "").strip()
                    s = re.sub(r"\s+", " ", s)  # collapse spaces
                    s = s.replace(" ", "_")
                    return s.lower()

                # Known typo mappings (normalized form)
                typo_map = {
                    _norm_col("Repayment_on_thrid_Party"): _norm_col("Repayment_on_third_Party"),
                }
                # Build map from normalized -> canonical expected name
                expected_map = { _norm_col(k): k for k in field_names }
                new_cols = []
                for c in df.columns:
                    nc = _norm_col(c)
                    # Fix known typos first
                    if nc in typo_map:
                        nc = typo_map[nc]
                    # Map to canonical if matches
                    if nc in expected_map:
                        new_cols.append(expected_map[nc])
                    else:
                        new_cols.append(c)
                df.columns = new_cols
                # Pastikan urutan kolom sesuai field_names
                missing = [f for f in field_names if f not in df.columns]
                if missing:
                    st.error(f"Kolom berikut tidak ditemukan di file: {missing}")
                    st.caption("Tips: header akan dicocokkan tanpa spasi/kapital dan perbaikan typo umum (thrid->third). Pastikan nama kolom sesuai template.")
                else:
                    # Helper to coerce values into SQLite-friendly types
                    def _to_sql_value(v):
                        try:
                            import pandas as _pd
                            import numpy as _np
                        except Exception:
                            _pd = None; _np = None
                        # Treat NaN/NaT as NULL
                        try:
                            if _pd is not None and (_pd.isna(v) if not isinstance(v, str) else False):
                                return None
                        except Exception:
                            pass
                        # Pandas Timestamp -> ISO string
                        try:
                            if _pd is not None and isinstance(v, _pd.Timestamp):
                                # keep space separator for readability
                                return v.to_pydatetime().isoformat(sep=' ')
                        except Exception:
                            pass
                        # Python datetime/date -> ISO string
                        from datetime import datetime as _dt, date as _d
                        if isinstance(v, _dt):
                            return v.isoformat(sep=' ')
                        if isinstance(v, _d):
                            return v.isoformat()
                        # Numpy scalars -> Python scalars
                        try:
                            if hasattr(v, 'item'):
                                return v.item()
                        except Exception:
                            pass
                        return v

                    count = 0
                    placeholders = ','.join(['?' for _ in field_names])
                    for _, row in df.iterrows():
                        try:
                            vals = []
                            for f in field_names:
                                vals.append(_to_sql_value(row.get(f)))
                            execute(
                                f"INSERT INTO supervisor_data ({','.join(field_names)}) VALUES ({placeholders})",
                                tuple(vals)
                            )
                            count += 1
                        except Exception as e:
                            st.warning(f"Baris gagal: {e}")
                    st.success(f"Berhasil input {count} data supervisor.")
                    # Audit log supervisor upload
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (user.get('id') if user else None, "UPLOAD_SUPERVISOR", f"Uploaded supervisor data: {count} rows from '{uploaded.name}'"))
                    except Exception:
                        pass
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")

        st.markdown("---")
        st.subheader("📘 Masked Company Dictionary")
        with st.form("masked_company_form_supervisor"):
            mc_masked = st.text_input("Masked Company Name", key="mc_masked_supervisor")
            mc_canon = st.text_input("Canonical Name (Nama Perusahaan Sebenarnya)", key="mc_canon_supervisor")
            mc_notes = st.text_input("Catatan (opsional)", key="mc_notes_supervisor")
            sub_mc = st.form_submit_button("Simpan/Perbarui")
            if sub_mc and mc_masked.strip():
                try:
                    execute(
                        "INSERT INTO masked_companies (masked_name, canonical_name, mapping_notes) VALUES (?,?,?)\n                         ON CONFLICT(masked_name) DO UPDATE SET canonical_name=excluded.canonical_name, mapping_notes=excluded.mapping_notes",
                        (mc_masked.strip(), mc_canon.strip() if mc_canon else None, mc_notes.strip() if mc_notes else None)
                    )
                    st.success("Dictionary tersimpan.")
                except Exception as e:
                    st.error(f"Gagal menyimpan: {e}")
        # list recent 20 mappings
        mc_rows = fetchall("SELECT masked_name, canonical_name, mapping_notes, created_at FROM masked_companies ORDER BY id DESC LIMIT 20")
        if mc_rows:
            st.dataframe(pd.DataFrame(mc_rows), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("💸 Upload Payment Recap (CSV/XLSX)")
        st.caption("Kolom minimal: Agreement_No, paid_amount, paid_date, status. Duplikat (Agreement_No, paid_date) akan diabaikan.")
        pay_file = st.file_uploader("Pilih file payment recap", type=["csv", "xlsx"], key="pay_recap_supervisor")
        if pay_file is not None:
            try:
                if pay_file.name.lower().endswith(".csv"):
                    dfp = pd.read_csv(pay_file)
                else:
                    try:
                        import openpyxl  # noqa: F401
                        dfp = pd.read_excel(pay_file, engine="openpyxl")
                    except Exception:
                        dfp = pd.read_excel(pay_file)
                # normalize columns
                dfp.columns = [str(c).strip() for c in dfp.columns]
                required_cols = {"Agreement_No", "paid_amount", "paid_date", "status"}
                if not required_cols.issubset(set(dfp.columns)):
                    st.error(f"Kolom wajib tidak lengkap. Ditemukan: {list(dfp.columns)}")
                else:
                    u = current_user() or {}
                    uploader = (u.get('full_name') or u.get('login_id') or '-')
                    inserted = 0; skipped = 0
                    for _, r in dfp.iterrows():
                        agr = str(r.get("Agreement_No") or "").strip()
                        amt = r.get("paid_amount")
                        pdt = str(r.get("paid_date") or "").strip()
                        stt = str(r.get("status") or "").strip()
                        if not agr or not pdt:
                            skipped += 1
                            continue
                        try:
                            # Try parse date to ISO (yyyy-mm-dd)
                            try:
                                if isinstance(r.get("paid_date"), (datetime,)):
                                    pdt_iso = r.get("paid_date").date().isoformat()
                                else:
                                    pdt_iso = pd.to_datetime(pdt, errors='coerce').date().isoformat()
                            except Exception:
                                pdt_iso = pdt
                            # amount numeric
                            try:
                                amt_num = float(amt) if amt is not None and str(amt).strip() != '' else 0.0
                            except Exception:
                                amt_num = 0.0
                            # upsert by (Agreement_No, paid_date)
                            execute(
                                "INSERT OR IGNORE INTO payments (Agreement_No, paid_amount, paid_date, status, source_file, uploaded_by) VALUES (?,?,?,?,?,?)",
                                (agr, amt_num, pdt_iso, stt, pay_file.name, uploader)
                            )
                            # If already exists and status/amount differ, update
                            execute(
                                "UPDATE payments SET paid_amount=COALESCE(?, paid_amount), status=COALESCE(?, status), source_file=? WHERE Agreement_No=? AND paid_date=?",
                                (amt_num, stt or None, pay_file.name, agr, pdt_iso)
                            )
                            inserted += 1
                        except Exception:
                            skipped += 1
                    st.success(f"Selesai. Baris diproses: {inserted}. Dilewati: {skipped}.")
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")

    with tabs[2]:
        unassigned_rows = fetchall("SELECT id FROM assign_tracer WHERE IFNULL(Assigned_To,'')='' ORDER BY id DESC")
        unassigned_count = len(unassigned_rows)
        st.info(f"Baris belum ter-assign saat ini: {unassigned_count}")

        if unassigned_count > 0:
            # Build tracer options in this scope (approved users)
            _user_rows_ma = fetchall("SELECT COALESCE(full_name, name) AS full_name FROM users WHERE approved=1 AND role='Tracer' ORDER BY COALESCE(full_name,name) ASC")
            tracer_names = [r['full_name'] for r in _user_rows_ma if r.get('full_name')]

            with st.form("multi_assign_form"):
                selected_tracers = st.multiselect(
                    "Pilih tracer (minimal 2)", options=tracer_names, default=[], key="multi_assign_tracers"
                )
                # Advanced options hidden by default
                with st.expander("Opsi lanjutan", expanded=False):
                    col_ma1, col_ma2 = st.columns(2)
                    with col_ma1:
                        limit_n = st.number_input("Jumlah baris yang akan di-assign (0 = semua)", min_value=0, value=0, step=1, key="multi_assign_limit")
                    with col_ma2:
                        do_shuffle = st.checkbox("Acak urutan baris", value=True, key="multi_assign_shuffle")

                # Small summary to clarify distribution
                if selected_tracers:
                    import math as _math
                    per_tracer_est = _math.ceil(unassigned_count / max(len(selected_tracers), 1))
                    st.caption(f"Perkiraan distribusi: ~{per_tracer_est} baris per tracer")

                submitted = st.form_submit_button("Assign Sekarang", type="primary")

            if submitted:
                if not selected_tracers or len(selected_tracers) < 2:
                    st.warning("Pilih minimal 2 tracer.")
                else:
                    ids = [r['id'] for r in unassigned_rows]
                    try:
                        import random
                        if st.session_state.get("multi_assign_shuffle", True):
                            random.shuffle(ids)
                        # Batasi sesuai input
                        limit_val = st.session_state.get("multi_assign_limit", 0)
                        if limit_val and limit_val > 0:
                            ids = ids[: min(limit_val, len(ids))]
                        # Round-robin distribution
                        updates = []  # list of tuples (assignee, id)
                        for idx, rec_id in enumerate(ids):
                            assignee = selected_tracers[idx % len(selected_tracers)]
                            updates.append((assignee, rec_id))

                        # Commit updates in a single transaction (and generate TRC_Code if missing)
                        try:
                            conn = sqlite3.connect(DB_PATH)
                            cur = conn.cursor()
                            # First, set assignees
                            cur.executemany("UPDATE assign_tracer SET Assigned_To=? WHERE id=?", updates)
                            # Generate TRC codes for rows where TRC_Code is NULL/empty
                            def _gen_trc_code(assignee: str) -> str:
                                try:
                                    first = (assignee or "").strip().split(" ")[0]
                                    suffix = first[:3].upper()
                                except Exception:
                                    suffix = "XXX"
                                ymd = datetime.now().strftime('%y%m%d')
                                return f"TRC-{ymd}-{suffix}"
                            updates_trc = [(_gen_trc_code(assignee), rec_id) for assignee, rec_id in updates]
                            cur.executemany(
                                "UPDATE assign_tracer SET TRC_Code = COALESCE(NULLIF(TRC_Code, ''), ?) WHERE id=?",
                                updates_trc
                            )
                            conn.commit()
                            conn.close()
                        except Exception as e:
                            st.error(f"Gagal menyimpan assign: {e}")
                        else:
                            st.success(f"Berhasil assign {len(ids)} baris ke {len(selected_tracers)} tracer.")
                            # Audit log
                            u = current_user()
                            try:
                                details = f"Multi-assign {len(ids)} rows to {len(selected_tracers)} tracers: {', '.join(selected_tracers)}"
                                execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id') if u else None, "MULTI_ASSIGN", details))
                            except Exception:
                                pass
                            st.rerun()
                    except Exception as e:
                        st.error(f"Gagal melakukan multi-assign: {e}")
        else:
            st.caption("Tidak ada baris yang perlu di-assign saat ini.")

        # Optional: Unassign tool to prepare rows for assignment
        st.markdown("---")
        with st.expander("Unassign beberapa loan (opsional)"):
            st.caption("Tempel daftar Agreement_No (satu per baris) untuk mengosongkan Assigned_To sehingga bisa di-assign ulang.")
            text_ids = st.text_area("Agreement_No list", height=120, placeholder="Contoh:\n123456\n987654\n...")
            if st.button("Unassign sekarang"):
                ids = [s.strip() for s in (text_ids or "").splitlines() if s.strip()]
                if not ids:
                    st.warning("Masukkan minimal satu Agreement_No.")
                else:
                    try:
                        conn = sqlite3.connect(DB_PATH, timeout=30)
                        try:
                            conn.execute("PRAGMA journal_mode=WAL;")
                            conn.execute("PRAGMA busy_timeout=10000;")
                            conn.execute("PRAGMA synchronous=NORMAL;")
                        except Exception:
                            pass
                        cur = conn.cursor()
                        cur.executemany("UPDATE assign_tracer SET Assigned_To='' WHERE Agreement_No=?", [(x,) for x in ids])
                        conn.commit()
                        conn.close()
                        st.success(f"Berhasil unassign {len(ids)} loan. Silakan refresh atau klik Assign setelah ini.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal unassign: {e}")

        # Base tracer fields from upload/form (TRC_Code will be generated if missing)
        tracer_fields = [
            "Agreement_No", "Debtor_Name", "NIK_KTP", "EMPLOYMENT_UPDATE", "EMPLOYER", "Debtor_Legal_Name", "Employee_Name", "Employee_ID_Number", "Debtor_Relation_to_Employee"
        ]

        # Default assignee for upload rows (used when file has no Assigned_To)
        _user_rows_up = fetchall("SELECT COALESCE(full_name, name) AS full_name FROM users WHERE approved=1 AND role='Tracer' ORDER BY COALESCE(full_name,name) ASC")
        _tracer_names_up = [r['full_name'] for r in _user_rows_up if r.get('full_name')]
        _assign_opts_up = ["(Unassigned)"] + (_tracer_names_up if _tracer_names_up else [])
        col_up1, col_up2 = st.columns([2,1])
        with col_up1:
            _sel_up = st.selectbox("Default tracer untuk file (digunakan jika kolom Assigned_To tidak ada)", options=_assign_opts_up, key="tr_upload_default_sel")
        with col_up2:
            _custom_up = st.text_input("Nama custom (jika Other)", key="tr_upload_default_custom")
        default_assigned = _custom_up.strip() if _sel_up == "Other…" else _sel_up
        update_existing = st.checkbox(
            "Update baris yang sudah ada (Agreement_No sama)",
            value=False,
            help="Jika dicentang, baris existing akan diperbarui; Assigned_To tidak diubah."
        )

        tracer_uploaded = st.file_uploader("Upload file Excel/CSV Tracer", type=["csv", "xlsx"], key="tracer_upload")
        if tracer_uploaded:
            user = current_user()
            try:
                if tracer_uploaded.name.endswith(".csv"):
                    tracer_df = pd.read_csv(tracer_uploaded)
                else:
                    tracer_df = pd.read_excel(tracer_uploaded)
                # Normalize tracer headers (trim/BOM/case-insensitive + spaces to underscores)
                def _norm_col2(s: str) -> str:
                    if s is None:
                        return ""
                    s = str(s).replace("\ufeff", "").strip()
                    s = re.sub(r"\s+", " ", s)
                    s = s.replace(" ", "_")
                    return s.lower()
                # Header aliasing for friendlier uploads
                alias_map = {
                    "agreement_no.": "Agreement_No",
                    "agreement_no": "Agreement_No",
                    "agreement_number": "Agreement_No",
                    "virtual_account_number": "Agreement_No",
                    "tracer": "Assigned_To",
                    "assigned_to": "Assigned_To",
                    "trace_date": "Trace_Date",  # optional, currently unused
                    "trc_code": "TRC_Code",
                }
                expected_map_tr = { _norm_col2(k): k for k in (tracer_fields + ["Assigned_To", "TRC_Code"]) }
                new_cols = []
                for c in tracer_df.columns:
                    key = _norm_col2(c)
                    # map via alias first, then to expected target
                    if key in alias_map:
                        target = alias_map[key]
                    else:
                        target = expected_map_tr.get(key, c)
                    new_cols.append(target)
                tracer_df.columns = new_cols
                # Validate base required columns
                missing = [f for f in tracer_fields if f not in tracer_df.columns]
                if missing:
                    st.error(f"Kolom berikut tidak ditemukan di file: {missing}")
                else:
                    # If Assigned_To column not in file, use default selection; allow Unassigned
                    if 'Assigned_To' not in tracer_df.columns:
                        if default_assigned == "(Unassigned)":
                            tracer_df['Assigned_To'] = ""
                        elif not default_assigned:
                            st.error("File tidak memiliki kolom 'Assigned_To'. Pilih tracer default atau pilih '(Unassigned)'.")
                            return
                        else:
                            tracer_df['Assigned_To'] = default_assigned
                    # Clean Agreement_No and drop empty/duplicates inside the file
                    try:
                        tracer_df['Agreement_No'] = tracer_df['Agreement_No'].astype(str).str.strip()
                    except Exception:
                        pass
                    tracer_df = tracer_df[tracer_df['Agreement_No'] != '']
                    _rows_before = len(tracer_df)
                    tracer_df = tracer_df.drop_duplicates(subset=['Agreement_No'], keep='first')
                    _dupes_dropped = _rows_before - len(tracer_df)
                    count = 0
                    # Insert with TRC_Code generated from assignee at upload time
                    def _gen_trc_code(assignee: str) -> str:
                        try:
                            first = (assignee or "").strip().split(" ")[0]
                            suffix = first[:3].upper()
                        except Exception:
                            suffix = "XXX"
                        ymd = datetime.now().strftime('%y%m%d')
                        return f"TRC-{ymd}-{suffix}"
                    insert_fields = ["TRC_Code"] + tracer_fields + ["Assigned_To"]
                    updated = 0
                    skipped = 0
                    # Use single connection and transaction to avoid locks
                    try:
                        conn = sqlite3.connect(DB_PATH, timeout=30)
                        conn.row_factory = sqlite3.Row
                        try:
                            conn.execute("PRAGMA journal_mode=WAL;")
                            conn.execute("PRAGMA busy_timeout=10000;")
                            conn.execute("PRAGMA synchronous=NORMAL;")
                        except Exception:
                            pass
                        cur = conn.cursor()
                        for _, row in tracer_df.iterrows():
                            try:
                                assignee = row.get('Assigned_To')
                                trc_val = row.get('TRC_Code') if 'TRC_Code' in tracer_df.columns else None
                                if not trc_val or str(trc_val).strip() == "":
                                    trc_val = _gen_trc_code(assignee)
                                agr = (row.get('Agreement_No') or '').strip()
                                if not agr:
                                    skipped += 1
                                    continue
                                cur.execute("SELECT id, Assigned_To, COALESCE(TRC_Code,'') AS TRC_Code FROM assign_tracer WHERE Agreement_No=?", (agr,))
                                existing = cur.fetchone()
                                if existing:
                                    if update_existing:
                                        params = [
                                            trc_val,
                                            row.get('Debtor_Name'),
                                            row.get('NIK_KTP'),
                                            row.get('EMPLOYMENT_UPDATE'),
                                            row.get('EMPLOYER'),
                                            row.get('Debtor_Legal_Name'),
                                            row.get('Employee_Name'),
                                            row.get('Employee_ID_Number'),
                                            row.get('Debtor_Relation_to_Employee'),
                                            agr
                                        ]
                                        cur.execute(
                                            """
                                            UPDATE assign_tracer SET
                                                TRC_Code = COALESCE(NULLIF(TRC_Code,''), ?),
                                                Debtor_Name = COALESCE(NULLIF(?,''), Debtor_Name),
                                                NIK_KTP = COALESCE(NULLIF(?,''), NIK_KTP),
                                                EMPLOYMENT_UPDATE = COALESCE(NULLIF(?,''), EMPLOYMENT_UPDATE),
                                                EMPLOYER = COALESCE(NULLIF(?,''), EMPLOYER),
                                                Debtor_Legal_Name = COALESCE(NULLIF(?,''), Debtor_Legal_Name),
                                                Employee_Name = COALESCE(NULLIF(?,''), Employee_Name),
                                                Employee_ID_Number = COALESCE(NULLIF(?,''), Employee_ID_Number),
                                                Debtor_Relation_to_Employee = COALESCE(NULLIF(?,''), Debtor_Relation_to_Employee)
                                            WHERE Agreement_No=?
                                            """,
                                            tuple(params)
                                        )
                                        updated += 1
                                    else:
                                        skipped += 1
                                else:
                                    values = [trc_val] + [row.get(f) for f in tracer_fields] + [assignee]
                                    cur.execute(
                                        f"INSERT INTO assign_tracer ({','.join(insert_fields)}) VALUES ({','.join(['?' for _ in insert_fields])})",
                                        tuple(values)
                                    )
                                    count += 1
                            except Exception as e:
                                st.warning(f"Baris gagal: {e}")
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        st.error(f"Gagal memproses batch: {e}")
                    st.success(f"Selesai. Insert baru: {count}, Update: {updated}, Skip: {skipped}. Duplikat di file: {_dupes_dropped}.")
                    # Audit log tracer upload
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (user.get('id') if user else None, "UPLOAD_TRACER", f"Uploaded tracer assignment: {count} rows from '{tracer_uploaded.name}'"))
                    except Exception:
                        pass
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")

    # --- Agent Assigning Tab ---
    with tabs[3]:
        st.subheader("Assign ke Agent (Round-robin)")
        # Determine unassigned agreements from assign_tracer
        base_unassigned = fetchall(
            """
            SELECT a.Agreement_No, a.Assigned_To AS tracer, a.Masked_Company_Name
            FROM assign_tracer a
            LEFT JOIN agent_assignments ag ON ag.Agreement_No = a.Agreement_No
            WHERE IFNULL(a.Agreement_No,'')<>'' AND ag.Agreement_No IS NULL
            ORDER BY a.id DESC
            """
        )
        st.caption(f"Belum ter-assign ke Agent: {len(base_unassigned)} loan")

        # Filters
        f1, f2, f3 = st.columns(3)
        with f1:
            # agent candidates (approved users, role Agent preferred)
            agent_rows = fetchall("SELECT COALESCE(full_name,name) AS full_name, role FROM users WHERE approved=1 ORDER BY COALESCE(full_name,name)")
            agent_names = [r['full_name'] for r in agent_rows if r.get('full_name') and (r.get('role') in ("Agent", "Superuser", "Supervisor"))]
            selected_agents = st.multiselect("Pilih agent (>=2)", options=agent_names, default=[])
        with f2:
            filter_tracer = st.text_input("Filter by Tracer (opsional)")
        with f3:
            filter_mask = st.text_input("Filter by Masked Company (opsional)")

        # Build filtered list
        loans = []
        for r in base_unassigned:
            if filter_tracer and filter_tracer.strip().lower() not in (r.get('tracer') or '').lower():
                continue
            if filter_mask and filter_mask.strip().lower() not in (r.get('Masked_Company_Name') or '').lower():
                continue
            loans.append(r['Agreement_No'])

        cset1, cset2 = st.columns(2)
        with cset1:
            limit_n = st.number_input("Jumlah yang akan di-assign (0=semua)", min_value=0, value=min(len(loans), 100), step=1)
        with cset2:
            do_shuffle = st.checkbox("Acak urutan loan", value=True)

        if selected_agents:
            import math as _math
            per_est = _math.ceil((len(loans) if (limit_n == 0) else min(limit_n, len(loans))) / max(len(selected_agents), 1))
            st.caption(f"Perkiraan: ~{per_est} loan per agent")

        # Load saved next offset if any
        try:
            saved_off = int(get_setting('agent_rr_next_start', '0') or '0')
        except Exception:
            saved_off = 0
        rr1, rr2 = st.columns(2)
        with rr1:
            start_offset = st.number_input("Mulai dari agen ke- (offset)", min_value=0, value=int(saved_off), step=1, key="agent_rr_offset")
        with rr2:
            remember_idx = st.checkbox("Ingat offset berikutnya (auto resume)", value=False, key="agent_rr_remember")
            st.caption(f"Offset tersimpan saat ini: {saved_off}")
            if st.button("Hapus offset tersimpan"):
                try:
                    set_setting('agent_rr_next_start', '0')
                    st.rerun()
                except Exception:
                    pass

        if st.button("Assign ke Agent Sekarang", type="primary"):
            if not selected_agents or len(selected_agents) < 2:
                st.warning("Pilih minimal 2 agent.")
            elif not loans:
                st.info("Tidak ada loan memenuhi filter.")
            else:
                try:
                    ids = list(loans)
                    if do_shuffle:
                        import random
                        random.shuffle(ids)
                    if limit_n and limit_n > 0:
                        ids = ids[: min(len(ids), int(limit_n))]
                    # Build inserts round-robin
                    ins_rows = []
                    u = current_user() or {}
                    by = (u.get('full_name') or u.get('login_id') or '-')
                    n_agents = max(len(selected_agents), 1)
                    offset = int(start_offset) % n_agents
                    for i, agr in enumerate(ids):
                        agent_to = selected_agents[(i + offset) % n_agents]
                        ins_rows.append((agr, agent_to, by))
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        cur.executemany("INSERT OR IGNORE INTO agent_assignments (Agreement_No, Agent_Assigned_To, assigned_by) VALUES (?,?,?)", ins_rows)
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        st.error(f"Gagal menyimpan assign agent: {e}")
                    else:
                        st.success(f"Berhasil assign {len(ins_rows)} loan ke {len(selected_agents)} agent.")
                        # Audit
                        try:
                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id') if u else None, "AGENT_ASSIGN", f"Assign {len(ins_rows)} loans to {', '.join(selected_agents)}"))
                        except Exception:
                            pass
                        # Auto-remember next offset globally
                        if remember_idx and n_agents > 0:
                            try:
                                next_offset = (offset + len(ins_rows)) % n_agents
                                set_setting('agent_rr_next_start', str(next_offset))
                                st.caption(f"Offset berikutnya disimpan: {next_offset}")
                            except Exception:
                                pass
                        st.rerun()
                except Exception as e:
                    st.error(f"Gagal assign: {e}")

        st.markdown("---")
        st.subheader("Upload Agent Assignments (CSV/XLSX)")
        st.caption("Kolom: Agreement_No, Agent_Assigned_To. Duplikat Agreement_No akan diabaikan.")
        f = st.file_uploader("Pilih file", type=["csv", "xlsx"], key="agent_assign_upload")
        if f is not None:
            try:
                if f.name.lower().endswith('.csv'):
                    dfa = pd.read_csv(f)
                else:
                    try:
                        import openpyxl  # noqa: F401
                        dfa = pd.read_excel(f, engine='openpyxl')
                    except Exception:
                        dfa = pd.read_excel(f)
                dfa.columns = [str(c).strip() for c in dfa.columns]
                req = {"Agreement_No", "Agent_Assigned_To"}
                if not req.issubset(set(dfa.columns)):
                    st.error(f"Kolom wajib tidak lengkap. Ditemukan: {list(dfa.columns)}")
                else:
                    ok = 0; skip = 0
                    u = current_user() or {}
                    by = (u.get('full_name') or u.get('login_id') or '-')
                    for _, r in dfa.iterrows():
                        agr = str(r.get('Agreement_No') or '').strip()
                        agt = str(r.get('Agent_Assigned_To') or '').strip()
                        if not agr or not agt:
                            skip += 1
                            continue
                        try:
                            execute("INSERT OR IGNORE INTO agent_assignments (Agreement_No, Agent_Assigned_To, assigned_by) VALUES (?,?,?)", (agr, agt, by))
                            ok += 1
                        except Exception:
                            skip += 1
                    st.success(f"Upload selesai. Disimpan: {ok}. Dilewati: {skip}.")
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")

    # --- Trace Results Tab ---
    with tabs[4]:
        st.subheader("Trace Results (Touch Logs)")
        st.caption("Tambah catatan trace dan lihat log.")

        with st.form("trace_add_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                agr_input = st.text_input("Agreement_No (Loan)")
            with c2:
                tracer_sel = st.text_input("Tracer", value=(current_user().get('full_name') if current_user() else ''))
            with c3:
                status_sel = st.selectbox("Status", ["", "TRACED", "EMAILED", "RTP", "PAYING", "UNREACHABLE", "OTHER"])
            c4, c5 = st.columns(2)
            with c4:
                party_sel = st.selectbox("Party", ["", "COMPANY", "RELATIVES", "PERSONAL", "OTHER"])
            with c5:
                touch_type = st.selectbox("Touch Type", ["", "CALL", "WHATSAPP", "SMS", "EMAIL", "VISIT", "OTHER"])
            notes = st.text_area("Notes")
            submitted = st.form_submit_button("Tambah Trace")
            if submitted:
                if not agr_input.strip():
                    st.warning("Isi Agreement_No.")
                else:
                    try:
                        u = current_user() or {}
                        execute(
                            "INSERT INTO trace_results (Agreement_No, tracer, status, notes, touch_type, party, created_by) VALUES (?,?,?,?,?,?,?)",
                            (agr_input.strip(), tracer_sel.strip() if tracer_sel else None, status_sel or None, notes.strip() if notes else None, touch_type or None, party_sel or None, (u.get('full_name') or u.get('login_id') or '-'))
                        )
                        st.success("Trace ditambahkan.")
                    except Exception as e:
                        st.error(f"Gagal menyimpan: {e}")

        st.markdown("---")
        st.subheader("Lihat Log")
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            date_from = st.date_input("Dari Tanggal", value=None, key="trace_from")
        with fc2:
            date_to = st.date_input("Sampai Tanggal", value=None, key="trace_to")
        with fc3:
            f_status = st.multiselect("Status", ["TRACED", "EMAILED", "RTP", "PAYING", "UNREACHABLE", "OTHER"])
        with fc4:
            f_tracer = st.text_input("Tracer")
        f_agr = st.text_input("Cari Agreement_No", key="trace_q_agr")

        q = "SELECT Agreement_No, tracer, status, party, touch_type, notes, touched_at, created_by FROM trace_results WHERE 1=1"
        params = []
        if f_agr:
            q += " AND Agreement_No LIKE ?"
            params.append(f"%{f_agr}%")
        if f_tracer:
            q += " AND COALESCE(tracer,'') LIKE ?"
            params.append(f"%{f_tracer}%")
        if f_status:
            placeholders = ",".join(["?"] * len(f_status))
            q += f" AND COALESCE(status,'') IN ({placeholders})"
            params.extend(f_status)
        # Date filtering on touched_at (TEXT ISO). We'll compare date part.
        if date_from:
            q += " AND date(touched_at) >= date(?)"
            params.append(str(date_from))
        if date_to:
            q += " AND date(touched_at) <= date(?)"
            params.append(str(date_to))
        q += " ORDER BY touched_at DESC LIMIT 500"

        logs = fetchall(q, tuple(params))
        if logs:
            st.dataframe(pd.DataFrame(logs), use_container_width=True, hide_index=True)
        else:
            st.info("Belum ada data sesuai filter.")

    # --- Monitoring Tab (moved to first) end ---

def page_tracer():
    require_roles(("Superuser", "Tracer"))
    u = current_user()
    tracer_name = (u.get('full_name') or u.get('name')) if u else None
    st.title("Tracer Menu")
    if not tracer_name:
        st.error("Tidak dapat menentukan nama tracer. Silakan login ulang.")
        return
    st.caption(f"Assignment untuk: {tracer_name}")

    # Fetch rows assigned to this tracer (Assigned_To = user name)
    rows = fetchall(
        "SELECT id, TRC_Code, Agreement_No, Debtor_Name, NIK_KTP, EMPLOYMENT_UPDATE, EMPLOYER, Debtor_Legal_Name, Employee_Name, Employee_ID_Number, Debtor_Relation_to_Employee, Masked_Company_Name, created_at "
        "FROM assign_tracer WHERE IFNULL(Assigned_To,'') = ? ORDER BY id DESC LIMIT 500",
        (tracer_name,)
    )
    if not rows:
        st.info("Belum ada assignment untuk Anda.")
        return

    st.subheader("Daftar Assignment")
    # Quick search
    qcol1, qcol2 = st.columns([2,1])
    with qcol1:
        q_ag = st.text_input("Cari Agreement_No (Loan Number)", key="tr_q_ag")
    with qcol2:
        q_nik = st.text_input("Cari NIK", key="tr_q_nik")

    # Apply quick client-side filtering on loaded rows
    filtered_rows = []
    for r in rows:
        if q_ag and q_ag.strip() not in str(r.get('Agreement_No') or ''):
            continue
        if q_nik and q_nik.strip() not in str(r.get('NIK_KTP') or ''):
            continue
        filtered_rows.append(r)

    # Quick table view of key identifiers
    df_view = pd.DataFrame([
        {
            'ID': r['id'],
            'TRC Code': r['TRC_Code'],
            'Agreement No.': r['Agreement_No'],
            'Debtor Name': r['Debtor_Name'],
            'NIK KTP': r['NIK_KTP'],
            'Assigned At': r['created_at'],
        } for r in filtered_rows
    ])
    st.dataframe(df_view, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Update Detail Employment")
    st.caption("Pilih satu baris kemudian isi data yang diperlukan.")

    # Select a row to update
    id_options = [r['id'] for r in filtered_rows]
    sel_id = st.selectbox("Pilih ID Assignment", id_options, key="tr_sel_id")
    sel_row = next((r for r in filtered_rows if r['id'] == sel_id), None)
    if not sel_row:
        st.warning("Data tidak ditemukan.")
        return

    with st.form("tracer_update_form"):
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("TRC Code", value=sel_row.get('TRC_Code',''), disabled=True, key="tr_v_trc")
            st.text_input("Agreement No.", value=sel_row.get('Agreement_No',''), disabled=True, key="tr_v_agmt")
            st.text_input("Debtor Name", value=sel_row.get('Debtor_Name',''), disabled=True, key="tr_v_debtor")
            st.text_input("NIK KTP", value=sel_row.get('NIK_KTP',''), disabled=True, key="tr_v_nik")
        with col2:
            emp_update = st.text_input("EMPLOYMENT UPDATE", value=sel_row.get('EMPLOYMENT_UPDATE',''), key="tr_emp_update")
            employer = st.text_input("EMPLOYER", value=sel_row.get('EMPLOYER',''), key="tr_employer")
            debtor_legal = st.text_input("Debtor Legal Name", value=sel_row.get('Debtor_Legal_Name',''), key="tr_debtor_legal")
            employee_name = st.text_input("Employee Name", value=sel_row.get('Employee_Name',''), key="tr_employee_name")
            employee_id = st.text_input("Employee ID Number", value=sel_row.get('Employee_ID_Number',''), key="tr_employee_id")
            relation = st.text_input("Debtor Relation to Employee", value=sel_row.get('Debtor_Relation_to_Employee',''), key="tr_relation")

        st.markdown("---")
        st.subheader("Masked Company")
        dict_rows = fetchall("SELECT masked_name, canonical_name FROM masked_companies ORDER BY masked_name ASC")
        options = [d['masked_name'] for d in dict_rows]
        current_masked = sel_row.get('Masked_Company_Name') or ""
        masked_sel = st.selectbox("Pilih Masked Company (opsional)", ["(ketik manual)"] + options, index=0, key="tr_mask_sel")
        if masked_sel == "(ketik manual)":
            masked_manual = st.text_input("Masked Company Name", value=current_masked, key="tr_mask_manual")
            masked_value = masked_manual.strip()
        else:
            masked_value = masked_sel
        if masked_value:
            canon = next((d['canonical_name'] for d in dict_rows if d['masked_name'] == masked_value), None)
            if canon:
                st.caption(f"Canonical: {canon}")

        submitted = st.form_submit_button("Simpan Perubahan")
        if submitted:
            try:
                execute(
                    "UPDATE assign_tracer SET EMPLOYMENT_UPDATE=?, EMPLOYER=?, Debtor_Legal_Name=?, Employee_Name=?, Employee_ID_Number=?, Debtor_Relation_to_Employee=?, Masked_Company_Name=? WHERE id=? AND IFNULL(Assigned_To,'')=?",
                    (
                        (emp_update.strip() if emp_update is not None else None),
                        (employer.strip() if employer is not None else None),
                        (debtor_legal.strip() if debtor_legal is not None else None),
                        (employee_name.strip() if employee_name is not None else None),
                        (employee_id.strip() if employee_id is not None else None),
                        (relation.strip() if relation is not None else None),
                        (masked_value if masked_value else None),
                        sel_id, tracer_name
                    )
                )
                # Audit log tracer update
                try:
                    execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id') if u else None, "TRACER_UPDATE", f"Tracer '{tracer_name}' updated assignment ID {sel_id}"))
                except Exception:
                    pass
                st.success("Data berhasil diperbarui.")
                st.rerun()
            except Exception as e:
                st.error(f"Gagal update: {e}")

if __name__ == '__main__':
    main()