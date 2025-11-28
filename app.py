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
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from PIL import Image
import base64
import webbrowser
import urllib.parse

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

# ============================================
# GLOBAL GLASSMORPHISM THEME INJECTION
# Applied to all pages for consistent UI/UX
# ============================================
st.markdown("""
    <style>
    /* ============================================
       GLOBAL APP BACKGROUND & THEME
    ============================================ */
    
    /* Sidebar Toggle Button - CRITICAL: Always visible and accessible */
    button[data-testid="baseButton-header"],
    button[data-testid="collapsedControl"],
    [data-testid="collapsedControl"] button,
    section[data-testid="stSidebar"] button[kind="header"] {
        visibility: visible !important;
        display: flex !important;
        opacity: 1 !important;
        pointer-events: auto !important;
        z-index: 999999 !important;
        position: relative !important;
    }
    
    /* Ensure header toolbar is visible for sidebar toggle */
    header[data-testid="stHeader"],
    .stApp > header {
        visibility: visible !important;
        display: block !important;
        height: auto !important;
        min-height: 2.5rem !important;
    }
    
    /* Subtle gradient background for main app */
    .stApp {
        background: linear-gradient(135deg, 
            #f5f7fa 0%, 
            #e8eef5 50%, 
            #f0f4f8 100%) !important;
    }
    
    /* Main content area with subtle glass effect */
    .main .block-container {
        background: rgba(255, 255, 255, 0.3) !important;
        backdrop-filter: blur(10px) !important;
        border-radius: 20px !important;
        padding: 1rem 2rem 2rem 2rem !important;
        padding-top: 1rem !important;
        margin-top: 1rem !important;
    }
    
    /* Compact content area but ensure header space exists */
    section.main > div {
        padding-top: 0.5rem !important;
    }
    
    .main {
        padding-top: 0.5rem !important;
    }
    
    /* ============================================
       GLOBAL GLASSMORPHISM BUTTON STYLES
    ============================================ */
    
    /* Primary Buttons - Glass Effect */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, 
            rgba(99, 102, 241, 0.9) 0%, 
            rgba(139, 92, 246, 0.9) 100%) !important;
        backdrop-filter: blur(10px) saturate(150%) !important;
        -webkit-backdrop-filter: blur(10px) saturate(150%) !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
        border-radius: 12px !important;
        color: #FFFFFF !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
        box-shadow: 0 4px 16px rgba(99, 102, 241, 0.25), 
                    inset 0 1px 0 rgba(255, 255, 255, 0.2) !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, 
            rgba(99, 102, 241, 1) 0%, 
            rgba(139, 92, 246, 1) 100%) !important;
        box-shadow: 0 6px 20px rgba(99, 102, 241, 0.35) !important;
        transform: translateY(-2px) !important;
    }
    
    /* Secondary Buttons - Subtle Glass */
    .stButton > button[kind="secondary"] {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.2) 0%, 
            rgba(255, 255, 255, 0.1) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(0, 0, 0, 0.1) !important;
        border-radius: 12px !important;
        color: #374151 !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08) !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button[kind="secondary"]:hover {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.3) 0%, 
            rgba(255, 255, 255, 0.15) 100%) !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12) !important;
        transform: translateY(-2px) !important;
    }
    
    /* Default Buttons - Enhanced Glass */
    .stButton > button:not([kind]) {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.25) 0%, 
            rgba(255, 255, 255, 0.12) 100%) !important;
        backdrop-filter: blur(10px) saturate(150%) !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
        border-radius: 12px !important;
        color: #1F2937 !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08), 
                    inset 0 1px 0 rgba(255, 255, 255, 0.4) !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    
    .stButton > button:not([kind]):hover {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.35) 0%, 
            rgba(255, 255, 255, 0.18) 100%) !important;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.12) !important;
        transform: translateY(-2px) !important;
    }
    
    /* Input Fields - Glass Effect */
    .stTextInput input, .stTextArea textarea, .stSelectbox select,
    .stNumberInput input, .stDateInput input, .stTimeInput input {
        background: rgba(255, 255, 255, 0.6) !important;
        backdrop-filter: blur(8px) !important;
        border: 1px solid rgba(0, 0, 0, 0.1) !important;
        border-radius: 10px !important;
        padding: 10px 14px !important;
        transition: all 0.3s ease !important;
    }
    
    .stTextInput input:focus, .stTextArea textarea:focus, .stSelectbox select:focus {
        background: rgba(255, 255, 255, 0.8) !important;
        border-color: rgba(99, 102, 241, 0.5) !important;
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1) !important;
    }
    
    /* Tabs - Modern Glass Design */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(255, 255, 255, 0.3) !important;
        backdrop-filter: blur(10px) !important;
        padding: 8px !important;
        border-radius: 14px !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        border-radius: 10px !important;
        padding: 10px 20px !important;
        font-weight: 600 !important;
        color: #4B5563 !important;
        transition: all 0.3s ease !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.5) 0%, 
            rgba(255, 255, 255, 0.3) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(99, 102, 241, 0.3) !important;
        color: #6366F1 !important;
    }
    
    /* Expander - Glass Container */
    .streamlit-expanderHeader {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.5) 0%, 
            rgba(255, 255, 255, 0.3) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
        border-radius: 12px !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
    }
    
    .streamlit-expanderHeader:hover {
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.6) 0%, 
            rgba(255, 255, 255, 0.4) 100%) !important;
    }
    
    /* Data Editor - Glass Table */
    [data-testid="stDataFrame"] {
        background: rgba(255, 255, 255, 0.5) !important;
        backdrop-filter: blur(10px) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
    }
    
    /* Success/Info/Warning/Error Messages - Glass Cards */
    .stAlert {
        backdrop-filter: blur(10px) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
    }
    
    /* Metric Cards - Enhanced Glass */
    [data-testid="stMetricValue"] {
        font-weight: 700 !important;
    }
    
    /* Download Button - Special Style */
    .stDownloadButton > button {
        background: linear-gradient(135deg, 
            rgba(16, 185, 129, 0.9) 0%, 
            rgba(5, 150, 105, 0.9) 100%) !important;
        backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 255, 255, 0.3) !important;
        color: #FFFFFF !important;
    }
    
    .stDownloadButton > button:hover {
        box-shadow: 0 6px 20px rgba(16, 185, 129, 0.35) !important;
        transform: translateY(-2px) !important;
    }
    </style>
""", unsafe_allow_html=True)

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    # Ensure users table exists (authentication)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            full_name TEXT,
            login_id TEXT,
            email TEXT,
            password_hash TEXT,
            role TEXT,
            approved INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            division TEXT,
            nik TEXT,
            dob TEXT,
            phone_number TEXT,
            alamat TEXT,
            work_email TEXT,
            join_date TEXT,
            nomor_rekening_bca TEXT,
            nama_rekening_bca TEXT,
            sertifikasi_drive_id TEXT,
            sertifikasi_filename TEXT
        );
        """
    )
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_login ON users(login_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_approved ON users(approved)")
    except Exception:
        pass
    
    # Add new columns to existing users table if not exists
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(users)").fetchall()]
        new_cols = {
            'division': 'TEXT',
            'nik': 'TEXT',
            'dob': 'TEXT',
            'phone_number': 'TEXT',
            'alamat': 'TEXT',
            'work_email': 'TEXT',
            'join_date': 'TEXT',
            'nomor_rekening_bca': 'TEXT',
            'nama_rekening_bca': 'TEXT',
            'sertifikasi_drive_id': 'TEXT',
            'sertifikasi_filename': 'TEXT'
        }
        for col, dtype in new_cols.items():
            if col not in cols:
                c.execute(f"ALTER TABLE users ADD COLUMN {col} {dtype}")
    except Exception:
        pass
    # Seed default users if database is fresh
    try:
        cnt = (c.execute("SELECT COUNT(*) FROM users").fetchone()[0])
    except Exception:
        cnt = 0
    if not cnt:
        try:
            # Minimal seed: Superuser admin + Tracer + Agent + Supervisor
            def _hp(pw: str) -> str:
                return hashlib.sha256(pw.encode()).hexdigest()
            rows = [
                ("admin", "Administrator", "admin", "", _hp("admin123"), "Superuser", 1),
                ("supervisor", "Supervisor", "supervisor", "", _hp("supervisor123"), "Supervisor", 1),
                ("tracer", "Tracer", "tracer", "", _hp("tracer123"), "Tracer", 1),
                ("agent", "Agent", "agent", "", _hp("agent123"), "Agent", 1),
            ]
            c.executemany(
                "INSERT INTO users (name, full_name, login_id, email, password_hash, role, approved) VALUES (?,?,?,?,?,?,?)",
                rows
            )
            conn.commit()
        except Exception:
            pass
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
    # Ensure recent required columns exist in supervisor_data (idempotent ALTERs)
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(supervisor_data)").fetchall()]
        for col in [
            'NIK_KTP', 'EMPLOYMENT_UPDATE', 'EMPLOYER',
            'Debtor_Legal_Name', 'Employee_Name', 'Employee_ID_Number', 'Debtor_Relation_to_Employee',
            # Agent-updated fields (optional on upload)
            'STATUS', 'REGISTERED_PHONE', 'Additional_Contacts', 'Remarks_Suggested_NIK_Prospect', 'Payment', 'Paid_Off_Status', 'Paid_Off',
            # Approval fields
            'approval_status', 'approved_by', 'approved_at'
        ]:
            if col not in cols:
                c.execute(f"ALTER TABLE supervisor_data ADD COLUMN {col} TEXT")
    except Exception:
        pass
    # --- New foundational tables ---
    # 1) Agent assignments (one agent per Agreement_No) - ENHANCED WITH ROTATION TRACKING
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT,
            Agent_Assigned_To TEXT,
            assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
            assigned_by TEXT,
            active INTEGER DEFAULT 1,
            assignment_type TEXT DEFAULT 'agent',
            auto_return_date TEXT,
            completed_at TEXT,
            completion_reason TEXT
        );
        """
    )
    
    # Add new columns to existing agent_assignments table for rotation system
    try:
        c.execute("ALTER TABLE agent_assignments ADD COLUMN assignment_type TEXT DEFAULT 'agent'")
        c.execute("ALTER TABLE agent_assignments ADD COLUMN auto_return_date TEXT")
        c.execute("ALTER TABLE agent_assignments ADD COLUMN completed_at TEXT")
        c.execute("ALTER TABLE agent_assignments ADD COLUMN completion_reason TEXT")
    except Exception:
        pass
    
    # Remove old unique index (allow multiple assignments per Agreement_No for history)
    try:
        c.execute("DROP INDEX IF EXISTS idx_agent_assignments_unique")
    except Exception:
        pass
    
    # Create indexes for performance
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_agent_assignments_agreement ON agent_assignments(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_agent_assignments_active ON agent_assignments(active)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_agent_assignments_type ON agent_assignments(assignment_type)")
    except Exception:
        pass
    
    # 1b) Assignment history tracking table (who touched what, when)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS assignment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT NOT NULL,
            assigned_to TEXT NOT NULL,
            assignment_type TEXT DEFAULT 'agent',
            assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            assigned_by TEXT,
            completion_notes TEXT
        );
        """
    )
    
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_assignment_history_agreement ON assignment_history(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_assignment_history_assigned_to ON assignment_history(assigned_to)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_assignment_history_type ON assignment_history(assignment_type)")
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
            uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
            proof_image_drive_id TEXT,
            proof_image_filename TEXT
        );
        """
    )
    # Migration / upload history for undo support
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS migration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_type TEXT,
            target_table TEXT,
            affected_ids TEXT,
            source_file TEXT,
            user_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            undone INTEGER DEFAULT 0,
            undone_at TEXT
        );
        """
    )
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_migration_history_created_at ON migration_history(created_at)")
    except Exception:
        pass
    try:
        # Drop old UNIQUE index if exists (allow multiple payments on same date)
        c.execute("DROP INDEX IF EXISTS idx_payments_unique")
        # Create regular indexes (non-unique)
        c.execute("CREATE INDEX IF NOT EXISTS idx_payments_agreement ON payments(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(paid_date)")
        # Add new columns for proof images if not exists
        c.execute("ALTER TABLE payments ADD COLUMN proof_image_drive_id TEXT")
        c.execute("ALTER TABLE payments ADD COLUMN proof_image_filename TEXT")
        # Add approval workflow columns
        c.execute("ALTER TABLE payments ADD COLUMN approval_status TEXT DEFAULT 'pending'")
        c.execute("ALTER TABLE payments ADD COLUMN approval_by TEXT")
        c.execute("ALTER TABLE payments ADD COLUMN approval_at TEXT")
        c.execute("ALTER TABLE payments ADD COLUMN rejection_notes TEXT")
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
    # 6) Internal memos (Agent  Supervisor communication)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS memos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Agreement_No TEXT,
            author_role TEXT,
            author_name TEXT,
            target_role TEXT,
            message TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_memos_agreement ON memos(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_memos_target ON memos(target_role)")
    except Exception:
        pass
    # 7) Frozen entities (freeze by NIK or Agreement_No to prevent future assignments)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS frozen_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            NIK_KTP TEXT,
            Agreement_No TEXT,
            reason TEXT,
            note TEXT,
            active INTEGER DEFAULT 1,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_frozen_nik ON frozen_entities(NIK_KTP)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_frozen_agr ON frozen_entities(Agreement_No)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_frozen_active ON frozen_entities(active)")
    except Exception:
        pass
    # ensure assign_tracer has decoded company name field for auto-decode feature
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(assign_tracer)").fetchall()]
        if 'Decoded_Company_Name' not in cols:
            c.execute("ALTER TABLE assign_tracer ADD COLUMN Decoded_Company_Name TEXT")
    except Exception:
        pass
    
    # ensure agent_results has approval fields for cicilan approval workflow
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(agent_results)").fetchall()]
        if 'approval_status' not in cols:
            c.execute("ALTER TABLE agent_results ADD COLUMN approval_status TEXT DEFAULT 'pending'")
        if 'approval_by' not in cols:
            c.execute("ALTER TABLE agent_results ADD COLUMN approval_by TEXT")
        if 'approval_at' not in cols:
            c.execute("ALTER TABLE agent_results ADD COLUMN approval_at TEXT")
        if 'rejection_notes' not in cols:
            c.execute("ALTER TABLE agent_results ADD COLUMN rejection_notes TEXT")
    except Exception:
        pass
    
    conn.commit()
    conn.close()

# OLD page_agent() function removed - using new version with Supervisor access at line ~2749

# -------------------------
# Helper functions
# -------------------------
# -------------------------
# Helper functions
# -------------------------
def hash_password(pw: str):
    return hashlib.sha256(pw.encode()).hexdigest()

def verify_password(pw: str, h: str):
    return hash_password(pw) == h

def current_user():
    return st.session_state.get("user")

# -------------------------
# Contract Detail Screenshot & WhatsApp Helper
# -------------------------
def generate_contract_detail_html(case_data: dict, include_screenshot_js: bool = False) -> str:
    """Generate beautiful HTML for contract detail that matches the image style.
    
    Args:
        case_data: Dictionary containing contract details
        include_screenshot_js: If True, includes JavaScript for auto-screenshot to clipboard
    """
    # JavaScript untuk auto-screenshot ke clipboard (jika diminta)
    screenshot_js = ""
    if include_screenshot_js:
        screenshot_js = """
        <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
        <script>
        function captureAndCopyToClipboard() {
            const container = document.querySelector('.container');
            const button = document.getElementById('screenshotBtn');
            const statusDiv = document.getElementById('status');
            
            // Hide button before screenshot
            button.style.display = 'none';
            statusDiv.textContent = '📸 Capturing screenshot...';
            
            html2canvas(container, {
                scale: 2,
                useCORS: true,
                backgroundColor: null,
                logging: false
            }).then(canvas => {
                // Convert canvas to blob
                canvas.toBlob(blob => {
                    // Copy to clipboard using Clipboard API
                    const item = new ClipboardItem({'image/png': blob});
                    navigator.clipboard.write([item]).then(() => {
                        statusDiv.textContent = '✅ Screenshot copied to clipboard! Paste (Ctrl+V) in WhatsApp';
                        statusDiv.style.color = '#10b981';
                        button.style.display = 'block';
                        
                        // Reset message after 5 seconds
                        setTimeout(() => {
                            statusDiv.textContent = '';
                        }, 5000);
                    }).catch(err => {
                        statusDiv.textContent = '❌ Failed to copy. Please use Windows + Shift + S manually';
                        statusDiv.style.color = '#e74c3c';
                        button.style.display = 'block';
                        console.error('Clipboard error:', err);
                    });
                }, 'image/png');
            }).catch(err => {
                statusDiv.textContent = '❌ Screenshot failed. Please use Windows + Shift + S manually';
                statusDiv.style.color = '#e74c3c';
                button.style.display = 'block';
                console.error('Screenshot error:', err);
            });
        }
        </script>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        {screenshot_js}
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                padding: 0;
                margin: 0;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
            }}
            
            .wrapper {{
                width: 100%;
                padding: 20px;
                display: flex;
                justify-content: center;
            }}
            
            .container {{
                max-width: 850px;
                width: 100%;
                background: white;
                border-radius: 20px;
                box-shadow: 0 25px 70px rgba(0,0,0,0.35);
                overflow: hidden;
                animation: fadeInUp 0.5s ease-out;
            }}
            
            @keyframes fadeInUp {{
                from {{
                    opacity: 0;
                    transform: translateY(30px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 45px 35px;
                text-align: left;
                position: relative;
                overflow: hidden;
            }}
            
            .header::before {{
                content: '';
                position: absolute;
                top: 0;
                right: 0;
                width: 300px;
                height: 300px;
                background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
                border-radius: 50%;
                transform: translate(50%, -50%);
            }}
            
            .header h1 {{
                margin: 0 0 12px 0;
                font-size: 34px;
                font-weight: 700;
                letter-spacing: -0.5px;
                position: relative;
                z-index: 1;
            }}
            
            .header p {{
                margin: 0;
                font-size: 16px;
                opacity: 0.95;
                font-style: italic;
                font-weight: 400;
                position: relative;
                z-index: 1;
            }}
            
            .content {{
                padding: 35px;
                background: linear-gradient(to bottom, #ffffff 0%, #f8f9fa 100%);
            }}
            
            .detail-row {{
                display: flex;
                padding: 16px 20px;
                border-bottom: 1px solid #e9ecef;
                transition: background 0.2s ease;
                border-radius: 8px;
                margin-bottom: 2px;
            }}
            
            .detail-row:hover {{
                background: rgba(102, 126, 234, 0.04);
            }}
            
            .detail-row:last-child {{
                border-bottom: none;
                margin-bottom: 0;
            }}
            
            .detail-label {{
                flex: 0 0 260px;
                font-weight: 600;
                color: #495057;
                font-size: 14px;
                display: flex;
                align-items: center;
            }}
            
            .detail-value {{
                flex: 1;
                color: #212529;
                font-size: 14px;
                line-height: 1.5;
                word-break: break-word;
            }}
            
            .detail-value.highlight {{
                color: #667eea;
                font-weight: 700;
                font-size: 15px;
            }}
            
            .detail-value.na {{
                color: #e74c3c;
                font-weight: 600;
                font-style: italic;
            }}
            
            .screenshot-controls {{
                text-align: center;
                padding: 30px 35px;
                background: linear-gradient(135deg, 
                    rgba(102, 126, 234, 0.05) 0%, 
                    rgba(118, 75, 162, 0.05) 100%);
                border-top: 2px solid rgba(102, 126, 234, 0.15);
            }}
            
            #screenshotBtn {{
                background: linear-gradient(135deg, #10b981 0%, #059669 100%);
                color: white;
                border: none;
                padding: 14px 36px;
                font-size: 16px;
                font-weight: 700;
                border-radius: 12px;
                cursor: pointer;
                box-shadow: 0 6px 20px rgba(16, 185, 129, 0.35);
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                display: inline-flex;
                align-items: center;
                gap: 8px;
                letter-spacing: 0.3px;
                position: relative;
                overflow: hidden;
            }}
            
            #screenshotBtn::before {{
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: linear-gradient(90deg, 
                    transparent, 
                    rgba(255,255,255,0.3), 
                    transparent);
                transition: left 0.5s ease;
            }}
            
            #screenshotBtn:hover::before {{
                left: 100%;
            }}
            
            #screenshotBtn:hover {{
                transform: translateY(-3px);
                box-shadow: 0 10px 30px rgba(16, 185, 129, 0.45);
            }}
            
            #screenshotBtn:active {{
                transform: translateY(-1px);
                box-shadow: 0 4px 15px rgba(16, 185, 129, 0.35);
            }}
            
            #status {{
                margin-top: 16px;
                font-size: 14px;
                font-weight: 600;
                min-height: 22px;
                padding: 8px 16px;
                border-radius: 8px;
                display: inline-block;
                transition: all 0.3s ease;
            }}
            
            #status:empty {{
                padding: 0;
                margin: 0;
            }}
            
            /* Responsive adjustments */
            @media (max-width: 768px) {{
                .container {{
                    border-radius: 15px;
                    margin: 10px;
                }}
                
                .header {{
                    padding: 30px 25px;
                }}
                
                .header h1 {{
                    font-size: 28px;
                }}
                
                .content {{
                    padding: 25px 20px;
                }}
                
                .detail-row {{
                    flex-direction: column;
                    gap: 6px;
                    padding: 14px 16px;
                }}
                
                .detail-label {{
                    flex: none;
                    font-size: 13px;
                }}
                
                .detail-value {{
                    font-size: 13px;
                }}
                
                .screenshot-controls {{
                    padding: 25px 20px;
                }}
                
                #screenshotBtn {{
                    padding: 12px 28px;
                    font-size: 15px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="wrapper">
            <div class="container">
                <div class="header">
                    <h1>Here's your Contract Details</h1>
                    <p>We are happy to help with any settlement scheme of your choosing!</p>
                </div>
                <div class="content">
                    <div class="detail-row">
                        <div class="detail-label">Debtor Name</div>
                        <div class="detail-value highlight">: {case_data.get('Debtor_Name', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">PhoneNumber</div>
                        <div class="detail-value highlight">: {case_data.get('PhoneNumber', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Gender</div>
                        <div class="detail-value">: {case_data.get('Gender', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Legal Address</div>
                        <div class="detail-value">: {case_data.get('Legal_Address', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">DOB</div>
                        <div class="detail-value">: {case_data.get('DOB', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Email</div>
                        <div class="detail-value">: {case_data.get('Email', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Last known Office Name</div>
                        <div class="detail-value">: {case_data.get('Last_Known_Office_Name', '')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Last Known Job Position</div>
                        <div class="detail-value">: {case_data.get('Last_Known_Job_Position', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Last Known Work Phone</div>
                        <div class="detail-value">: {case_data.get('Last_Known_Work_Phone', 'None')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Debtor Phone Number II</div>
                        <div class="detail-value">: {case_data.get('Debtor_Phone_Number_II', 'None')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Debtor Other Phone Number(s)</div>
                        <div class="detail-value na">: {case_data.get('Debtor_Other_Phone_Numbers', '#N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Date of Contract</div>
                        <div class="detail-value">: {case_data.get('Date_of_Contract', 'N/A')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">DPD</div>
                        <div class="detail-value">: {case_data.get('DPD', 'N/A')}</div>
                    </div>
                </div>
                {'<div class="screenshot-controls"><button id="screenshotBtn" onclick="captureAndCopyToClipboard()">📸 Copy Screenshot to Clipboard</button><div id="status"></div></div>' if include_screenshot_js else ''}
            </div>
        </div>
    </body>
    </html>
    """
    return html

def open_whatsapp_with_clipboard_instruction(phone_number: str):
    """Open WhatsApp with the given phone number and show clipboard instruction."""
    # Clean phone number
    phone = str(phone_number).strip()
    # Remove non-digits
    phone = re.sub(r'\D', '', phone)
    
    # Convert to international format if needed
    if phone.startswith('08'):
        phone = '62' + phone[1:]  # 08xxx -> 628xxx
    elif phone.startswith('8'):
        phone = '62' + phone  # 8xxx -> 628xxx
    elif not phone.startswith('62'):
        phone = '62' + phone
    
    # WhatsApp Web URL
    wa_url = f"https://wa.me/{phone}"
    
    return wa_url

def login_user(user_row):
    st.session_state["user"] = dict(user_row)
    # Setelah login, langsung arahkan ke halaman pertama yang diizinkan untuk role ini
    try:
        role = (user_row.get('role') if isinstance(user_row, dict) else None)
        st.session_state['page'] = first_allowed_page_for_role(role) if role else 'Dashboard'
    except Exception:
        # fallback ke Dashboard bila terjadi error
        st.session_state['page'] = 'Dashboard'

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
    for k in ["auto_restore_checked", "auto_backup_checked", "auto_restore_attempted", "logout_reminder_shown"]:
        if k in st.session_state:
            del st.session_state[k]
    # Hapus pesan login agar tidak muncul setelah logout
    if "login_status_message" in st.session_state:
        st.session_state.login_status_message = {"type": None, "text": ""}
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


def undo_migration(history_id: int) -> tuple:
    """Undo a recorded migration/upload action.

    Behavior:
    - For DB table imports: deletes rows with ids listed in migration_history.affected_ids
    - Marks the migration_history entry as undone and records timestamp

    Returns (success: bool, message: str)
    """
    try:
        hist = fetchone("SELECT * FROM migration_history WHERE id = ?", (history_id,))
        if not hist:
            return False, "History entry not found"
        if hist.get('undone'):
            return False, "Already undone"

        operation = hist.get('operation_type')
        target = hist.get('target_table')
        affected = hist.get('affected_ids') or '[]'
        try:
            ids = json.loads(affected)
        except Exception:
            ids = []

        if not ids:
            # Nothing to undo
            execute("UPDATE migration_history SET undone=1, undone_at=? WHERE id=?", (datetime.utcnow().isoformat(), history_id))
            return True, "Nothing to undo (no affected IDs)"

        # Only handle simple DB table deletions here
        if operation and operation.upper().endswith("_IMPORT") and target:
            # Build delete query safely
            placeholders = ','.join(['?'] * len(ids))
            q = f"DELETE FROM {target} WHERE id IN ({placeholders})"
            try:
                conn = sqlite3.connect(DB_PATH, timeout=30)
                cur = conn.cursor()
                cur.execute(q, tuple(ids))
                conn.commit()
                conn.close()
            except Exception as e:
                return False, f"Failed to delete imported rows: {e}"

            # mark history undone
            execute("UPDATE migration_history SET undone=1, undone_at=? WHERE id=?", (datetime.utcnow().isoformat(), history_id))
            # add an audit log entry
            try:
                execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (hist.get('user_id'), 'UNDO_IMPORT', f"Undid {operation} on {target}, ids={ids}"))
            except Exception:
                pass
            return True, f"Undone import on {target} (deleted {len(ids)} rows)"

        # For other operation types, return message (special handling e.g., DRIVE_UPLOAD must be performed in page context)
        return False, "Unsupported operation for automatic undo; use page-specific undo if available"
    except Exception as e:
        return False, f"Error during undo: {e}"

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

# Timezone helpers (WIB = GMT+07:00)
def now_wib() -> datetime:
    """Return current time in GMT+7 (WIB) as a naive datetime for app display/logic."""
    try:
        return datetime.utcnow() + timedelta(hours=7)
    except Exception:
        # Fallback to server local time if UTC unavailable
        return datetime.now()

def today_wib() -> date:
    """Return today's date in WIB timezone."""
    return now_wib().date()

# -------------------------
# Freeze helpers
# -------------------------
def is_frozen_by_nik(nik: str) -> bool:
    try:
        nik = (nik or '').strip()
        if not nik:
            return False
        row = fetchone("SELECT 1 AS x FROM frozen_entities WHERE active=1 AND NIK_KTP=? LIMIT 1", (nik,))
        return bool(row)
    except Exception:
        return False

def is_frozen_by_agreement(agreement_no: str) -> bool:
    try:
        agr = (agreement_no or '').strip()
        if not agr:
            return False
        # Direct freeze by Agreement_No
        row = fetchone("SELECT 1 AS x FROM frozen_entities WHERE active=1 AND Agreement_No=? LIMIT 1", (agr,))
        if row:
            return True
        # Indirect via NIK of this Agreement_No
        info = fetchone("SELECT NIK_KTP FROM assign_tracer WHERE Agreement_No=?", (agr,)) or {}
        nik = (info.get('NIK_KTP') or '').strip()
        if not nik:
            return False
        return is_frozen_by_nik(nik)
    except Exception:
        return False

# -------------------------
# Company Decode Helper
# -------------------------
def decode_company_name(masked_name: str) -> str:
    """
    Auto-decode company name from masked format using library.
    
    Args:
        masked_name: Masked company name (e.g., "VI****** CA** IN******* PT")
    
    Returns:
        Decoded company name if found in library, otherwise returns original masked_name
    
    Examples:
        >>> decode_company_name("VI****** CA** IN******* PT")
        "VICTORIA CARE INDONESIA PT"
        >>> decode_company_name("Unknown Company")
        "Unknown Company"
    """
    try:
        if not masked_name or not masked_name.strip():
            return masked_name
        
        masked_clean = masked_name.strip()
        
        # Try exact match first
        result = fetchone(
            "SELECT canonical_name FROM masked_companies WHERE masked_name = ?", 
            (masked_clean,)
        )
        
        if result:
            return result.get('canonical_name', masked_name)
        
        # If no exact match, try case-insensitive match
        result = fetchone(
            "SELECT canonical_name FROM masked_companies WHERE LOWER(masked_name) = LOWER(?)", 
            (masked_clean,)
        )
        
        if result:
            return result.get('canonical_name', masked_name)
        
        # No match found, return original
        return masked_name
        
    except Exception:
        # On error, return original masked name
        return masked_name

# -------------------------
# Assignment Rotation & Mutual Exclusion Helpers
# -------------------------
def get_active_assignment(agreement_no: str):
    """Get current active assignment for a case (agent or tracer).
    Returns dict with keys: Agreement_No, assigned_to, assignment_type, assigned_at, auto_return_date
    or None if no active assignment.
    """
    try:
        row = fetchone("""
            SELECT Agreement_No, Agent_Assigned_To as assigned_to, assignment_type,
                   assigned_at, auto_return_date
            FROM agent_assignments
            WHERE Agreement_No=? AND active=1
            LIMIT 1
        """, (agreement_no,))
        return row
    except Exception:
        return None

def get_assignment_history(agreement_no: str, assignment_type: str = None):
    """Get all historical assignments for a case.
    If assignment_type is specified ('agent' or 'tracer'), filter by type.
    Returns list of dicts with assigned_to names.
    """
    try:
        if assignment_type:
            rows = fetchall("""
                SELECT assigned_to, assignment_type, assigned_at, completed_at
                FROM assignment_history
                WHERE Agreement_No=? AND assignment_type=?
                ORDER BY assigned_at DESC
            """, (agreement_no, assignment_type))
        else:
            rows = fetchall("""
                SELECT assigned_to, assignment_type, assigned_at, completed_at
                FROM assignment_history
                WHERE Agreement_No=?
                ORDER BY assigned_at DESC
            """, (agreement_no,))
        return rows
    except Exception:
        return []

def get_agents_who_touched_case(agreement_no: str):
    """Get list of all agent names who have ever been assigned this case.
    Returns set of agent names.
    """
    try:
        rows = fetchall("""
            SELECT DISTINCT assigned_to
            FROM assignment_history
            WHERE Agreement_No=? AND assignment_type='agent'
        """, (agreement_no,))
        return set(r['assigned_to'] for r in rows if r.get('assigned_to'))
    except Exception:
        return set()

def get_all_active_agents():
    """Get list of all users with Agent role (approved and active).
    Returns list of agent names (login_id or full_name).
    """
    try:
        rows = fetchall("""
            SELECT COALESCE(full_name, login_id) as agent_name
            FROM users
            WHERE role='Agent' AND approved=1
            ORDER BY agent_name
        """)
        return [r['agent_name'] for r in rows if r.get('agent_name')]
    except Exception:
        return []

def get_agent_allowed_dts(user_id: int) -> list:
    """Get list of allowed DTs (Lending Entities) for an agent.
    
    Returns: List of DT names. Empty list means no restrictions (can accept all DTs).
    """
    try:
        rows = fetchall("SELECT lending_entity FROM agent_dt_restrictions WHERE user_id = ?", (user_id,))
        return [r['lending_entity'] for r in rows]
    except Exception:
        return []


def set_agent_allowed_dts(user_id: int, dt_list: list, created_by: str = None) -> tuple:
    """Set allowed DTs for an agent. Replaces existing restrictions.
    
    Args:
        user_id: User ID of the agent
        dt_list: List of DT names (Lending Entities) to allow. Empty list removes all restrictions.
        created_by: Name of user making the change
    
    Returns: (success: bool, message: str)
    """
    try:
        # Delete existing restrictions
        execute("DELETE FROM agent_dt_restrictions WHERE user_id = ?", (user_id,))
        
        # Add new restrictions
        if dt_list:
            for dt in dt_list:
                if dt and dt.strip():
                    execute(
                        "INSERT INTO agent_dt_restrictions (user_id, lending_entity, created_by) VALUES (?, ?, ?)",
                        (user_id, dt.strip(), created_by)
                    )
        
        dt_count = len(dt_list) if dt_list else 0
        return True, f"DT restrictions updated: {dt_count} DT(s) allowed" if dt_count > 0 else "All DT restrictions removed (agent can accept all DTs)"
    except Exception as e:
        return False, f"Error setting DT restrictions: {e}"


def check_agent_dt_restriction(agent_name: str, case_dt: str) -> tuple:
    """Check if agent is allowed to take case from this DT.
    
    Args:
        agent_name: Full name or login_id of the agent
        case_dt: DT (Lending Entity) of the case
    
    Returns: (allowed: bool, reason: str)
    """
    try:
        # Get agent user_id
        agent = fetchone(
            "SELECT id FROM users WHERE (full_name = ? OR login_id = ?) AND role = 'Agent'",
            (agent_name, agent_name)
        )
        if not agent:
            return False, "Agent not found"
        
        # Get allowed DTs
        allowed_dts = get_agent_allowed_dts(agent['id'])
        
        # No restrictions = can accept all DTs
        if not allowed_dts:
            return True, "No DT restrictions"
        
        # Check if case DT is in allowed list
        if case_dt in allowed_dts:
            return True, f"DT '{case_dt}' allowed"
        else:
            return False, f"Agent restricted to DT(s): {', '.join(allowed_dts)}"
    except Exception as e:
        return True, f"Error checking DT restriction (allowing): {e}"


def has_recent_payment(agreement_no: str, days: int = 30) -> tuple:
    """Check if case has payment within specified days.
    
    Args:
        agreement_no: Case identifier
        days: Number of days to check (default 30 for 1 month)
    
    Returns: (has_recent: bool, last_payment_date: str or None)
    """
    try:
        cutoff_date = (now_wib() - timedelta(days=days)).date().isoformat()
        payment = fetchone(
            """SELECT paid_date, paid_amount FROM payments 
            WHERE Agreement_No=? AND COALESCE(paid_amount,0) > 0 
            AND paid_date >= ?
            ORDER BY paid_date DESC LIMIT 1""",
            (agreement_no, cutoff_date)
        )
        if payment:
            return True, payment.get('paid_date')
        return False, None
    except Exception:
        return False, None


def has_pending_approved_ptp(agreement_no: str) -> tuple:
    """Check if case has approved PTP with future date.
    
    Returns: (has_pending: bool, ptp_date: str or None)
    """
    try:
        today = today_wib().isoformat()
        ptp = fetchone(
            """SELECT agent_ptp_date FROM agent_results 
            WHERE Agreement_No=? 
            AND approval_status='approved' 
            AND agent_ptp_date IS NOT NULL 
            AND agent_ptp_date >= ?
            ORDER BY agent_ptp_date ASC LIMIT 1""",
            (agreement_no, today)
        )
        if ptp:
            return True, ptp.get('agent_ptp_date')
        return False, None
    except Exception:
        return False, None


def get_case_touch_count(agreement_no: str) -> int:
    """Get number of times this case has been handled (assignment count).
    
    Returns: Count of unique assignments
    """
    try:
        result = fetchone(
            "SELECT COUNT(*) as cnt FROM assignment_history WHERE Agreement_No=?",
            (agreement_no,)
        )
        return result.get('cnt', 0) if result else 0
    except Exception:
        return 0


def can_agent_take_case(agent_name: str, agreement_no: str) -> tuple:
    """Check if agent can take this case based on comprehensive business rules.
    
    Rules:
    1. Case must not have active assignment
    2. Case must not be frozen
    3. Case must not have recent payment (within 1 month)
    4. Case must not have pending approved PTP
    5. Agent must not have handled this case before
    6. Agent must be allowed to handle this DT (if restrictions exist)
    7. Agent can only take case if ALL other agents have touched it (rotation complete)
    
    Returns: (can_take: bool, reason: str)
    """
    try:
        # Check 0: DT restriction (check first to fail fast)
        case_info = fetchone(
            "SELECT Lending_Entity FROM supervisor_data WHERE Case_ID = ? OR Virtual_Account_Number = ? OR Third_Uid = ? LIMIT 1",
            (agreement_no, agreement_no, agreement_no)
        )
        if case_info:
            case_dt = case_info.get('Lending_Entity', '')
            if case_dt:
                dt_allowed, dt_reason = check_agent_dt_restriction(agent_name, case_dt)
                if not dt_allowed:
                    return False, dt_reason
        
        # Check 1: Active assignment exists?
        active = get_active_assignment(agreement_no)
        if active:
            assigned_type = active.get('assignment_type', 'agent')
            assigned_to = active.get('assigned_to', 'Unknown')
            return False, f"Case sedang di-assign ke {assigned_type}: {assigned_to}"
        
        # Check 2: Frozen?
        if is_frozen_by_agreement(agreement_no):
            return False, "Case ini frozen (diblokir)"
        
        # Check 3: Recent payment (within 1 month)?
        has_recent, last_pay_date = has_recent_payment(agreement_no, days=30)
        if has_recent:
            return False, f"Case ada pembayaran baru ({last_pay_date}), tunggu 1 bulan"
        
        # Check 4: Pending approved PTP?
        has_ptp, ptp_date = has_pending_approved_ptp(agreement_no)
        if has_ptp:
            return False, f"Case ada PTP approved yang belum jatuh tempo ({ptp_date})"
        
        # Check 5: Agent already handled this case before?
        history = get_assignment_history(agreement_no, 'agent')
        agent_names = [h.get('assigned_to') for h in history if h.get('assigned_to')]
        if agent_name in agent_names:
            return False, "Agent sudah pernah handle case ini sebelumnya"
        
        # Check 4: Rotation rule - all other agents must have touched this case
        all_agents = set(get_all_active_agents())
        if not all_agents:
            return True, "OK (no other agents exist)"
        
        touched_agents = get_agents_who_touched_case(agreement_no)
        
        # If agent never touched: check if rotation complete
        if agent_name not in touched_agents:
            # Agent can take if: NO agents have touched yet, OR all OTHER agents have touched
            other_agents = all_agents - {agent_name}
            if not touched_agents:
                # Fresh case, any agent can take
                return True, "OK (fresh case)"
            elif other_agents.issubset(touched_agents):
                # All other agents have touched, rotation complete
                return True, "OK (rotation complete)"
            else:
                untouched = other_agents - touched_agents
                return False, f"Agent lain harus handle dulu: {', '.join(sorted(untouched))}"
        else:
            # Agent has touched before: check if ALL other agents have touched since then
            other_agents = all_agents - {agent_name}
            if not other_agents:
                # Only one agent exists
                return True, "OK (only agent)"
            elif other_agents.issubset(touched_agents):
                # All other agents have touched
                return True, "OK (rotation complete)"
            else:
                untouched = other_agents - touched_agents
                return False, f"Agent lain harus handle dulu: {', '.join(sorted(untouched))}"
        
    except Exception as e:
        return False, f"Error checking: {str(e)}"

def get_available_cases_for_agent(agent_name: str, lending_entity_filter: list = None, employment_filter: str = None, limit: int = 100) -> list:
    """Get list of cases available for agent to take, sorted by priority.
    
    Priority order:
    1. Least-handled cases first (fewer touches = higher priority)
    2. Older cases first (by Assignment_Date)
    
    Args:
        agent_name: Agent login_id or full_name
        lending_entity_filter: Optional list of Lending_Entity to filter by
        employment_filter: Optional EMPLOYMENT_UPDATE filter ('DEBTOR', 'SPOUSE', etc)
        limit: Maximum number of cases to return
    
    Returns: List of dict with case info and touch_count
    """
    try:
        # Build base query
        query = """
            SELECT DISTINCT 
                s.Case_ID,
                s.Virtual_Account_Number,
                s.Third_Uid,
                s.Customer_name,
                s.Lending_Entity,
                s.DPD,
                s.Assignment_Date,
                s.Phone_Number_1,
                t.EMPLOYMENT_UPDATE
            FROM supervisor_data s
            LEFT JOIN assign_tracer t ON (s.Case_ID = t.Agreement_No OR s.Virtual_Account_Number = t.Agreement_No)
            WHERE 1=1
        """
        params = []
        
        # Filter by lending entity if specified
        if lending_entity_filter:
            placeholders = ','.join(['?' for _ in lending_entity_filter])
            query += f" AND s.Lending_Entity IN ({placeholders})"
            params.extend(lending_entity_filter)
        
        # Filter by employment update if specified
        if employment_filter:
            query += " AND t.EMPLOYMENT_UPDATE = ?"
            params.append(employment_filter)
        
        query += " ORDER BY s.Assignment_Date ASC"
        
        all_cases = fetchall(query, tuple(params))
        
        # Filter and sort by business rules
        available = []
        for case in all_cases:
            agreement_no = case.get('Case_ID') or case.get('Virtual_Account_Number') or case.get('Third_Uid')
            if not agreement_no:
                continue
            
            # Apply business rules
            can_take, reason = can_agent_take_case(agent_name, agreement_no)
            if can_take:
                # Add touch count for sorting
                touch_count = get_case_touch_count(agreement_no)
                case['touch_count'] = touch_count
                case['Agreement_No'] = agreement_no
                available.append(case)
        
        # Sort by touch count (ascending) then by Assignment_Date
        available.sort(key=lambda x: (x.get('touch_count', 0), x.get('Assignment_Date', '')))
        
        return available[:limit]
    except Exception as e:
        st.error(f"Error getting available cases: {e}")
        return []


def assign_case_to_agent(agreement_no: str, agent_name: str, assigned_by: str) -> tuple:
    """Assign case to agent with 7-day auto-return.
    
    Returns: (success: bool, message: str)
    """
    try:
        # Validate assignment
        can_assign, reason = can_agent_take_case(agent_name, agreement_no)
        if not can_assign:
            return False, reason
        
        # Calculate auto-return date (7 days from now)
        auto_return = (today_wib() + timedelta(days=7)).isoformat()
        now_iso = now_wib().isoformat()
        
        # Create active assignment
        execute("""
            INSERT INTO agent_assignments 
            (Agreement_No, Agent_Assigned_To, assigned_at, assigned_by, active, 
             assignment_type, auto_return_date)
            VALUES (?, ?, ?, ?, 1, 'agent', ?)
        """, (agreement_no, agent_name, now_iso, assigned_by, auto_return))
        
        # Log to history
        execute("""
            INSERT INTO assignment_history
            (Agreement_No, assigned_to, assignment_type, assigned_at, assigned_by)
            VALUES (?, ?, 'agent', ?, ?)
        """, (agreement_no, agent_name, now_iso, assigned_by))
        
        return True, f"Case berhasil di-assign ke Agent {agent_name} (auto-return: {auto_return})"
        
    except Exception as e:
        return False, f"Error assigning case: {str(e)}"

def assign_case_to_tracer(agreement_no: str, tracer_name: str, assigned_by: str) -> tuple:
    """Assign case to tracer (no time limit, mutual exclusive with agent).
    
    Returns: (success: bool, message: str)
    """
    try:
        # Check if already assigned
        active = get_active_assignment(agreement_no)
        if active:
            assigned_type = active.get('assignment_type', 'agent')
            assigned_to = active.get('assigned_to', 'Unknown')
            return False, f"Case sedang di-assign ke {assigned_type}: {assigned_to}"
        
        # Check frozen
        if is_frozen_by_agreement(agreement_no):
            return False, "Case ini frozen (diblokir)"
        
        now_iso = now_wib().isoformat()
        
        # Create active assignment (no auto_return_date for tracer)
        execute("""
            INSERT INTO agent_assignments 
            (Agreement_No, Agent_Assigned_To, assigned_at, assigned_by, active, assignment_type)
            VALUES (?, ?, ?, ?, 1, 'tracer')
        """, (agreement_no, tracer_name, now_iso, assigned_by))
        
        # Log to history
        execute("""
            INSERT INTO assignment_history
            (Agreement_No, assigned_to, assignment_type, assigned_at, assigned_by)
            VALUES (?, ?, 'tracer', ?, ?)
        """, (agreement_no, tracer_name, now_iso, assigned_by))
        
        return True, f"Case berhasil di-assign ke Tracer {tracer_name}"
        
    except Exception as e:
        return False, f"Error assigning case: {str(e)}"

def unassign_case(agreement_no: str, reason: str = "Manual unassign") -> tuple:
    """Remove active assignment and return case to database pool.
    
    Returns: (success: bool, message: str)
    """
    try:
        # Get current active assignment
        active = get_active_assignment(agreement_no)
        if not active:
            return False, "Case tidak memiliki assignment aktif"
        
        now_iso = now_wib().isoformat()
        
        # Deactivate assignment
        execute("""
            UPDATE agent_assignments
            SET active=0, completed_at=?, completion_reason=?
            WHERE Agreement_No=? AND active=1
        """, (now_iso, reason, agreement_no))
        
        # Update history completion
        execute("""
            UPDATE assignment_history
            SET completed_at=?, completion_notes=?
            WHERE Agreement_No=? AND assigned_to=? AND assignment_type=? 
            AND completed_at IS NULL
        """, (now_iso, reason, agreement_no, active['assigned_to'], active['assignment_type']))
        
        return True, f"Case berhasil di-unassign (reason: {reason})"
        
    except Exception as e:
        return False, f"Error unassigning case: {str(e)}"

def check_and_auto_return_expired_assignments():
    """Background task: Auto-return agent assignments that passed 7-day deadline WITHOUT payment.
    Should be run periodically (e.g., on dashboard load).
    
    Returns: number of cases auto-returned
    """
    try:
        today = today_wib().isoformat()
        
        # Find expired agent assignments without payment
        expired = fetchall("""
            SELECT aa.id, aa.Agreement_No, aa.Agent_Assigned_To
            FROM agent_assignments aa
            WHERE aa.active=1 
            AND aa.assignment_type='agent'
            AND aa.auto_return_date IS NOT NULL
            AND DATE(aa.auto_return_date) <= DATE(?)
            AND aa.Agreement_No NOT IN (
                SELECT DISTINCT Agreement_No FROM payments 
                WHERE COALESCE(paid_amount,0) > 0
            )
        """, (today,))
        
        count = 0
        for row in expired:
            success, msg = unassign_case(
                row['Agreement_No'], 
                f"Auto-return: 7 hari habis tanpa pembayaran"
            )
            if success:
                count += 1
        
        return count
        
    except Exception as e:
        st.warning(f"Error auto-returning cases: {str(e)}")
        return 0

# -------------------------
# Backup helpers
# -------------------------
def perform_backup(service, folder_id=FOLDER_ID_DEFAULT):
    """Create a timestamped backup of the SQLite DB to Google Drive and record in backup_log.

    Returns (success: bool, info_message: str)
    
    CRITICAL SAFEGUARDS:
    1. NEVER backup fresh/seed DB (prevent overwriting real data)
    2. NEVER backup within 15 minutes after restore (prevent backup loop)
    3. Check capacity before uploading
    """
    if not os.path.exists(DB_PATH):
        return False, f"❌ Database '{DB_PATH}' tidak ditemukan." 
    
    # ========================================================================
    # SAFEGUARD #1: BLOKIR backup jika DB masih fresh (hanya seed data)
    # ========================================================================
    try:
        if _is_probably_fresh_seed_db():
            return False, "🚫 Backup DITOLAK: DB masih fresh (user=4 seed, total data=0). Tidak akan overwrite backup lama di Drive."
    except Exception as e:
        # Jika error saat cek, TOLAK backup untuk keamanan
        return False, f"🚫 Backup DITOLAK: Gagal cek fresh DB ({e}). Untuk keamanan, backup dibatalkan."
    
    # ========================================================================
    # SAFEGUARD #2: BLOKIR backup jika baru saja restore (< 15 menit)
    # ========================================================================
    try:
        last_restore_time = get_setting('auto_restore_last_time')
        if last_restore_time:
            from dateutil import parser
            restore_dt = parser.isoparse(last_restore_time)
            now_dt = datetime.utcnow()
            minutes_since_restore = (now_dt - restore_dt).total_seconds() / 60
            if minutes_since_restore < 15:  # Grace period 15 menit
                return False, f"⏸️ Backup DITUNDA: Baru restore {int(minutes_since_restore)} menit lalu. Tunggu 15 menit untuk stabilisasi."
    except Exception:
        # Jika error parsing, abaikan safeguard ini (tapi safeguard #1 tetap aktif)
        pass
    
    # ========================================================================
    # SAFEGUARD #3: Cek kapasitas Drive sebelum upload
    # ========================================================================
    base_name = get_setting('auto_backup_filename', 'auto_backup.sqlite') or 'auto_backup.sqlite'
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
            return False, "❌ Backup GAGAL: Kapasitas Drive sudah penuh."
        if used_bytes_now + db_size > capacity:
            return False, f"❌ Backup GAGAL: Ukuran backup ({db_size} bytes) akan melebihi kapasitas."
    
    # ========================================================================
    # Eksekusi Backup (semua safeguard passed)
    # ========================================================================
    try:
        with open(DB_PATH, 'rb') as f:
            data = f.read()
        fid = upload_or_replace(service, folder_id, base_name, data, mimetype='application/x-sqlite3')
        if fid:
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (base_name, fid, 'SUCCESS', 'overwrite' if existing_files else 'created'))
            # Update timestamp backup terakhir
            set_setting('last_backup_time', datetime.utcnow().isoformat())
            return True, f"✅ Backup sukses: {base_name} ({len(data)} bytes)"
        else:
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (base_name, None, 'FAILED', 'Upload gagal (no file ID returned)'))
            return False, "❌ Upload Drive gagal: Tidak ada file ID." 
    except Exception as e:
        execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                (base_name, None, 'FAILED', str(e)))
        return False, f"❌ Backup gagal: {e}" 

def auto_daily_backup(service, folder_id=FOLDER_ID_DEFAULT):
    """Run once per session start (post-login). If last SUCCESS backup is not today -> perform one."""
    # Cek backup sukses terakhir
    row = fetchone("SELECT backup_time FROM backup_log WHERE status='SUCCESS' ORDER BY id DESC LIMIT 1")
    today_str = today_wib().isoformat()
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
    
    SAFEGUARDS: Sama seperti perform_backup - cek fresh DB dan grace period setelah restore
    """
    enabled = get_setting('scheduled_backup_enabled', 'false') == 'true'
    if not enabled:
        return False, '⏭️ Scheduled backup tidak aktif'
    
    # ========================================================================
    # SAFEGUARD #1: BLOKIR jika DB masih fresh
    # ========================================================================
    try:
        if _is_probably_fresh_seed_db():
            return False, "🚫 Scheduled backup DITOLAK: DB masih fresh (user=4 seed, total data=0)."
    except Exception as e:
        return False, f"🚫 Scheduled backup DITOLAK: Gagal cek fresh DB ({e})."
    
    # ========================================================================
    # SAFEGUARD #2: BLOKIR jika baru restore (< 15 menit)
    # ========================================================================
    try:
        last_restore_time = get_setting('auto_restore_last_time')
        if last_restore_time:
            from dateutil import parser
            restore_dt = parser.isoparse(last_restore_time)
            now_dt = datetime.utcnow()
            minutes_since_restore = (now_dt - restore_dt).total_seconds() / 60
            if minutes_since_restore < 15:
                return False, f"⏸️ Scheduled backup DITUNDA: Baru restore {int(minutes_since_restore)} menit lalu."
    except Exception:
        pass
    
    base_name = get_setting('scheduled_backup_filename', 'scheduled_backup.sqlite') or 'scheduled_backup.sqlite'
    # Determine local time (assume server already GMT+7 or adjust here if needed)
    now_local = now_wib()  # Use WIB regardless of server timezone
    slot = determine_slot(now_local)
    if slot == 'slot_unknown':
        return False, '⏭️ Di luar slot waktu yang didefinisikan'
    last_slot_done = get_setting('scheduled_backup_last_slot')
    today_tag = today_wib().isoformat()
    last_slot_date = get_setting('scheduled_backup_last_date')
    composite_last = f"{last_slot_date}:{last_slot_done}" if last_slot_done and last_slot_date else None
    composite_now = f"{today_tag}:{slot}"
    if composite_last == composite_now:
        return False, f'✅ Slot {slot} sudah di-backup hari ini'
    # Do backup overwrite single file
    if not os.path.exists(DB_PATH):
        return False, '❌ Database tidak ditemukan'
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
                return False, '❌ Scheduled backup dibatalkan: Kapasitas Drive penuh.'
            if used_bytes_now + len(data) > capacity:
                return False, '❌ Scheduled backup dibatalkan: Ukuran melebihi kapasitas.'
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
    - Jumlah user = 4 (seed default: admin, supervisor, tracer, agent)
    - Total data di sistem = 0 (tidak ada supervisor_data, agent_assignments, payments, dll)
    - backup_log kosong
    
    Logika: DB fresh = seed users only + no real data + no backup history
    """
    try:
        # Cek 1: User count harus tepat 4 (seed users)
        user_cnt = fetchone("SELECT COUNT(*) c FROM users")['c']
        if user_cnt != 4:
            return False  # Jika > 4 berarti ada user baru, jika < 4 berarti ada yang dihapus
        
        # Cek 2: Backup log harus kosong (belum pernah backup)
        bkup_cnt = fetchone("SELECT COUNT(*) c FROM backup_log")['c']
        if bkup_cnt > 0:
            return False
        
        # Cek 3: Total data di sistem harus 0
        # Hitung dari tabel-tabel utama yang menyimpan data operasional
        total_data = 0
        
        # Data supervisor (input kasus)
        total_data += fetchone("SELECT COUNT(*) c FROM supervisor_data")['c']
        
        # Assignment tracer
        total_data += fetchone("SELECT COUNT(*) c FROM assign_tracer")['c']
        
        # Assignment agent
        total_data += fetchone("SELECT COUNT(*) c FROM agent_assignments")['c']
        
        # Hasil trace
        total_data += fetchone("SELECT COUNT(*) c FROM trace_results")['c']
        
        # Hasil agent
        total_data += fetchone("SELECT COUNT(*) c FROM agent_results")['c']
        
        # Payments
        total_data += fetchone("SELECT COUNT(*) c FROM payments")['c']
        
        # Memos
        total_data += fetchone("SELECT COUNT(*) c FROM memos")['c']
        
        # Frozen entities
        total_data += fetchone("SELECT COUNT(*) c FROM frozen_entities WHERE active=1")['c']
        
        # Record notes (catatan manual)
        try:
            total_data += fetchone("SELECT COUNT(*) c FROM record_notes")['c']
        except Exception:
            pass  # Jika tabel belum ada, abaikan
        
        # Jika total data > 0, berarti sudah ada data real
        if total_data > 0:
            return False
        
        # Semua kondisi terpenuhi: user=4, no backup, no data -> DB FRESH
        return True
        
    except Exception:
        # Jika error saat pengecekan, anggap tidak fresh (safe default)
        return False

def _pick_latest_drive_backup_file(service, folder_id):
    """Pilih backup terbaru dari Google Drive.
    Prioritas: 
    1. File auto_backup.sqlite (file utama yang selalu dibackup)
    2. File .sqlite atau .db lainnya (jika ada)
    
    Filter: hindari backup yang terlalu baru (< 1 menit) untuk keamanan.
    """
    try:
        files = list_files_in_folder(service, folder_id)
    except Exception as e:
        st.session_state['restore_debug'] = f"Error list files: {e}"
        return None
    
    if not files:
        st.session_state['restore_debug'] = "Folder kosong"
        return None
    
    # Filter hanya file SQLite/DB
    candidates = [f for f in files if f.get('name','').endswith('.sqlite') or f.get('name','').endswith('.db')]
    
    if not candidates:
        st.session_state['restore_debug'] = f"Tidak ada file .sqlite/.db (total {len(files)} files)"
        return None
    
    # PRIORITAS: Cari file auto_backup.sqlite terlebih dahulu
    auto_backup_file = None
    for f in candidates:
        if f.get('name') == 'auto_backup.sqlite':
            auto_backup_file = f
            break
    
    # Jika auto_backup.sqlite ditemukan, gunakan file tersebut
    if auto_backup_file:
        st.session_state['restore_debug'] = f"Found auto_backup.sqlite (prioritas utama)"
        st.session_state['restore_picked'] = f"auto_backup.sqlite ({auto_backup_file.get('size', 0)} bytes) [PRIORITY]"
        return auto_backup_file
    
    # Jika tidak ada auto_backup.sqlite, lanjutkan dengan logic normal
    # Sort by modified time (terbaru dulu)
    try:
        candidates.sort(key=lambda x: x.get('modifiedTime',''), reverse=True)
    except Exception:
        pass
    
    # Filter: hindari backup yang terlalu baru (< 1 menit)
    try:
        now_utc = datetime.utcnow()
        safe_candidates = []
        too_new_candidates = []
        
        for f in candidates:
            fname = f.get('name', '?')
            fsize = f.get('size', 0)
            mod_time_str = f.get('modifiedTime')
            
            if mod_time_str:
                try:
                    from dateutil import parser
                    mod_dt = parser.isoparse(mod_time_str.replace('Z', '+00:00'))
                    # Hilangkan timezone info untuk comparison
                    mod_dt_naive = mod_dt.replace(tzinfo=None)
                    minutes_old = (now_utc - mod_dt_naive).total_seconds() / 60
                    
                    # Ambil backup yang umurnya >= 1 menit (lebih longgar dari 2 menit)
                    if minutes_old >= 1:
                        safe_candidates.append(f)
                    else:
                        too_new_candidates.append(f)
                except Exception as parse_err:
                    # Jika gagal parse waktu, anggap aman (include dalam safe list)
                    safe_candidates.append(f)
            else:
                # Jika tidak ada modifiedTime, anggap aman
                safe_candidates.append(f)
        
        # Debug info
        st.session_state['restore_debug'] = f"Found {len(candidates)} backups, {len(safe_candidates)} safe, {len(too_new_candidates)} too new (no auto_backup.sqlite found)"
        
        # Jika ada safe candidates, gunakan yang paling baru dari safe list
        if safe_candidates:
            picked = safe_candidates[0]
            st.session_state['restore_picked'] = f"{picked.get('name')} ({picked.get('size',0)} bytes)"
            return picked
        
        # Fallback: jika semua terlalu baru, gunakan yang paling baru
        # (better restore fresh backup than lose all data)
        if too_new_candidates:
            picked = too_new_candidates[0]
            st.session_state['restore_picked'] = f"{picked.get('name')} ({picked.get('size',0)} bytes) [FALLBACK-TOO NEW]"
            return picked
        
        # Fallback final: ambil candidate pertama
        if candidates:
            picked = candidates[0]
            st.session_state['restore_picked'] = f"{picked.get('name')} ({picked.get('size',0)} bytes) [FALLBACK-NO FILTER]"
            return picked
        
        return None
        
    except Exception as filter_err:
        # Jika filtering gagal, fallback ke candidate pertama (paling baru)
        st.session_state['restore_debug'] = f"Filter error: {filter_err}, using first candidate"
        if candidates:
            picked = candidates[0]
            st.session_state['restore_picked'] = f"{picked.get('name')} ({picked.get('size',0)} bytes) [FALLBACK-ERROR]"
            return picked
        return None

def attempt_auto_restore_if_seed(service, folder_id=FOLDER_ID_DEFAULT):
    """WAJIB restore dari backup Drive jika terdeteksi DB fresh.
    
    IMPORTANT: Fungsi ini diabaikan flag 'auto_restore_enabled' untuk keamanan data.
    Setiap kali DB terdeteksi fresh (reboot/autosleep), WAJIB restore dari Drive
    untuk mencegah kehilangan data.
    
    Returns: (success: bool, message: str)
    """
    # Hapus pengecekan setting - restore WAJIB untuk DB fresh
    # if get_setting('auto_restore_enabled', 'true') != 'true':
    #     return False, 'Auto-restore disabled'
    
    if st.session_state.get('auto_restore_attempted'):
        return False, '⏭️ Restore sudah dicoba sebelumnya di sesi ini.'
    
    st.session_state['auto_restore_attempted'] = True
    
    if not _is_probably_fresh_seed_db():
        return False, '✅ DB tidak fresh, tidak perlu restore.'
    
    # Cari backup terbaru di Drive
    try:
        latest = _pick_latest_drive_backup_file(service, folder_id)
    except Exception as e:
        debug_info = st.session_state.get('restore_debug', 'No debug info')
        return False, f'❌ Error saat mencari backup: {e}\nDebug: {debug_info}'
    
    if not latest:
        debug_info = st.session_state.get('restore_debug', 'No debug info')
        return False, f'⚠️ Tidak ada backup ditemukan di Drive.\nDebug: {debug_info}'
    
    fid = latest.get('id')
    fname = latest.get('name')
    fsize = latest.get('size', 0)
    
    # Log info untuk debugging
    st.session_state['restore_attempt_file'] = fname
    st.session_state['restore_attempt_size'] = fsize
    
    try:
        # Download dan validasi
        data = download_file_bytes(service, fid)
        
        if not data:
            return False, f'❌ File {fname} gagal didownload (data kosong).'
        
        # Validasi: harus SQLite format
        if not data.startswith(b'SQLite format 3\x00'):
            return False, f'❌ File {fname} bukan SQLite valid (magic header tidak cocok).'
        
        # Validasi: ukuran harus masuk akal (> 50KB untuk DB dengan data)
        if len(data) < 50000:
            return False, f'⚠️ File {fname} terlalu kecil ({len(data)} bytes), mungkin DB kosong.'
        
        # Backup DB lama sebelum overwrite (safety)
        try:
            if os.path.exists(DB_PATH):
                backup_old = DB_PATH + '.before_restore.bak'
                import shutil
                shutil.copy2(DB_PATH, backup_old)
                st.session_state['restore_old_backup'] = backup_old
        except Exception as backup_err:
            # Tidak kritis jika backup lokal gagal, lanjutkan restore
            st.session_state['restore_old_backup_error'] = str(backup_err)
        
        # Tulis DB baru
        with open(DB_PATH, 'wb') as f:
            f.write(data)
        
        # Catat restore berhasil
        set_setting('auto_restore_last_file', fname)
        set_setting('auto_restore_last_time', datetime.utcnow().isoformat())
        
        # Log ke backup_log untuk audit
        try:
            execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)",
                    (fname, fid, 'RESTORED', f'Auto-restore berhasil ({len(data)} bytes)'))
        except Exception:
            pass  # Tidak kritis jika logging gagal
        
        return True, f'✅ Restore berhasil dari {fname} ({len(data):,} bytes)'
        
    except Exception as e:
        error_detail = str(e)
        return False, f'❌ Restore gagal: {error_detail}\nFile: {fname} ({fsize} bytes)\nDebug: {st.session_state.get("restore_debug", "N/A")}'

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
    {"label": "Guide",       "page": "Guide", "roles": ALL_ROLES, "primary": False},
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
    
    # Hide sidebar on login page
    st.markdown("""
        <style>
        [data-testid="stSidebar"] {
            display: none;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if "login_status_message" not in st.session_state:
        st.session_state.login_status_message = {"type": None, "text": ""}

    # Bagi layout menjadi 5 kolom, form login di kolom tengah (col 3)
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    
    with col3:
        st.image("logo.png", width=650)
        st.markdown("---")
            
        tab = st.tabs(["Login", "Register"])
        
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
                        # BACKUP DIHAPUS DARI LOGIN
                        # Backup hanya dilakukan saat logout untuk mengurangi beban sistem
                        # dan memastikan hanya backup data terakhir setelah user selesai bekerja
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
            st.caption("Lengkapi semua informasi di bawah ini untuk membuat akun baru.")
            
            with st.form("registration_form"):
                st.markdown("#### Basic Information")
                col1, col2 = st.columns(2)
                with col1:
                    reg_id = st.text_input("Login ID *", key="reg_login_id", placeholder="e.g., johndoe", help="Username untuk login")
                    full_name = st.text_input("Full Name *", key="reg_full_name")
                    email_r = st.text_input("Email", key="reg_email")
                    work_email = st.text_input("Work Email", key="reg_work_email")
                    division = st.selectbox(
                        "Division *", 
                        options=["Telecollection Officer", "Supervisor", "Skiptrace Officer"],
                        key="reg_division", 
                        help="Pilih divisi/departemen Anda"
                    )
                
                with col2:
                    nik = st.text_input("NIK *", key="reg_nik", max_chars=16, help="Nomor Induk Kependudukan (max 16 karakter)")
                    phone_number = st.text_input("Phone Number *", key="reg_phone", placeholder="+62...")
                    dob = st.date_input(
                        "Date of Birth *", 
                        key="reg_dob",
                        min_value=date(1950, 1, 1),
                        max_value=today_wib(),
                        value=date(1990, 1, 1),
                        help="Tanggal lahir Anda"
                    )
                    join_date = st.date_input(
                        "Join Date *", 
                        key="reg_join_date",
                        min_value=date(2000, 1, 1),
                        max_value=today_wib(),
                        value=today_wib(),
                        help="Tanggal bergabung dengan perusahaan"
                    )
                
                alamat = st.text_area("Alamat *", key="reg_alamat", height=100, help="Alamat lengkap tempat tinggal")
                
                st.markdown("#### Banking Information")
                col3, col4 = st.columns(2)
                with col3:
                    nomor_rekening_bca = st.text_input("Nomor Rekening BCA *", key="reg_bca_no")
                with col4:
                    nama_rekening_bca = st.text_input("Nama Rekening BCA *", key="reg_bca_name", help="Nama sesuai rekening BCA")
                
                st.markdown("#### Certification (Optional)")
                sertifikasi_file = st.file_uploader(
                    "Sertifikasi Penagihan SPPI/AFPI", 
                    type=["pdf", "jpg", "jpeg", "png"],
                    key="reg_cert_upload",
                    help="Upload sertifikat penagihan (opsional)"
                )
                
                st.markdown("#### Security")
                col5, col6 = st.columns(2)
                with col5:
                    pw1 = st.text_input("Password *", type="password", key="reg_pw1")
                with col6:
                    pw2 = st.text_input("Confirm Password *", type="password", key="reg_pw2")
                
                st.caption("Fields marked with * are required")
                
                submitted = st.form_submit_button("📝 Register", use_container_width=True)
                
                if submitted:
                    # Validation
                    if not all([reg_id, full_name, nik, phone_number, division, alamat, nomor_rekening_bca, nama_rekening_bca, pw1]):
                        st.error("❌ Please fill in all required fields (*)")
                    elif pw1 != pw2:
                        st.error("❌ Password and confirmation do not match.")
                    elif len(nik) < 16:
                        st.error("❌ NIK must be 16 characters.")
                    else:
                        try:
                            # Check if login_id or NIK already exists
                            existing_id = fetchone("SELECT id FROM users WHERE login_id=?", (reg_id.strip(),))
                            existing_nik = fetchone("SELECT id FROM users WHERE nik=?", (nik.strip(),))
                            
                            if existing_id:
                                st.error(f"❌ Login ID '{reg_id.strip()}' already exists!")
                            elif existing_nik:
                                st.error(f"❌ NIK '{nik.strip()}' is already registered!")
                            else:
                                # Handle certificate upload if provided
                                cert_drive_id = None
                                cert_filename = None
                                
                                if sertifikasi_file:
                                    try:
                                        service, _ = build_drive_service()
                                        timestamp = now_wib().strftime("%Y%m%d_%H%M%S")
                                        file_ext = sertifikasi_file.name.split('.')[-1]
                                        cert_filename = f"cert_{reg_id.strip()}_{timestamp}.{file_ext}"
                                        cert_bytes = sertifikasi_file.read()
                                        cert_drive_id = upload_bytes(service, FOLDER_ID_DEFAULT, cert_filename, cert_bytes, sertifikasi_file.type)
                                        
                                        if not cert_drive_id:
                                            st.warning("⚠️ Certificate upload failed, but registration will continue without it.")
                                    except Exception as e:
                                        st.warning(f"⚠️ Certificate upload error: {e}. Registration will continue without it.")
                                
                                # Insert user with all fields
                                uid = execute(
                                    """INSERT INTO users (
                                        login_id, full_name, name, email, password_hash, role, approved,
                                        division, nik, dob, phone_number, alamat, work_email, join_date,
                                        nomor_rekening_bca, nama_rekening_bca, sertifikasi_drive_id, sertifikasi_filename
                                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                    (
                                        reg_id.strip(), 
                                        full_name.strip(), 
                                        full_name.strip(), 
                                        (email_r.strip() or None),
                                        hash_password(pw1), 
                                        "Agent",  # Default role
                                        0,  # Awaiting approval
                                        division.strip(),
                                        nik.strip(),
                                        dob.isoformat(),
                                        phone_number.strip(),
                                        alamat.strip(),
                                        (work_email.strip() or None),
                                        join_date.isoformat(),
                                        nomor_rekening_bca.strip(),
                                        nama_rekening_bca.strip(),
                                        cert_drive_id,
                                        cert_filename
                                    )
                                )
                                
                                # Audit log registration
                                try:
                                    execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                                           (uid, "REGISTER", f"User {reg_id.strip()} registered with complete profile."))
                                except Exception:
                                    pass
                                
                                st.success("✅ Registration successful! Please wait for admin approval.")
                                st.info("📧 You will be notified once your account is approved.")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Registration failed: {e}")

def page_gdrive():
    require_roles(ALL_ROLES)
    st.header("📂 Google Drive Files")
    try:
        service, _sa_email = build_drive_service()
    except Exception:
        return
    
    # ========================================================================
    # AUTO-CHECK SCHEDULED BACKUP (Silent Background)
    # ========================================================================
    try:
        if get_setting('scheduled_backup_enabled', 'false') == 'true':
            ok, msg = check_scheduled_backup(service, FOLDER_ID_DEFAULT)
            if ok:
                st.toast(f"📅 {msg}", icon="✅")
            # Jika tidak ok, tidak perlu notif (silent - sudah backup hari ini atau di luar slot)
    except Exception:
        pass  # Silent fail untuk background check
    
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
        
        # Tampilkan notifikasi jika ada
        if "record_note_notification" in st.session_state:
            notif = st.session_state.record_note_notification
            if notif["type"] == "success":
                st.success(notif["message"])
            elif notif["type"] == "error":
                st.error(notif["message"])
            elif notif["type"] == "warning":
                st.warning(notif["message"])
            del st.session_state.record_note_notification
        
        # Form counter untuk reset
        if "record_note_form_counter" not in st.session_state:
            st.session_state.record_note_form_counter = 0
        
        # Add new note
        with st.form(f'add_note_form_{st.session_state.record_note_form_counter}'):
            new_note = st.text_input('Catatan baru')
            submitted = st.form_submit_button('Tambah Catatan')
            if submitted:
                if not new_note.strip():
                    st.session_state.record_note_notification = {
                        "type": "warning",
                        "message": "⚠️ Catatan tidak boleh kosong!"
                    }
                    st.rerun()
                else:
                    try:
                        creator = (user.get('login_id') or user.get('email') or '-') if user else '-'
                        execute("INSERT INTO record_notes (note, created_by) VALUES (?, ?)", (new_note.strip(), creator))
                        st.session_state.record_note_notification = {
                            "type": "success",
                            "message": "✅ Catatan berhasil ditambahkan!"
                        }
                        st.session_state.record_note_form_counter += 1
                        st.rerun()
                    except Exception as e:
                        st.session_state.record_note_notification = {
                            "type": "error",
                            "message": f"❌ Gagal menambahkan catatan: {e}"
                        }
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
            
            st.divider()
            
            # --- Dynamic Slot Editor (RAPIKAN UI) ---
            with st.expander("🕒 Edit Slot Jadwal Backup (Advanced)", expanded=False):
                st.markdown("""
                ### 📋 Pengaturan Slot Jadwal Backup
                
                **Cara Kerja:**
                - Setiap slot menentukan **rentang jam** untuk backup otomatis (format 24 jam: 0-23)
                - Backup akan dijalankan **sekali per slot per hari** saat ada aktivitas di halaman G Drive
                - Jika **Start > End** = melewati tengah malam (contoh: 23→6 = malam hingga pagi)
                
                **Aturan:**
                - ✅ Nama slot harus **unik**
                - ✅ Start dan End **tidak boleh sama** (durasi minimal 1 jam)
                - ✅ Tidak boleh ada **overlap** antar slot
                
                ---
                """)
                
                hours = list(range(24))
                # Ambil slot saat ini dari setting / default
                if 'slot_editor_state' not in st.session_state:
                    st.session_state.slot_editor_state = get_schedule_slots()
                slots_state = st.session_state.slot_editor_state

                # Tampilkan current slot dengan styling lebih baik
                if slots_state:
                    st.markdown("#### 📌 Slot Aktif Saat Ini")
                    to_remove_indexes = []
                    
                    for idx, slot_obj in enumerate(slots_state):
                        # Hitung durasi
                        st_h = int(slot_obj['start'])
                        en_h = int(slot_obj['end'])
                        dur = (en_h - st_h) if st_h < en_h else ((24 - st_h) + en_h)
                        
                        with st.container():
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%);
                                        padding: 15px; border-radius: 10px; margin-bottom: 10px; border: 1px solid rgba(99, 102, 241, 0.2);">
                                <strong>Slot {idx + 1}:</strong> <code>{slot_obj['name']}</code> 
                                <span style="color: #6366F1;">({st_h:02d}:00 - {en_h:02d}:00, durasi: {dur}h)</span>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            c1, c2, c3, c4 = st.columns([1.5, 1.5, 3, 1])
                            with c1:
                                slots_state[idx]['start'] = st.selectbox(
                                    'Jam Mulai', hours, 
                                    index=hours.index(int(slot_obj['start'])), 
                                    key=f'slot_start_{idx}',
                                    help="Jam mulai backup (0-23)"
                                )
                            with c2:
                                slots_state[idx]['end'] = st.selectbox(
                                    'Jam Selesai', hours, 
                                    index=hours.index(int(slot_obj['end'])), 
                                    key=f'slot_end_{idx}',
                                    help="Jam selesai backup (0-23)"
                                )
                            with c3:
                                slots_state[idx]['name'] = st.text_input(
                                    'Nama Slot', 
                                    value=slot_obj['name'], 
                                    key=f'slot_name_{idx}',
                                    placeholder="Contoh: slot_pagi, slot_siang",
                                    help="Nama unik untuk slot ini"
                                )
                            with c4:
                                if st.button('🗑️ Hapus', key=f'del_slot_{idx}', type="secondary"):
                                    to_remove_indexes.append(idx)
                    
                    # Hapus slot yang diminta
                    if to_remove_indexes:
                        for ridx in sorted(to_remove_indexes, reverse=True):
                            if 0 <= ridx < len(slots_state):
                                slots_state.pop(ridx)
                        st.success(f"✅ {len(to_remove_indexes)} slot berhasil dihapus!")
                        st.rerun()
                
                st.divider()
                
                # Form tambah slot baru dengan styling lebih baik
                st.markdown("#### ➕ Tambah Slot Baru")
                
                col_new1, col_new2, col_new3, col_new4 = st.columns([1.5, 1.5, 3, 1.2])
                new_start = col_new1.selectbox('Jam Mulai', hours, key='new_slot_start', help="Jam mulai (0-23)")
                new_end = col_new2.selectbox('Jam Selesai', hours, index=hours.index((new_start+1) % 24), key='new_slot_end', help="Jam selesai (0-23)")
                new_name = col_new3.text_input('Nama Slot', key='new_slot_name', placeholder='Contoh: slot_dawn, slot_midnight', help="Nama unik untuk slot")
                
                # Preview durasi slot baru
                new_dur = (new_end - new_start) if new_start < new_end else ((24 - new_start) + new_end)
                if new_start != new_end:
                    st.caption(f"⏱️ Durasi: {new_dur} jam ({new_start:02d}:00 - {new_end:02d}:00)")
                
                if col_new4.button('➕ Tambah Slot', key='add_slot_btn', type="primary"):
                    if new_name.strip() == '':
                        st.error('❌ Nama slot tidak boleh kosong.')
                    elif any(s['name'] == new_name.strip() for s in slots_state):
                        st.error('❌ Nama slot harus unik. Sudah ada slot dengan nama tersebut.')
                    elif new_start == new_end:
                        st.error('❌ Start dan End tidak boleh sama (durasi minimal 1 jam).')
                    else:
                        slots_state.append({'start': int(new_start), 'end': int(new_end), 'name': new_name.strip()})
                        st.success(f'✅ Slot "{new_name.strip()}" berhasil ditambahkan!')
                        st.rerun()

                st.divider()

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

                # Action buttons dengan spacing lebih baik
                st.markdown("#### 💾 Aksi")
                save_col, reset_col, preview_col = st.columns([1, 1, 1])
                
                with save_col:
                    if st.button('💾 Simpan Slot Jadwal', key='save_slots_btn', type="primary", use_container_width=True):
                        # Basic structure validation
                        if not _validate_slot_struct(slots_state):
                            st.error('❌ Struktur slot tidak valid (nama unik, rentang jam 0-23, start != end).')
                        else:
                            conflicts = _check_overlaps(slots_state)
                            if conflicts:
                                conflict_msgs = []
                                for h, names in sorted(conflicts.items()):
                                    conflict_msgs.append(f"  • Jam {h}:00 → {', '.join(sorted(names))}")
                                st.error('❌ **Terdapat tumpang tindih slot:**\n\n' + '\n'.join(conflict_msgs))
                            else:
                                set_setting('scheduled_backup_slots_json', json.dumps(slots_state))
                                st.success('✅ Slot jadwal berhasil tersimpan ke konfigurasi!')
                                st.balloons()
                
                with reset_col:
                    if st.button('♻️ Reset ke Default', key='reset_slots_btn', use_container_width=True):
                        st.session_state.slot_editor_state = DEFAULT_SCHEDULE_SLOTS.copy()
                        set_setting('scheduled_backup_slots_json', json.dumps(DEFAULT_SCHEDULE_SLOTS))
                        st.info('ℹ️ Slot dikembalikan ke konfigurasi default (4 slot standar).')
                        st.rerun()
                
                with preview_col:
                    if st.button('📄 Preview JSON', key='export_slots_btn', use_container_width=True):
                        st.code(json.dumps(slots_state, indent=2), language='json')

                # Preview tabel dengan styling lebih baik
                if slots_state:
                    st.divider()
                    st.markdown("#### 📊 Preview Slot Aktif")
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
                    # Record in migration_history for undo (delete from Drive)
                    try:
                        hist_id = execute(
                            "INSERT INTO migration_history (operation_type, target_table, affected_ids, source_file, user_id) VALUES (?,?,?,?,?)",
                            ('DRIVE_UPLOAD', 'gdrive_files', json.dumps([fid]), uploaded.name, user.get('id') if user else None),
                        )
                        if hist_id:
                            if st.button("🗑️ Undo Upload (Delete from Drive)", key=f"undo_drive_{hist_id}"):
                                try:
                                    delete_file(service, fid)
                                    execute("UPDATE migration_history SET undone=1, undone_at=? WHERE id=?", (datetime.utcnow().isoformat(), hist_id))
                                    execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (user.get('id') if user else None, 'UNDO_UPLOAD', f"Deleted file '{uploaded.name}' (ID: {fid})"))
                                    st.success(f"✅ File '{uploaded.name}' deleted from Drive")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error deleting file: {e}")
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
        # Use neutral blue/green colors (avoid brown tones); fall back to Altair defaults if capacity unknown
        color_scale = alt.Scale(domain=["Used", "Free"], range=["#1E88E5", "#4CAF50"]) if CAPACITY_BYTES > 0 else alt.Undefined
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

    # ========================================================================
    # CRITICAL: PRE-LOGIN AUTO-RESTORE (WAJIB sync dari Drive sebelum backup!)
    # ========================================================================
    # Ini adalah safeguard utama untuk mencegah overwrite backup lama saat reboot/autosleep
    # Flow: Deteksi DB fresh → Tampilkan halaman Restore System → Manual restore
    
    # Initialize page first if not exists
    if "page" not in st.session_state:
        st.session_state.page = "Authentication"
    if "user" not in st.session_state:
        st.session_state.user = None
    
    if "prelogin_auto_restore_done" not in st.session_state:
        is_fresh = _is_probably_fresh_seed_db()
        
        # Debug info - hapus setelah testing
        st.session_state['debug_is_fresh'] = is_fresh
        st.session_state['debug_user_count'] = fetchone("SELECT COUNT(*) c FROM users").get('c', 0)
        st.session_state['debug_backup_count'] = fetchone("SELECT COUNT(*) c FROM backup_log").get('c', 0)
        
        # Hitung total data di sistem untuk debug
        try:
            total_sys_data = 0
            total_sys_data += fetchone("SELECT COUNT(*) c FROM supervisor_data").get('c', 0)
            total_sys_data += fetchone("SELECT COUNT(*) c FROM assign_tracer").get('c', 0)
            total_sys_data += fetchone("SELECT COUNT(*) c FROM agent_assignments").get('c', 0)
            total_sys_data += fetchone("SELECT COUNT(*) c FROM trace_results").get('c', 0)
            total_sys_data += fetchone("SELECT COUNT(*) c FROM agent_results").get('c', 0)
            total_sys_data += fetchone("SELECT COUNT(*) c FROM payments").get('c', 0)
            st.session_state['debug_total_data'] = total_sys_data
        except Exception:
            st.session_state['debug_total_data'] = 'Error'
        
        if is_fresh:
            # DB FRESH terdeteksi! Redirect ke halaman Restore System
            try:
                if "service_account" not in st.secrets:
                    st.session_state['prelogin_auto_restore_result'] = {
                        'success': False,
                        'message': '⚠️ DB fresh terdeteksi, tapi service_account tidak tersedia untuk restore.',
                        'time': datetime.utcnow().isoformat()
                    }
                    st.session_state.page = 'RestoreStatus'
                else:
                    service_pre, _ = build_drive_service()
                    st.session_state['restore_service'] = service_pre
                    st.session_state['restore_folder_id'] = FOLDER_ID_DEFAULT
                    st.session_state.page = 'RestoreSystem'
            except Exception as e:
                st.session_state['prelogin_auto_restore_result'] = {
                    'success': False,
                    'message': f'❌ Gagal connect ke Google Drive: {e}',
                    'time': datetime.utcnow().isoformat()
                }
                st.session_state.page = 'RestoreStatus'
        else:
            # DB tidak fresh, lanjutkan normal
            st.session_state['prelogin_auto_restore_result'] = {
                'success': True,
                'message': '✅ DB sudah berisi data, tidak perlu restore.',
                'time': datetime.utcnow().isoformat()
            }
            # Page sudah di-set ke Authentication di atas, tidak perlu set lagi
        
        st.session_state['prelogin_auto_restore_done'] = True


    user = current_user()

    # Sidebar with Glassmorphism Design
    st.sidebar.image("logo.png", use_container_width=True)
    st.sidebar.title("Navigasi")
    
    # Glassmorphism Sidebar & Button Style inspired by modern UI/UX
    st.sidebar.markdown(
        """
        <style>
        /* ============================================
           GLASSMORPHISM SIDEBAR THEME
           Inspired by modern dashboard designs
        ============================================ */
        
        /* Sidebar Background - Glass Effect */
        [data-testid="stSidebar"] {
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 0.15) 0%, 
                rgba(255, 255, 255, 0.08) 100%) !important;
            backdrop-filter: blur(20px) saturate(180%) !important;
            -webkit-backdrop-filter: blur(20px) saturate(180%) !important;
            border-right: 1px solid rgba(255, 255, 255, 0.18) !important;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.12) !important;
        }
        
        /* Sidebar Content - Enhanced Typography */
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
            color: #1F2937 !important;
        }
        
        /* Logo Container - Soft Shadow */
        [data-testid="stSidebar"] img {
            border-radius: 16px !important;
            padding: 8px !important;
            background: rgba(255, 255, 255, 0.5) !important;
            backdrop-filter: blur(10px) !important;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08) !important;
            margin-bottom: 16px !important;
        }
        
        /* Navigation Buttons - Glassmorphism Cards */
        div[data-testid="stSidebar"] .stButton { 
            margin-bottom: 8px; 
        }
        
        div[data-testid="stSidebar"] .stButton > button {
            /* Glass morphism effect */
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 0.25) 0%, 
                rgba(255, 255, 255, 0.12) 100%) !important;
            backdrop-filter: blur(10px) saturate(150%) !important;
            -webkit-backdrop-filter: blur(10px) saturate(150%) !important;
            
            /* Border & Shadow */
            border: 1px solid rgba(255, 255, 255, 0.3) !important;
            border-radius: 14px !important;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.06), 
                        inset 0 1px 0 rgba(255, 255, 255, 0.5) !important;
            
            /* Typography */
            color: #1F2937 !important;
            font-weight: 600 !important;
            font-size: 14px !important;
            letter-spacing: 0.3px !important;
            
            /* Spacing */
            padding: 12px 16px !important;
            min-height: 44px !important;
            width: 100% !important;
            text-align: left !important;
            
            /* Transition */
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
            position: relative !important;
            overflow: visible !important;
        }
        
        /* Button Hover - Glow Effect */
        div[data-testid="stSidebar"] .stButton > button:hover:not(:disabled) {
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 0.35) 0%, 
                rgba(255, 255, 255, 0.18) 100%) !important;
            border-color: rgba(255, 255, 255, 0.5) !important;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.12), 
                        inset 0 1px 0 rgba(255, 255, 255, 0.6),
                        0 0 20px rgba(99, 102, 241, 0.15) !important;
            transform: translateY(-2px) !important;
            color: #111827 !important;
        }
        
        /* Active/Disabled Button - MEGA HIGHLIGHT */
        div[data-testid="stSidebar"] .stButton > button:disabled,
        div[data-testid="stSidebar"] .stButton > button[disabled],
        div[data-testid="stSidebar"] .stButton > button[aria-disabled="true"] {
            /* Super vibrant gradient background */
            background: linear-gradient(135deg, 
                #6366F1 0%, 
                #8B5CF6 100%) !important;
            
            /* Strong borders with left accent */
            border: 2px solid rgba(255, 255, 255, 0.6) !important;
            border-left: 6px solid #FFFFFF !important;
            
            /* Multiple layered shadows for depth */
            box-shadow: 
                0 0 0 3px rgba(99, 102, 241, 0.2),
                0 12px 35px rgba(99, 102, 241, 0.5), 
                inset 0 2px 6px rgba(255, 255, 255, 0.4),
                0 0 30px rgba(99, 102, 241, 0.4) !important;
            
            /* Stronger backdrop filter */
            backdrop-filter: blur(20px) saturate(200%) !important;
            -webkit-backdrop-filter: blur(20px) saturate(200%) !important;
            
            /* Bold white text */
            color: #FFFFFF !important;
            font-weight: 900 !important;
            font-size: 15px !important;
            letter-spacing: 0.6px !important;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.2) !important;
            
            /* Elevated and shifted */
            opacity: 1 !important;
            transform: scale(1.05) translateX(6px) !important;
            
            /* Pointer events */
            cursor: not-allowed !important;
            pointer-events: none !important;
        }
        
        /* Active Button Indicator Dot */
        div[data-testid="stSidebar"] .stButton > button:disabled::after,
        div[data-testid="stSidebar"] .stButton > button[disabled]::after {
            content: '●';
            position: absolute;
            right: 14px;
            top: 50%;
            transform: translateY(-50%);
            color: #FFFFFF;
            font-size: 12px;
            text-shadow: 0 0 8px rgba(255, 255, 255, 0.8);
            animation: pulse-active 2s ease-in-out infinite;
        }
        
        @keyframes pulse-active {
            0%, 100% { 
                opacity: 1; 
                transform: translateY(-50%) scale(1);
                text-shadow: 0 0 8px rgba(255, 255, 255, 0.8);
            }
            50% { 
                opacity: 0.7; 
                transform: translateY(-50%) scale(1.3);
                text-shadow: 0 0 15px rgba(255, 255, 255, 1);
            }
        }
        
        /* Button Ripple Effect - Only for non-disabled buttons */
        div[data-testid="stSidebar"] .stButton > button:not(:disabled)::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, 
                transparent, 
                rgba(255, 255, 255, 0.3), 
                transparent);
            transition: left 0.5s;
            z-index: 1;
        }
        
        div[data-testid="stSidebar"] .stButton > button:not(:disabled):hover::before {
            left: 100%;
        }
        
        /* Logout Button - Special Accent */
        div[data-testid="stSidebar"] .stButton:last-of-type > button {
            background: linear-gradient(135deg, 
                rgba(239, 68, 68, 0.15) 0%, 
                rgba(220, 38, 38, 0.10) 100%) !important;
            border-color: rgba(239, 68, 68, 0.3) !important;
            color: #DC2626 !important;
        }
        
        div[data-testid="stSidebar"] .stButton:last-of-type > button:hover {
            background: linear-gradient(135deg, 
                rgba(239, 68, 68, 0.25) 0%, 
                rgba(220, 38, 38, 0.18) 100%) !important;
            border-color: rgba(239, 68, 68, 0.5) !important;
            color: #B91C1C !important;
            box-shadow: 0 8px 24px rgba(239, 68, 68, 0.2) !important;
        }
        
        /* Divider Lines - Subtle Glass Effect */
        [data-testid="stSidebar"] hr {
            border: none !important;
            height: 1px !important;
            background: linear-gradient(90deg, 
                transparent, 
                rgba(255, 255, 255, 0.3), 
                transparent) !important;
            margin: 16px 0 !important;
        }
        
        /* User Info Card - Glass Container */
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] strong {
            display: inline-block;
            padding: 6px 12px;
            background: rgba(255, 255, 255, 0.2);
            backdrop-filter: blur(5px);
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.25);
            margin: 2px 0;
        }
        
        /* Caption Text - Enhanced Readability */
        [data-testid="stSidebar"] .stCaption {
            color: #6B7280 !important;
            font-size: 12px !important;
            font-weight: 500 !important;
        }
        
        /* Responsive Mobile Adjustments */
        @media (max-width: 768px) {
            div[data-testid="stSidebar"] .stButton > button {
                font-size: 13px !important;
                padding: 10px 14px !important;
                min-height: 40px !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if user:
        # Enhanced User Profile Card with Modern UI/UX
        full_name = user.get('full_name') or user.get('name') or user.get('login_id') or 'User'
        email = user.get('email') or '-'
        division = user.get('division') or '-'
        role = user.get('role') or 'User'
        
        # Modern User Profile Card
        profile_html = f"""
        <div style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.15) 0%, rgba(139, 92, 246, 0.10) 100%); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.3); border-radius: 16px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08);">
            <div style="text-align: center; margin-bottom: 16px;">
                <div style="width: 64px; height: 64px; background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%); border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; margin-bottom: 12px; font-size: 28px; font-weight: 700; color: white; box-shadow: 0 4px 12px rgba(99, 102, 241, 0.3);">
                    {full_name[0].upper()}
                </div>
                <div style="font-size: 18px; font-weight: 700; color: #1F2937; margin-bottom: 4px;">
                    {full_name}
                </div>
            </div>
            <div style="background: rgba(255, 255, 255, 0.5); backdrop-filter: blur(5px); border-radius: 12px; padding: 14px; border: 1px solid rgba(255, 255, 255, 0.4);">
                <div style="margin-bottom: 10px;">
                    <div style="font-size: 10px; font-weight: 600; color: #6B7280; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">📧 Email</div>
                    <div style="font-size: 12px; color: #1F2937; font-weight: 500; word-break: break-all;">{email}</div>
                </div>
                <div style="margin-bottom: 10px;">
                    <div style="font-size: 10px; font-weight: 600; color: #6B7280; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">🏢 Division</div>
                    <div style="font-size: 12px; color: #1F2937; font-weight: 500;">{division}</div>
                </div>
                <div>
                    <div style="font-size: 10px; font-weight: 600; color: #6B7280; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">👑 Role</div>
                    <div style="display: inline-block; background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%); color: white; padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 700; letter-spacing: 0.3px; box-shadow: 0 2px 8px rgba(99, 102, 241, 0.25);">{role}</div>
                </div>
            </div>
        </div>
        """
        st.sidebar.markdown(profile_html, unsafe_allow_html=True)
        
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
    elif st.session_state.page not in ['RestoreStatus', 'RestoreSystem']:
        # Hanya tampilkan tombol login jika bukan di halaman restore
        if st.sidebar.button("🔐 Login / Register", use_container_width=True):
            st.session_state.page = "Authentication"
            st.rerun()
    
    # Halaman Restore System (saat DB fresh, sebelum login)
    # AUTO-RESTORE: Langsung restore tanpa tombol, otomatis
    if st.session_state.page == 'RestoreSystem' and not user:
        st.markdown("""
        <style>
        /* Collapse sidebar on restore page but keep toggle button visible */
        [data-testid="stSidebar"][aria-expanded="true"] {
            display: flex !important;
        }
        
        [data-testid="stSidebar"][aria-expanded="false"] {
            display: none !important;
        }
        
        /* Ensure sidebar toggle button is always visible and accessible */
        button[data-testid="baseButton-header"],
        button[kind="header"],
        [data-testid="collapsedControl"] {
            visibility: visible !important;
            display: flex !important;
            opacity: 1 !important;
            pointer-events: auto !important;
            z-index: 999999 !important;
        }
        
        /* Limit content width to compact centered layout */
        .main .block-container {
            max-width: 600px !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
            padding-top: 3rem !important;
        }
        
        /* Center the container */
        section[data-testid="stMain"] > div {
            max-width: 100%;
            display: flex;
            justify-content: center;
        }
        
        /* Responsive for mobile */
        @media (max-width: 768px) {
            .main .block-container {
                max-width: 100% !important;
                padding-left: 1rem !important;
                padding-right: 1rem !important;
            }
        }
        </style>
        """, unsafe_allow_html=True)
        
        st.image("logo.png", width=180)
        st.title("🔄 Auto Restore System")
        st.markdown("---")
        
        st.info("💡 Database fresh terdeteksi! Sedang melakukan restore otomatis dari Google Drive...")
        
        # Progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Step 1: Build service
            status_text.text("🔌 Menghubungkan ke Google Drive...")
            progress_bar.progress(20)
            import time
            time.sleep(0.5)
            
            service = st.session_state.get('restore_service')
            folder_id = st.session_state.get('restore_folder_id', FOLDER_ID_DEFAULT)
            
            if not service:
                raise Exception("Service Google Drive tidak tersedia")
            
            # Step 2: Find backup
            status_text.text("🔍 Mencari file backup...")
            progress_bar.progress(40)
            time.sleep(0.5)
            
            # Step 3: Download and restore
            status_text.text("⬇️ Mendownload dan restore database...")
            progress_bar.progress(60)
            
            ok_pre, msg_pre = attempt_auto_restore_if_seed(service, folder_id)
            
            # Step 4: Complete
            progress_bar.progress(100)
            status_text.text("✅ Restore selesai!")
            
            st.session_state['prelogin_auto_restore_result'] = {
                'success': ok_pre,
                'message': msg_pre,
                'time': datetime.utcnow().isoformat()
            }
            
            # Sinkronkan flag untuk mencegah backup langsung setelah restore
            st.session_state['auto_restore_checked'] = 'restored' if ok_pre else 'checked'
            
            if ok_pre:
                # Re-init DB setelah restore untuk memuat data fresh
                init_db()
                # Set flag agar tidak backup dalam 15 menit ke depan
                set_setting('auto_restore_last_time', datetime.utcnow().isoformat())
                
                # Success - show toast and redirect to login
                st.toast("✅ Restore berhasil! Mengarahkan ke halaman login...", icon="✅")
                time.sleep(1.5)  # Give time to show toast
                st.session_state.page = 'Authentication'
                st.session_state['prelogin_auto_restore_done'] = True
                st.rerun()
            else:
                # Failed - redirect to status page
                st.session_state.page = 'RestoreStatus'
                st.rerun()
                
        except Exception as e:
            st.error(f"❌ Error saat restore: {e}")
            st.session_state['prelogin_auto_restore_result'] = {
                'success': False,
                'message': f'❌ Auto-Restore error: {e}',
                'time': datetime.utcnow().isoformat()
            }
            st.session_state.page = 'RestoreStatus'
            import time
            time.sleep(2)
            st.rerun()
        
        return
    
    # Halaman status restore (sebelum login) bila baru saja wake & mencoba restore
    if st.session_state.page == 'RestoreStatus' and not user:
        st.markdown("""
        <style>
        /* Hide sidebar on restore status page */
        [data-testid="stSidebar"] {display: none !important;}
        
        /* Limit content width to compact centered layout */
        .main .block-container {
            max-width: 600px !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
            padding-top: 3rem !important;
        }
        
        /* Center the container */
        section[data-testid="stMain"] > div {
            max-width: 100%;
            display: flex;
            justify-content: center;
        }
        
        /* Responsive for mobile */
        @media (max-width: 768px) {
            .main .block-container {
                max-width: 100% !important;
                padding-left: 1rem !important;
                padding-right: 1rem !important;
            }
        }
        </style>
        """, unsafe_allow_html=True)
        
        st.image("logo.png", width=180)
        st.title('⏳ Status Restore')
        st.markdown("---")
        
        res = st.session_state.get('prelogin_auto_restore_result', {})
        if res.get('success'):
            st.success(f"✅ Berhasil restore: {res.get('message','')} ")
            st.info("💡 Database telah dipulihkan dari backup Google Drive. Silakan login untuk melanjutkan.")
            
            # Tambahkan informasi tentang backup pertama setelah login
            st.caption("📌 Setelah login pertama kali, sistem akan membuat backup baru untuk checkpoint.")
            
            # Auto-redirect ke login setelah 2 detik
            import time
            with st.spinner("Mengarahkan ke halaman login..."):
                time.sleep(2)
            st.session_state.page = 'Authentication'
            st.rerun()
        else:
            st.error("❌ Restore gagal!")
            st.warning(res.get('message','Tidak ada informasi restore.'))
            st.caption(f"⏰ Waktu: {res.get('time','-')}")
            
            # Debug info untuk troubleshooting
            with st.expander("🔍 Detail Error & Debug Info", expanded=True):
                st.markdown("**Error Message:**")
                st.code(res.get('message', 'No message'))
                
                if st.session_state.get('restore_debug'):
                    st.markdown("**Pencarian File:**")
                    st.code(st.session_state.get('restore_debug'))
                
                if st.session_state.get('restore_picked'):
                    st.markdown("**File yang Dipilih:**")
                    st.info(st.session_state.get('restore_picked'))
                
                if st.session_state.get('restore_attempt_file'):
                    st.markdown("**Attempt Info:**")
                    st.write(f"- File: {st.session_state.get('restore_attempt_file')}")
                    st.write(f"- Size: {st.session_state.get('restore_attempt_size', 0)} bytes")
            
            st.markdown("---")
            st.markdown("### 🔧 Langkah Selanjutnya")
            st.markdown("""
            1. **Periksa koneksi Google Drive** - Pastikan service account memiliki akses
            2. **Pastikan file backup tersedia** di Drive folder
            3. **Cek ukuran file** - File backup harus > 50KB
            4. **Format file** - Harus .sqlite atau .db format
            5. **Hubungi administrator** jika masalah berlanjut
            """)
            
            # Retry button
            col1, col2 = st.columns(2)
            with col1:
                if st.button('🔄 Coba Restore Lagi', type='primary', use_container_width=True):
                    # Reset flags
                    if 'auto_restore_attempted' in st.session_state:
                        del st.session_state['auto_restore_attempted']
                    if 'prelogin_auto_restore_done' in st.session_state:
                        del st.session_state['prelogin_auto_restore_done']
                    st.rerun()
            with col2:
                if st.button('⏭️ Skip ke Login', use_container_width=True):
                    st.session_state.page = 'Authentication'
                    st.rerun()
        
        st.markdown("---")
        if st.button('🔐 Lanjut ke Login »', type='primary', use_container_width=True):
            st.session_state.page = 'Authentication'
            st.rerun()
        return

    if not user:
        page_auth()
        return

    # ========================================================================
    # POST-LOGIN REMINDER: Show logout reminder toast (5 seconds)
    # ========================================================================
    if 'logout_reminder_shown' not in st.session_state:
        st.session_state.logout_reminder_shown = True
        st.toast("⚠️ Setelah menggunakan apps, jangan lupa logout untuk menghindari data tidak tersimpan!", icon="⚠️")

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
    if st.session_state.page == "Guide":
        page_guide()
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
    require_roles(("Superuser", "Supervisor", "Tracer", "Agent"))
    
    # Import dependencies at the top
    import pandas as pd
    from datetime import datetime, timedelta
    
    # Get current user info
    user_obj = current_user() or {}
    user_role = user_obj.get("role", "")
    user_id = user_obj.get("id")
    is_supervisor = user_role in ("Superuser", "Supervisor")
    
    # ========================================================================
    # HEADER WITH GLASSMORPHISM STYLE
    # ========================================================================
    header_subtitle = "Semua aktivitas aplikasi direkam di sini. Waktu: GMT+07:00 (WIB)" if is_supervisor else "Aktivitas Anda direkam di sini. Waktu: GMT+07:00 (WIB)"
    st.markdown(f"""
        <div style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%); 
                    backdrop-filter: blur(10px); 
                    border: 1px solid rgba(99, 102, 241, 0.2); 
                    border-radius: 16px; 
                    padding: 24px; 
                    margin-bottom: 24px;">
            <h1 style="margin: 0; color: #1F2937; font-size: 32px; font-weight: 700;">
                📋 Audit Log System
            </h1>
            <p style="margin: 8px 0 0 0; color: #6B7280; font-size: 14px;">
                {header_subtitle}
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    # ========================================================================
    # STATISTICS CARDS (filtered by user for non-supervisors)
    # ========================================================================
    stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
    
    # Get statistics - filter by user_id for non-supervisors
    if is_supervisor:
        total_logs = fetchone("SELECT COUNT(*) as count FROM audit_logs")['count']
        total_users = fetchone("SELECT COUNT(DISTINCT user_id) as count FROM audit_logs WHERE user_id IS NOT NULL")['count']
        today_logs = fetchone("""
            SELECT COUNT(*) as count FROM audit_logs 
            WHERE DATE(timestamp) = DATE('now', '+7 hours')
        """)['count']
        login_count = fetchone("""
            SELECT COUNT(*) as count FROM audit_logs 
            WHERE action = 'LOGIN' AND DATE(timestamp) = DATE('now', '+7 hours')
        """)['count']
    else:
        total_logs = fetchone("SELECT COUNT(*) as count FROM audit_logs WHERE user_id = ?", (user_id,))['count']
        total_users = 1  # Only current user
        today_logs = fetchone("""
            SELECT COUNT(*) as count FROM audit_logs 
            WHERE user_id = ? AND DATE(timestamp) = DATE('now', '+7 hours')
        """, (user_id,))['count']
        login_count = fetchone("""
            SELECT COUNT(*) as count FROM audit_logs 
            WHERE user_id = ? AND action = 'LOGIN' AND DATE(timestamp) = DATE('now', '+7 hours')
        """, (user_id,))['count']
    
    with stats_col1:
        st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(37, 99, 235, 0.1) 100%); 
                        backdrop-filter: blur(10px); 
                        border: 1px solid rgba(59, 130, 246, 0.2); 
                        border-radius: 12px; 
                        padding: 16px; 
                        text-align: center;">
                <div style="font-size: 14px; color: #6B7280; margin-bottom: 8px;">Total Logs</div>
                <div style="font-size: 28px; font-weight: 700; color: #3B82F6;">""" + f"{total_logs:,}" + """</div>
            </div>
        """, unsafe_allow_html=True)
    
    with stats_col2:
        st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.1) 0%, rgba(5, 150, 105, 0.1) 100%); 
                        backdrop-filter: blur(10px); 
                        border: 1px solid rgba(16, 185, 129, 0.2); 
                        border-radius: 12px; 
                        padding: 16px; 
                        text-align: center;">
                <div style="font-size: 14px; color: #6B7280; margin-bottom: 8px;">Active Users</div>
                <div style="font-size: 28px; font-weight: 700; color: #10B981;">""" + f"{total_users}" + """</div>
            </div>
        """, unsafe_allow_html=True)
    
    with stats_col3:
        st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(245, 158, 11, 0.1) 0%, rgba(217, 119, 6, 0.1) 100%); 
                        backdrop-filter: blur(10px); 
                        border: 1px solid rgba(245, 158, 11, 0.2); 
                        border-radius: 12px; 
                        padding: 16px; 
                        text-align: center;">
                <div style="font-size: 14px; color: #6B7280; margin-bottom: 8px;">Today's Logs</div>
                <div style="font-size: 28px; font-weight: 700; color: #F59E0B;">""" + f"{today_logs}" + """</div>
            </div>
        """, unsafe_allow_html=True)
    
    with stats_col4:
        st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(139, 92, 246, 0.1) 0%, rgba(124, 58, 237, 0.1) 100%); 
                        backdrop-filter: blur(10px); 
                        border: 1px solid rgba(139, 92, 246, 0.2); 
                        border-radius: 12px; 
                        padding: 16px; 
                        text-align: center;">
                <div style="font-size: 14px; color: #6B7280; margin-bottom: 8px;">Today's Logins</div>
                <div style="font-size: 28px; font-weight: 700; color: #8B5CF6;">""" + f"{login_count}" + """</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # ========================================================================
    # FILTERS WITH ENHANCED UI
    # ========================================================================
    with st.expander("🔍 Filter & Search Options", expanded=True):
        filter_col1, filter_col2, filter_col3 = st.columns([2, 2, 1])
        
        with filter_col1:
            # User filter only visible for supervisors
            if is_supervisor:
                # Get all unique users
                all_users = fetchall("""
                    SELECT DISTINCT COALESCE(users.full_name, users.name, users.login_id) AS user_name
                    FROM audit_logs
                    LEFT JOIN users ON audit_logs.user_id = users.id
                    WHERE users.id IS NOT NULL
                    ORDER BY user_name
                """)
                user_options = ["All Users"] + [u["user_name"] for u in all_users if u["user_name"]]
                selected_user = st.selectbox(
                    "👤 Filter by User", 
                    options=user_options, 
                    key="audit_user_filter",
                    help="Pilih user untuk melihat aktivitas spesifik"
                )
            else:
                # Non-supervisors automatically filtered to their own activities
                selected_user = user_obj.get("full_name") or user_obj.get("name") or user_obj.get("login_id")
                st.info(f"👤 Menampilkan aktivitas Anda: **{selected_user}**")
        
        with filter_col2:
            # Date range filter
            date_range = st.date_input(
                "📅 Filter by Date Range",
                value=(today_wib() - timedelta(days=7), today_wib()),
                max_value=today_wib(),
                key="audit_date_range",
                help="Pilih rentang tanggal untuk filter log (default: 7 hari terakhir)"
            )
        
        with filter_col3:
            st.markdown("<br>", unsafe_allow_html=True)
            clear_filter = st.button("🔄 Reset Filter", use_container_width=True, type="secondary")
            if clear_filter:
                st.rerun()
        
        # Action filter
        st.markdown("---")
        action_col1, action_col2 = st.columns([3, 1])
        with action_col1:
            all_actions = fetchall("SELECT DISTINCT action FROM audit_logs WHERE action IS NOT NULL ORDER BY action")
            action_options = ["All Actions"] + [a["action"] for a in all_actions]
            selected_action = st.selectbox(
                "⚡ Filter by Action Type",
                options=action_options,
                key="audit_action_filter",
                help="Filter berdasarkan jenis aktivitas (LOGIN, LOGOUT, UPDATE, dll)"
            )
        
        with action_col2:
            st.markdown("<br>", unsafe_allow_html=True)
            limit = st.number_input("📊 Limit Records", min_value=10, max_value=1000, value=100, step=10)
    
    # ========================================================================
    # BUILD QUERY WITH FILTERS
    # ========================================================================
    # Base query
    query = """
        SELECT audit_logs.timestamp, COALESCE(users.full_name, users.name, users.login_id) AS user, 
               audit_logs.action, audit_logs.details, audit_logs.user_id
        FROM audit_logs
        LEFT JOIN users ON audit_logs.user_id = users.id
        WHERE 1=1
    """
    params = []
    
    # Auto-filter by user_id for non-supervisors
    if not is_supervisor:
        query += " AND audit_logs.user_id = ?"
        params.append(user_id)
    
    # Apply user filter (for supervisors only)
    if is_supervisor and selected_user != "All Users":
        query += " AND COALESCE(users.full_name, users.name, users.login_id) = ?"
        params.append(selected_user)
    
    # Apply action filter
    if selected_action != "All Actions":
        query += " AND audit_logs.action = ?"
        params.append(selected_action)
    
    # Apply date range filter
    if date_range and len(date_range) == 2:
        start_date = date_range[0]
        end_date = date_range[1]
        
        # Convert to datetime with time boundaries (start of day to end of day)
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Adjust for GMT+7 offset (subtract 7 hours to match UTC in database)
        start_datetime_utc = start_datetime - timedelta(hours=7)
        end_datetime_utc = end_datetime - timedelta(hours=7)
        
        query += " AND audit_logs.timestamp BETWEEN ? AND ?"
        params.append(start_datetime_utc.isoformat())
        params.append(end_datetime_utc.isoformat())
    
    query += f" ORDER BY audit_logs.id DESC LIMIT {limit}"
    
    # Execute query
    rows = fetchall(query, tuple(params))
    
    if not rows:
        st.info("📭 Tidak ada aktivitas yang sesuai dengan filter.")
        return
    
    # Convert UTC to GMT+7
    def to_gmt7(ts):
        try:
            dt = datetime.fromisoformat(ts)
            dt7 = dt + timedelta(hours=7)
            return dt7.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ts
    
    # Colorize action badges
    def colorize_action(action):
        colors = {
            "LOGIN": "🟢",
            "LOGOUT": "🔴",
            "CREATE": "🟦",
            "UPDATE": "🟨",
            "DELETE": "🟥",
            "UPLOAD": "🟪",
            "DOWNLOAD": "🟧",
            "BACKUP": "🔵",
            "RESTORE": "🟣",
        }
        return f"{colors.get(action, '⚪')} {action}"
    
    df = pd.DataFrame([
        {
            "Timestamp": to_gmt7(r["timestamp"]),
            "User": r["user"] or "Unknown",
            "Action": colorize_action(r["action"] or "N/A"),
            "Details": r["details"] or "-"
        } for r in rows
    ])
    
    # ========================================================================
    # DISPLAY RESULTS WITH ENHANCED UI
    # ========================================================================
    st.markdown("---")
    
    result_col1, result_col2 = st.columns([2, 1])
    with result_col1:
        st.markdown(f"### 📊 Filtered Results: **{len(df)}** entries")
    with result_col2:
        # Download button for filtered data
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
            type="primary"
        )
    
    # Display dataframe with custom styling
    st.dataframe(
        df, 
        use_container_width=True, 
        hide_index=True,
        height=500,
        column_config={
            "Timestamp": st.column_config.TextColumn("🕒 Timestamp", width="medium"),
            "User": st.column_config.TextColumn("👤 User", width="medium"),
            "Action": st.column_config.TextColumn("⚡ Action", width="small"),
            "Details": st.column_config.TextColumn("📝 Details", width="large"),
        }
    )
    
    # ========================================================================
    # FOOTER INFO
    # ========================================================================
    st.markdown("---")
    st.caption(f"💡 **Tips:** Gunakan filter untuk mempersempit pencarian. Data diurutkan dari terbaru ke terlama. Maksimal {limit} records ditampilkan.")
    
    # Stay on Audit Log page without redirecting
    return

# -------------------------
# Agent Page (placeholder)
# -------------------------
def page_agent():
    require_roles(("Superuser", "Supervisor", "Agent"))
    u = current_user()
    user_role = u.get('role') if u else None
    agent_name = (u.get('full_name') or u.get('login_id') or '-') if u else '-'
    st.title("Agent Menu")
    
    # Jika Supervisor/Superuser: tampilkan semua assignment
    # Jika Agent: hanya tampilkan assignment untuk dirinya sendiri
    if user_role in ("Superuser", "Supervisor"):
        st.caption(f"Mode: **{user_role}** — Melihat semua assignment agent")
        # Enhanced query dengan kolom tambahan:
        # - Assignment Date (dari agent_assignments)
        # - Status (dari supervisor_data)
        # - Employment Update & Employer (dari assign_tracer - hasil trace)
        # - Updated Company Contacts (placeholder untuk hasil googling)
        # - Work status indicator (sudah dikerjakan atau belum)
        rows = fetchall("""
            SELECT 
                aa.Agreement_No AS Case_ID, 
                aa.Agent_Assigned_To, 
                aa.assigned_at AS Assignment_Date,
                CAST(COALESCE(sd.Principle_Outstanding, '0') AS REAL) AS Principle_Outstanding,
                COALESCE(sd.STATUS, '-') AS Status,
                COALESCE(at.EMPLOYMENT_UPDATE, '-') AS Employment_Update,
                COALESCE(at.EMPLOYER, '-') AS Employer,
                COALESCE(sd.Additional_Contacts, '-') AS Updated_Company_Contacts,
                (
                    COALESCE(
                        (SELECT SUM(paid_amount) 
                         FROM payments p 
                         WHERE p.Agreement_No = aa.Agreement_No),
                        0
                    ) +
                    COALESCE(
                        (SELECT SUM(agent_ptp_amount) 
                         FROM agent_results ar 
                         WHERE ar.Agreement_No = aa.Agreement_No 
                         AND IFNULL(ar.approval_status, 'approved') = 'approved'),
                        0
                    )
                ) AS Total_Approved_Payment,
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM trace_results tr 
                        WHERE tr.Agreement_No = aa.Agreement_No
                    ) OR EXISTS (
                        SELECT 1 FROM agent_results ar2 
                        WHERE ar2.Agreement_No = aa.Agreement_No
                    ) THEN 1
                    ELSE 0
                END AS Is_Worked_On
            FROM agent_assignments aa
            LEFT JOIN supervisor_data sd 
                ON sd.Case_ID = aa.Agreement_No 
                OR sd.Virtual_Account_Number = aa.Agreement_No
                OR sd.Third_Uid = aa.Agreement_No
            LEFT JOIN assign_tracer at
                ON at.Agreement_No = aa.Agreement_No
            WHERE aa.active = 1 
              AND IFNULL(aa.assignment_type, 'agent') = 'agent'
            ORDER BY aa.assigned_at DESC 
            LIMIT 500
        """)
    else:
        # Agent view: Check for rejected payments/cicilan
        # Try-except untuk backward compatibility jika kolom approval belum ada
        rejected_payments = []
        rejected_cicilan = []
        
        try:
            rejected_payments = fetchall("""
                SELECT id, Agreement_No AS Case_ID, paid_amount, paid_date, 
                       IFNULL(rejection_notes, '') as rejection_notes, 
                       IFNULL(approval_by, '') as approval_by, 
                       IFNULL(approval_at, '') as approval_at
                FROM payments 
                WHERE uploaded_by = ? AND IFNULL(approval_status, 'pending') = 'rejected'
                ORDER BY approval_at DESC
                LIMIT 10
            """, (agent_name,))
        except Exception:
            pass  # Kolom approval_status belum ada
        
        try:
            rejected_cicilan = fetchall("""
                SELECT id, Agreement_No AS Case_ID, agent_ptp_amount, agent_ptp_date, 
                       IFNULL(rejection_notes, '') as rejection_notes, 
                       IFNULL(approval_by, '') as approval_by, 
                       IFNULL(approval_at, '') as approval_at
                FROM agent_results 
                WHERE agent = ? AND IFNULL(approval_status, 'pending') = 'rejected'
                ORDER BY approval_at DESC
                LIMIT 10
            """, (agent_name,))
        except Exception:
            pass  # Kolom approval_status belum ada
        
        # Show rejection notifications
        if rejected_payments or rejected_cicilan:
            st.error("⚠️ **PERHATIAN: Ada laporan yang ditolak oleh Supervisor!**")
            
            if rejected_payments:
                with st.expander(f"❌ {len(rejected_payments)} Payment Report Ditolak - Klik untuk detail", expanded=True):
                    for rp in rejected_payments:
                        st.markdown(f"""
                        **Case ID:** {rp['Case_ID']}  
                        **Jumlah:** Rp {rp['paid_amount']:,.0f}  
                        **Tanggal:** {rp['paid_date']}  
                        **Ditolak oleh:** {rp['approval_by']} pada {rp['approval_at']}  
                        **Alasan:** {rp['rejection_notes']}
                        """)
                        st.markdown("---")
            
            if rejected_cicilan:
                with st.expander(f"❌ {len(rejected_cicilan)} Cicilan Ditolak - Klik untuk detail", expanded=True):
                    for rc in rejected_cicilan:
                        st.markdown(f"""
                        **Case ID:** {rc['Case_ID']}  
                        **Jumlah Cicilan:** Rp {rc['agent_ptp_amount']:,.0f}  
                        **Tanggal:** {rc['agent_ptp_date']}  
                        **Ditolak oleh:** {rc['approval_by']} pada {rc['approval_at']}  
                        **Alasan:** {rc['rejection_notes']}
                        """)
                        st.markdown("---")
            
            st.info("💡 Silakan perbaiki dan submit ulang laporan sesuai catatan Supervisor")
            st.markdown("---")
        
        # Simple PTP notif today
        today_str = today_wib().isoformat()
        ptp_today = fetchone("SELECT COUNT(*) c FROM agent_results WHERE agent=? AND DATE(agent_ptp_date)=?", (agent_name, today_str))
        count_ptp = ptp_today.get('c') if ptp_today else 0
        if count_ptp and count_ptp > 0:
            st.success(f"Hai {agent_name}, hari ini kamu ada {count_ptp} PTP. Klik di bawah untuk lihat daftar.")
        
        # Agent's assigned loans - Enhanced dengan kolom tambahan
        rows = fetchall("""
            SELECT 
                aa.Agreement_No AS Case_ID, 
                aa.Agent_Assigned_To, 
                aa.assigned_at AS Assignment_Date,
                COALESCE(sd.STATUS, '-') AS Status,
                COALESCE(at.EMPLOYMENT_UPDATE, '-') AS Employment_Update,
                COALESCE(at.EMPLOYER, '-') AS Employer,
                COALESCE(sd.Additional_Contacts, '-') AS Updated_Company_Contacts,
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM trace_results tr 
                        WHERE tr.Agreement_No = aa.Agreement_No
                    ) OR EXISTS (
                        SELECT 1 FROM agent_results ar2 
                        WHERE ar2.Agreement_No = aa.Agreement_No
                    ) THEN 1
                    ELSE 0
                END AS Is_Worked_On
            FROM agent_assignments aa
            LEFT JOIN supervisor_data sd 
                ON sd.Case_ID = aa.Agreement_No 
                OR sd.Virtual_Account_Number = aa.Agreement_No
                OR sd.Third_Uid = aa.Agreement_No
            LEFT JOIN assign_tracer at
                ON at.Agreement_No = aa.Agreement_No
            WHERE aa.Agent_Assigned_To=? 
              AND aa.active = 1
              AND IFNULL(aa.assignment_type, 'agent') = 'agent'
            ORDER BY aa.assigned_at DESC 
            LIMIT 500
        """, (agent_name,))
    
    if not rows:
        st.info("Belum ada assignment.")
        return

    # Tabs layout to tidy up Agent Menu
    # Supervisor memiliki tab Payment & Cicilan Approval
    if user_role in ("Superuser", "Supervisor"):
        tabs = st.tabs([
            "Cases",
            "Payment & Cicilan Approval",
            "My PTP",
            "Monthly Payment Recap",
            "All-time Payment Recap",
            "Email Templates",
        ])
    else:
        tabs = st.tabs([
            "Cases",
            "My PTP",
            "Monthly Payment Recap",
            "All-time Payment Recap",
            "Email Templates",
        ])

    # --- Cases tab ---
    with tabs[0]:
        q_ag = st.text_input("Cari Case_ID", key="ag_q_no")
        filtered = [r for r in rows if (not q_ag or q_ag.strip() in str(r.get('Case_ID') or ''))]

        st.subheader("Assignments")
        
        # Build enhanced table dengan kolom baru
        data = []
        for r in filtered:
            row_data = {
                "Case_ID": r.get("Case_ID"),
                "Assignment_Date": r.get("Assignment_Date", "-"),
                "Status": r.get("Status", "-"),
                "Employment_Update": r.get("Employment_Update", "-"),
                "Employer": r.get("Employer", "-"),
                "Updated_Company_Contacts": r.get("Updated_Company_Contacts", "-"),
                "Work_Status": "✅ Dikerjakan" if r.get("Is_Worked_On") == 1 else "⏳ Belum",
            }
            
            # Tambahkan kolom khusus Supervisor
            if user_role in ("Superuser", "Supervisor"):
                row_data["Agent"] = r.get("Agent_Assigned_To")
                row_data["Principle_Outstanding"] = r.get("Principle_Outstanding")
                row_data["Total_Approved_Payment"] = r.get("Total_Approved_Payment")
            
            data.append(row_data)
        
        df = pd.DataFrame(data)
        prev_selected = set(st.session_state.get("agent_selected_list", []) or [])
        
        # Select-all / clear options
        col_sa, col_cl = st.columns([1, 1])
        with col_sa:
            select_all = st.checkbox("Pilih semua", key="ag_select_all")
        with col_cl:
            clear_all = st.checkbox("Kosongkan pilihan", key="ag_clear_all")
        
        if not df.empty:
            if select_all:
                df.insert(0, "Selected", True)
            elif clear_all:
                df.insert(0, "Selected", False)
            else:
                df.insert(0, "Selected", df["Case_ID"].apply(lambda x: x in prev_selected))
        else:
            df["Selected"] = []

        # Enhanced column config dengan kolom baru
        col_config = {
            "Selected": st.column_config.CheckboxColumn("Selected", help="Centang untuk memilih Case_ID"),
            "Case_ID": st.column_config.TextColumn("Case ID", width="medium"),
            "Assignment_Date": st.column_config.TextColumn("Assignment Date", width="medium"),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Employment_Update": st.column_config.TextColumn("Employment Update", width="medium"),
            "Employer": st.column_config.TextColumn("Employer", width="medium"),
            "Updated_Company_Contacts": st.column_config.TextColumn("Updated Company Contacts", width="large"),
            "Work_Status": st.column_config.TextColumn("Work Status", width="small", help="Sudah dikerjakan atau belum"),
        }
        
        disabled_cols = ["Case_ID", "Assignment_Date", "Status", "Employment_Update", "Employer", "Updated_Company_Contacts", "Work_Status"]
        
        if user_role in ("Superuser", "Supervisor"):
            col_config["Agent"] = st.column_config.TextColumn("Agent", width="medium")
            col_config["Principle_Outstanding"] = st.column_config.NumberColumn(
                "Principle Outstanding",
                help="Sisa pokok pinjaman yang belum dibayar",
                format="Rp %.0f",
                width="medium"
            )
            col_config["Total_Approved_Payment"] = st.column_config.NumberColumn(
                "Total Approved Payment",
                help="Total pembayaran cicilan yang sudah di-approve",
                format="Rp %.0f",
                width="medium"
            )
            disabled_cols.extend(["Agent", "Principle_Outstanding", "Total_Approved_Payment"])

        edited = st.data_editor(
            df,
            hide_index=True,
            use_container_width=True,
            column_config=col_config,
            disabled=disabled_cols,
        )

        # Determine selections from edited table
        selected_list = []
        if edited is not None and not edited.empty:
            try:
                selected_list = [
                    str(row["Case_ID"]) for _, row in edited.iterrows() if bool(row.get("Selected"))
                ]
            except Exception:
                selected_list = []
        st.session_state["agent_selected_list"] = selected_list
        sel = selected_list[0] if selected_list else None
        st.session_state["agent_selected"] = sel

        if sel:
            st.markdown("---")
            st.subheader(f"Case Details: {sel}")
            info = fetchone("SELECT Debtor_Name, NIK_KTP FROM assign_tracer WHERE Agreement_No=?", (sel,)) or {}
            
            # Ambil data lengkap dari supervisor_data untuk Contract Detail
            sup_data = fetchone("""
                SELECT Phone_Number_1, Phone_Number_2, Principle_Outstanding, 
                       Customer_name, email, Gender, Home_Address, 
                       Customer_Occupation, DPD, Assignment_Date
                FROM supervisor_data 
                WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=? 
                LIMIT 1
            """, (sel, sel, sel)) or {}
            
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.text_input("Debtor Name", value=info.get('Debtor_Name',''), disabled=True, key=f"debtor_name_{sel}")
            with c2:
                st.text_input("NIK", value=info.get('NIK_KTP',''), disabled=True, key=f"nik_{sel}")
            with c3:
                phone = sup_data.get('Phone_Number_1', '') or ''
                st.text_input("Phone", value=phone, disabled=True, key=f"phone_{sel}")
            with c4:
                principle_outstanding = sup_data.get('Principle_Outstanding', 'N/A') or 'N/A'
                st.text_input("Principle Outstanding", value=principle_outstanding, disabled=True, key=f"po_{sel}")
            
            if phone:
                st.markdown(f"[Click to call]({'tel:'+str(phone)})  |  [SIP]({'sip:'+str(phone)})")
            
            # ===== CONTRACT DETAIL SCREENSHOT & WHATSAPP FEATURE =====
            st.markdown("---")
            st.markdown("### 📸 Contract Detail Screenshot & WhatsApp")
            
            # Tombol untuk generate dan show contract detail
            col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
            
            with col_btn1:
                if st.button("📋 Show Contract Detail", key=f"show_contract_{sel}", use_container_width=True):
                    st.session_state[f"show_contract_html_{sel}"] = True
            
            with col_btn2:
                if phone:
                    wa_url = open_whatsapp_with_clipboard_instruction(phone)
                    # Gunakan link_button untuk membuka WhatsApp di tab baru
                    st.link_button("💬 Open WhatsApp", wa_url, use_container_width=True, type="primary")
                else:
                    st.button("💬 WhatsApp Unavailable", key=f"open_wa_disabled_{sel}", use_container_width=True, disabled=True)
            
            with col_btn3:
                st.caption("🎯 Klik 'Show Contract Detail' untuk tampilkan detail dengan tombol auto-screenshot")
            
            # Info message untuk fitur auto-screenshot
            if not st.session_state.get(f"show_contract_html_{sel}", False):
                st.markdown("---")            # Display Contract Detail HTML jika tombol diklik
            if st.session_state.get(f"show_contract_html_{sel}", False):
                st.markdown("---")
                
                # Prepare data untuk contract detail
                contract_data = {
                    'Debtor_Name': info.get('Debtor_Name', 'N/A') or sup_data.get('Customer_name', 'N/A'),
                    'PhoneNumber': phone or 'N/A',
                    'Gender': sup_data.get('Gender', 'N/A') or 'N/A',
                    'Legal_Address': sup_data.get('Home_Address', 'N/A') or 'N/A',
                    'DOB': '2025-10-24',  # Default, could be enhanced with actual DOB field
                    'Email': sup_data.get('email', 'N/A') or 'N/A',
                    'Last_Known_Office_Name': '',
                    'Last_Known_Job_Position': sup_data.get('Customer_Occupation', 'N/A') or 'Lainnya',
                    'Last_Known_Work_Phone': 'None',
                    'Debtor_Phone_Number_II': sup_data.get('Phone_Number_2', 'None') or 'None',
                    'Debtor_Other_Phone_Numbers': '#N/A',
                    'Date_of_Contract': sup_data.get('Assignment_Date', 'N/A') or 'N/A',
                    'DPD': sup_data.get('DPD', 'N/A') or 'N/A',
                }
                
                # Generate HTML dengan auto-screenshot JavaScript
                contract_html = generate_contract_detail_html(contract_data, include_screenshot_js=True)
                
                # Display dengan component HTML (tinggi lebih besar untuk tombol screenshot)
                st.components.v1.html(contract_html, height=1200, scrolling=True)
                
                st.success("""
                ✅ **Auto-Screenshot Aktif!**
                
                **Cara Pakai:**
                1. Scroll ke bawah Contract Detail di atas
                2. Klik tombol **"📸 Copy Screenshot to Clipboard"** di bawah contract detail
                3. Screenshot otomatis masuk ke clipboard
                4. Buka WhatsApp (klik 'Open WhatsApp' di atas)
                5. **Ctrl + V** untuk paste di chat
                
                **No need to press Windows + Shift + S!** 🎉
                """)
                
                # Tombol untuk hide contract detail
                if st.button("❌ Hide Contract Detail", key=f"hide_contract_{sel}"):
                    st.session_state[f"show_contract_html_{sel}"] = False
                    st.rerun()
            
        else:
            st.info("Centang satu baris untuk melihat detail kasus.")
        
        # Inline sub-tabs for the selected case actions (di dalam tab Cases)
        st.markdown("---")
        
        # Cek STATUS terlebih dahulu untuk menentukan tab yang ditampilkan
        sup_agent = fetchone(
            "SELECT id, STATUS, REGISTERED_PHONE, Additional_Contacts, Remarks_Suggested_NIK_Prospect, Payment, Paid_Off_Status, Paid_Off "
            "FROM supervisor_data WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=? ORDER BY id DESC LIMIT 1",
            (sel, sel, sel)
        ) if sel else {}
        
        current_status = (sup_agent.get('STATUS', '') or '').strip().upper() if sup_agent else ''
        
        # Sub-tabs: tampilkan "Report Payment/PTP" hanya jika STATUS = "PTP"
        if current_status == "PTP":
            sub_tabs = st.tabs(["Update Data", "Report Payment/PTP", "Internal Memo"])
        else:
            sub_tabs = st.tabs(["Update Data", "Internal Memo"]) 

        # --- Update Data sub-tab ---
        with sub_tabs[0]:
            if not sel:
                st.info("Pilih Case ID pada tabel di atas terlebih dahulu.")
            else:
                st.subheader("Update Data untuk Supervisor (Agent fields)")
                st.caption("Kolom-kolom ini berasal dari data upload supervisor dan diupdate oleh Agent.")
                with st.form("agent_update_supervisor_fields"):
                    csa, csb = st.columns(2)
                    with csa:
                        # Agent status codes (requested)
                        _status_options = [
                            "PTP",  # O1001
                            "DIS",  # O1002
                            "CMP",  # O1003
                            "PAD",  # O1004
                            "HUP",  # O1005
                            "WCN",  # O1006
                            "TPM",  # O1007
                            "TPC",  # O1008
                            "RNA",  # O1009
                            "PHO",  # O1010
                            "NIS",  # O1011
                            "INS",  # O1012
                        ]
                        v_status = st.selectbox(
                            "STATUS",
                            _status_options,
                            index=(_status_options.index(sup_agent.get('STATUS','')) if sup_agent.get('STATUS','') in _status_options else 0),
                            help="Status terkini dari case ini"
                        )
                        
                        # Paid Off dropdown
                        current_paid_off = sup_agent.get('Paid_Off', 'No') or 'No'
                        v_paid_off = st.selectbox(
                            "Paid Off",
                            options=["No", "Yes"],
                            index=0 if current_paid_off.upper() != 'YES' else 1,
                            help="Apakah pinjaman sudah lunas?"
                        )
                        
                        v_reg_phone = st.text_input("REGISTERED PHONE", value=sup_agent.get('REGISTERED_PHONE','') or "")
                    with csb:
                        v_add_contacts = st.text_area("Remarks", value=sup_agent.get('Additional_Contacts','') or "", height=80)
                        v_remarks = st.text_area("Suggested NIK", value=sup_agent.get('Remarks_Suggested_NIK_Prospect','') or "", height=80)
                    submit_sup = st.form_submit_button("Simpan ke supervisor_data")
                    if submit_sup:
                        try:
                            if sup_agent.get('id') is not None:
                                execute(
                                    "UPDATE supervisor_data SET STATUS=?, Paid_Off=?, REGISTERED_PHONE=?, Additional_Contacts=?, Remarks_Suggested_NIK_Prospect=? WHERE id=?",
                                    (v_status.strip(), v_paid_off, v_reg_phone.strip(), v_add_contacts.strip(), v_remarks.strip(), sup_agent.get('id'))
                                )
                            else:
                                execute(
                                    "UPDATE supervisor_data SET STATUS=?, Paid_Off=?, REGISTERED_PHONE=?, Additional_Contacts=?, Remarks_Suggested_NIK_Prospect=? WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=?",
                                    (v_status.strip(), v_paid_off, v_reg_phone.strip(), v_add_contacts.strip(), v_remarks.strip(), sel, sel, sel)
                                )
                            try:
                                u = current_user() or {}
                                execute(
                                    "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                    (u.get('id') if u else None, "AGENT_UPDATE_SUP_FIELDS", f"{sel} -> STATUS='{v_status}' PAID_OFF='{v_paid_off}' REG_PHONE='{v_reg_phone}'")
                                )
                            except Exception:
                                pass
                            st.toast("✅ Data supervisor berhasil diperbarui!", icon="✅")
                            st.rerun()
                        except Exception as e:
                            st.toast(f"❌ Gagal memperbarui data: {e}", icon="❌")

        # --- Report Payment/PTP sub-tab (hanya tampil jika STATUS = PTP) ---
        if current_status == "PTP":
            with sub_tabs[1]:
                # Tampilkan notifikasi jika ada di session state
                if "payment_notification" in st.session_state:
                    notif = st.session_state.payment_notification
                    if notif["type"] == "success":
                        st.success(notif["message"])
                    elif notif["type"] == "error":
                        st.error(notif["message"])
                    elif notif["type"] == "warning":
                        st.warning(notif["message"])
                    # Hapus notifikasi setelah ditampilkan
                    del st.session_state.payment_notification
                
                if not sel:
                    st.info("Pilih Case ID pada tabel di atas terlebih dahulu.")
                else:
                    st.subheader("Report Payment/PTP")
                    
                    # Form counter untuk reset form setelah submit berhasil
                    if "payment_form_counter" not in st.session_state:
                        st.session_state.payment_form_counter = 0
                    
                    # Pre-form: pilih skema dulu untuk menentukan apakah perlu input cicilan
                    scheme = st.selectbox(
                        "Skema Pelunasan",
                        [
                            "FULL OS",
                            "CICIL OS",
                            "LUNDIS",
                            "CICIL LUNDIS",
                            "LUNAS POKOK",
                            "CICIL POKOK",
                        ],
                        index=0,
                        key=f"payment_scheme_select_{st.session_state.payment_form_counter}"
                    )
                    
                    cicil_schemes = {"CICIL OS", "CICIL LUNDIS", "CICIL POKOK"}
                    is_cicil = (scheme or "").upper() in cicil_schemes
                    
                    # Inputs for installment plan when scheme is a CICIL type (OUTSIDE form)
                    plan_dates = []
                    plan_amount = 0.0
                    plan_count = 0
                    if is_cicil:
                        st.markdown("#### Rencana Cicilan")
                        plan_count = st.number_input(
                            "Dicicil berapa kali",
                            min_value=1,
                            max_value=24,
                            value=2,
                            step=1,
                            help="Jumlah rencana cicilan yang akan dibuat sebagai PTP.",
                            key=f"payment_plan_count_{st.session_state.payment_form_counter}"
                        )
                        plan_amount = st.number_input(
                            "Nominal tiap cicilan (opsional)",
                            min_value=0.0,
                            step=10000.0,
                            help="Bila diisi 0, PTP akan direkam tanpa nominal.",
                            key=f"payment_plan_amount_{st.session_state.payment_form_counter}"
                        )
                        # Dynamic date inputs for each planned installment
                        for i in range(int(plan_count)):
                            default_dt = today_wib() + relativedelta(months=i+1)
                            d = st.date_input(f"Tanggal cicilan {i+1}", value=default_dt, key=f"ptp_plan_date_{i}_{st.session_state.payment_form_counter}")
                            plan_dates.append(d)
                    
                    st.markdown("---")
                    
                    # Now the form with the rest of the payment info
                    with st.form(f"agent_report_payment_ptp_{st.session_state.payment_form_counter}"):
                        c1, c2 = st.columns(2)
                        with c1:
                            paid_date = st.date_input("Tanggal Pembayaran", value=today_wib())
                        with c2:
                            paid_amount = st.number_input("Nominal Pembayaran", min_value=0.0, step=10000.0)
                        
                        # Upload bukti gambar (percakapan/pembayaran)
                        st.markdown("---")
                        st.markdown("#### Bukti Pembayaran / Percakapan (Opsional)")
                        uploaded_proof = st.file_uploader(
                            "Upload gambar bukti (WhatsApp chat, transfer, dll)",
                            type=["png", "jpg", "jpeg", "pdf"],
                            help="Gambar akan disimpan di Google Drive untuk review Supervisor",
                            key=f"payment_proof_upload_{st.session_state.payment_form_counter}"
                        )
            
                        st.markdown("---")
                        st.markdown("#### Kontak Debitur (opsional untuk diperbarui)")
                        # Prefill from supervisor_data
                        sup_contact = fetchone(
                            "SELECT email, REGISTERED_PHONE, Phone_Number_1, Additional_Contacts FROM supervisor_data WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=? LIMIT 1",
                            (sel, sel, sel),
                        ) or {}
                        col_e1, col_e2 = st.columns(2)
                        with col_e1:
                            updated_email = st.text_input(
                                "Email terupdate",
                                value=(sup_contact.get("email") or ""),
                                placeholder="contoh: user@mail.com",
                            )
                        with col_e2:
                            wa_number = st.text_input(
                                "Nomor WhatsApp",
                                value=(sup_contact.get("REGISTERED_PHONE") or sup_contact.get("Phone_Number_1") or ""),
                                placeholder="contoh: 08xxxxxxxxxx / +628xxxxxxxxxx",
                            )
                        # New WA field (does not replace registered phone)
                        new_wa_number = st.text_input(
                            "Nomor WhatsApp baru (opsional)",
                            value="",
                            placeholder="Nomor WA lain yang dipakai debitur saat ini",
                            help="Tidak menggantikan nomor terdaftar. Akan ditambahkan ke Remarks."
                        )

                        submit = st.form_submit_button("Simpan")
                    if submit:
                        if paid_amount is None or float(paid_amount) <= 0:
                            st.session_state.payment_notification = {
                                "type": "warning",
                                "message": "⚠️ Nominal Pembayaran harus lebih dari 0!"
                            }
                            st.rerun()
                        elif not paid_date:
                            st.session_state.payment_notification = {
                                "type": "warning",
                                "message": "⚠️ Tanggal Pembayaran wajib diisi!"
                            }
                            st.rerun()
                        elif is_cicil and (not plan_dates or any(d is None for d in plan_dates)):
                            st.session_state.payment_notification = {
                                "type": "warning",
                                "message": "⚠️ Untuk skema CICIL, isi semua tanggal rencana!"
                            }
                            st.rerun()
                        else:
                            try:
                                # Upload bukti gambar ke Google Drive jika ada
                                proof_drive_id = None
                                proof_filename = None
                                upload_message = ""
                                if uploaded_proof is not None:
                                    try:
                                        service, _ = build_drive_service()
                                        # Generate filename dengan timestamp dan case_id
                                        timestamp = now_wib().strftime("%Y%m%d_%H%M%S")
                                        original_filename = uploaded_proof.name
                                        ext = original_filename.split('.')[-1] if '.' in original_filename else 'jpg'
                                        proof_filename = f"payment_proof_{sel}_{timestamp}.{ext}"
                                        
                                        # Upload ke folder yang sama dengan backup
                                        proof_bytes = uploaded_proof.read()
                                        mimetype = uploaded_proof.type or "image/jpeg"
                                        proof_drive_id = upload_bytes(service, FOLDER_ID_DEFAULT, proof_filename, proof_bytes, mimetype)
                                        
                                        upload_message = f" Bukti gambar tersimpan: {proof_filename}"
                                    except Exception as e:
                                        upload_message = f" (Catatan: Gagal upload gambar - {str(e)[:50]})"
                                
                                # 1) Simpan pembayaran dengan info bukti gambar dan approval_status='pending'
                                execute(
                                    "INSERT INTO payments (Agreement_No, paid_amount, paid_date, status, source_file, uploaded_by, proof_image_drive_id, proof_image_filename, approval_status) VALUES (?,?,?,?,?,?,?,?,?)",
                                    (sel, float(paid_amount or 0), (paid_date.isoformat() if paid_date else None), scheme, None, agent_name, proof_drive_id, proof_filename, 'pending')
                                )
                                # Audit log pembayaran
                                try:
                                    u = current_user() or {}
                                    execute(
                                        "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                        (u.get('id') if u else None, "AGENT_REPORT_PAYMENT", f"{sel}|{paid_amount}|{paid_date}|{scheme}")
                                    )
                                except Exception:
                                    pass

                                # 1b) Optional: update contact info if provided (registered email/WA)
                                try:
                                    em = (updated_email or "").strip()
                                    wa = (wa_number or "").strip()
                                    if em or wa:
                                        execute(
                                            """
                                            UPDATE supervisor_data
                                            SET 
                                                email = COALESCE(NULLIF(?, ''), email),
                                                REGISTERED_PHONE = COALESCE(NULLIF(?, ''), REGISTERED_PHONE),
                                                Phone_Number_1 = COALESCE(NULLIF(?, ''), Phone_Number_1)
                                            WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=?
                                            """,
                                            (em, wa, wa, sel, sel, sel),
                                        )
                                        # Audit log contact update
                                        try:
                                            execute(
                                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                (u.get('id') if u else None, "AGENT_UPDATE_CONTACT", f"{sel}|email:{em}|wa:{wa}")
                                            )
                                        except Exception:
                                            pass
                                except Exception:
                                    # Non-blocking if contact update fails
                                    pass

                                # 1c) If agent provided a new alternate WA number, append to Additional_Contacts
                                try:
                                    new_wa = (new_wa_number or "").strip()
                                    if new_wa:
                                        existing_add = (sup_contact.get("Additional_Contacts") or "").strip()
                                        stamp = now_wib().strftime("%Y-%m-%d")
                                        line = f"WA baru {stamp} oleh {agent_name}: {new_wa}"
                                        new_add = (existing_add + ("\n" if existing_add else "") + line)
                                        execute(
                                            "UPDATE supervisor_data SET Additional_Contacts=? WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=?",
                                            (new_add, sel, sel, sel),
                                        )
                                        # Audit log for new WA
                                        try:
                                            execute(
                                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                (u.get('id') if u else None, "AGENT_ADD_NEW_WA", f"{sel}|{new_wa}")
                                            )
                                        except Exception:
                                            pass
                                except Exception:
                                    # Non-blocking if Additional_Contacts update fails
                                    pass

                                # 2) Jika cicil, buat jadwal PTP sesuai rencana
                                cicilan_info = ""
                                if is_cicil and plan_dates:
                                    try:
                                        for idx, d in enumerate(plan_dates, start=1):
                                            if not d:
                                                continue
                                            execute(
                                                "INSERT INTO agent_results (Agreement_No, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes) VALUES (?,?,?,?,?,?)",
                                                (sel, agent_name, "PTP", float(plan_amount or 0), (d.isoformat() if hasattr(d, 'isoformat') else str(d)), f"Rencana cicilan {idx}/{int(plan_count)} dari skema {scheme}")
                                            )
                                            # Audit log tiap PTP rencana
                                            try:
                                                u = current_user() or {}
                                                execute(
                                                    "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                    (u.get('id') if u else None, "AGENT_REPORT_PTP", f"{sel}|{plan_amount}|{d}|{scheme}|plan {idx}/{int(plan_count)}")
                                                )
                                            except Exception:
                                                pass
                                        cicilan_info = f" + {int(plan_count)} rencana PTP cicilan"
                                    except Exception as e:
                                        cicilan_info = f" (Gagal simpan rencana PTP: {str(e)[:30]})"

                                # Simpan notifikasi sukses ke session state
                                st.session_state.payment_notification = {
                                    "type": "success",
                                    "message": f"✅ Laporan pembayaran berhasil disimpan!{upload_message}{cicilan_info}"
                                }
                                # Increment form counter untuk reset form
                                st.session_state.payment_form_counter += 1
                                st.rerun()
                            except Exception as e:
                                st.session_state.payment_notification = {
                                    "type": "error",
                                    "message": f"❌ Gagal menyimpan laporan: {e}"
                                }
                                st.rerun()

        # --- Internal Memo sub-tab (index dinamis: 2 jika ada PTP, 1 jika tidak ada PTP) ---
        memo_tab_index = 2 if current_status == "PTP" else 1
        with sub_tabs[memo_tab_index]:
            # Tampilkan notifikasi memo jika ada
            if "memo_notification" in st.session_state:
                notif = st.session_state.memo_notification
                if notif["type"] == "success":
                    st.success(notif["message"])
                elif notif["type"] == "error":
                    st.error(notif["message"])
                elif notif["type"] == "warning":
                    st.warning(notif["message"])
                del st.session_state.memo_notification
            
            if not sel:
                st.info("Pilih Case ID pada tabel di atas terlebih dahulu.")
            else:
                # Enhanced Chat-like CSS (no container, direct render)
                st.markdown(
                    """
                    <style>
                    .agent-msg { 
                        display: flex;
                        margin: 10px 0;
                        animation: fadeIn 0.3s ease-in;
                    }
                    @keyframes fadeIn {
                        from { opacity: 0; transform: translateY(10px); }
                        to { opacity: 1; transform: translateY(0); }
                    }
                    .agent-msg.left { justify-content: flex-start; }
                    .agent-msg.right { justify-content: flex-end; }
                    .agent-bubble { 
                        max-width: 70%;
                        padding: 10px 14px;
                        border-radius: 16px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                        word-wrap: break-word;
                    }
                    .agent-msg.left .agent-bubble { 
                        background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%);
                        color: #1e293b;
                        border-bottom-left-radius: 4px;
                        border: 1px solid #cbd5e1;
                    }
                    .agent-msg.right .agent-bubble { 
                        background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
                        color: #1e293b;
                        border-bottom-right-radius: 4px;
                        border: 1px solid #10b981;
                    }
                    .agent-meta { 
                        font-size: 10px;
                        color: #64748b;
                        margin-top: 4px;
                        font-style: italic;
                    }
                    .agent-name { 
                        font-weight: 700;
                        font-size: 12px;
                        margin-bottom: 4px;
                        color: #0f172a;
                        letter-spacing: 0.3px;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )

                # Load recent memos for this case (ascending for chat)
                recent = fetchall(
                    "SELECT author_role, author_name, target_role, message, created_at FROM memos WHERE Agreement_No=? ORDER BY id DESC LIMIT 100",
                    (sel,)
                ) or []
                recent = list(reversed(recent))  # oldest at top

                # Render messages directly without container
                if not recent:
                    st.info("💬 Belum ada memo untuk case ini")
                else:
                    for r in recent:
                        author_role = (r.get('author_role') or '').strip()
                        author_name = (r.get('author_name') or '').strip()
                        msg = (r.get('message') or '').strip()
                        ts = (r.get('created_at') or '').replace('T', ' ')
                        
                        # Tentukan apakah pesan ini dari user saat ini
                        if user_role == 'Agent':
                            mine = (author_role == 'Agent' and author_name == agent_name)
                        else:
                            mine = (author_role in ('Supervisor', 'Superuser') and author_name == agent_name)
                        
                        side = 'right' if mine else 'left'
                        name = 'Saya' if mine else (author_name or author_role or 'Unknown')
                        safe_msg = msg.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('\n','<br/>')
                        
                        st.markdown(f"""
                            <div class='agent-msg {side}'>
                                <div class='agent-bubble'>
                                    <div class='agent-name'>{name}</div>
                                    <div class='text'>{safe_msg}</div>
                                    <div class='agent-meta'>{ts}</div>
                                </div>
                            </div>
                        """, unsafe_allow_html=True)

                # Form label dan role tergantung user
                if user_role in ('Supervisor', 'Superuser'):
                    form_label = "Tulis memo untuk Agent"
                    send_as_role = "Supervisor"
                    target_role = "Agent"
                else:
                    form_label = "Tulis memo untuk Supervisor"
                    send_as_role = "Agent"
                    target_role = "Supervisor"

                # Form counter untuk reset
                if "memo_form_counter" not in st.session_state:
                    st.session_state.memo_form_counter = 0

                # Input send box with better UX
                with st.form(f"agent_internal_memo_chat_{st.session_state.memo_form_counter}"):
                    msg = st.text_area(
                        form_label,
                        value="",
                        placeholder="Ketik pesan Anda di sini...",
                        height=80,
                        help="Tekan Ctrl+Enter atau klik tombol Kirim"
                    )
                    send = st.form_submit_button("📤 Kirim", use_container_width=True)
                    if send:
                        if not msg or not msg.strip():
                            st.session_state.memo_notification = {
                                "type": "warning",
                                "message": "⚠️ Pesan tidak boleh kosong!"
                            }
                            st.rerun()
                        else:
                            try:
                                execute(
                                    "INSERT INTO memos (Agreement_No, author_role, author_name, target_role, message) VALUES (?,?,?,?,?)",
                                    (sel, send_as_role, agent_name, target_role, msg.strip())
                                )
                                st.session_state.memo_notification = {
                                    "type": "success",
                                    "message": "✅ Memo berhasil dikirim!"
                                }
                                st.session_state.memo_form_counter += 1
                                st.rerun()
                            except Exception as e:
                                st.session_state.memo_notification = {
                                    "type": "error",
                                    "message": f"❌ Gagal mengirim memo: {e}"
                                }
                                st.rerun()

    # --- Payment & Cicilan Approval tab (sejajar dengan Cases, hanya untuk Supervisor) ---
    if user_role in ('Supervisor', 'Superuser'):
        with tabs[1]:
            st.subheader("💰 Payment & Cicilan Approval")
            st.caption("Review dan approve/reject semua laporan pembayaran dan cicilan dari Agent")
            
            # Sub-tabs untuk memisahkan Payment Reports dan Cicilan
            approval_subtabs = st.tabs(["💵 Payment Reports", "📋 Cicilan Installments"])
            
            # ===== SUB-TAB 1: PAYMENT REPORTS =====
            with approval_subtabs[0]:
                st.markdown("### 💵 Payment Reports dengan Bukti")
                st.caption("Review laporan pembayaran dari Agent beserta bukti gambar yang dilampirkan")
                
                # Filter payment reports
                pcol1, pcol2, pcol3, pcol4 = st.columns(4)
                with pcol1:
                    p_case_filter = st.text_input("Filter Case ID", key="payment_case_filter")
                with pcol2:
                    p_agent_filter = st.text_input("Filter Agent", key="payment_agent_filter")
                with pcol3:
                    p_with_proof = st.selectbox("Bukti Gambar", ["All", "With Proof", "No Proof"], key="payment_proof_filter")
                with pcol4:
                    p_status_filter = st.multiselect(
                        "Status Approval",
                        options=['pending', 'approved', 'rejected'],
                        default=['pending'],
                        key="payment_status_filter"
                    )
                
                # Query payments dengan bukti gambar
                # Try-except untuk backward compatibility
                payment_rows = []
                try:
                    p_query = """
                        SELECT p.id, p.Agreement_No AS Case_ID, p.paid_amount, p.paid_date, 
                               p.status, p.uploaded_by, p.uploaded_at,
                               p.proof_image_drive_id, p.proof_image_filename,
                               IFNULL(p.approval_status, 'pending') as approval_status,
                               IFNULL(p.approval_by, '') as approval_by, 
                               IFNULL(p.approval_at, '') as approval_at, 
                               IFNULL(p.rejection_notes, '') as rejection_notes,
                               sd.Customer_name, sd.Principle_Outstanding
                        FROM payments p
                        LEFT JOIN supervisor_data sd ON sd.Case_ID = p.Agreement_No 
                            OR sd.Virtual_Account_Number = p.Agreement_No
                            OR sd.Third_Uid = p.Agreement_No
                        WHERE 1=1
                    """
                    p_params = []
                    
                    if p_case_filter:
                        p_query += " AND p.Agreement_No LIKE ?"
                        p_params.append(f"%{p_case_filter}%")
                    if p_agent_filter:
                        p_query += " AND p.uploaded_by LIKE ?"
                        p_params.append(f"%{p_agent_filter}%")
                    if p_with_proof == "With Proof":
                        p_query += " AND p.proof_image_drive_id IS NOT NULL"
                    elif p_with_proof == "No Proof":
                        p_query += " AND p.proof_image_drive_id IS NULL"
                    
                    # Filter by approval status
                    if p_status_filter:
                        placeholders = ','.join(['?'] * len(p_status_filter))
                        p_query += f" AND IFNULL(p.approval_status, 'pending') IN ({placeholders})"
                        p_params.extend(p_status_filter)
                    
                    p_query += " ORDER BY p.paid_date DESC, p.uploaded_at DESC LIMIT 100"
                    
                    payment_rows = fetchall(p_query, tuple(p_params))
                except Exception as e:
                    # Fallback: Query tanpa approval columns (backward compatibility)
                    st.warning(f"⚠️ Menggunakan mode kompatibilitas. Kolom approval belum tersedia di database.")
                    try:
                        p_query_fallback = """
                            SELECT p.id, p.Agreement_No AS Case_ID, p.paid_amount, p.paid_date, 
                                   p.status, p.uploaded_by, p.uploaded_at,
                                   p.proof_image_drive_id, p.proof_image_filename,
                                   'pending' as approval_status,
                                   '' as approval_by, 
                                   '' as approval_at, 
                                   '' as rejection_notes,
                                   sd.Customer_name, sd.Principle_Outstanding
                            FROM payments p
                            LEFT JOIN supervisor_data sd ON sd.Case_ID = p.Agreement_No 
                                OR sd.Virtual_Account_Number = p.Agreement_No
                                OR sd.Third_Uid = p.Agreement_No
                            WHERE 1=1
                        """
                        p_params_fallback = []
                        
                        if p_case_filter:
                            p_query_fallback += " AND p.Agreement_No LIKE ?"
                            p_params_fallback.append(f"%{p_case_filter}%")
                        if p_agent_filter:
                            p_query_fallback += " AND p.uploaded_by LIKE ?"
                            p_params_fallback.append(f"%{p_agent_filter}%")
                        if p_with_proof == "With Proof":
                            p_query_fallback += " AND p.proof_image_drive_id IS NOT NULL"
                        elif p_with_proof == "No Proof":
                            p_query_fallback += " AND p.proof_image_drive_id IS NULL"
                        
                        p_query_fallback += " ORDER BY p.paid_date DESC, p.uploaded_at DESC LIMIT 100"
                        
                        payment_rows = fetchall(p_query_fallback, tuple(p_params_fallback))
                    except Exception as e2:
                        st.error(f"❌ Error query payments: {e2}")
                        payment_rows = []
                
                # Summary metrics untuk payment
                if payment_rows:
                    df_payments = pd.DataFrame(payment_rows)
                    pcol1, pcol2, pcol3, pcol4 = st.columns(4)
                    with pcol1:
                        pending_pay = len(df_payments[df_payments['approval_status'] == 'pending'])
                        st.metric("⏳ Menunggu Approval", pending_pay)
                    with pcol2:
                        approved_pay = len(df_payments[df_payments['approval_status'] == 'approved'])
                        st.metric("✅ Sudah Disetujui", approved_pay)
                    with pcol3:
                        rejected_pay = len(df_payments[df_payments['approval_status'] == 'rejected'])
                        st.metric("❌ Ditolak", rejected_pay)
                    with pcol4:
                        total_pending_amount = df_payments[df_payments['approval_status'] == 'pending']['paid_amount'].sum()
                        st.metric("💰 Total Pending", f"Rp {total_pending_amount:,.0f}")
                    
                    st.divider()
                
                if not payment_rows:
                    st.info("✅ Tidak ada payment report ditemukan sesuai filter.")
                else:
                    st.caption(f"Menampilkan {len(payment_rows)} payment reports")
                    
                    for p_row in payment_rows:
                        # Icon berdasarkan status
                        if p_row['approval_status'] == 'pending':
                            icon = "⏳"
                        elif p_row['approval_status'] == 'approved':
                            icon = "✅"
                        else:
                            icon = "❌"
                        
                        proof_icon = '📎' if p_row.get('proof_image_drive_id') else '📄'
                        
                        with st.expander(
                            f"{icon} {proof_icon} "
                            f"{p_row['Case_ID']} - {p_row.get('Customer_name', 'N/A')} - "
                            f"Rp {p_row['paid_amount']:,.0f} ({p_row['paid_date']})"
                        ):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Case ID:** {p_row['Case_ID']}")
                                st.write(f"**Customer:** {p_row.get('Customer_name', 'N/A')}")
                                st.write(f"**Jumlah Bayar:** Rp {p_row['paid_amount']:,.0f}")
                                st.write(f"**Tanggal Bayar:** {p_row['paid_date']}")
                                st.write(f"**Status Laporan:** {p_row['status']}")
                            with col2:
                                try:
                                    outstanding = float(p_row.get('Principle_Outstanding') or 0)
                                    st.write(f"**Outstanding:** Rp {outstanding:,.0f}")
                                    
                                    # Hitung sisa outstanding jika payment di-approve
                                    if p_row['approval_status'] == 'pending':
                                        try:
                                            paid = float(p_row['paid_amount'])
                                            sisa_outstanding = max(0, outstanding - paid)
                                            st.caption(f"💡 Sisa setelah approve: Rp {sisa_outstanding:,.0f}")
                                            # Tambahkan indikator jika akan menjadi Paid Off
                                            if sisa_outstanding == 0:
                                                st.success("🎉 Status akan berubah menjadi **PAID OFF** setelah approve!")
                                        except:
                                            pass
                                    elif p_row['approval_status'] == 'approved':
                                        st.caption(f"✅ Outstanding telah dikurangi")
                                except:
                                    st.write(f"**Outstanding:** {p_row.get('Principle_Outstanding', 'N/A')}")
                                
                                st.write(f"**Uploaded by:** {p_row['uploaded_by']}")
                                st.write(f"**Uploaded at:** {p_row['uploaded_at']}")
                            
                            st.write(f"**Status Approval:** {p_row['approval_status'].upper()}")
                            
                            # Tampilkan info approval/rejection
                            if p_row['approval_status'] == 'approved' and p_row.get('approval_by'):
                                st.success(f"✅ Disetujui oleh {p_row['approval_by']} pada {p_row['approval_at']}")
                            elif p_row['approval_status'] == 'rejected' and p_row.get('approval_by'):
                                st.error(f"❌ Ditolak oleh {p_row['approval_by']} pada {p_row['approval_at']}")
                                if p_row.get('rejection_notes'):
                                    st.warning(f"**Alasan:** {p_row['rejection_notes']}")
                            
                            # Tampilkan bukti gambar jika ada
                            if p_row.get('proof_image_drive_id'):
                                st.markdown("#### 📎 Bukti Pembayaran / Percakapan")
                                st.caption(f"Filename: {p_row.get('proof_image_filename', 'N/A')}")
                                
                                try:
                                    service, _ = build_drive_service()
                                    # Download gambar dari Drive
                                    img_bytes = download_file_bytes(service, p_row['proof_image_drive_id'])
                                    
                                    # Tampilkan gambar jika format image
                                    if p_row.get('proof_image_filename', '').lower().endswith(('.png', '.jpg', '.jpeg')):
                                        st.image(img_bytes, caption=p_row.get('proof_image_filename'), use_container_width=True)
                                    else:
                                        # Untuk PDF atau format lain, tampilkan download button
                                        st.download_button(
                                            label=f"📥 Download {p_row.get('proof_image_filename')}",
                                            data=img_bytes,
                                            file_name=p_row.get('proof_image_filename', 'proof.pdf'),
                                            mime="application/octet-stream",
                                            key=f"dl_proof_{p_row['id']}"
                                        )
                                except Exception as e:
                                    st.error(f"❌ Gagal load bukti gambar dari Drive: {e}")
                                    st.caption(f"Drive ID: {p_row['proof_image_drive_id']}")
                            else:
                                st.info("Tidak ada bukti gambar dilampirkan untuk payment ini.")
                            
                            # Approval/Rejection buttons (hanya untuk pending)
                            if p_row['approval_status'] == 'pending':
                                st.markdown("---")
                                col1, col2, col3 = st.columns([1, 1, 2])
                                
                                with col1:
                                    if st.button("✅ APPROVE", key=f"approve_pay_{p_row['id']}", type="primary", use_container_width=True):
                                        try:
                                            # Update approval status
                                            execute("""
                                                UPDATE payments 
                                                SET approval_status = 'approved',
                                                    approval_by = ?,
                                                    approval_at = CURRENT_TIMESTAMP
                                                WHERE id = ?
                                            """, (agent_name, p_row['id']))
                                            
                                            # PENTING: Kurangi Principle_Outstanding di supervisor_data
                                            paid_amt = p_row.get('paid_amount', 0) or 0
                                            try:
                                                paid_amt = float(paid_amt)
                                            except (ValueError, TypeError):
                                                paid_amt = 0
                                            
                                            if paid_amt > 0:
                                                # Kurangi Principle_Outstanding
                                                execute("""
                                                    UPDATE supervisor_data
                                                    SET Principle_Outstanding = CAST(
                                                        CASE 
                                                            WHEN CAST(IFNULL(Principle_Outstanding, '0') AS REAL) - ? < 0 
                                                            THEN 0 
                                                            ELSE CAST(IFNULL(Principle_Outstanding, '0') AS REAL) - ?
                                                        END AS TEXT
                                                    )
                                                    WHERE Case_ID = ?
                                                """, (paid_amt, paid_amt, p_row['Case_ID']))
                                                
                                                # Cek apakah Outstanding sudah mencapai nol, jika ya set Paid_Off = Yes
                                                check_outstanding = fetchone("""
                                                    SELECT CAST(IFNULL(Principle_Outstanding, '0') AS REAL) as outstanding
                                                    FROM supervisor_data
                                                    WHERE Case_ID = ?
                                                """, (p_row['Case_ID'],))
                                                
                                                if check_outstanding and check_outstanding.get('outstanding', 0) == 0:
                                                    # Outstanding sudah nol, set Paid_Off = Yes
                                                    execute("""
                                                        UPDATE supervisor_data
                                                        SET Paid_Off = 'Yes'
                                                        WHERE Case_ID = ?
                                                    """, (p_row['Case_ID'],))
                                                    
                                                    # Log status paid off
                                                    execute("""
                                                        INSERT INTO audit_logs (user_id, action, details)
                                                        VALUES (?, 'SET_PAID_OFF', ?)
                                                    """, (u.get('id'), 
                                                          f"Set Paid_Off=Yes for {p_row['Case_ID']} (Outstanding reached 0 via payment approval)"))
                                                
                                                # Log perubahan Principle_Outstanding
                                                execute("""
                                                    INSERT INTO audit_logs (user_id, action, details)
                                                    VALUES (?, 'REDUCE_OUTSTANDING', ?)
                                                """, (u.get('id'), 
                                                      f"Reduced Principle_Outstanding for {p_row['Case_ID']} by {paid_amt:,.0f} (payment approval)"))
                                            
                                            # Log audit approval
                                            execute("""
                                                INSERT INTO audit_logs (user_id, action, details)
                                                VALUES (?, 'APPROVE_PAYMENT', ?)
                                            """, (u.get('id'), f"Approved payment {p_row['Case_ID']} - Amount: Rp {paid_amt:,.0f}"))
                                            
                                            # Cek apakah case sudah Paid Off untuk pesan yang lebih informatif
                                            paid_off_check = fetchone("SELECT Paid_Off FROM supervisor_data WHERE Case_ID = ?", (p_row['Case_ID'],))
                                            if paid_off_check and paid_off_check.get('Paid_Off', '').upper() == 'YES':
                                                st.toast(f"✅ Payment di-approve! 🎉 Status PAID OFF!", icon="✅")
                                            else:
                                                st.toast(f"✅ Payment di-approve! Outstanding dikurangi: Rp {paid_amt:,.0f}", icon="✅")
                                            st.rerun()
                                        except Exception as e:
                                            st.toast(f"❌ Gagal approve: {e}", icon="❌")
                                
                                with col2:
                                    if st.button("❌ REJECT", key=f"reject_pay_{p_row['id']}", type="secondary", use_container_width=True):
                                        st.session_state[f"show_reject_form_{p_row['id']}"] = True
                                        st.rerun()
                                
                                with col3:
                                    st.caption("Klik APPROVE untuk menyetujui atau REJECT untuk menolak")
                                
                                # Form untuk reject dengan notes
                                if st.session_state.get(f"show_reject_form_{p_row['id']}", False):
                                    with st.form(f"reject_form_{p_row['id']}"):
                                        st.warning("⚠️ Anda akan menolak payment report ini")
                                        rejection_notes = st.text_area(
                                            "Alasan Penolakan (wajib)",
                                            placeholder="Jelaskan mengapa payment ditolak, agar Agent dapat memperbaiki...",
                                            key=f"reject_notes_{p_row['id']}"
                                        )
                                        
                                        col_submit, col_cancel = st.columns(2)
                                        with col_submit:
                                            submit_reject = st.form_submit_button("🚫 Confirm Reject", type="primary", use_container_width=True)
                                        with col_cancel:
                                            cancel_reject = st.form_submit_button("↩️ Cancel", use_container_width=True)
                                        
                                        if submit_reject:
                                            if not rejection_notes or not rejection_notes.strip():
                                                st.error("❌ Alasan penolakan wajib diisi!")
                                            else:
                                                try:
                                                    execute("""
                                                        UPDATE payments 
                                                        SET approval_status = 'rejected',
                                                            approval_by = ?,
                                                            approval_at = CURRENT_TIMESTAMP,
                                                            rejection_notes = ?
                                                        WHERE id = ?
                                                    """, (agent_name, rejection_notes.strip(), p_row['id']))
                                                    
                                                    # Log audit
                                                    execute("""
                                                        INSERT INTO audit_logs (user_id, action, details)
                                                        VALUES (?, 'REJECT_PAYMENT', ?)
                                                    """, (u.get('id'), f"Rejected payment {p_row['Case_ID']} - Reason: {rejection_notes.strip()}"))
                                                    
                                                    # Hapus flag show reject form
                                                    if f"show_reject_form_{p_row['id']}" in st.session_state:
                                                        del st.session_state[f"show_reject_form_{p_row['id']}"]
                                                    
                                                    st.toast("⚠️ Payment berhasil di-reject! Agent akan diberitahu.", icon="⚠️")
                                                    st.rerun()
                                                except Exception as e:
                                                    st.toast(f"❌ Gagal reject: {e}", icon="❌")
                                        
                                        if cancel_reject:
                                            if f"show_reject_form_{p_row['id']}" in st.session_state:
                                                del st.session_state[f"show_reject_form_{p_row['id']}"]
                                            st.rerun()
            
            # ===== SUB-TAB 2: CICILAN INSTALLMENTS =====
            with approval_subtabs[1]:
                st.markdown("### 📋 Cicilan Installments")
                st.caption("Daftar pengajuan cicilan yang menunggu persetujuan dari Supervisor")
                
                # Query untuk mendapatkan case dengan status cicilan
                cicilan_cases = fetchall("""
                    SELECT 
                        ar.id as result_id,
                        ar.Agreement_No AS Case_ID,
                        ar.agent as Agent,
                        ar.agent_status as Status_Cicilan,
                        ar.agent_ptp_amount as Jumlah_Cicilan,
                        ar.agent_ptp_date as Tanggal_Cicilan,
                        ar.updated_at as Tanggal_Pengajuan,
                        IFNULL(ar.approval_status, 'pending') as Approval_Status,
                        ar.approval_by as Approved_By,
                        ar.approval_at as Approval_Date,
                        ar.rejection_notes,
                        sd.Customer_name,
                        sd.Principle_Outstanding
                    FROM agent_results ar
                    LEFT JOIN supervisor_data sd ON sd.Case_ID = ar.Agreement_No 
                        OR sd.Virtual_Account_Number = ar.Agreement_No 
                        OR sd.Third_Uid = ar.Agreement_No
                    WHERE ar.agent_status IN ('CICIL OS', 'CICIL LUNDIS', 'CICIL POKOK')
                    ORDER BY 
                        CASE WHEN IFNULL(ar.approval_status, 'pending') = 'pending' THEN 0 ELSE 1 END,
                        ar.updated_at DESC
                    LIMIT 200
                """)
                
                # Summary metrics
                if cicilan_cases:
                    df_cicilan = pd.DataFrame(cicilan_cases)
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        pending_count = len(df_cicilan[df_cicilan['Approval_Status'] == 'pending'])
                        st.metric("⏳ Menunggu Approval", pending_count)
                    with col2:
                        approved_count = len(df_cicilan[df_cicilan['Approval_Status'] == 'approved'])
                        st.metric("✅ Sudah Disetujui", approved_count)
                    with col3:
                        rejected_count = len(df_cicilan[df_cicilan['Approval_Status'] == 'rejected'])
                        st.metric("❌ Ditolak", rejected_count)
                    with col4:
                        total_pending_cicilan = df_cicilan[df_cicilan['Approval_Status'] == 'pending']['Jumlah_Cicilan'].sum()
                        st.metric("💰 Total Pending", f"Rp {total_pending_cicilan:,.0f}")
                    
                    st.divider()
                    
                    # Filter by status
                    status_filter = st.multiselect(
                        "Filter Status Approval",
                        options=['pending', 'approved', 'rejected'],
                        default=['pending'],
                        key="cicilan_status_filter"
                    )
                    
                    if status_filter:
                        df_filtered = df_cicilan[df_cicilan['Approval_Status'].isin(status_filter)]
                    else:
                        df_filtered = df_cicilan
                    
                    # Display table with approval actions
                    st.markdown("#### 📊 Daftar Pengajuan Cicilan")
                    
                    for idx, row in df_filtered.iterrows():
                        # Icon berdasarkan status
                        if row['Approval_Status'] == 'pending':
                            icon = "⏳"
                        elif row['Approval_Status'] == 'approved':
                            icon = "✅"
                        else:
                            icon = "❌"
                        
                        with st.expander(
                            f"{icon} {row['Case_ID']} - {row['Customer_name']} - {row['Status_Cicilan']}"
                        ):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Case ID:** {row['Case_ID']}")
                                st.write(f"**Customer:** {row['Customer_name']}")
                                st.write(f"**Agent:** {row['Agent']}")
                                st.write(f"**Status Cicilan:** {row['Status_Cicilan']}")
                            with col2:
                                try:
                                    outstanding = float(row['Principle_Outstanding'] or 0)
                                    st.write(f"**Outstanding:** Rp {outstanding:,.0f}")
                                    
                                    # Hitung sisa outstanding jika cicilan di-approve
                                    if row.get('Jumlah_Cicilan') and row['Approval_Status'] == 'pending':
                                        try:
                                            cicilan = float(row['Jumlah_Cicilan'])
                                            sisa_outstanding = max(0, outstanding - cicilan)
                                            st.caption(f"💡 Sisa setelah approve: Rp {sisa_outstanding:,.0f}")
                                            # Tambahkan indikator jika akan menjadi Paid Off
                                            if sisa_outstanding == 0:
                                                st.success("🎉 Status akan berubah menjadi **PAID OFF** setelah approve!")
                                        except:
                                            pass
                                    elif row['Approval_Status'] == 'approved':
                                        st.caption(f"✅ Outstanding telah dikurangi")
                                except:
                                    st.write(f"**Outstanding:** {row['Principle_Outstanding']}")
                                
                                if row.get('Jumlah_Cicilan'):
                                    st.write(f"**Jumlah Cicilan:** Rp {row['Jumlah_Cicilan']:,.0f}")
                                if row.get('Tanggal_Cicilan'):
                                    st.write(f"**Tanggal Jatuh Tempo:** {row['Tanggal_Cicilan']}")
                                st.write(f"**Tanggal Pengajuan:** {row['Tanggal_Pengajuan']}")
                            
                            st.write(f"**Status:** {row['Approval_Status'].upper()}")
                            
                            if row['Approval_Status'] == 'approved' and row.get('Approved_By'):
                                st.success(f"✅ Disetujui oleh {row['Approved_By']} pada {row['Approval_Date']}")
                            elif row['Approval_Status'] == 'rejected' and row.get('Approved_By'):
                                st.error(f"❌ Ditolak oleh {row['Approved_By']} pada {row['Approval_Date']}")
                                if row.get('rejection_notes'):
                                    st.warning(f"**Alasan:** {row['rejection_notes']}")
                            elif row['Approval_Status'] == 'pending':
                                # Approval buttons
                                st.markdown("---")
                                col1, col2, col3 = st.columns([1, 1, 2])
                                with col1:
                                    if st.button("✅ APPROVE", key=f"approve_cicil_{row['result_id']}", type="primary", use_container_width=True):
                                        try:
                                            # Update approval status
                                            execute("""
                                                UPDATE agent_results 
                                                SET approval_status = 'approved',
                                                    approval_by = ?,
                                                    approval_at = CURRENT_TIMESTAMP
                                                WHERE id = ?
                                            """, (agent_name, row['result_id']))
                                            
                                            # PENTING: Kurangi Principle_Outstanding di supervisor_data
                                            # Ambil jumlah pembayaran yang di-approve
                                            ptp_amount = row.get('Jumlah_Cicilan', 0) or 0
                                            try:
                                                ptp_amount = float(ptp_amount)
                                            except (ValueError, TypeError):
                                                ptp_amount = 0
                                            
                                            if ptp_amount > 0:
                                                # Kurangi Principle_Outstanding
                                                execute("""
                                                    UPDATE supervisor_data
                                                    SET Principle_Outstanding = CAST(
                                                        CASE 
                                                            WHEN CAST(IFNULL(Principle_Outstanding, '0') AS REAL) - ? < 0 
                                                            THEN 0 
                                                            ELSE CAST(IFNULL(Principle_Outstanding, '0') AS REAL) - ?
                                                        END AS TEXT
                                                    )
                                                    WHERE Case_ID = ?
                                                """, (ptp_amount, ptp_amount, row['Case_ID']))
                                                
                                                # Cek apakah Outstanding sudah mencapai nol, jika ya set Paid_Off = Yes
                                                check_outstanding = fetchone("""
                                                    SELECT CAST(IFNULL(Principle_Outstanding, '0') AS REAL) as outstanding
                                                    FROM supervisor_data
                                                    WHERE Case_ID = ?
                                                """, (row['Case_ID'],))
                                                
                                                if check_outstanding and check_outstanding.get('outstanding', 0) == 0:
                                                    # Outstanding sudah nol, set Paid_Off = Yes
                                                    execute("""
                                                        UPDATE supervisor_data
                                                        SET Paid_Off = 'Yes'
                                                        WHERE Case_ID = ?
                                                    """, (row['Case_ID'],))
                                                    
                                                    # Log status paid off
                                                    execute("""
                                                        INSERT INTO audit_logs (user_id, action, details)
                                                        VALUES (?, 'SET_PAID_OFF', ?)
                                                    """, (u.get('id'), 
                                                          f"Set Paid_Off=Yes for {row['Case_ID']} (Outstanding reached 0 via cicilan approval)"))
                                                
                                                # Log perubahan Principle_Outstanding
                                                execute("""
                                                    INSERT INTO audit_logs (user_id, action, details)
                                                    VALUES (?, 'REDUCE_OUTSTANDING', ?)
                                                """, (u.get('id'), 
                                                      f"Reduced Principle_Outstanding for {row['Case_ID']} by {ptp_amount:,.0f} (cicilan approval)"))
                                            
                                            # Log audit approval
                                            execute("""
                                                INSERT INTO audit_logs (user_id, action, details)
                                                VALUES (?, 'APPROVE_CICILAN', ?)
                                            """, (u.get('id'), f"Approved {row['Case_ID']} - {row['Status_Cicilan']} - Amount: {ptp_amount:,.0f}"))
                                            
                                            # Cek apakah case sudah Paid Off untuk pesan yang lebih informatif
                                            paid_off_check = fetchone("SELECT Paid_Off FROM supervisor_data WHERE Case_ID = ?", (row['Case_ID'],))
                                            if paid_off_check and paid_off_check.get('Paid_Off', '').upper() == 'YES':
                                                st.toast(f"✅ Cicilan di-approve! 🎉 Status PAID OFF!", icon="✅")
                                            else:
                                                st.toast(f"✅ Cicilan di-approve! Outstanding dikurangi: Rp {ptp_amount:,.0f}", icon="✅")
                                            st.rerun()
                                        except Exception as e:
                                            st.toast(f"❌ Gagal approve: {e}", icon="❌")
                                
                                with col2:
                                    if st.button("❌ REJECT", key=f"reject_cicil_{row['result_id']}", type="secondary", use_container_width=True):
                                        st.session_state[f"show_reject_cicilan_form_{row['result_id']}"] = True
                                        st.rerun()
                                
                                with col3:
                                    st.caption("Klik APPROVE untuk menyetujui atau REJECT untuk menolak")
                                
                                # Form untuk reject dengan notes
                                if st.session_state.get(f"show_reject_cicilan_form_{row['result_id']}", False):
                                    with st.form(f"reject_cicilan_form_{row['result_id']}"):
                                        st.warning("⚠️ Anda akan menolak pengajuan cicilan ini")
                                        rejection_notes = st.text_area(
                                            "Alasan Penolakan (wajib)",
                                            placeholder="Jelaskan mengapa cicilan ditolak, agar Agent dapat memperbaiki...",
                                            key=f"reject_cicilan_notes_{row['result_id']}"
                                        )
                                        
                                        col_submit, col_cancel = st.columns(2)
                                        with col_submit:
                                            submit_reject = st.form_submit_button("🚫 Confirm Reject", type="primary", use_container_width=True)
                                        with col_cancel:
                                            cancel_reject = st.form_submit_button("↩️ Cancel", use_container_width=True)
                                        
                                        if submit_reject:
                                            if not rejection_notes or not rejection_notes.strip():
                                                st.error("❌ Alasan penolakan wajib diisi!")
                                            else:
                                                try:
                                                    execute("""
                                                        UPDATE agent_results 
                                                        SET approval_status = 'rejected',
                                                            approval_by = ?,
                                                            approval_at = CURRENT_TIMESTAMP,
                                                            rejection_notes = ?
                                                        WHERE id = ?
                                                    """, (agent_name, rejection_notes.strip(), row['result_id']))
                                                    
                                                    # Log audit
                                                    execute("""
                                                        INSERT INTO audit_logs (user_id, action, details)
                                                        VALUES (?, 'REJECT_CICILAN', ?)
                                                    """, (u.get('id'), f"Rejected {row['Case_ID']} - {row['Status_Cicilan']} - Reason: {rejection_notes.strip()}"))
                                                    
                                                    # Hapus flag show reject form
                                                    if f"show_reject_cicilan_form_{row['result_id']}" in st.session_state:
                                                        del st.session_state[f"show_reject_cicilan_form_{row['result_id']}"]
                                                    
                                                    st.toast("⚠️ Cicilan berhasil di-reject! Agent akan diberitahu.", icon="⚠️")
                                                    st.rerun()
                                                except Exception as e:
                                                    st.toast(f"❌ Gagal reject: {e}", icon="❌")
                                        
                                        if cancel_reject:
                                            if f"show_reject_cicilan_form_{row['result_id']}" in st.session_state:
                                                del st.session_state[f"show_reject_cicilan_form_{row['result_id']}"]
                                            st.rerun()
                else:
                    st.info("✅ Tidak ada pengajuan cicilan yang perlu di-review")
            st.subheader("📋 Cicilan Approval")
            st.caption("Daftar pengajuan cicilan yang menunggu persetujuan dari Supervisor")
            
            # Query untuk mendapatkan case dengan status cicilan
            cicilan_cases = fetchall("""
                SELECT 
                    ar.Agreement_No AS Case_ID,
                    ar.agent as Agent,
                    ar.agent_status as Status_Cicilan,
                    ar.agent_ptp_amount as Jumlah_Cicilan,
                    ar.agent_ptp_date as Tanggal_Cicilan,
                    ar.updated_at as Tanggal_Pengajuan,
                    IFNULL(ar.approval_status, 'pending') as Approval_Status,
                    ar.approval_by as Approved_By,
                    ar.approval_at as Approval_Date,
                    sd.Customer_name,
                    sd.Principle_Outstanding
                FROM agent_results ar
                LEFT JOIN supervisor_data sd ON sd.Case_ID = ar.Agreement_No 
                    OR sd.Virtual_Account_Number = ar.Agreement_No 
                    OR sd.Third_Uid = ar.Agreement_No
                WHERE ar.agent_status IN ('CICIL OS', 'CICIL LUNDIS', 'CICIL POKOK')
                ORDER BY 
                    CASE WHEN IFNULL(ar.approval_status, 'pending') = 'pending' THEN 0 ELSE 1 END,
                    ar.updated_at DESC
                LIMIT 200
            """)
            
            if not cicilan_cases:
                st.info("✅ Tidak ada pengajuan cicilan yang perlu di-review")
            else:
                # Summary metrics
                df_cicilan = pd.DataFrame(cicilan_cases)
                col1, col2, col3 = st.columns(3)
                with col1:
                    pending_count = len(df_cicilan[df_cicilan['Approval_Status'] == 'pending'])
                    st.metric("⏳ Menunggu Approval", pending_count)
                with col2:
                    approved_count = len(df_cicilan[df_cicilan['Approval_Status'] == 'approved'])
                    st.metric("✅ Sudah Disetujui", approved_count)
                with col3:
                    rejected_count = len(df_cicilan[df_cicilan['Approval_Status'] == 'rejected'])
                    st.metric("❌ Ditolak", rejected_count)
                
                st.divider()
                
                # Filter by status
                status_filter = st.multiselect(
                    "Filter Status Approval",
                    options=['pending', 'approved', 'rejected'],
                    default=['pending'],
                    key="cicilan_status_filter"
                )
                
                if status_filter:
                    df_filtered = df_cicilan[df_cicilan['Approval_Status'].isin(status_filter)]
                else:
                    df_filtered = df_cicilan
                
                # Display table with approval actions
                st.markdown("### 📊 Daftar Pengajuan Cicilan")
                
                for idx, row in df_filtered.iterrows():
                    with st.expander(
                        f"{'⏳' if row['Approval_Status'] == 'pending' else '✅' if row['Approval_Status'] == 'approved' else '❌'} "
                        f"{row['Case_ID']} - {row['Customer_name']} - {row['Status_Cicilan']}"
                    ):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Case ID:** {row['Case_ID']}")
                            st.write(f"**Customer:** {row['Customer_name']}")
                            st.write(f"**Agent:** {row['Agent']}")
                            st.write(f"**Status Cicilan:** {row['Status_Cicilan']}")
                        with col2:
                            try:
                                outstanding = float(row['Principle_Outstanding'] or 0)
                                st.write(f"**Outstanding:** Rp {outstanding:,.0f}")
                                
                                # Hitung sisa outstanding jika cicilan di-approve
                                if row.get('Jumlah_Cicilan') and row['Approval_Status'] == 'pending':
                                    try:
                                        cicilan = float(row['Jumlah_Cicilan'])
                                        sisa_outstanding = max(0, outstanding - cicilan)
                                        st.caption(f"💡 Sisa setelah approve: Rp {sisa_outstanding:,.0f}")
                                        # Tambahkan indikator jika akan menjadi Paid Off
                                        if sisa_outstanding == 0:
                                            st.success("🎉 Status akan berubah menjadi **PAID OFF** setelah approve!")
                                    except:
                                        pass
                                elif row['Approval_Status'] == 'approved':
                                    st.caption(f"✅ Outstanding telah dikurangi")
                            except:
                                st.write(f"**Outstanding:** {row['Principle_Outstanding']}")
                            
                            if row.get('Jumlah_Cicilan'):
                                st.write(f"**Jumlah Cicilan:** Rp {row['Jumlah_Cicilan']:,.0f}")
                            if row.get('Tanggal_Cicilan'):
                                st.write(f"**Tanggal Jatuh Tempo:** {row['Tanggal_Cicilan']}")
                            st.write(f"**Tanggal Pengajuan:** {row['Tanggal_Pengajuan']}")
                        
                        st.write(f"**Status:** {row['Approval_Status'].upper()}")
                        
                        if row['Approval_Status'] == 'approved' and row.get('Approved_By'):
                            st.success(f"✅ Disetujui oleh {row['Approved_By']} pada {row['Approval_Date']}")
                        elif row['Approval_Status'] == 'rejected' and row.get('Approved_By'):
                            st.error(f"❌ Ditolak oleh {row['Approved_By']} pada {row['Approval_Date']}")
                        elif row['Approval_Status'] == 'pending':
                            # Approval buttons
                            col1, col2, col3 = st.columns([1, 1, 2])
                            with col1:
                                if st.button("✅ APPROVE", key=f"approve_{row['Case_ID']}", type="primary", use_container_width=True):
                                    try:
                                        # Update approval status
                                        execute("""
                                            UPDATE agent_results 
                                            SET approval_status = 'approved',
                                                approval_by = ?,
                                                approval_at = CURRENT_TIMESTAMP
                                            WHERE Agreement_No = ?
                                        """, (agent_name, row['Case_ID']))
                                        
                                        # PENTING: Kurangi Principle_Outstanding di supervisor_data
                                        # Ambil jumlah pembayaran yang di-approve
                                        ptp_amount = row.get('Jumlah_Cicilan', 0) or 0
                                        try:
                                            ptp_amount = float(ptp_amount)
                                        except (ValueError, TypeError):
                                            ptp_amount = 0
                                        
                                        if ptp_amount > 0:
                                            # Kurangi Principle_Outstanding
                                            execute("""
                                                UPDATE supervisor_data
                                                SET Principle_Outstanding = CAST(
                                                    CASE 
                                                        WHEN CAST(IFNULL(Principle_Outstanding, '0') AS REAL) - ? < 0 
                                                        THEN 0 
                                                        ELSE CAST(IFNULL(Principle_Outstanding, '0') AS REAL) - ?
                                                    END AS TEXT
                                                )
                                                WHERE Case_ID = ?
                                            """, (ptp_amount, ptp_amount, row['Case_ID']))
                                            
                                            # Cek apakah Outstanding sudah mencapai nol, jika ya set Paid_Off = Yes
                                            check_outstanding = fetchone("""
                                                SELECT CAST(IFNULL(Principle_Outstanding, '0') AS REAL) as outstanding
                                                FROM supervisor_data
                                                WHERE Case_ID = ?
                                            """, (row['Case_ID'],))
                                            
                                            if check_outstanding and check_outstanding.get('outstanding', 0) == 0:
                                                # Outstanding sudah nol, set Paid_Off = Yes
                                                execute("""
                                                    UPDATE supervisor_data
                                                    SET Paid_Off = 'Yes'
                                                    WHERE Case_ID = ?
                                                """, (row['Case_ID'],))
                                                
                                                # Log status paid off
                                                execute("""
                                                    INSERT INTO audit_logs (user_id, action, details)
                                                    VALUES (?, 'SET_PAID_OFF', ?)
                                                """, (u.get('id'), 
                                                      f"Set Paid_Off=Yes for {row['Case_ID']} (Outstanding reached 0)"))
                                            
                                            # Log perubahan Principle_Outstanding
                                            execute("""
                                                INSERT INTO audit_logs (user_id, action, details)
                                                VALUES (?, 'REDUCE_OUTSTANDING', ?)
                                            """, (u.get('id'), 
                                                  f"Reduced Principle_Outstanding for {row['Case_ID']} by {ptp_amount:,.0f}"))
                                        
                                        # Log audit approval
                                        execute("""
                                            INSERT INTO audit_logs (user_id, action, details)
                                            VALUES (?, 'APPROVE_CICILAN', ?)
                                        """, (u.get('id'), f"Approved {row['Case_ID']} - {row['Status_Cicilan']} - Amount: {ptp_amount:,.0f}"))
                                        
                                        # Cek apakah case sudah Paid Off untuk pesan yang lebih informatif
                                        paid_off_check = fetchone("SELECT Paid_Off FROM supervisor_data WHERE Case_ID = ?", (row['Case_ID'],))
                                        if paid_off_check and paid_off_check.get('Paid_Off', '').upper() == 'YES':
                                            st.toast(f"✅ Cicilan di-approve! Outstanding: Rp {ptp_amount:,.0f}. 🎉 Status PAID OFF!", icon="✅")
                                        else:
                                            st.toast(f"✅ Cicilan di-approve! Outstanding dikurangi: Rp {ptp_amount:,.0f}", icon="✅")
                                        st.rerun()
                                    except Exception as e:
                                        st.toast(f"❌ Gagal approve: {e}", icon="❌")
                            
                            with col2:
                                if st.button("❌ REJECT", key=f"reject_{row['Case_ID']}", type="secondary", use_container_width=True):
                                    try:
                                        execute("""
                                            UPDATE agent_results 
                                            SET approval_status = 'rejected',
                                                approval_by = ?,
                                                approval_at = CURRENT_TIMESTAMP
                                            WHERE Agreement_No = ?
                                        """, (agent_name, row['Case_ID']))
                                        
                                        # Log audit
                                        execute("""
                                            INSERT INTO audit_logs (user_id, action, details)
                                            VALUES (?, 'REJECT_CICILAN', ?)
                                        """, (u.get('id'), f"Rejected {row['Case_ID']} - {row['Status_Cicilan']}"))
                                        
                                        st.toast("⚠️ Cicilan berhasil di-reject!", icon="⚠️")
                                        st.rerun()
                                    except Exception as e:
                                        st.toast(f"❌ Gagal reject: {e}", icon="❌")
                            
                            with col3:
                                st.caption("Klik APPROVE untuk menyetujui atau REJECT untuk menolak")

    # --- My PTP tab ---
    # Index tab disesuaikan: untuk Supervisor tabs[2], untuk Agent tabs[1]
    ptp_tab_idx = 2 if user_role in ('Supervisor', 'Superuser') else 1
    with tabs[ptp_tab_idx]:
        st.subheader("My PTP")
        ptps = fetchall("SELECT Agreement_No as Case_ID, agent_ptp_amount, agent_ptp_date, agent_status, updated_at FROM agent_results WHERE agent=? AND IFNULL(agent_status,'')='PTP' ORDER BY agent_ptp_date DESC LIMIT 500", (agent_name,))
        st.dataframe(pd.DataFrame(ptps), use_container_width=True, hide_index=True)

    # --- Monthly Payment Recap tab ---
    # Index disesuaikan: untuk Supervisor tabs[3], untuk Agent tabs[2]
    monthly_tab_idx = 3 if user_role in ('Supervisor', 'Superuser') else 2
    with tabs[monthly_tab_idx]:
        st.subheader("Monthly Payment Recap")
        start_of_month = today_wib().replace(day=1)
        rec = fetchall(
            """
            SELECT p.Agreement_No as Case_ID, p.paid_amount, p.paid_date, p.status
            FROM payments p
            JOIN agent_assignments a ON a.Agreement_No = p.Agreement_No
            WHERE a.Agent_Assigned_To=? AND DATE(p.paid_date) >= DATE(?)
            ORDER BY p.paid_date DESC
            """,
            (agent_name, start_of_month.isoformat()),
        )
        st.dataframe(pd.DataFrame(rec), use_container_width=True, hide_index=True)
        total = sum([float(r.get('paid_amount') or 0) for r in rec]) if rec else 0
        st.metric("Total Bulan Ini", f"{total:,.0f}")

    # --- All-time Payment Recap tab ---
    # Index disesuaikan: untuk Supervisor tabs[4], untuk Agent tabs[3]
    alltime_tab_idx = 4 if user_role in ('Supervisor', 'Superuser') else 3
    with tabs[alltime_tab_idx]:
        st.subheader("All-time Payment Recap")
        rec_all = fetchall(
            """
            SELECT p.Agreement_No as Case_ID, p.paid_amount, p.paid_date, p.status
            FROM payments p
            JOIN agent_assignments a ON a.Agreement_No = p.Agreement_No
            WHERE a.Agent_Assigned_To=?
            ORDER BY p.paid_date DESC
            """,
            (agent_name,),
        )
        st.dataframe(pd.DataFrame(rec_all), use_container_width=True, hide_index=True)
        total_all = sum([float(r.get('paid_amount') or 0) for r in rec_all]) if rec_all else 0
        st.metric("Total Sepanjang Waktu", f"{total_all:,.0f}")

    # --- Email Templates tab ---
    # Index disesuaikan: untuk Supervisor tabs[5], untuk Agent tabs[4]
    email_tab_idx = 5 if user_role in ('Supervisor', 'Superuser') else 4
    with tabs[email_tab_idx]:
        sel = st.session_state.get('agent_selected')
        info = fetchone("SELECT Debtor_Name, NIK_KTP FROM assign_tracer WHERE Agreement_No=?", (sel,)) or {} if sel else {}
        st.subheader("Email Templates")
        st.caption("Pilih template lalu salin konten untuk dikirim via email/WA.")
        tpl = st.selectbox("Kategori", ["COMPANY", "RELATIVES", "PERSONAL"], index=0, key="tpl_sel")
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
    """Clean, compact dashboard inspired by the provided mockup.
    - Top header with logo and title
    - 4 KPI cards: Pending, Selesai, Total User, Bulan Ini
    - Pending approvals banner
    - Recent Activity table and Upcoming PTP deadlines
    """
    require_roles(ALL_ROLES)
    
    # ========== AUTO-RETURN EXPIRED ASSIGNMENTS ==========
    # Check for agent assignments that passed 7-day deadline without payment
    if "auto_return_checked_today" not in st.session_state:
        try:
            returned_count = check_and_auto_return_expired_assignments()
            if returned_count > 0:
                st.toast(f"🔄 {returned_count} case otomatis kembali ke database (7 hari habis)", icon="🔄")
            st.session_state['auto_return_checked_today'] = today_wib().isoformat()
        except Exception as e:
            st.warning(f"⚠️ Error checking auto-return: {str(e)}")
    
    # Post-restore checkpoint backup (sekali per sesi, 15 menit setelah restore)
    if "post_restore_backup_done" not in st.session_state:
        try:
            last_restore_time = get_setting('auto_restore_last_time')
            if last_restore_time and "service_account" in st.secrets:
                from dateutil import parser
                restore_dt = parser.isoparse(last_restore_time)
                now_dt = datetime.utcnow()
                minutes_since = (now_dt - restore_dt).total_seconds() / 60
                
                # Jika sudah 15-60 menit setelah restore, buat backup checkpoint
                if 15 <= minutes_since <= 60:
                    service_cp, _ = build_drive_service()
                    ok_cp, msg_cp = perform_backup(service_cp, FOLDER_ID_DEFAULT)
                    if ok_cp:
                        st.toast("✅ Backup checkpoint post-restore berhasil.")
                    st.session_state['post_restore_backup_done'] = True
                elif minutes_since > 60:
                    # Sudah terlalu lama, skip
                    st.session_state['post_restore_backup_done'] = True
        except Exception:
            pass

    # Header - personalized greeting (remove logo and static title)
    user_obj = current_user() or {}
    name = (
        user_obj.get("full_name")
        or user_obj.get("name")
        or user_obj.get("login_id")
        or user_obj.get("email")
        or "Pengguna"
    )
    hour = now_wib().hour
    if 4 <= hour < 10:
        greet = "Selamat pagi"
    elif 10 <= hour < 15:
        greet = "Selamat siang"
    elif 15 <= hour < 18:
        greet = "Selamat sore"
    else:
        greet = "Selamat malam"
    st.markdown(f"<h2 style='margin-bottom:0'>Halo, {greet} {name}</h2>", unsafe_allow_html=True)

    # -------- KPI calculations (Financial Focus) --------
    today = today_wib()
    start_of_month = today.replace(day=1)
    last_month_end = start_of_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # A) Running Month Saving = Total paid_amount bulan berjalan
    running_month_saving = (fetchone("""
        SELECT COALESCE(SUM(paid_amount), 0) as total
        FROM payments
        WHERE DATE(paid_date) >= DATE(?)
    """, (start_of_month.isoformat(),)) or {}).get('total', 0)

    # B) Last Month Total = Total paid_amount bulan lalu
    last_month_saving = (fetchone("""
        SELECT COALESCE(SUM(paid_amount), 0) as total
        FROM payments
        WHERE DATE(paid_date) BETWEEN DATE(?) AND DATE(?)
    """, (last_month_start.isoformat(), last_month_end.isoformat())) or {}).get('total', 0)
    
    # Comparison: hijau jika running month > last month, merah jika lebih kecil
    saving_trend = "up" if running_month_saving >= last_month_saving else "down"
    saving_diff = running_month_saving - last_month_saving
    saving_pct = 0.0
    try:
        if last_month_saving > 0:
            saving_pct = (saving_diff / last_month_saving) * 100.0
    except Exception:
        saving_pct = 0.0

    # C) Stock PTP = Total agent_ptp_amount dengan status PTP (belum paid)
    stock_ptp = (fetchone("""
        SELECT COALESCE(SUM(agent_ptp_amount), 0) as total
        FROM agent_results
        WHERE UPPER(agent_status) = 'PTP'
        AND Agreement_No NOT IN (
            SELECT DISTINCT Agreement_No FROM payments WHERE COALESCE(paid_amount,0) > 0
        )
    """) or {}).get('total', 0)

    # D) Forecast Closing = Running Month Saving + Stock PTP bulan berjalan
    # PTP bulan berjalan = PTP yang dibuat bulan ini
    ptp_this_month = (fetchone("""
        SELECT COALESCE(SUM(agent_ptp_amount), 0) as total
        FROM agent_results
        WHERE UPPER(agent_status) = 'PTP'
        AND DATE(updated_at) >= DATE(?)
        AND Agreement_No NOT IN (
            SELECT DISTINCT Agreement_No FROM payments WHERE COALESCE(paid_amount,0) > 0
        )
    """, (start_of_month.isoformat(),)) or {}).get('total', 0)
    
    forecast_closing = running_month_saving + ptp_this_month

    # -------- Enhanced KPI cards (Financial Dashboard) --------
    st.markdown(
        """
        <style>
        /* Enhanced KPI cards with modern financial dashboard aesthetic */
        .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 18px; margin: 16px 0 8px 0; }
        @media (max-width: 1200px) { .kpi-grid { grid-template-columns: repeat(2, 1fr); gap: 14px; } }
        @media (max-width: 700px) { .kpi-grid { grid-template-columns: 1fr; } }
        
        /* Financial KPI card with gradient background */
        .kpi-card { 
            position: relative; 
            overflow: hidden; 
            background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #E5E7EB; 
            border-radius: 20px; 
            padding: 24px 20px; 
            box-shadow: 0 4px 12px rgba(16,24,40,0.08), 0 1px 3px rgba(16,24,40,0.05);
            transition: all 0.3s ease;
        }
        .kpi-card:hover {
            box-shadow: 0 8px 24px rgba(16,24,40,0.12), 0 2px 6px rgba(16,24,40,0.08);
            transform: translateY(-2px);
        }
        
        /* Accent circle (decorative element) */
        .kpi-card::after { 
            content:""; 
            position:absolute; 
            right:-40px; 
            top:-50px; 
            width:200px; 
            height:200px; 
            border-radius: 50%; 
            background: radial-gradient(circle at center, var(--accent-light, #EEF4FF), rgba(255,255,255,0) 60%); 
            opacity:.5;
            z-index: 0;
        }
        
        /* Content layer above decoration */
        .kpi-card > * { position: relative; z-index: 1; }
        
        /* Title with icon support */
        .kpi-title { 
            display: flex;
            align-items: center;
            gap: 8px;
            letter-spacing: .5px; 
            text-transform: uppercase; 
            font-size: 11px; 
            font-weight: 700;
            color: #6B7280; 
            margin-bottom: 12px;
        }
        
        /* Large value with currency formatting */
        .kpi-value { 
            font-size: 32px; 
            font-weight: 800; 
            color: var(--accent, #111827); 
            line-height: 1.1;
            margin-bottom: 8px;
            letter-spacing: -0.5px;
        }
        
        /* Subtitle with trend indicators */
        .kpi-sub { 
            font-size: 13px; 
            color: #9CA3AF; 
            margin-top: 4px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        /* Trend badge */
        .trend-badge {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        .trend-up { background: #ECFDF3; color: #047857; }
        .trend-down { background: #FEF2F2; color: #DC2626; }
        
        /* Color variants for different metrics */
        .accent-teal { --accent: #0D9488; --accent-light: #CCFBF1; }
        .accent-emerald { --accent: #059669; --accent-light: #D1FAE5; }
        .accent-amber { --accent: #F59E0B; --accent-light: #FEF3C7; }
        .accent-violet { --accent: #7C3AED; --accent-light: #EDE9FE; }
        
        /* Approval banner */
        .approval-banner {
            background: #FFFFFF;
            border: 1px solid #E5E7EB;
            border-radius: 12px;
            padding: 14px 18px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        }
        
        /* Pills */
        .pill { display:inline-flex; align-items:center; gap:4px; padding:3px 10px; border-radius: 999px; font-size:12px; font-weight: 700; }
        .pill-warning { background:#FFF7ED; color:#C2410C; }
        .pill-success { background:#ECFDF3; color:#027A48; }
        .pill-info { background:#EEF4FF; color:#3538CD; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Render enhanced financial KPI cards
    kpi_cols = st.columns(4)
    
    with kpi_cols[0]:
        st.markdown(
            f"""
            <div class='kpi-card accent-teal'>
                <div class='kpi-title'>💰 Running Month Saving</div>
                <div class='kpi-value'>Rp {running_month_saving:,.0f}</div>
                <div class='kpi-sub'>Total pembayaran bulan ini</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    with kpi_cols[1]:
        trend_color = "#059669" if saving_trend == "up" else "#DC2626"
        trend_icon = "↗" if saving_trend == "up" else "↘"
        trend_class = "trend-up" if saving_trend == "up" else "trend-down"
        st.markdown(
            f"""
            <div class='kpi-card accent-emerald'>
                <div class='kpi-title'>📊 Last Month</div>
                <div class='kpi-value'>Rp {last_month_saving:,.0f}</div>
                <div class='kpi-sub'>
                    <span class='trend-badge {trend_class}'>{trend_icon} {abs(saving_pct):.1f}%</span>
                    <span style='color: {trend_color}; font-weight: 600;'>
                        {"Lebih tinggi" if saving_trend == "up" else "Lebih rendah"}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    with kpi_cols[2]:
        st.markdown(
            f"""
            <div class='kpi-card accent-amber'>
                <div class='kpi-title'>🎯 Stock PTP</div>
                <div class='kpi-value'>Rp {stock_ptp:,.0f}</div>
                <div class='kpi-sub'>Total janji bayar aktif</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    with kpi_cols[3]:
        st.markdown(
            f"""
            <div class='kpi-card accent-violet'>
                <div class='kpi-title'>🚀 Forecast Closing</div>
                <div class='kpi-value'>Rp {forecast_closing:,.0f}</div>
                <div class='kpi-sub'>Saving + Prospect bulan ini</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # -------- Pending approvals banner --------
    pending_approvals = get_pending_users_count()
    if pending_approvals > 0:
        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class='approval-banner'>
              <div style='display:flex; align-items:center; gap:16px;'>
                <div style='font-size:32px;'>📝</div>
                <div style='flex:1;'>
                  <div style='font-weight:700; font-size:15px; color:#111827; margin-bottom:4px;'>Pending User Approvals</div>
                  <div style='font-size:13px; color:#6B7280;'>
                    <span class='pill pill-warning'><strong>{pending_approvals}</strong> users</span>
                    waiting for admin approval to access the system.
                  </div>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    st.markdown("---")

    # -------- Bottom tables: Recent logs | Upcoming deadlines --------
    left, right = st.columns([3, 2])

    # Recent Activity Logs - filtered by user role
    with left:
        st.subheader("Recent Activity Logs 🧾")
        
        # Get current user info for filtering
        user_obj = current_user() or {}
        user_role = user_obj.get("role", "")
        user_id = user_obj.get("id")
        is_supervisor = user_role in ("Superuser", "Supervisor")
        
        # Query - supervisors see all, others see only their own
        if is_supervisor:
            logs = fetchall(
                """
                SELECT audit_logs.timestamp, COALESCE(users.full_name, users.name, users.login_id) AS user,
                       audit_logs.action, audit_logs.details
                FROM audit_logs
                LEFT JOIN users ON users.id = audit_logs.user_id
                ORDER BY audit_logs.id DESC LIMIT 10
                """
            )
        else:
            logs = fetchall(
                """
                SELECT audit_logs.timestamp, COALESCE(users.full_name, users.name, users.login_id) AS user,
                       audit_logs.action, audit_logs.details
                FROM audit_logs
                LEFT JOIN users ON users.id = audit_logs.user_id
                WHERE audit_logs.user_id = ?
                ORDER BY audit_logs.id DESC LIMIT 10
                """,
                (user_id,)
            )
        
        if logs:
            # Format to local GMT+7 display similar to Audit page
            def _to_gmt7(ts):
                try:
                    dt = datetime.fromisoformat(ts)
                    return (dt + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return ts
            df_logs = pd.DataFrame([
                {"timestamp": _to_gmt7(r.get("timestamp","")), "user": r.get("user",""), "action": r.get("action",""), "detail": r.get("details",""),}
                for r in logs
            ])
            show_cols = [c for c in ["timestamp","user","action","detail"] if c in df_logs.columns]
            st.dataframe(df_logs[show_cols], hide_index=True, use_container_width=True)
        else:
            st.info("Belum ada aktivitas.")

    # Upcoming Document Deadlines (PTP)
    with right:
        st.subheader("Upcoming Document Deadlines ⏰")
        ptps = fetchall(
            """
            SELECT Agreement_No, agent, agent_ptp_amount, agent_ptp_date, agent_status
            FROM agent_results
            WHERE COALESCE(agent_status,'')='PTP' AND DATE(agent_ptp_date) >= DATE(?)
            ORDER BY DATE(agent_ptp_date) ASC
            LIMIT 10
            """,
            (today.isoformat(),)
        )
        if ptps:
            df_ptp = pd.DataFrame([
                {
                    "Nomor Dokumen": r.get('Agreement_No'),
                    "Agent": r.get('agent'),
                    "PTP Amount": r.get('agent_ptp_amount'),
                    "Batas Waktu": r.get('agent_ptp_date'),
                    "Status": r.get('agent_status'),
                } for r in ptps
            ])
            st.dataframe(df_ptp, use_container_width=True, hide_index=True)
        else:
            st.info("Tidak ada PTP yang akan jatuh tempo.")

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
    
    # Tabs untuk memisahkan kategori informasi
    # Tambahkan tab User Management khusus untuk Supervisor/Superuser
    user_role = u.get('role')
    if user_role in ("Superuser", "Supervisor"):
        tabs = st.tabs(["Basic Info", "Personal Details", "Banking Info", "Certification", "User Management"])
    else:
        tabs = st.tabs(["Basic Info", "Personal Details", "Banking Info", "Certification"])
    
    # --- Basic Info Tab ---
    with tabs[0]:
        st.subheader("Basic Information")
        with st.form("basic_info_form"):
            full_name = st.text_input("Full Name", value=user_row.get('full_name') or "")
            email = st.text_input("Email", value=user_row.get('email') or "")
            work_email = st.text_input("Work Email", value=user_row.get('work_email') or "")
            current_division = user_row.get('division') or "Telecollection Officer"
            division_options = ["Telecollection Officer", "Supervisor", "Skiptrace Officer"]
            # Set index based on current value, default to first option if not found
            division_index = division_options.index(current_division) if current_division in division_options else 0
            division = st.selectbox(
                "Division", 
                options=division_options,
                index=division_index,
                help="Pilih divisi/departemen Anda"
            )
            
            st.markdown("**Change Password (Optional)**")
            pw1 = st.text_input("New Password", type="password", key="user_pw1", placeholder="Leave blank to keep current")
            pw2 = st.text_input("Confirm New Password", type="password", key="user_pw2", placeholder="Leave blank to keep current")
            
            submitted_basic = st.form_submit_button("Update Basic Info")
            if submitted_basic:
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
                if work_email.strip() != (user_row.get('work_email') or ""):
                    updates.append("work_email=?")
                    params.append(work_email.strip())
                    changed = True
                if division.strip() != (user_row.get('division') or ""):
                    updates.append("division=?")
                    params.append(division.strip())
                    changed = True
                
                if pw1 or pw2:
                    if pw1 != pw2:
                        st.toast("❌ Password dan konfirmasi tidak cocok!", icon="❌")
                    elif pw1.strip():
                        updates.append("password_hash=?")
                        params.append(hash_password(pw1.strip()))
                        changed = True
                
                if not changed:
                    st.toast("ℹ️ Tidak ada perubahan untuk disimpan", icon="ℹ️")
                else:
                    params.append(u.get('id'))
                    try:
                        execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(params))
                        updated_user = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
                        login_user(updated_user)
                        try:
                            detail = f"Updated: {', '.join([s.split('=')[0] for s in updates])}"
                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                                    (u.get('id'), "USER_UPDATE", detail))
                        except Exception:
                            pass
                        st.toast("✅ Informasi dasar berhasil diperbarui!", icon="✅")
                        st.rerun()
                    except Exception as e:
                        st.toast(f"❌ Gagal update: {e}", icon="❌")
    
    # --- Personal Details Tab ---
    with tabs[1]:
        st.subheader("Personal Details")
        with st.form("personal_details_form"):
            nik = st.text_input("NIK (Nomor Induk Kependudukan)", value=user_row.get('nik') or "", 
                                max_chars=16, help="16 digit NIK sesuai KTP")
            dob = st.date_input("Date of Birth", 
                                value=datetime.strptime(user_row.get('dob'), "%Y-%m-%d").date() if user_row.get('dob') else None,
                                min_value=date(1950, 1, 1), max_value=date.today())
            phone_number = st.text_input("Phone Number", value=user_row.get('phone_number') or "", 
                                         placeholder="e.g., 08123456789")
            alamat = st.text_area("Alamat Lengkap", value=user_row.get('alamat') or "", 
                                  height=100, help="Alamat sesuai KTP atau domisili")
            join_date = st.date_input("Join Date", 
                                      value=datetime.strptime(user_row.get('join_date'), "%Y-%m-%d").date() if user_row.get('join_date') else date.today(),
                                      max_value=date.today())
            
            submitted_personal = st.form_submit_button("Update Personal Details")
            if submitted_personal:
                updates = []
                params = []
                
                if nik.strip() != (user_row.get('nik') or ""):
                    updates.append("nik=?")
                    params.append(nik.strip())
                if dob:
                    dob_str = dob.isoformat()
                    if dob_str != user_row.get('dob'):
                        updates.append("dob=?")
                        params.append(dob_str)
                if phone_number.strip() != (user_row.get('phone_number') or ""):
                    updates.append("phone_number=?")
                    params.append(phone_number.strip())
                if alamat.strip() != (user_row.get('alamat') or ""):
                    updates.append("alamat=?")
                    params.append(alamat.strip())
                if join_date:
                    join_str = join_date.isoformat()
                    if join_str != user_row.get('join_date'):
                        updates.append("join_date=?")
                        params.append(join_str)
                
                if not updates:
                    st.toast("ℹ️ Tidak ada perubahan untuk disimpan", icon="ℹ️")
                else:
                    params.append(u.get('id'))
                    try:
                        execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(params))
                        updated_user = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
                        login_user(updated_user)
                        try:
                            detail = f"Updated personal details: {', '.join([s.split('=')[0] for s in updates])}"
                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                                    (u.get('id'), "USER_UPDATE", detail))
                        except Exception:
                            pass
                        st.toast("✅ Detail personal berhasil diperbarui!", icon="✅")
                        st.rerun()
                    except Exception as e:
                        st.toast(f"❌ Gagal update: {e}", icon="❌")
    
    # --- Banking Info Tab ---
    with tabs[2]:
        st.subheader("Banking Information")
        st.caption("Untuk keperluan payroll dan reimbursement")
        with st.form("banking_info_form"):
            nomor_rekening_bca = st.text_input("Nomor Rekening BCA", 
                                               value=user_row.get('nomor_rekening_bca') or "",
                                               help="Nomor rekening BCA untuk transfer gaji")
            nama_rekening_bca = st.text_input("Nama Pemilik Rekening BCA", 
                                              value=user_row.get('nama_rekening_bca') or "",
                                              help="Nama sesuai buku rekening BCA")
            
            submitted_banking = st.form_submit_button("Update Banking Info")
            if submitted_banking:
                updates = []
                params = []
                
                if nomor_rekening_bca.strip() != (user_row.get('nomor_rekening_bca') or ""):
                    updates.append("nomor_rekening_bca=?")
                    params.append(nomor_rekening_bca.strip())
                if nama_rekening_bca.strip() != (user_row.get('nama_rekening_bca') or ""):
                    updates.append("nama_rekening_bca=?")
                    params.append(nama_rekening_bca.strip())
                
                if not updates:
                    st.toast("ℹ️ Tidak ada perubahan untuk disimpan", icon="ℹ️")
                else:
                    params.append(u.get('id'))
                    try:
                        execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(params))
                        updated_user = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
                        login_user(updated_user)
                        try:
                            detail = "Updated banking information"
                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                                    (u.get('id'), "USER_UPDATE", detail))
                        except Exception:
                            pass
                        st.toast("✅ Informasi banking berhasil diperbarui!", icon="✅")
                        st.rerun()
                    except Exception as e:
                        st.toast(f"❌ Gagal update: {e}", icon="❌")
    
    # --- Certification Tab ---
    with tabs[3]:
        st.subheader("Sertifikasi Penagihan")
        st.caption("Upload sertifikasi SPPI/AFPI (opsional) - PDF atau JPG")
        
        # Display current certificate if exists
        if user_row.get('sertifikasi_drive_id'):
            st.success(f"✅ Certificate uploaded: **{user_row.get('sertifikasi_filename')}**")
            col1, col2 = st.columns(2)
            with col1:
                # Download button
                try:
                    service, _ = build_drive_service()
                    cert_bytes = download_file_bytes(service, user_row.get('sertifikasi_drive_id'))
                    st.download_button(
                        label="📥 Download Certificate",
                        data=cert_bytes,
                        file_name=user_row.get('sertifikasi_filename'),
                        mime="application/octet-stream"
                    )
                except Exception as e:
                    st.error(f"Failed to load certificate: {e}")
            with col2:
                if st.button("🗑️ Remove Certificate"):
                    try:
                        execute("UPDATE users SET sertifikasi_drive_id=NULL, sertifikasi_filename=NULL WHERE id=?", 
                                (u.get('id'),))
                        updated_user = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
                        login_user(updated_user)
                        st.success("Certificate removed.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to remove: {e}")
        else:
            st.info("No certificate uploaded yet.")
        
        st.markdown("---")
        st.markdown("**Upload New Certificate**")
        uploaded_cert = st.file_uploader(
            "Choose certificate file (PDF/JPG/PNG)",
            type=["pdf", "jpg", "jpeg", "png"],
            key="cert_uploader"
        )
        
        if uploaded_cert is not None:
            if st.button("Upload Certificate to Google Drive"):
                try:
                    service, _ = build_drive_service()
                    timestamp = now_wib().strftime("%Y%m%d_%H%M%S")
                    original_filename = uploaded_cert.name
                    ext = original_filename.split('.')[-1] if '.' in original_filename else 'pdf'
                    cert_filename = f"cert_{user_row.get('login_id')}_{timestamp}.{ext}"
                    cert_bytes = uploaded_cert.read()
                    mimetype = uploaded_cert.type or "application/pdf"
                    
                    # Upload to Google Drive
                    cert_drive_id = upload_bytes(service, FOLDER_ID_DEFAULT, cert_filename, cert_bytes, mimetype)
                    
                    if cert_drive_id:
                        # Save to database
                        execute(
                            "UPDATE users SET sertifikasi_drive_id=?, sertifikasi_filename=? WHERE id=?",
                            (cert_drive_id, cert_filename, u.get('id'))
                        )
                        updated_user = fetchone("SELECT * FROM users WHERE id=?", (u.get('id'),))
                        login_user(updated_user)
                        
                        # Audit log
                        try:
                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                                    (u.get('id'), "CERT_UPLOAD", f"Uploaded: {cert_filename}"))
                        except Exception:
                            pass
                        
                        st.success(f"✅ Certificate uploaded successfully: {cert_filename}")
                        st.rerun()
                    else:
                        st.error("Failed to upload certificate to Google Drive.")
                except Exception as e:
                    st.error(f"Upload failed: {e}")
    
    # --- User Management Tab (Khusus Supervisor/Superuser) ---
    if user_role in ("Superuser", "Supervisor"):
        with tabs[4]:
            st.subheader("👥 User Management")
            st.caption("Manage users, approve pending registrations, and edit user details")
            
            # Sub-tabs untuk User Management
            mgmt_tabs = st.tabs(["Pending Approvals", "All Users", "Add New User", "🎯 Agent DT Restrictions"])
            
            # --- Pending Approvals Sub-tab ---
            with mgmt_tabs[0]:
                st.markdown("#### 📋 Pending User Approvals")
                pending_users = fetchall("SELECT * FROM users WHERE approved=0 ORDER BY created_at DESC")
                
                if not pending_users:
                    st.info("✅ No pending approvals.")
                else:
                    st.warning(f"⚠️ {len(pending_users)} user(s) waiting for approval")
                    
                    for pending_user in pending_users:
                        with st.expander(f"👤 {pending_user.get('full_name') or pending_user.get('name')} - {pending_user.get('login_id')}"):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Name:** {pending_user.get('full_name') or pending_user.get('name')}")
                                st.write(f"**Login ID:** {pending_user.get('login_id')}")
                                st.write(f"**Email:** {pending_user.get('email')}")
                                st.write(f"**Role:** {pending_user.get('role')}")
                            with col2:
                                st.write(f"**Division:** {pending_user.get('division') or 'N/A'}")
                                st.write(f"**Phone:** {pending_user.get('phone_number') or 'N/A'}")
                                st.write(f"**Registered:** {pending_user.get('created_at')}")
                            
                            st.markdown("---")
                            acol1, acol2, acol3 = st.columns([1, 1, 2])
                            with acol1:
                                if st.button("✅ Approve", key=f"approve_{pending_user.get('id')}"):
                                    try:
                                        execute("UPDATE users SET approved=1 WHERE id=?", (pending_user.get('id'),))
                                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                (u.get('id'), "USER_APPROVE", f"Approved user: {pending_user.get('login_id')}"))
                                        st.success(f"✅ Approved {pending_user.get('login_id')}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Failed to approve: {e}")
                            with acol2:
                                if st.button("❌ Reject", key=f"reject_{pending_user.get('id')}"):
                                    try:
                                        execute("DELETE FROM users WHERE id=?", (pending_user.get('id'),))
                                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                (u.get('id'), "USER_REJECT", f"Rejected user: {pending_user.get('login_id')}"))
                                        st.success(f"❌ Rejected and deleted {pending_user.get('login_id')}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Failed to reject: {e}")
            
            # --- All Users Sub-tab ---
            with mgmt_tabs[1]:
                st.markdown("#### 📊 All Users")
                
                # Filter
                fcol1, fcol2, fcol3 = st.columns(3)
                with fcol1:
                    filter_role = st.selectbox("Filter by Role", ["All", "Superuser", "Supervisor", "Tracer", "Agent"], key="filter_role")
                with fcol2:
                    filter_status = st.selectbox("Filter by Status", ["All", "Approved", "Pending"], key="filter_status")
                with fcol3:
                    search_name = st.text_input("Search by Name/Login ID", key="search_user")
                
                # Build query
                query = "SELECT * FROM users WHERE 1=1"
                params = []
                
                if filter_role != "All":
                    query += " AND role=?"
                    params.append(filter_role)
                
                if filter_status == "Approved":
                    query += " AND approved=1"
                elif filter_status == "Pending":
                    query += " AND approved=0"
                
                if search_name:
                    query += " AND (full_name LIKE ? OR name LIKE ? OR login_id LIKE ?)"
                    params.extend([f"%{search_name}%", f"%{search_name}%", f"%{search_name}%"])
                
                query += " ORDER BY created_at DESC"
                
                all_users = fetchall(query, tuple(params))
                
                if not all_users:
                    st.info("No users found with current filters.")
                else:
                    st.caption(f"Found {len(all_users)} user(s)")
                    
                    # Display users in data editor for easy viewing
                    users_data = []
                    for usr in all_users:
                        users_data.append({
                            'ID': usr.get('id'),
                            'Name': usr.get('full_name') or usr.get('name'),
                            'Login ID': usr.get('login_id'),
                            'Email': usr.get('email'),
                            'Role': usr.get('role'),
                            'Division': usr.get('division') or '',
                            'Approved': '✅' if usr.get('approved') else '❌',
                            'Created': usr.get('created_at')
                        })
                    
                    df_users = pd.DataFrame(users_data)
                    st.dataframe(df_users, use_container_width=True, hide_index=True)
                    
                    st.markdown("---")
                    st.markdown("#### Edit/Delete User")
                    
                    selected_user_id = st.selectbox(
                        "Select user to edit/delete",
                        options=[usr.get('id') for usr in all_users],
                        format_func=lambda x: f"{next((u.get('full_name') or u.get('name') for u in all_users if u.get('id') == x), 'Unknown')} ({next((u.get('login_id') for u in all_users if u.get('id') == x), 'N/A')})",
                        key="select_edit_user"
                    )
                    
                    if selected_user_id:
                        selected_user = next((u for u in all_users if u.get('id') == selected_user_id), None)
                        
                        if selected_user:
                            edit_col, delete_col = st.columns([3, 1])
                            
                            with edit_col:
                                st.markdown("**Edit User Details**")
                                with st.form(f"edit_user_form_{selected_user_id}"):
                                    edit_full_name = st.text_input("Full Name", value=selected_user.get('full_name') or "")
                                    edit_login_id = st.text_input("Login ID", value=selected_user.get('login_id') or "")
                                    edit_email = st.text_input("Email", value=selected_user.get('email') or "")
                                    edit_role = st.selectbox("Role", ["Superuser", "Supervisor", "Tracer", "Agent"], 
                                                            index=["Superuser", "Supervisor", "Tracer", "Agent"].index(selected_user.get('role')) if selected_user.get('role') in ["Superuser", "Supervisor", "Tracer", "Agent"] else 0)
                                    current_edit_division = selected_user.get('division') or "Telecollection Officer"
                                    division_opts = ["Telecollection Officer", "Supervisor", "Skiptrace Officer"]
                                    edit_division_index = division_opts.index(current_edit_division) if current_edit_division in division_opts else 0
                                    edit_division = st.selectbox("Division", options=division_opts, index=edit_division_index)
                                    edit_approved = st.checkbox("Approved", value=bool(selected_user.get('approved')))
                                    
                                    # Option to reset password
                                    st.markdown("**Reset Password (Optional)**")
                                    new_password = st.text_input("New Password", type="password", key=f"new_pwd_{selected_user_id}")
                                    
                                    submitted_edit = st.form_submit_button("💾 Save Changes")
                                    
                                    if submitted_edit:
                                        try:
                                            updates = []
                                            params_update = []
                                            
                                            if edit_full_name != (selected_user.get('full_name') or ""):
                                                updates.append("full_name=?")
                                                params_update.append(edit_full_name)
                                            
                                            if edit_login_id != (selected_user.get('login_id') or ""):
                                                updates.append("login_id=?")
                                                params_update.append(edit_login_id)
                                            
                                            if edit_email != (selected_user.get('email') or ""):
                                                updates.append("email=?")
                                                params_update.append(edit_email)
                                            
                                            if edit_role != selected_user.get('role'):
                                                updates.append("role=?")
                                                params_update.append(edit_role)
                                            
                                            if edit_division != (selected_user.get('division') or ""):
                                                updates.append("division=?")
                                                params_update.append(edit_division)
                                            
                                            if edit_approved != bool(selected_user.get('approved')):
                                                updates.append("approved=?")
                                                params_update.append(1 if edit_approved else 0)
                                            
                                            if new_password:
                                                updates.append("password_hash=?")
                                                params_update.append(hash_password(new_password))
                                            
                                            if updates:
                                                params_update.append(selected_user_id)
                                                execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", tuple(params_update))
                                                
                                                # Audit log
                                                execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                        (u.get('id'), "USER_EDIT", f"Edited user: {edit_login_id}"))
                                                
                                                st.success(f"✅ User {edit_login_id} updated successfully!")
                                                st.rerun()
                                            else:
                                                st.info("No changes detected.")
                                        except Exception as e:
                                            st.error(f"Failed to update user: {e}")
                            
                            with delete_col:
                                st.markdown("**Delete User**")
                                st.warning("⚠️ This action cannot be undone!")
                                
                                if st.button("🗑️ Delete User", key=f"delete_user_{selected_user_id}"):
                                    if selected_user_id == u.get('id'):
                                        st.error("❌ You cannot delete yourself!")
                                    else:
                                        try:
                                            execute("DELETE FROM users WHERE id=?", (selected_user_id,))
                                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                    (u.get('id'), "USER_DELETE", f"Deleted user: {selected_user.get('login_id')}"))
                                            st.success(f"✅ User {selected_user.get('login_id')} deleted successfully!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Failed to delete user: {e}")
            
            # --- Add New User Sub-tab ---
            with mgmt_tabs[2]:
                st.markdown("#### ➕ Add New User")
                st.caption("Create a new user account (will be automatically approved)")
                
                with st.form("add_new_user_form"):
                    new_full_name = st.text_input("Full Name *", key="new_full_name")
                    new_login_id = st.text_input("Login ID *", key="new_login_id", help="Unique username for login")
                    new_email = st.text_input("Email", key="new_email")
                    new_password = st.text_input("Password *", type="password", key="new_password")
                    new_password_confirm = st.text_input("Confirm Password *", type="password", key="new_password_confirm")
                    new_role = st.selectbox("Role *", ["Supervisor", "Tracer", "Agent", "Superuser"], key="new_role")
                    new_division = st.selectbox("Division", options=["Telecollection Officer", "Supervisor", "Skiptrace Officer"], key="new_division")
                    
                    submitted_new = st.form_submit_button("➕ Create User")
                    
                    if submitted_new:
                        if not new_full_name or not new_login_id or not new_password:
                            st.error("❌ Please fill in all required fields (*)")
                        elif new_password != new_password_confirm:
                            st.error("❌ Passwords do not match!")
                        else:
                            # Check if login_id already exists
                            existing = fetchone("SELECT id FROM users WHERE login_id=?", (new_login_id,))
                            if existing:
                                st.error(f"❌ Login ID '{new_login_id}' already exists!")
                            else:
                                try:
                                    execute(
                                        """INSERT INTO users (name, full_name, login_id, email, password_hash, role, division, approved) 
                                           VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
                                        (new_full_name, new_full_name, new_login_id, new_email, 
                                         hash_password(new_password), new_role, new_division)
                                    )
                                    
                                    # Audit log
                                    execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                            (u.get('id'), "USER_CREATE", f"Created user: {new_login_id} (Role: {new_role})"))
                                    
                                    st.success(f"✅ User '{new_login_id}' created successfully!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to create user: {e}")
            
            # --- Agent DT Restrictions Sub-tab ---
            with mgmt_tabs[3]:
                st.markdown("#### 🎯 Agent DT (Lending Entity) Restrictions")
                st.caption("Configure which Lending Entities (DT) each Agent can accept assignments from. Leave empty to allow all DTs.")
                
                # Get all agents
                all_agents = fetchall("SELECT id, full_name, login_id FROM users WHERE role='Agent' AND approved=1 ORDER BY full_name")
                
                if not all_agents:
                    st.warning("⚠️ No approved Agents found in the system.")
                else:
                    # Get all unique DTs from supervisor_data
                    dt_rows = fetchall("SELECT DISTINCT Lending_Entity FROM supervisor_data WHERE Lending_Entity IS NOT NULL AND Lending_Entity != '' ORDER BY Lending_Entity")
                    available_dts = [row['Lending_Entity'] for row in dt_rows if row.get('Lending_Entity')]
                    
                    if not available_dts:
                        st.warning("⚠️ No Lending Entities found in supervisor_data.")
                    else:
                        st.info(f"📊 Found {len(available_dts)} unique Lending Entity/DT values")
                        
                        # Agent selector
                        selected_agent_id = st.selectbox(
                            "Select Agent to configure",
                            options=[a['id'] for a in all_agents],
                            format_func=lambda x: f"{next((a['full_name'] for a in all_agents if a['id'] == x), 'Unknown')} ({next((a['login_id'] for a in all_agents if a['id'] == x), 'N/A')})",
                            key="dt_restriction_agent_select"
                        )
                        
                        if selected_agent_id:
                            selected_agent = next((a for a in all_agents if a['id'] == selected_agent_id), None)
                            
                            if selected_agent:
                                st.markdown(f"**Configuring restrictions for:** {selected_agent['full_name']} ({selected_agent['login_id']})")
                                
                                # Get current allowed DTs for this agent
                                current_dts = get_agent_allowed_dts(selected_agent_id)
                                
                                # Display current restrictions
                                if current_dts:
                                    st.success(f"✅ Current restrictions: Agent can only accept from **{len(current_dts)} DT(s)**: {', '.join(current_dts)}")
                                else:
                                    st.info("ℹ️ No restrictions set - Agent can accept assignments from **all DTs**")
                                
                                st.markdown("---")
                                
                                # Multiselect for allowed DTs
                                st.markdown("**Set Allowed Lending Entities (DT)**")
                                st.caption("Select one or more DTs that this agent can accept. Leave empty to remove all restrictions.")
                                
                                selected_dts = st.multiselect(
                                    "Allowed DTs (Lending Entities)",
                                    options=available_dts,
                                    default=current_dts,
                                    key="dt_restriction_multiselect",
                                    help="Select multiple DTs using the dropdown. Agent will only be able to accept assignments from these selected DTs."
                                )
                                
                                # Set button
                                col1, col2 = st.columns([1, 3])
                                with col1:
                                    if st.button("💾 Set DT Restrictions", type="primary"):
                                        success, message = set_agent_allowed_dts(
                                            selected_agent_id, 
                                            selected_dts,
                                            created_by=u.get('full_name') or u.get('login_id')
                                        )
                                        
                                        if success:
                                            # Audit log
                                            try:
                                                detail = f"Set DT restrictions for {selected_agent['login_id']}: {len(selected_dts)} DT(s)"
                                                execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                                        (u.get('id'), "DT_RESTRICTION_SET", detail))
                                            except Exception:
                                                pass
                                            
                                            st.success(f"✅ {message}")
                                            st.rerun()
                                        else:
                                            st.error(f"❌ {message}")
                                
                                # Show preview of restrictions impact
                                if selected_dts:
                                    st.markdown("---")
                                    st.markdown("**Preview: Assignment Restrictions**")
                                    st.caption(f"With these settings, agent **{selected_agent['full_name']}** will:")
                                    st.markdown(f"- ✅ **CAN** accept assignments from: {', '.join(selected_dts)}")
                                    
                                    blocked_dts = [dt for dt in available_dts if dt not in selected_dts]
                                    if blocked_dts:
                                        st.markdown(f"- ❌ **CANNOT** accept assignments from: {', '.join(blocked_dts)}")
                                
                                # Show all agent restrictions summary
                                st.markdown("---")
                                st.markdown("**All Agent DT Restrictions Summary**")
                                
                                restrictions_summary = []
                                for agent in all_agents:
                                    agent_dts = get_agent_allowed_dts(agent['id'])
                                    restrictions_summary.append({
                                        'Agent': f"{agent['full_name']} ({agent['login_id']})",
                                        'Allowed DTs': ', '.join(agent_dts) if agent_dts else 'All DTs (No restrictions)',
                                        'Count': len(agent_dts) if agent_dts else len(available_dts)
                                    })
                                
                                if restrictions_summary:
                                    df_restrictions = pd.DataFrame(restrictions_summary)
                                    st.dataframe(df_restrictions, use_container_width=True, hide_index=True)

# -------------------------
# Supervisor Page
# -------------------------
def page_supervisor():
    require_roles(("Superuser", "Supervisor"))
    st.title("Supervisor Menu")
    
    # Initialize database connection for this page
    conn = get_db()
    
    # Monitoring first so it's the default view
    tabs = st.tabs(["Monitoring", "Payment Recap", "Input", "Trace Assigning", "Agent Assigning", "Trace Results", "Enriched & Lookup", "Freeze Manager", "Company Library", "Data Migration"])

    # --- Monitoring Tab ---
    with tabs[0]:

        # Quick KPI: total rows in system
        try:
            _total_rows_supervisor = (fetchone("SELECT COUNT(*) c FROM supervisor_data") or {}).get('c', 0)
        except Exception:
            _total_rows_supervisor = 0
        kpi_col = st.columns(4)
        with kpi_col[0]:
            st.metric("Total data di sistem", f"{_total_rows_supervisor:,}")

        # Advanced filters in expander
        # All additional fields except the four primary ones
        base_filter_fields = [
            "Lending_Entity", "Date", "Task_ID", "Gender", "Customer_Occupation", "DPD",
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

        # Row display limit control
        disp_c1, disp_c2 = st.columns([1,3])
        with disp_c1:
            rows_limit = st.number_input(
                "Jumlah baris ditampilkan",
                min_value=10,
                max_value=2000,
                value=100,
                step=50,
                key="monitor_limit_rows",
                help="Atur berapa banyak baris yang ditampilkan di tabel Monitoring (default 100)."
            )

        # Quick filters placed directly above the table: Case ID | Customer Name | Phone Number | Email
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            q_case_id = st.text_input("Case ID", key="monitor_case_id")
        with fc2:
            q_customer = st.text_input("Customer Name", key="monitor_customer_name")
        with fc3:
            q_phone = st.text_input("Phone Number", key="monitor_phone")
        with fc4:
            q_email = st.text_input("Email", key="monitor_email")

    # Build query
        query = "SELECT * FROM supervisor_data WHERE 1=1"
        params = []
        # Primary
        if q_case_id:
            query += " AND Case_ID LIKE ?"
            params.append(f"%{q_case_id}%")
        if q_customer:
            query += " AND Customer_name LIKE ?"
            params.append(f"%{q_customer}%")
        if q_phone:
            query += " AND (Phone_Number_1 LIKE ? OR Phone_Number_2 LIKE ?)"
            params.extend([f"%{q_phone}%", f"%{q_phone}%"])
        if q_email:
            query += " AND email LIKE ?"
            params.append(f"%{q_email}%")
        # Extras
        for f, v in extra_filters.items():
            if v:
                query += f" AND {f} LIKE ?"
                params.append(f"%{v}%")
        try:
            _lim = int(rows_limit) if rows_limit else 100
        except Exception:
            _lim = 100
        _lim = max(1, min(2000, _lim))
        query += f" ORDER BY id DESC LIMIT {_lim}"

        rows = fetchall(query, tuple(params))
        if not rows:
            st.info("Tidak ada data supervisor ditemukan.")
        else:
            df = pd.DataFrame(rows)
            
            # === ADD ASSIGNMENT STATUS COLUMNS ===
            # Add columns: "Status Assignment", "Currently Assigned To", "Assignment History"
            assignment_statuses = []
            for _, row in df.iterrows():
                case_id = str(row.get('Case_ID', ''))
                if not case_id:
                    assignment_statuses.append({
                        'Status_Assignment': '-',
                        'Currently_Assigned_To': '-',
                        'Assignment_History': '-'
                    })
                    continue
                
                # Get active assignment
                active = get_active_assignment(case_id)
                if active:
                    assign_type = active.get('assignment_type', 'agent').upper()
                    assigned_to = active.get('assigned_to', 'Unknown')
                    auto_return = active.get('auto_return_date', '')
                    
                    if assign_type == 'AGENT':
                        status = f"🎯 Agent Active (Return: {auto_return[:10] if auto_return else 'N/A'})"
                    else:
                        status = f"🔍 Tracer Active"
                    
                    # Get history
                    history = get_assignment_history(case_id)
                    history_str = ""
                    if history:
                        history_names = [f"{h.get('assigned_to', '?')} ({h.get('assignment_type', '?')})" 
                                       for h in history[:5]]  # Show last 5
                        history_str = " → ".join(history_names)
                    else:
                        history_str = "First assignment"
                    
                    assignment_statuses.append({
                        'Status_Assignment': status,
                        'Currently_Assigned_To': f"{assign_type}: {assigned_to}",
                        'Assignment_History': history_str
                    })
                else:
                    # Not assigned - check history
                    history = get_assignment_history(case_id)
                    if history:
                        history_names = [f"{h.get('assigned_to', '?')} ({h.get('assignment_type', '?')})" 
                                       for h in history[:5]]
                        history_str = " → ".join(history_names)
                        assignment_statuses.append({
                            'Status_Assignment': '📂 Available (Returned)',
                            'Currently_Assigned_To': '-',
                            'Assignment_History': history_str
                        })
                    else:
                        assignment_statuses.append({
                            'Status_Assignment': '📂 Available (Fresh)',
                            'Currently_Assigned_To': '-',
                            'Assignment_History': 'Never assigned'
                        })
            
            # Add status columns to dataframe
            status_df = pd.DataFrame(assignment_statuses)
            df = pd.concat([df, status_df], axis=1)
            
            # Optional small caption to indicate filtered vs total
        
            # Pastikan kolom id ada untuk identifikasi & hapus
            if 'id' not in df.columns:
                # Jika tidak ada, buat id sementara dari index (tidak digunakan untuk hapus DB)
                df.insert(0, 'id', pd.RangeIndex(start=1, stop=len(df)+1, step=1))

            select_all = st.checkbox("Pilih semua pada tabel ini", value=False, key="monitor_select_all")

            # Tambahkan kolom checkbox untuk memilih baris
            if 'Selected' not in df.columns:
                df.insert(0, 'Selected', select_all)
            else:
                df['Selected'] = df['Selected'].fillna(False) | bool(select_all)

            # Render editable table: hanya kolom 'Selected' yang bisa diubah
            try:
                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'Selected': st.column_config.CheckboxColumn(
                            label=' ', help='Centang untuk memilih baris', default=select_all
                        )
                    },
                    disabled=[c for c in df.columns if c != 'Selected'],
                    key='monitor_table_editor'
                )
            except Exception:
                # Fallback untuk versi Streamlit lama tanpa data_editor: tampilkan tabel biasa
                edited_df = df.copy()
                st.dataframe(df, use_container_width=True, hide_index=True)

            # Kumpulkan id yang dipilih
            try:
                selected_ids = [int(x) for x in edited_df.loc[edited_df['Selected'] == True, 'id'].tolist()]
            except Exception:
                selected_ids = []

            # --- Detail Contract Section ---
            # Tampilkan detail jika hanya 1 row yang dipilih
            if len(selected_ids) == 1:
                st.markdown("---")
                
                try:
                    detail_row = fetchone("SELECT * FROM supervisor_data WHERE id = ?", (selected_ids[0],))
                    
                    if detail_row:
                        # Get agent assignment info
                        agent_assign = fetchone("SELECT Agent_Assigned_To, assigned_at FROM agent_assignments WHERE Agreement_No = ? AND active = 1", (detail_row.get('Case_ID', ''),))
                        agent_name = agent_assign.get('Agent_Assigned_To', 'N/A') if agent_assign else 'N/A'
                        
                        # Top Info Card - Handling Agent Details
                        st.markdown(f"""
                        <div style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%); 
                                    padding: 20px; border-radius: 12px; color: white; margin-bottom: 20px;
                                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px;">
                                <div>
                                    <div style="font-size: 11px; opacity: 0.7; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Handling Agent</div>
                                    <div style="font-weight: 700; font-size: 16px;">{agent_name}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; opacity: 0.7; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Agent Phone Number</div>
                                    <div style="font-weight: 700; font-size: 16px;">-</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; opacity: 0.7; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Loan ID</div>
                                    <div style="font-weight: 700; font-size: 16px;">{detail_row.get('Case_ID', '#N/A')}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; opacity: 0.7; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Product</div>
                                    <div style="font-weight: 700; font-size: 16px;">{detail_row.get('Product', '#N/A')}</div>
                                </div>
                            </div>
                            <div style="margin-top: 12px;">
                                <div style="font-size: 11px; opacity: 0.7; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;">NIK</div>
                                <div style="font-weight: 700; font-size: 16px;">#N/A</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Header with styling - Here's your Contract Details
                        st.markdown(f"""
                        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                    padding: 30px; border-radius: 16px; color: white;">
                            <h2 style="margin: 0 0 10px 0; color: white;">Here's your Contract Details</h2>
                            <p style="margin: 0; font-style: italic; opacity: 0.9;">We are happy to help with any settlement scheme of your choosing!</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Helper function to display info rows with modern styling
                        def display_info_section(title, data_list, bg_header="#2D1810"):
                            """Display an info section with header and data rows"""
                            # Section header
                            if title:
                                st.markdown(f"""
                                <div style="background: {bg_header}; color: white; padding: 12px 20px; 
                                            font-weight: 700; font-size: 15px; border-radius: 10px 10px 0 0; 
                                            margin-top: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                    {title}
                                </div>
                                """, unsafe_allow_html=True)
                            
                            # Data rows
                            rows_html = '<div style="border: 1px solid #e5e7eb; border-radius: 0 0 10px 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.05);">'
                            
                            for idx, (label, value) in enumerate(data_list):
                                if label == "":
                                    continue
                                    
                                # Alternating row colors
                                bg_color = "#f9fafb" if idx % 2 == 0 else "#ffffff"
                                
                                # Style for #N/A values
                                if str(value) == "#N/A":
                                    value_style = "color: #ef4444; font-weight: 600;"
                                else:
                                    value_style = "color: #111827; font-weight: 400;"
                                
                                rows_html += f"""
                                <div style="display: flex; background: {bg_color}; padding: 12px 20px; 
                                            border-bottom: 1px solid #e5e7eb; transition: background 0.2s;">
                                    <div style="flex: 2; color: #374151; font-size: 14px; font-weight: 500;">{label}</div>
                                    <div style="flex: 3; {value_style} font-size: 14px; padding-left: 10px;">: {value}</div>
                                </div>
                                """
                            
                            rows_html += '</div>'
                            st.markdown(rows_html, unsafe_allow_html=True)
                        
                        # Main Info Section
                        main_data = [
                            ("Debtor Name", detail_row.get('Customer_name', '#N/A')),
                            ("PhoneNumber", detail_row.get('Phone_Number_1', '#N/A')),
                            ("Gender", detail_row.get('Gender', '#N/A')),
                            ("Legal Address", detail_row.get('Home_Address', '#N/A')),
                            ("DOB", detail_row.get('Date', '#N/A')),
                            ("Email", detail_row.get('email', '#N/A')),
                            ("Last known Office Name", detail_row.get('EMPLOYER', '#N/A') if 'EMPLOYER' in detail_row else '#N/A'),
                            ("Last Known Job Position", detail_row.get('Customer_Occupation', '#N/A')),
                            ("Last Known Work Phone", detail_row.get('Phone_Number_2', '#N/A')),
                            ("Debtor Phone Number II", detail_row.get('Phone_Number_2', '#N/A')),
                            ("Debtor Other Phone Number(s)", '#N/A'),
                            ("Date of Contract", detail_row.get('Assignment_Date', '#N/A')),
                            ("DPD", detail_row.get('DPD', '#N/A')),
                        ]
                        
                        # Render main section without header
                        st.markdown("""
                        <div style="border: 1px solid #e5e7eb; border-radius: 10px; overflow: hidden; 
                                    box-shadow: 0 2px 8px rgba(0,0,0,0.05); margin-top: 5px;">
                        """, unsafe_allow_html=True)
                        
                        rows_html = ""
                        for idx, (label, value) in enumerate(main_data):
                            if label == "":
                                continue
                            bg_color = "#f9fafb" if idx % 2 == 0 else "#ffffff"
                            value_style = "color: #ef4444; font-weight: 600;" if str(value) == "#N/A" else "color: #111827; font-weight: 400;"
                            
                            rows_html += f"""
                            <div style="display: flex; background: {bg_color}; padding: 12px 20px; 
                                        border-bottom: 1px solid #e5e7eb;">
                                <div style="flex: 2; color: #374151; font-size: 14px; font-weight: 500;">{label}</div>
                                <div style="flex: 3; {value_style} font-size: 14px; padding-left: 10px;">: {value}</div>
                            </div>
                            """
                        
                        st.markdown(rows_html + "</div>", unsafe_allow_html=True)
                        
                        # Additional sections in expander
                        with st.expander("📞 Contact Person Details"):
                            contacts = []
                            for i in range(1, 9):
                                contact_type = detail_row.get(f'Contact_Type_{i}', '')
                                contact_name = detail_row.get(f'Contact_Name_{i}', '')
                                contact_phone = detail_row.get(f'Contact_Phone_{i}', '')
                                
                                if contact_type or contact_name or contact_phone:
                                    contacts.append({
                                        'No': i,
                                        'Type': contact_type or '-',
                                        'Name': contact_name or '-',
                                        'Phone': contact_phone or '-'
                                    })
                            
                            if contacts:
                                st.dataframe(pd.DataFrame(contacts), use_container_width=True, hide_index=True)
                            else:
                                st.info("Tidak ada kontak tersimpan")
                        
                        with st.expander("💰 Financial Details"):
                            fin_col1, fin_col2 = st.columns(2)
                            with fin_col1:
                                st.markdown("**Outstanding Amount**")
                                st.text(f"Principle Outstanding: {detail_row.get('Principle_Outstanding', '#N/A')}")
                                st.text(f"Principal Overdue: {detail_row.get('Principal_Overdue_CURR', '#N/A')}")
                                st.text(f"Interest Overdue: {detail_row.get('Interest_Overdue_CURR', '#N/A')}")
                                st.text(f"Last Late Fee: {detail_row.get('Last_Late_Fee', '#N/A')}")
                            
                            with fin_col2:
                                st.markdown("**Third Party Debt**")
                                st.text(f"Total Debt: {detail_row.get('Total_debt_in_third_party', '#N/A')}")
                                st.text(f"Repayment: {detail_row.get('Repayment_on_third_Party', '#N/A')}")
                                st.text(f"Remaining: {detail_row.get('Remaining_Loan_on_third_Party', '#N/A')}")
                        
                        with st.expander("📍 Address Details"):
                            st.text(f"Province: {detail_row.get('Province', '#N/A')}")
                            st.text(f"City: {detail_row.get('City', '#N/A')}")
                            st.text(f"Street: {detail_row.get('Street', '#N/A')}")
                            st.text(f"Room Number: {detail_row.get('RoomNumber', '#N/A')}")
                            st.text(f"Postcode: {detail_row.get('Postcode', '#N/A')}")
                        
                        with st.expander("📝 Trace & Payment History"):
                            # Check latest trace result
                            latest_trace = fetchone("SELECT tracer, status, notes, touched_at FROM trace_results WHERE Agreement_No = ? ORDER BY id DESC LIMIT 1", (detail_row.get('Case_ID', ''),))
                            if latest_trace:
                                st.markdown("**Latest Trace**")
                                st.text(f"Tracer: {latest_trace.get('tracer', 'N/A')}")
                                st.text(f"Status: {latest_trace.get('status', 'N/A')}")
                                st.text(f"Notes: {latest_trace.get('notes', 'N/A')}")
                                st.text(f"Touched At: {latest_trace.get('touched_at', 'N/A')}")
                                st.markdown("---")
                            
                            # Check payment history
                            payments = fetchall("SELECT paid_amount, paid_date, status FROM payments WHERE Agreement_No = ? ORDER BY id DESC LIMIT 5", (detail_row.get('Case_ID', ''),))
                            if payments:
                                st.markdown("**Payment History (Latest 5)**")
                                payment_df = pd.DataFrame(payments)
                                st.dataframe(payment_df, use_container_width=True, hide_index=True)
                            else:
                                st.info("Belum ada riwayat pembayaran")
                    
                    else:
                        st.warning("Data detail tidak ditemukan.")
                        
                except Exception as e:
                    st.error(f"Error menampilkan detail: {e}")
                
                st.markdown("---")
            
            elif len(selected_ids) > 1:
                st.info(f"💡 Pilih hanya 1 row untuk melihat Detail Contract. Saat ini {len(selected_ids)} row terpilih.")

            cdel1, cdel2 = st.columns([1, 6])
            with cdel1:
                do_delete = st.button(
                    "Hapus yang dipilih",
                    type="primary",
                    help="Menghapus baris terpilih dari database"
                )
            with cdel2:
                if selected_ids:
                    st.warning(f"Akan menghapus {len(selected_ids)} baris.")
                else:
                    st.caption("Tidak ada baris yang dipilih.")

            if do_delete:
                if not selected_ids:
                    st.warning("Pilih minimal satu baris terlebih dahulu.")
                else:
                    try:
                        placeholders = ",".join(["?"] * len(selected_ids))
                        execute(f"DELETE FROM supervisor_data WHERE id IN ({placeholders})", tuple(selected_ids))
                        # Audit log
                        try:
                            u = current_user() or {}
                            sample_ids = selected_ids[:20]
                            more = "..." if len(selected_ids) > 20 else ""
                            execute(
                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                (u.get('id') if u else None, "DELETE_SUPERVISOR_ROWS", f"Deleted {len(selected_ids)} rows: {sample_ids}{more}")
                            )
                        except Exception:
                            pass
                        st.success(f"Berhasil menghapus {len(selected_ids)} baris.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal menghapus data: {e}")

            # Dangerous zone: delete ALL data
            st.markdown("---")
            with st.expander("Hapus SEMUA data (berbahaya)"):
                total_rows = 0
                try:
                    total_rows = (fetchone("SELECT COUNT(*) c FROM supervisor_data") or {}).get('c', 0)
                except Exception:
                    pass
                st.warning(f"Total baris saat ini di supervisor_data: {total_rows}")
                ack = st.checkbox("Saya paham tindakan ini permanen dan tidak dapat dibatalkan.", key="confirm_delete_all_ack")
                confirm_text = st.text_input("Ketik: HAPUS SEMUA", key="confirm_delete_all_text", placeholder="HAPUS SEMUA")
                btn_all = st.button("Hapus semua data supervisor_data", type="secondary", help="Menghapus semua baris pada tabel supervisor_data.")
                if btn_all:
                    if not ack or (confirm_text or '').strip().upper() != "HAPUS SEMUA":
                        st.error("Konfirmasi tidak valid. Centang persetujuan dan ketik persis: HAPUS SEMUA")
                    else:
                        try:
                            deleted_before = (fetchone("SELECT COUNT(*) c FROM supervisor_data") or {}).get('c', 0)
                            execute("DELETE FROM supervisor_data")
                            # Audit log
                            try:
                                u = current_user() or {}
                                execute(
                                    "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                    (u.get('id') if u else None, "DELETE_SUPERVISOR_ALL", f"Deleted all rows from supervisor_data (approx {deleted_before} rows)")
                                )
                            except Exception:
                                pass
                            st.success(f"Berhasil menghapus semua data (±{deleted_before} baris).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Gagal menghapus semua data: {e}")
        
        # Enriched Monitoring & Lookup NIK dipindahkan ke tab khusus "Enriched & Lookup"

    # --- Payment Recap Tab ---
    with tabs[1]:
        st.subheader("📊 Payment Recap")
        
        # Summary Metrics
        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        
        # Get payment recap data
        recap_query = """
            SELECT 
                COALESCE(sd.Case_ID, p.Agreement_No) AS Case_ID,
                sd.Product,
                p.paid_date AS Date,
                sd.Customer_name,
                p.paid_amount AS Savings,
                ar.agent_status AS Skema_Pelunasan,
                COALESCE(sd.Paid_Off, 'NO') AS PAID_OFF,
                COALESCE(ar.agent, p.uploaded_by) AS Agent,
                strftime('%b, %Y', p.paid_date) AS Month,
                'N/A' AS Case_Batch
            FROM payments p
            LEFT JOIN supervisor_data sd 
                ON sd.Case_ID = p.Agreement_No 
                OR sd.Virtual_Account_Number = p.Agreement_No
                OR sd.Third_Uid = p.Agreement_No
            LEFT JOIN agent_results ar 
                ON ar.Agreement_No = p.Agreement_No
                AND ar.approval_status = 'approved'
            WHERE p.status = 'approved'
            ORDER BY p.paid_date DESC
        """
        
        recap_df = pd.read_sql_query(recap_query, conn)
        
        if not recap_df.empty:
            # Calculate metrics
            total_savings = recap_df['Savings'].sum()
            total_cases = recap_df['Case_ID'].nunique()
            total_agents = recap_df['Agent'].nunique()
            paid_off_count = len(recap_df[recap_df['PAID_OFF'] == 'YES'])
            
            with col_m1:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                            padding: 20px; border-radius: 12px; color: white; text-align: center;
                            box-shadow: 0 8px 24px rgba(102,126,234,0.35);">
                    <div style="font-size: 28px; font-weight: 900; margin-bottom: 5px;">
                        Rp {total_savings:,.0f}
                    </div>
                    <div style="font-size: 11px; opacity: 0.95; font-weight: 600;">
                        Total Savings
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            with col_m2:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                            padding: 20px; border-radius: 12px; color: white; text-align: center;
                            box-shadow: 0 8px 24px rgba(240,147,251,0.35);">
                    <div style="font-size: 28px; font-weight: 900; margin-bottom: 5px;">
                        {total_cases}
                    </div>
                    <div style="font-size: 11px; opacity: 0.95; font-weight: 600;">
                        Total Cases
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            with col_m3:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                            padding: 20px; border-radius: 12px; color: white; text-align: center;
                            box-shadow: 0 8px 24px rgba(79,172,254,0.35);">
                    <div style="font-size: 28px; font-weight: 900; margin-bottom: 5px;">
                        {total_agents}
                    </div>
                    <div style="font-size: 11px; opacity: 0.95; font-weight: 600;">
                        Active Agents
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            with col_m4:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%); 
                            padding: 20px; border-radius: 12px; color: white; text-align: center;
                            box-shadow: 0 8px 24px rgba(67,233,123,0.35);">
                    <div style="font-size: 28px; font-weight: 900; margin-bottom: 5px;">
                        {paid_off_count}
                    </div>
                    <div style="font-size: 11px; opacity: 0.95; font-weight: 600;">
                        Paid Off Cases
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            
            # ========== PAYMENT RECAP RESUME (MINIMALIST & MODERN) ==========
            st.markdown("### 📈 Payment Recap Resume")
            
            # Prepare data for resume
            recap_df['Date_parsed'] = pd.to_datetime(recap_df['Date'], errors='coerce')
            recap_df_sorted = recap_df.dropna(subset=['Date_parsed']).sort_values('Date_parsed', ascending=False)
            
            # Container for resume sections
            resume_col1, resume_col2 = st.columns([1, 1])
            
            with resume_col1:
                # --- Last Payment Recorded ---
                st.markdown("""
                <div style="background: linear-gradient(135deg, rgba(255,255,255,0.5) 0%, rgba(255,255,255,0.3) 100%);
                            backdrop-filter: blur(15px); border: 1px solid rgba(255,255,255,0.3);
                            border-radius: 14px; padding: 18px; margin-bottom: 16px;
                            box-shadow: 0 4px 16px rgba(0,0,0,0.08);">
                    <div style="font-size: 13px; font-weight: 700; color: #6366F1; margin-bottom: 12px; 
                                display: flex; align-items: center;">
                        <span style="font-size: 18px; margin-right: 8px;">📅</span>
                        Last Payment Recorded
                    </div>
                """, unsafe_allow_html=True)
                
                if not recap_df_sorted.empty:
                    last_payment = recap_df_sorted.iloc[0]
                    last_date = last_payment['Date_parsed'].strftime('%b %d, %Y')
                    last_amount = last_payment['Savings']
                    
                    # Compare To (previous month)
                    current_month = recap_df_sorted.iloc[0]['Date_parsed'].to_period('M')
                    prev_month = current_month - 1
                    
                    current_month_data = recap_df_sorted[recap_df_sorted['Date_parsed'].dt.to_period('M') == current_month]
                    prev_month_data = recap_df_sorted[recap_df_sorted['Date_parsed'].dt.to_period('M') == prev_month]
                    
                    current_total = current_month_data['Savings'].sum()
                    prev_total = prev_month_data['Savings'].sum() if not prev_month_data.empty else 0
                    
                    trend = "BETTER" if current_total > prev_total else "LOWER"
                    trend_color = "#10B981" if trend == "BETTER" else "#EF4444"
                    trend_icon = "📈" if trend == "BETTER" else "📉"
                    
                    st.markdown(f"""
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                        <div style="font-size: 11px; color: #6B7280; font-weight: 600;">Date</div>
                        <div style="font-size: 13px; color: #1F2937; font-weight: 700;">{last_date}</div>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                        <div style="font-size: 11px; color: #6B7280; font-weight: 600;">SUM of Amount</div>
                        <div style="font-size: 14px; color: #1F2937; font-weight: 800;">Rp {last_amount:,.0f}</div>
                    </div>
                    <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(0,0,0,0.08);">
                        <div style="font-size: 11px; color: #6B7280; font-weight: 600; margin-bottom: 6px;">Compare To</div>
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div style="font-size: 12px; color: #4B5563;">{prev_month.strftime('%b, %Y')}</div>
                            <div style="font-size: 12px; font-weight: 700; color: {trend_color}; 
                                        background: rgba({trend_color[1:]}, 0.1); padding: 4px 10px; 
                                        border-radius: 6px;">{trend_icon} {trend}</div>
                        </div>
                        <div style="font-size: 13px; color: #1F2937; font-weight: 700; margin-top: 6px; text-align: right;">
                            Rp {current_total:,.0f}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; color: #9CA3AF; font-size: 12px; padding: 20px;">
                        No payment data available
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
                
                # --- Product Summary (Cases & Savings per Product) ---
                st.markdown("""
                <div style="background: linear-gradient(135deg, rgba(255,255,255,0.5) 0%, rgba(255,255,255,0.3) 100%);
                            backdrop-filter: blur(15px); border: 1px solid rgba(255,255,255,0.3);
                            border-radius: 14px; padding: 18px; margin-bottom: 16px;
                            box-shadow: 0 4px 16px rgba(0,0,0,0.08);">
                    <div style="font-size: 13px; font-weight: 700; color: #6366F1; margin-bottom: 12px;
                                display: flex; align-items: center;">
                        <span style="font-size: 18px; margin-right: 8px;">📦</span>
                        Product Summary
                    </div>
                """, unsafe_allow_html=True)
                
                product_summary = recap_df.groupby('Product').agg({
                    'Case_ID': 'nunique',
                    'Savings': 'sum'
                }).reset_index()
                product_summary.columns = ['Product', 'Cases', 'Savings']
                product_summary = product_summary.sort_values('Savings', ascending=False).head(5)
                
                if not product_summary.empty:
                    for idx, row in product_summary.iterrows():
                        product_name = row['Product'] if pd.notna(row['Product']) else 'Unknown'
                        cases_count = int(row['Cases'])
                        savings_amount = row['Savings']
                        
                        st.markdown(f"""
                        <div style="display: flex; justify-content: space-between; align-items: center; 
                                    padding: 8px 0; border-bottom: 1px solid rgba(0,0,0,0.05);">
                            <div>
                                <div style="font-size: 12px; color: #1F2937; font-weight: 700;">{product_name}</div>
                                <div style="font-size: 10px; color: #9CA3AF;">{cases_count} cases</div>
                            </div>
                            <div style="font-size: 12px; color: #1F2937; font-weight: 700; text-align: right;">
                                Rp {savings_amount:,.0f}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Grand Total
                    grand_total = product_summary['Savings'].sum()
                    total_cases = product_summary['Cases'].sum()
                    st.markdown(f"""
                    <div style="margin-top: 12px; padding-top: 12px; border-top: 2px solid rgba(99,102,241,0.3);">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <div style="font-size: 13px; color: #6366F1; font-weight: 800;">Grand Total</div>
                                <div style="font-size: 10px; color: #9CA3AF;">{total_cases} cases</div>
                            </div>
                            <div style="font-size: 14px; color: #6366F1; font-weight: 900;">
                                Rp {grand_total:,.0f}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; color: #9CA3AF; font-size: 12px; padding: 20px;">
                        No product data
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
            
            with resume_col2:
                # --- Month Summary (Date & SUM of Amount by Month) ---
                st.markdown("""
                <div style="background: linear-gradient(135deg, rgba(255,255,255,0.5) 0%, rgba(255,255,255,0.3) 100%);
                            backdrop-filter: blur(15px); border: 1px solid rgba(255,255,255,0.3);
                            border-radius: 14px; padding: 18px; margin-bottom: 16px;
                            box-shadow: 0 4px 16px rgba(0,0,0,0.08);">
                    <div style="font-size: 13px; font-weight: 700; color: #6366F1; margin-bottom: 12px;
                                display: flex; align-items: center;">
                        <span style="font-size: 18px; margin-right: 8px;">📊</span>
                        Month Summary
                    </div>
                """, unsafe_allow_html=True)
                
                month_summary = recap_df.groupby('Month').agg({
                    'Savings': 'sum'
                }).reset_index()
                month_summary.columns = ['Month', 'Amount']
                # Sort by date (convert Month back to datetime for sorting)
                month_summary['Sort_Date'] = pd.to_datetime(month_summary['Month'], format='%b, %Y', errors='coerce')
                month_summary = month_summary.sort_values('Sort_Date', ascending=False).head(10)
                
                if not month_summary.empty:
                    for idx, row in month_summary.iterrows():
                        month_name = row['Month']
                        amount = row['Amount']
                        
                        st.markdown(f"""
                        <div style="display: flex; justify-content: space-between; align-items: center;
                                    padding: 8px 0; border-bottom: 1px solid rgba(0,0,0,0.05);">
                            <div style="font-size: 12px; color: #4B5563; font-weight: 600;">{month_name}</div>
                            <div style="font-size: 12px; color: #1F2937; font-weight: 700;">Rp {amount:,.0f}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Grand Total
                    grand_total_month = month_summary['Amount'].sum()
                    st.markdown(f"""
                    <div style="margin-top: 12px; padding-top: 12px; border-top: 2px solid rgba(99,102,241,0.3);">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div style="font-size: 13px; color: #6366F1; font-weight: 800;">Grand Total</div>
                            <div style="font-size: 14px; color: #6366F1; font-weight: 900;">Rp {grand_total_month:,.0f}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; color: #9CA3AF; font-size: 12px; padding: 20px;">
                        No monthly data
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
                
                # --- Team Averages (Agent Performance Ranking) ---
                st.markdown("""
                <div style="background: linear-gradient(135deg, rgba(255,255,255,0.5) 0%, rgba(255,255,255,0.3) 100%);
                            backdrop-filter: blur(15px); border: 1px solid rgba(255,255,255,0.3);
                            border-radius: 14px; padding: 18px; margin-bottom: 16px;
                            box-shadow: 0 4px 16px rgba(0,0,0,0.08);">
                    <div style="font-size: 13px; font-weight: 700; color: #6366F1; margin-bottom: 12px;
                                display: flex; align-items: center;">
                        <span style="font-size: 18px; margin-right: 8px;">👥</span>
                        Team Averages
                    </div>
                """, unsafe_allow_html=True)
                
                agent_summary = recap_df.groupby('Agent').agg({
                    'Savings': 'sum',
                    'Case_ID': 'nunique'
                }).reset_index()
                agent_summary.columns = ['Agent', 'Total_Savings', 'Paid_Cases']
                agent_summary = agent_summary.sort_values('Total_Savings', ascending=False).head(10)
                
                if not agent_summary.empty:
                    team_avg = agent_summary['Total_Savings'].mean()
                    
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                                padding: 12px; border-radius: 10px; margin-bottom: 12px; text-align: center;">
                        <div style="font-size: 11px; color: rgba(255,255,255,0.9); font-weight: 600;">Team Average</div>
                        <div style="font-size: 18px; color: white; font-weight: 900;">Rp {team_avg:,.0f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Top Agents
                    for idx, row in agent_summary.head(5).iterrows():
                        agent_name = row['Agent'] if pd.notna(row['Agent']) else 'Unknown'
                        total_savings = row['Total_Savings']
                        paid_cases = int(row['Paid_Cases'])
                        
                        # Color coding for top performers
                        if idx == 0:
                            bg_color = "linear-gradient(135deg, rgba(255,215,0,0.15) 0%, rgba(255,215,0,0.05) 100%)"
                            rank_icon = "🥇"
                        elif idx == 1:
                            bg_color = "linear-gradient(135deg, rgba(192,192,192,0.15) 0%, rgba(192,192,192,0.05) 100%)"
                            rank_icon = "🥈"
                        elif idx == 2:
                            bg_color = "linear-gradient(135deg, rgba(205,127,50,0.15) 0%, rgba(205,127,50,0.05) 100%)"
                            rank_icon = "🥉"
                        else:
                            bg_color = "rgba(0,0,0,0.02)"
                            rank_icon = "•"
                        
                        st.markdown(f"""
                        <div style="background: {bg_color}; padding: 10px; border-radius: 8px; margin-bottom: 8px;
                                    border: 1px solid rgba(0,0,0,0.05);">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div style="display: flex; align-items: center; gap: 8px;">
                                    <span style="font-size: 16px;">{rank_icon}</span>
                                    <div>
                                        <div style="font-size: 12px; color: #1F2937; font-weight: 700;">{agent_name}</div>
                                        <div style="font-size: 10px; color: #9CA3AF;">{paid_cases} paid cases</div>
                                    </div>
                                </div>
                                <div style="font-size: 12px; color: #1F2937; font-weight: 800;">
                                    Rp {total_savings:,.0f}
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Grand Total
                    grand_total_agent = agent_summary['Total_Savings'].sum()
                    total_paid_cases = agent_summary['Paid_Cases'].sum()
                    st.markdown(f"""
                    <div style="margin-top: 12px; padding-top: 12px; border-top: 2px solid rgba(99,102,241,0.3);">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <div style="font-size: 13px; color: #6366F1; font-weight: 800;">Grand Total</div>
                                <div style="font-size: 10px; color: #9CA3AF;">{total_paid_cases} cases</div>
                            </div>
                            <div style="font-size: 14px; color: #6366F1; font-weight: 900;">
                                Rp {grand_total_agent:,.0f}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="text-align: center; color: #9CA3AF; font-size: 12px; padding: 20px;">
                        No agent data
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
            
            # Filters
            st.markdown("### 🔍 Filters")
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            
            with col_f1:
                # Date range filter
                if not recap_df['Date'].isna().all():
                    min_date = pd.to_datetime(recap_df['Date'].min()).date()
                    max_date = pd.to_datetime(recap_df['Date'].max()).date()
                    date_range = st.date_input(
                        "Date Range",
                        value=(min_date, max_date),
                        min_value=min_date,
                        max_value=max_date,
                        key="recap_date_range"
                    )
                else:
                    date_range = None
            
            with col_f2:
                # Agent filter
                agents_list = ['All'] + sorted(recap_df['Agent'].dropna().unique().tolist())
                selected_agent = st.selectbox("Agent", agents_list, key="recap_agent")
            
            with col_f3:
                # Case ID search
                search_case = st.text_input("Search Case ID", key="recap_case_search")
            
            with col_f4:
                # Paid Off filter
                paid_off_filter = st.selectbox(
                    "Paid Off Status", 
                    ['All', 'YES', 'NO'],
                    key="recap_paid_off"
                )
            
            # Apply filters
            filtered_df = recap_df.copy()
            
            if date_range and len(date_range) == 2:
                filtered_df['Date_parsed'] = pd.to_datetime(filtered_df['Date'])
                filtered_df = filtered_df[
                    (filtered_df['Date_parsed'].dt.date >= date_range[0]) &
                    (filtered_df['Date_parsed'].dt.date <= date_range[1])
                ]
                filtered_df = filtered_df.drop(columns=['Date_parsed'])
            
            if selected_agent != 'All':
                filtered_df = filtered_df[filtered_df['Agent'] == selected_agent]
            
            if search_case:
                filtered_df = filtered_df[
                    filtered_df['Case_ID'].str.contains(search_case, case=False, na=False)
                ]
            
            if paid_off_filter != 'All':
                filtered_df = filtered_df[filtered_df['PAID_OFF'] == paid_off_filter]
            
            st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)
            
            # Add Compare Helper column if not exists
            if 'Compare_Helper' not in filtered_df.columns:
                filtered_df['Compare_Helper'] = 'NO'
            
            # Display data with editable Compare Helper
            st.markdown(f"### 📋 Payment Records ({len(filtered_df)} records)")
            
            # Group by Case_ID and aggregate
            if not filtered_df.empty:
                agg_df = filtered_df.groupby('Case_ID').agg({
                    'Product': 'first',
                    'Date': 'max',
                    'Customer_name': 'first',
                    'Savings': 'sum',
                    'Skema_Pelunasan': lambda x: ', '.join(x.dropna().unique()),
                    'PAID_OFF': 'first',
                    'Agent': 'first',
                    'Month': 'first',
                    'Case_Batch': 'first',
                    'Compare_Helper': 'first'
                }).reset_index()
                
                # Reorder columns
                column_order = [
                    'Case_ID', 'Product', 'Date', 'Customer_name', 
                    'Savings', 'Skema_Pelunasan', 'PAID_OFF', 
                    'Agent', 'Month', 'Case_Batch', 'Compare_Helper'
                ]
                agg_df = agg_df[column_order]
                
                # Format Savings column
                agg_df['Savings'] = agg_df['Savings'].apply(lambda x: f"Rp {x:,.0f}")
                
                # Display dataframe
                st.dataframe(
                    agg_df,
                    use_container_width=True,
                    height=500
                )
                
                # Export to Excel
                st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)
                
                # Convert to Excel
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    # Prepare export dataframe (remove formatting)
                    export_df = filtered_df.copy()
                    export_df = export_df.groupby('Case_ID').agg({
                        'Product': 'first',
                        'Date': 'max',
                        'Customer_name': 'first',
                        'Savings': 'sum',
                        'Skema_Pelunasan': lambda x: ', '.join(x.dropna().unique()),
                        'PAID_OFF': 'first',
                        'Agent': 'first',
                        'Month': 'first',
                        'Case_Batch': 'first',
                        'Compare_Helper': 'first'
                    }).reset_index()
                    export_df = export_df[column_order]
                    export_df.to_excel(writer, index=False, sheet_name='Payment Recap')
                output.seek(0)
                
                st.download_button(
                    label="📥 Download Excel",
                    data=output,
                    file_name=f"payment_recap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("No payment records found with the selected filters.")
        else:
            st.info("No payment data available yet.")

    # --- Input Tab ---
    with tabs[2]:
        st.subheader("Upload Excel/CSV Supervisor Data")
        field_names = [
            "DT", "Lending_Entity", "Date", "Case_ID", "Task_ID", "Customer_name", "email", "Gender", "Customer_Occupation", "DPD", "Principle_Outstanding", "Principal_Overdue_CURR", "Interest_Overdue_CURR", "Last_Late_Fee", "Return_Date", "Detail", "Loan_Type", "Third_Uid", "Product", "Home_Address", "Province", "City", "Street", "RoomNumber", "Postcode", "Assignment_Date", "Withdrawal_Date", "Phone_Number_1", "Phone_Number_2", "Contact_Type_1", "Contact_Name_1", "Contact_Phone_1", "Contact_Type_2", "Contact_Name_2", "Contact_Phone_2", "Contact_Type_3", "Contact_Name_3", "Contact_Phone_3", "Contact_Type_4", "Contact_Name_4", "Contact_Phone_4", "Contact_Type_5", "Contact_Name_5", "Contact_Phone_5", "Contact_Type_6", "Contact_Name_6", "Contact_Phone_6", "Contact_Type_7", "Contact_Name_7", "Contact_Phone_7", "Contact_Type_8", "Contact_Name_8", "Contact_Phone_8", "Total_debt_in_third_party", "Repayment_on_third_Party", "Remaining_Loan_on_third_Party", "Virtual_Account_Number",
            # Newly added meta fields required by user
            "NIK_KTP", "EMPLOYMENT_UPDATE", "EMPLOYER", "Debtor_Legal_Name", "Employee_Name", "Employee_ID_Number", "Debtor_Relation_to_Employee",
            # Agent-updated fields
            "STATUS", "REGISTERED_PHONE", "Additional_Contacts", "Remarks_Suggested_NIK_Prospect", "Payment", "Paid_Off_Status"
        ]
        
        # Download Template Button for Supervisor Data
        st.markdown("#### 📥 Download Template")
        st.info("💡 Download template Excel dengan semua kolom yang diperlukan. Isi data Anda sesuai format template.")
        
        # Create template DataFrame with all field names as columns
        template_supervisor_df = pd.DataFrame(columns=field_names)
        # Add sample rows for guidance
        sample_data = {
            field_names[0]: ['Sample data - replace with your actual data'],
            **{col: [''] for col in field_names[1:]}
        }
        template_supervisor_df = pd.DataFrame(sample_data)
        
        # Convert to Excel
        template_sup_buffer = io.BytesIO()
        with pd.ExcelWriter(template_sup_buffer, engine='openpyxl') as writer:
            template_supervisor_df.to_excel(writer, index=False, sheet_name='Supervisor Data')
            # Add instructions sheet
            instructions_df = pd.DataFrame({
                'Instructions': [
                    '1. Isi data Anda pada sheet "Supervisor Data"',
                    '2. JANGAN ubah nama kolom (header)',
                    '3. Case_ID wajib diisi dan unik',
                    '4. Kolom opsional boleh dikosongkan',
                    '5. Upload file ini setelah diisi',
                    '',
                    'Kolom WAJIB:',
                    '- Case_ID',
                    '- Customer_name',
                    '- Virtual_Account_Number',
                    '',
                    'Kolom OPSIONAL (boleh kosong):',
                    '- NIK_KTP, EMPLOYMENT_UPDATE, EMPLOYER',
                    '- Debtor_Legal_Name, Employee_Name, Employee_ID_Number',
                    '- Debtor_Relation_to_Employee',
                    '- STATUS, REGISTERED_PHONE, Additional_Contacts',
                    '- Remarks_Suggested_NIK_Prospect, Payment, Paid_Off_Status'
                ]
            })
            instructions_df.to_excel(writer, index=False, sheet_name='Instructions')
        template_sup_buffer.seek(0)
        
        col_btn1, col_btn2 = st.columns([1, 3])
        with col_btn1:
            st.download_button(
                label="📥 Download Template Excel",
                data=template_sup_buffer,
                file_name="supervisor_data_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Download template Excel untuk upload Supervisor Data",
                type="primary"
            )
        
        st.markdown("---")
        
        # Tampilkan pesan hasil upload sebelumnya (sekali tampil)
        _upload_result_msg = st.session_state.pop('sup_upload_result', None)
        if _upload_result_msg:
            st.success(_upload_result_msg)

        uploaded = st.file_uploader("Upload file Excel/CSV", type=["csv", "xlsx"], key="sup_upload_file")
        if uploaded is not None:
            # Step 1: Parse and preview ONLY (no insert yet)
            try:
                if uploaded.name.lower().endswith(".csv"):
                    df_preview = pd.read_csv(uploaded)
                else:
                    df_preview = pd.read_excel(uploaded)

                # Normalize columns to expected headers (preview uses same logic)
                def _norm_col(s: str) -> str:
                    if s is None:
                        return ""
                    s = str(s).replace("\ufeff", "").strip()
                    s = re.sub(r"\s+", " ", s)
                    s = s.replace(" ", "_")
                    return s.lower()

                typo_map = { _norm_col("Repayment_on_thrid_Party"): _norm_col("Repayment_on_third_Party") }
                expected_map = { _norm_col(k): k for k in field_names }
                new_cols = []
                for c in df_preview.columns:
                    nc = _norm_col(c)
                    if nc in typo_map:
                        nc = typo_map[nc]
                    new_cols.append(expected_map.get(nc, c))
                df_preview.columns = new_cols

                # Columns that may be missing and should be treated as optional (auto-filled as empty strings)
                optional_fill_cols = [
                    'NIK_KTP', 'EMPLOYMENT_UPDATE', 'EMPLOYER',
                    'Debtor_Legal_Name', 'Employee_Name', 'Employee_ID_Number', 'Debtor_Relation_to_Employee',
                    # Agent-updated fields treated as optional on upload
                    'STATUS', 'REGISTERED_PHONE', 'Additional_Contacts', 'Remarks_Suggested_NIK_Prospect', 'Payment', 'Paid_Off_Status'
                ]
                missing = [f for f in field_names if f not in df_preview.columns]
                # Split missing columns into optional and required
                missing_optional = [c for c in missing if c in optional_fill_cols]
                missing_required = [c for c in missing if c not in optional_fill_cols]
                # If only optional are missing, add them with empty strings
                if missing_optional:
                    try:
                        for col in missing_optional:
                            df_preview[col] = ""
                    except Exception:
                        pass
                    # Reorder columns so preview roughly matches field_names order for clarity
                    try:
                        df_preview = df_preview[[*(c for c in field_names if c in df_preview.columns), *[c for c in df_preview.columns if c not in field_names]]]
                    except Exception:
                        pass
                    st.info(f"Kolom opsional tidak ditemukan dan akan diisi kosong: {missing_optional}")
                if missing_required:
                    st.error(f"Kolom berikut tidak ditemukan di file: {missing_required}")
                    st.caption("Tips: header akan dicocokkan tanpa spasi/kapital dan perbaikan typo umum (thrid->third). Pastikan nama kolom sesuai template.")
                    st.button("Clear file", on_click=lambda: (st.session_state.pop('sup_upload_file', None), st.rerun()))
                else:
                    total_rows_file = len(df_preview)
                    st.info(f"File terdeteksi: {uploaded.name} — {total_rows_file:,} baris. Ini baru preview, data BELUM masuk ke sistem.")
                    # Show 5 random rows (or all if <5)
                    try:
                        sample_df = df_preview.sample(n=min(5, len(df_preview)), random_state=42)
                    except Exception:
                        sample_df = df_preview.head(min(5, len(df_preview)))
                    st.dataframe(sample_df, use_container_width=True, hide_index=True)

                    # Action buttons
                    b1, b2 = st.columns([1,1])
                    with b1:
                        do_commit = st.button("Upload ke sistem", type="primary", key="btn_commit_supervisor")
                    with b2:
                        st.button(
                            "Batalkan & Clear",
                            key="btn_clear_supervisor",
                            on_click=lambda: (st.session_state.__setitem__('sup_upload_file', None), st.rerun())
                        )

                    if do_commit:
                        # Re-read from the uploader because stream was consumed above
                        try:
                            uploaded.seek(0)
                        except Exception:
                            pass
                        try:
                            if uploaded.name.lower().endswith(".csv"):
                                df_full = pd.read_csv(uploaded)
                            else:
                                df_full = pd.read_excel(uploaded)
                        except Exception as e:
                            st.error(f"Gagal membaca file saat upload: {e}")
                            df_full = None

                        if df_full is not None:
                            # Apply same header normalization to full DF
                            new_cols2 = []
                            for c in df_full.columns:
                                nc = _norm_col(str(c))
                                if nc in typo_map:
                                    nc = typo_map[nc]
                                new_cols2.append(expected_map.get(nc, c))
                            df_full.columns = new_cols2

                            # Handle missing columns for full upload with the same optional logic
                            optional_fill_cols = [
                                'NIK_KTP', 'EMPLOYMENT_UPDATE', 'EMPLOYER',
                                'Debtor_Legal_Name', 'Employee_Name', 'Employee_ID_Number', 'Debtor_Relation_to_Employee',
                                'STATUS', 'REGISTERED_PHONE', 'Additional_Contacts', 'Remarks_Suggested_NIK_Prospect', 'Payment', 'Paid_Off_Status'
                            ]
                            miss2_all = [f for f in field_names if f not in df_full.columns]
                            miss2_optional = [c for c in miss2_all if c in optional_fill_cols]
                            miss2_required = [c for c in miss2_all if c not in optional_fill_cols]
                            if miss2_optional:
                                try:
                                    for col in miss2_optional:
                                        df_full[col] = ""
                                except Exception:
                                    pass
                                try:
                                    df_full = df_full[[*(c for c in field_names if c in df_full.columns), *[c for c in df_full.columns if c not in field_names]]]
                                except Exception:
                                    pass
                            if miss2_required:
                                st.error(f"Kolom wajib hilang saat upload: {miss2_required}")
                            else:
                                # Helper to coerce values
                                def _to_sql_value(v):
                                    try:
                                        import pandas as _pd
                                    except Exception:
                                        _pd = None
                                    try:
                                        if _pd is not None and (_pd.isna(v) if not isinstance(v, str) else False):
                                            return None
                                    except Exception:
                                        pass
                                    try:
                                        if _pd is not None and isinstance(v, _pd.Timestamp):
                                            return v.to_pydatetime().isoformat(sep=' ')
                                    except Exception:
                                        pass
                                    from datetime import datetime as _dt, date as _d
                                    if isinstance(v, _dt):
                                        return v.isoformat(sep=' ')
                                    if isinstance(v, _d):
                                        return v.isoformat()
                                    try:
                                        if hasattr(v, 'item'):
                                            return v.item()
                                    except Exception:
                                        pass
                                    return v

                                placeholders = ','.join(['?' for _ in field_names])
                                saved = 0
                                skipped = 0
                                replaced = 0
                                
                                # Create progress tracking UI
                                total_rows = len(df_full)
                                st.info(f"🚀 Memulai upload {total_rows:,} baris data...")
                                progress_bar = st.progress(0)
                                status_text = st.empty()
                                
                                # Process rows with progress updates
                                for idx, row in enumerate(df_full.iterrows(), 1):
                                    _, row = row  # unpack tuple (index, row)
                                    try:
                                        # Replace-by-Case_ID: if Case_ID already exists, delete then insert
                                        case_id_raw = row.get('Case_ID') if isinstance(row, dict) else None
                                        try:
                                            # fallback for Series
                                            if case_id_raw is None:
                                                case_id_raw = row['Case_ID']
                                        except Exception:
                                            pass
                                        case_id = str(case_id_raw).strip() if case_id_raw is not None and str(case_id_raw).strip() != '' else None

                                        if case_id:
                                            try:
                                                exists = (fetchone("SELECT COUNT(*) c FROM supervisor_data WHERE Case_ID=?", (case_id,)) or {}).get('c', 0)
                                            except Exception:
                                                exists = 0
                                            if exists and exists > 0:
                                                try:
                                                    execute("DELETE FROM supervisor_data WHERE Case_ID=?", (case_id,))
                                                except Exception:
                                                    pass
                                                replaced += 1

                                        vals = [_to_sql_value(row.get(f)) for f in field_names]
                                        execute(
                                            f"INSERT INTO supervisor_data ({','.join(field_names)}) VALUES ({placeholders})",
                                            tuple(vals)
                                        )
                                        saved += 1
                                    except Exception as e:
                                        skipped += 1
                                    
                                    # Update progress every 10 rows or on last row
                                    if idx % 10 == 0 or idx == total_rows:
                                        progress = idx / total_rows
                                        progress_bar.progress(progress)
                                        status_text.text(f"📊 Progress: {idx:,}/{total_rows:,} baris | ✅ Tersimpan: {saved:,} | 🔄 Replace: {replaced:,} | ⚠️ Dilewati: {skipped:,}")
                                
                                # Clear progress UI
                                progress_bar.empty()
                                status_text.empty()
                                # Simpan pesan hasil agar tampil sekali setelah rerun
                                st.session_state['sup_upload_result'] = f"Upload selesai. Disimpan baru: {saved:,}. Replace: {replaced:,}. Dilewati: {skipped:,}."
                                # Audit log
                                u = current_user() or {}
                                try:
                                    execute(
                                        "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                        (u.get('id') if u else None, "UPLOAD_SUPERVISOR", f"Uploaded supervisor data: new {saved}, replaced {replaced}, skipped {skipped} from '{uploaded.name}'")
                                    )
                                except Exception:
                                    pass
                                # Immediate user feedback before rerun
                                try:
                                    st.toast(f"Upload berhasil: baru {saved:,}, replace {replaced:,}, dilewati {skipped:,}.", icon="✅")
                                except Exception:
                                    pass
                                # Rerun without modifying widget state (Streamlit will clear the uploader automatically)
                                st.rerun()
            except Exception as e:
                st.error(f"Gagal memproses file: {e}")

        st.markdown("---")
        

    # --- Trace Assigning Tab ---
    with tabs[3]:
        st.subheader("Assign ke Tracer")
        q1, q2, q3, q4 = st.columns([1.2, 1.2, 1.2, 0.6])
        with q1:
            f_case = st.text_input("Filter Case_ID", key="ta_f_case")
        with q2:
            f_name = st.text_input("Filter Customer", key="ta_f_name")
        with q3:
            f_phone = st.text_input("Filter Phone", key="ta_f_phone")
        with q4:
            limit_rows = st.number_input("Limit Row", min_value=10, max_value=2000, value=200, step=10, key="ta_limit")

    # Build SQL with filters + exclude yang sudah di-assign ke Agent
        where = ["Case_ID IS NOT NULL", "TRIM(Case_ID)<>''"]
        # VALIDASI: Exclude Case_ID yang sudah di-assign ke Agent
        where.append("Case_ID NOT IN (SELECT Agreement_No FROM agent_assignments WHERE IFNULL(active,1)=1)")
        params = []
        if f_case:
            where.append("Case_ID LIKE ?")
            params.append(f"%{f_case.strip()}%")
        if f_name:
            where.append("Customer_name LIKE ?")
            params.append(f"%{f_name.strip()}%")
        # Removed Third_Uid filter per request
        if f_phone:
            where.append("(Phone_Number_1 LIKE ? OR Phone_Number_2 LIKE ?)")
            params.extend([f"%{f_phone.strip()}%", f"%{f_phone.strip()}%"])
        where_sql = " AND ".join(where) if where else "1=1"
        # Determine available columns dynamically to avoid errors on older DBs
        try:
            sup_cols_info = fetchall("PRAGMA table_info(supervisor_data)") or []
            sup_cols = {str(r.get('name')) for r in sup_cols_info}
        except Exception:
            sup_cols = set()
        base_cols = ["id", "Case_ID", "Customer_name", "NIK_KTP", "DPD", "Phone_Number_1", "Phone_Number_2"]
        extra_cols = [
            "EMPLOYMENT_UPDATE",
            "EMPLOYER",
            "Debtor_Legal_Name",
            "Employee_Name",
            "Employee_ID_Number",
            "Debtor_Relation_to_Employee",
        ]
        select_cols = base_cols + [c for c in extra_cols if c in sup_cols]
        select_sql = ", ".join(select_cols)
        rows_sup = fetchall(
            f"""
            SELECT {select_sql}
            FROM supervisor_data
            WHERE {where_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(params + [int(limit_rows)])
        )
        import pandas as _pd
        df_sup = _pd.DataFrame(rows_sup) if rows_sup else _pd.DataFrame(columns=select_cols)
        # Ensure extra columns exist in the DataFrame even if not in DB schema
        for col in extra_cols:
            if col not in df_sup.columns:
                df_sup[col] = ""
        # Add selection column
        select_all = st.checkbox("Pilih semua yang ditampilkan", key="ta_select_all")
        if "Selected" not in df_sup.columns:
            df_sup.insert(0, "Selected", bool(select_all))
        else:
            try:
                df_sup["Selected"] = bool(select_all)
            except Exception:
                pass
        st.caption(f"Menampilkan {len(df_sup)} baris dari supervisor_data")
        edited = st.data_editor(
            df_sup,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Selected": st.column_config.CheckboxColumn("Selected", default=select_all),
                "Case_ID": st.column_config.TextColumn("Case_ID", disabled=True),
                "Customer_name": st.column_config.TextColumn("Customer", disabled=True),
                "NIK_KTP": st.column_config.TextColumn("NIK", disabled=True),
                "DPD": st.column_config.TextColumn("DPD", disabled=True),
                "Phone_Number_1": st.column_config.TextColumn("Phone 1", disabled=True),
                "Phone_Number_2": st.column_config.TextColumn("Phone 2", disabled=True),
                "EMPLOYMENT_UPDATE": st.column_config.TextColumn("EMPLOYMENT_UPDATE", disabled=True),
                "EMPLOYER": st.column_config.TextColumn("EMPLOYER", disabled=True),
                "Debtor_Legal_Name": st.column_config.TextColumn("Debtor_Legal_Name", disabled=True),
                "Employee_Name": st.column_config.TextColumn("Employee_Name", disabled=True),
                "Employee_ID_Number": st.column_config.TextColumn("Employee_ID_Number", disabled=True),
                "Debtor_Relation_to_Employee": st.column_config.TextColumn("Debtor_Relation_to_Employee", disabled=True),
            },
            num_rows="fixed",
        )

        # Assignment controls
        c1, c2 = st.columns([1,1])
        with c1:
            tracer_users_tbl = fetchall("SELECT COALESCE(full_name, name) AS full_name FROM users WHERE approved=1 AND role='Tracer' ORDER BY COALESCE(full_name,name)")
            tracer_list_tbl = [r.get('full_name') for r in tracer_users_tbl if r.get('full_name')]
            target_tracer_tbl = st.selectbox("Pilih Tracer (single)", options=tracer_list_tbl, index=0 if tracer_list_tbl else None, key="ta_tbl_tracer")
            btn_assign_single = st.button("Assign terpilih ke Tracer", type="primary", key="btn_assign_tbl_single")
        with c2:
            tracers_multi = st.multiselect("Pilih beberapa Tracer (acak/round-robin)", options=tracer_list_tbl, default=[], key="ta_tbl_multitracers")
            btn_assign_multi = st.button("Random distribute ke tracer terpilih", key="btn_assign_tbl_multi")

        # Helper for TRC code
        def _gen_trc_code_for(name: str) -> str:
            try:
                prefix = (name or '').split(' ')[0][:3].upper() or 'TRC'
            except Exception:
                prefix = 'TRC'
            return f"TRC-{now_wib().strftime('%y%m%d')}-{prefix}"

        # Process single assign - USE NEW SYSTEM
        if btn_assign_single:
            # Avoid ambiguous truth-value of DataFrame; use explicit None check
            sel = [r for _, r in ((edited if edited is not None else _pd.DataFrame())).iterrows() if bool(r.get("Selected"))]
            if not sel:
                st.warning("Pilih minimal satu baris pada tabel di atas.")
            elif not target_tracer_tbl:
                st.warning("Pilih tracer terlebih dahulu.")
            else:
                try:
                    u = current_user() or {}
                    by = (u.get('full_name') or u.get('login_id') or '-')
                    inserted = 0; frozen = 0; already_assigned = 0
                    
                    for _, r in (edited[edited["Selected"] == True]).iterrows():
                        agr = str(r.get("Case_ID") or "").strip()
                        if not agr:
                            continue
                        nik_val = str(r.get("NIK_KTP") or "").strip() or None
                        debtor_nm = r.get("Customer_name")
                        
                        # Use new assignment system
                        success, msg = assign_case_to_tracer(agr, target_tracer_tbl, by)
                        
                        if success:
                            # Still need to populate assign_tracer table for compatibility
                            trc_code = _gen_trc_code_for(target_tracer_tbl)
                            try:
                                execute(
                                    """
                                    INSERT INTO assign_tracer (TRC_Code, Agreement_No, Debtor_Name, NIK_KTP, Assigned_To)
                                    VALUES (?,?,?,?,?)
                                    ON CONFLICT(Agreement_No) DO UPDATE SET
                                      Assigned_To=excluded.Assigned_To,
                                      Debtor_Name=COALESCE(excluded.Debtor_Name, assign_tracer.Debtor_Name),
                                      NIK_KTP=COALESCE(excluded.NIK_KTP, assign_tracer.NIK_KTP),
                                      TRC_Code=COALESCE(NULLIF(assign_tracer.TRC_Code, ''), excluded.TRC_Code)
                                    """,
                                    (trc_code, agr, debtor_nm, nik_val, target_tracer_tbl)
                                )
                            except Exception:
                                pass
                            inserted += 1
                        else:
                            if "frozen" in msg.lower():
                                frozen += 1
                            elif "di-assign" in msg.lower():
                                already_assigned += 1
                    done = (len(sel) - frozen - already_assigned)
                    msg = f"Assign selesai. Diproses: {done}."
                    if frozen > 0:
                        msg += f" Dilewati karena Freeze: {frozen}."
                    # Show summary
                    u = current_user() or {}
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                               (u.get('id'), "TRACE_ASSIGN_FROM_SUP_TABLE", 
                                f"{inserted} rows to {target_tracer_tbl}; frozen {frozen}; already_assigned {already_assigned}"))
                    except Exception:
                        pass
                    
                    st.success(f"✅ Berhasil assign: {inserted} case. ❄️ Frozen: {frozen}. 🔒 Already assigned: {already_assigned}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal assign: {e}")

        # Process multi assign (random/round-robin) - USE NEW SYSTEM
        if btn_assign_multi:
            sel_df = edited[edited["Selected"] == True] if isinstance(edited, _pd.DataFrame) else _pd.DataFrame()
            if sel_df.empty:
                st.warning("Pilih minimal satu baris pada tabel di atas.")
            elif not tracers_multi:
                st.warning("Pilih minimal satu tracer untuk distribusi.")
            else:
                try:
                    import random
                    u = current_user() or {}
                    by = (u.get('full_name') or u.get('login_id') or '-')
                    rows_to_assign = sel_df.to_dict(orient="records")
                    # Shuffle for random distribution
                    random.shuffle(rows_to_assign)
                    
                    counts = {t: 0 for t in tracers_multi}
                    frozen = 0; done = 0; already_assigned = 0
                    
                    for i, r in enumerate(rows_to_assign):
                        agr = str(r.get("Case_ID") or "").strip()
                        if not agr:
                            continue
                        nik_val = str(r.get("NIK_KTP") or "").strip() or None
                        debtor_nm = r.get("Customer_name")
                        
                        assignee = tracers_multi[i % len(tracers_multi)]
                        
                        # Use new assignment system
                        success, msg = assign_case_to_tracer(agr, assignee, by)
                        
                        if success:
                            # Populate assign_tracer table for compatibility
                            trc_code = _gen_trc_code_for(assignee)
                            try:
                                execute(
                                    """
                                    INSERT INTO assign_tracer (TRC_Code, Agreement_No, Debtor_Name, NIK_KTP, Assigned_To)
                                    VALUES (?,?,?,?,?)
                                    ON CONFLICT(Agreement_No) DO UPDATE SET
                                      Assigned_To=excluded.Assigned_To,
                                      Debtor_Name=COALESCE(excluded.Debtor_Name, assign_tracer.Debtor_Name),
                                      NIK_KTP=COALESCE(excluded.NIK_KTP, assign_tracer.NIK_KTP),
                                      TRC_Code=COALESCE(NULLIF(assign_tracer.TRC_Code, ''), excluded.TRC_Code)
                                    """,
                                    (trc_code, agr, debtor_nm, nik_val, assignee)
                                )
                            except Exception:
                                pass
                            counts[assignee] += 1
                            done += 1
                        else:
                            if "frozen" in msg.lower():
                                frozen += 1
                            elif "di-assign" in msg.lower():
                                already_assigned += 1
                    
                    # Summary
                    summary = ", ".join([f"{k}:{v}" for k,v in counts.items()])
                    msg = f"✅ Distribusi selesai. Berhasil: {done}."
                    if frozen > 0:
                        msg += f" ❄️ Frozen: {frozen}."
                    if already_assigned > 0:
                        msg += f" 🔒 Already assigned: {already_assigned}."
                    msg += f" 📊 Rincian: {summary}"
                    st.success(msg)
                    
                    # Audit
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                               (u.get('id'), "TRACE_ASSIGN_RANDOM_FROM_SUP_TABLE", 
                                f"done {done}; frozen {frozen}; already_assigned {already_assigned}; {summary}"))
                    except Exception:
                        pass
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal distribusi acak: {e}")

        st.markdown("---")
        # Pull minimal fields to evaluate freeze status
        unassigned_rows = fetchall("SELECT id, Agreement_No, NIK_KTP FROM assign_tracer WHERE IFNULL(Assigned_To,'')='' ORDER BY id DESC")
        # Filter out frozen rows (by Agreement_No or by NIK)
        filtered_rows = []
        frozen_skipped = 0
        for r in unassigned_rows:
            agr = (r.get('Agreement_No') or '').strip()
            nik = (r.get('NIK_KTP') or '').strip()
            if is_frozen_by_agreement(agr) or (nik and is_frozen_by_nik(nik)):
                frozen_skipped += 1
                continue
            filtered_rows.append(r)
        unassigned_count = len(filtered_rows)
        if frozen_skipped:
            st.warning(f"{frozen_skipped} baris dilewati karena status Freeze (berdasarkan NIK/Case_ID).")

        if unassigned_count > 0:
            # Build tracer options in this scope (approved users)
            _user_rows_ma = fetchall("SELECT COALESCE(full_name, name) AS full_name FROM users WHERE approved=1 AND role='Tracer' ORDER BY COALESCE(full_name,name) ASC")
            tracer_names = [r['full_name'] for r in _user_rows_ma if r.get('full_name')]

            with st.form("multi_assign_form"):
                selected_tracers = st.multiselect(
                    "Pilih tracer (minimal 1)", options=tracer_names, default=[], key="multi_assign_tracers"
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
                if not selected_tracers or len(selected_tracers) < 1:
                    st.warning("Pilih minimal 1 tracer.")
                else:
                    ids = [r['id'] for r in filtered_rows]
                    try:
                        import random
                        if st.session_state.get("multi_assign_shuffle", True):
                            random.shuffle(ids)
                        # Batasi sesuai input
                        limit_val = st.session_state.get("multi_assign_limit", 0)
                        if limit_val and limit_val > 0:
                            ids = ids[: min(limit_val, len(ids))]
                        # Round-robin distribution (only for non-frozen rows)
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
                                ymd = now_wib().strftime('%y%m%d')
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
            st.caption("---")


    # --- Agent Assigning Tab ---
    with tabs[4]:
        st.subheader("Assign ke Agent")
        
        # Advanced Filters Section
        with st.expander("🔍 Filter Lanjutan", expanded=False):
            st.caption("Filter ini membantu memilih case yang sesuai untuk di-assign ke agent")
            
            fcol1, fcol2 = st.columns(2)
            
            with fcol1:
                # Get unique Lending Entities
                lending_entities = fetchall("SELECT DISTINCT Lending_Entity FROM supervisor_data WHERE Lending_Entity IS NOT NULL AND Lending_Entity != '' ORDER BY Lending_Entity")
                le_options = ["-- Semua Lending Entity --"] + [le.get('Lending_Entity') for le in lending_entities if le.get('Lending_Entity')]
                selected_lending_entity = st.selectbox(
                    "Filter by Lending Entity / Product",
                    options=le_options,
                    key="aa_lending_entity_filter",
                    help="Filter case berdasarkan produk/lending entity tertentu"
                )
            
            with fcol2:
                # Get unique Employment Update values from assign_tracer
                employment_updates = fetchall("SELECT DISTINCT EMPLOYMENT_UPDATE FROM assign_tracer WHERE EMPLOYMENT_UPDATE IS NOT NULL AND EMPLOYMENT_UPDATE != '' ORDER BY EMPLOYMENT_UPDATE")
                emp_options = ["-- Semua Employment Update --"] + [emp.get('EMPLOYMENT_UPDATE') for emp in employment_updates if emp.get('EMPLOYMENT_UPDATE')]
                selected_employment = st.selectbox(
                    "Filter by Employment Update",
                    options=emp_options,
                    key="aa_employment_filter",
                    help="Filter berdasarkan status employment (DEBTOR/SPOUSE/dll) - berguna untuk produk tertentu seperti AkuLaku"
                )
            
            # Show filter info
            if selected_lending_entity != "-- Semua Lending Entity --" or selected_employment != "-- Semua Employment Update --":
                filter_info = []
                if selected_lending_entity != "-- Semua Lending Entity --":
                    filter_info.append(f"Lending Entity: **{selected_lending_entity}**")
                if selected_employment != "-- Semua Employment Update --":
                    filter_info.append(f"Employment: **{selected_employment}**")
                st.info("📌 Filter aktif: " + " | ".join(filter_info))
        
        # Basic Filters
        q1, q2, q3, q4 = st.columns([1.2, 1.2, 1.2, 0.6])
        with q1:
            fa_case = st.text_input("Filter Case_ID", key="aa_f_case")
        with q2:
            fa_name = st.text_input("Filter Customer", key="aa_f_name")
        with q3:
            fa_phone = st.text_input("Filter Phone", key="aa_f_phone")
        with q4:
            fa_limit = st.number_input("Limit Row", min_value=10, max_value=2000, value=200, step=10, key="aa_limit")

        hide_assigned = st.checkbox("Sembunyikan yang sudah di-assign ke Agent", value=True, key="aa_hide_assigned")

        # Build SQL with filters
        wh = ["s.Case_ID IS NOT NULL", "TRIM(s.Case_ID)<>''"]
        par = []
        if fa_case:
            wh.append("s.Case_ID LIKE ?")
            par.append(f"%{fa_case.strip()}%")
        if fa_name:
            wh.append("s.Customer_name LIKE ?")
            par.append(f"%{fa_name.strip()}%")
        if fa_phone:
            wh.append("(s.Phone_Number_1 LIKE ? OR s.Phone_Number_2 LIKE ?)")
            par.extend([f"%{fa_phone.strip()}%", f"%{fa_phone.strip()}%"])
        if hide_assigned:
            wh.append("s.Case_ID NOT IN (SELECT Agreement_No FROM agent_assignments WHERE IFNULL(active,1)=1)")
        
        # Apply advanced filters
        if selected_lending_entity and selected_lending_entity != "-- Semua Lending Entity --":
            wh.append("s.Lending_Entity = ?")
            par.append(selected_lending_entity)
        
        if selected_employment and selected_employment != "-- Semua Employment Update --":
            wh.append("t.EMPLOYMENT_UPDATE = ?")
            par.append(selected_employment)
        
        # Exclude data already assigned to tracer
        wh.append("s.Case_ID NOT IN (SELECT Agreement_No FROM assign_tracer WHERE IFNULL(Assigned_To,'')!='')")
        wh_sql = " AND ".join(wh) if wh else "1=1"

        # Determine available columns dynamically
        try:
            _sup_cols = fetchall("PRAGMA table_info(supervisor_data)") or []
            sup_cols = {str(r.get('name')) for r in _sup_cols}
        except Exception:
            sup_cols = set()
        base_cols = ["s.id", "s.Case_ID", "s.Customer_name", "s.NIK_KTP", "s.DPD", "s.Phone_Number_1", "s.Phone_Number_2", "s.Lending_Entity"]
        extra_cols = [
            # employment details (for context)
            ("t.EMPLOYMENT_UPDATE", "EMPLOYMENT_UPDATE"), 
            ("t.EMPLOYER", "EMPLOYER"), 
            ("s.Debtor_Legal_Name", "Debtor_Legal_Name"), 
            ("s.Employee_Name", "Employee_Name"), 
            ("s.Employee_ID_Number", "Employee_ID_Number"), 
            ("s.Debtor_Relation_to_Employee", "Debtor_Relation_to_Employee"),
            # agent-editable fields (for visibility)
            ("s.STATUS", "STATUS"), 
            ("s.REGISTERED_PHONE", "REGISTERED_PHONE"), 
            ("s.Additional_Contacts", "Additional_Contacts"), 
            ("s.Remarks_Suggested_NIK_Prospect", "Remarks_Suggested_NIK_Prospect"), 
            ("s.Payment", "Payment"), 
            ("s.Paid_Off_Status", "Paid_Off_Status")
        ]
        
        # Build SELECT clause
        sel_parts = base_cols.copy()
        for col_expr, col_alias in extra_cols:
            # Check if base column exists in supervisor_data
            base_col = col_expr.split('.')[-1]
            if base_col in sup_cols or col_expr.startswith('t.'):
                sel_parts.append(f"{col_expr} as {col_alias}")
        
        rows_sup = fetchall(
            f"""
            SELECT {', '.join(sel_parts)}
            FROM supervisor_data s
            LEFT JOIN assign_tracer t ON s.Case_ID = t.Agreement_No
            WHERE {wh_sql}
            ORDER BY s.id DESC
            LIMIT ?
            """,
            tuple(par + [int(fa_limit)])
        )
        import pandas as _pd
        
        # Clean up column names for DataFrame
        if rows_sup:
            clean_rows = []
            for row in rows_sup:
                clean_row = {}
                for k, v in row.items():
                    # Remove table prefix (s. or t.)
                    clean_key = k.split('.')[-1] if '.' in k else k
                    clean_row[clean_key] = v
                clean_rows.append(clean_row)
            df = _pd.DataFrame(clean_rows)
        else:
            df = _pd.DataFrame()
        
        # Ensure all expected columns exist
        expected_cols = ["id", "Case_ID", "Customer_name", "NIK_KTP", "DPD", "Phone_Number_1", "Phone_Number_2", "Lending_Entity", "EMPLOYMENT_UPDATE", "EMPLOYER"]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""
        
        # Add touch count and priority indicator for each case
        if not df.empty and 'Case_ID' in df.columns:
            touch_counts = []
            priorities = []
            for idx, row in df.iterrows():
                case_id = row.get('Case_ID', '')
                if case_id:
                    count = get_case_touch_count(case_id)
                    touch_counts.append(count)
                    # Priority: 🔴 High (0-1 touches), 🟡 Medium (2-3), 🟢 Low (4+)
                    if count <= 1:
                        priorities.append("🔴 High (Fresh)")
                    elif count <= 3:
                        priorities.append("🟡 Medium")
                    else:
                        priorities.append("🟢 Low (Handled)")
                else:
                    touch_counts.append(0)
                    priorities.append("🔴 High (Fresh)")
            
            df.insert(1, "Priority", priorities)
            df.insert(2, "Touch_Count", touch_counts)

        # Selection controls
        select_all = st.checkbox("Pilih semua yang ditampilkan", key="aa_select_all")
        if "Selected" not in df.columns:
            df.insert(0, "Selected", bool(select_all))
        else:
            try:
                df["Selected"] = bool(select_all)
            except Exception:
                pass
        st.caption(f"Menampilkan {len(df)} baris kandidat untuk assignment Agent")
        
        # Show priority legend
        st.markdown("""
        **Prioritas Assignment:** 🔴 High (0-1x handled) → 🟡 Medium (2-3x) → 🟢 Low (4+ handled)  
        💡 System otomatis prioritaskan case dengan touch count paling sedikit
        """)
        
        try:
            edited = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Selected": st.column_config.CheckboxColumn("Selected", default=select_all),
                    "Priority": st.column_config.TextColumn("Priority", disabled=True, width="small", help="Prioritas berdasarkan jumlah penanganan"),
                    "Touch_Count": st.column_config.NumberColumn("Handled", disabled=True, width="small", help="Berapa kali case ini sudah di-handle"),
                    "Case_ID": st.column_config.TextColumn("Case_ID", disabled=True),
                    "Customer_name": st.column_config.TextColumn("Customer", disabled=True),
                    "NIK_KTP": st.column_config.TextColumn("NIK", disabled=True),
                    "DPD": st.column_config.TextColumn("DPD", disabled=True),
                    "Phone_Number_1": st.column_config.TextColumn("Phone 1", disabled=True),
                    "Phone_Number_2": st.column_config.TextColumn("Phone 2", disabled=True),
                    "Lending_Entity": st.column_config.TextColumn("Lending Entity", disabled=True),
                    "EMPLOYMENT_UPDATE": st.column_config.TextColumn("Employment Update", disabled=True),
                    "EMPLOYER": st.column_config.TextColumn("Employer", disabled=True),
                    "STATUS": st.column_config.TextColumn("STATUS", disabled=True),
                    "REGISTERED_PHONE": st.column_config.TextColumn("REGISTERED PHONE", disabled=True),
                    "Additional_Contacts": st.column_config.TextColumn("Remarks", disabled=True),
                    "Remarks_Suggested_NIK_Prospect": st.column_config.TextColumn("Suggested NIK", disabled=True),
                    "Payment": st.column_config.TextColumn("Payment", disabled=True),
                    "Paid_Off_Status": st.column_config.TextColumn("Paid Off Status", disabled=True),
                },
                num_rows="fixed"
            )
        except Exception:
            edited = df
            st.dataframe(df, use_container_width=True, hide_index=True)

        try:
            selected_rows = edited[edited["Selected"] == True]
        except Exception:
            selected_rows = _pd.DataFrame(columns=df.columns)

        # Agent lists
        agents = [
            (r.get('n') or '-') for r in (fetchall("SELECT COALESCE(full_name, name, login_id) AS n FROM users WHERE role='Agent' AND approved=1 ORDER BY n") or [])
        ]
        agents = [a for a in agents if a and a.strip()]

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Assign ke satu Agent")
            sel_agent = st.selectbox("Pilih agent", options= agents, index=0, key="aa_single_agent")
            btn_single = st.button("Assign ke agent ini", type="primary", key="aa_btn_single")
            if btn_single:
                if not len(selected_rows):
                    st.warning("Pilih minimal satu baris dahulu.")
                else:
                    try:
                        u = current_user() or {}
                        by = (u.get('full_name') or u.get('login_id') or '-')
                        frozen_skips = 0
                        already_tracer = 0
                        assigned = 0
                        for _, r in selected_rows.iterrows():
                            agr = str(r.get('Case_ID') or '').strip()
                            if not agr:
                                continue
                            # Freeze check by Agreement_No
                            try:
                                if is_frozen_by_agreement(agr):
                                    frozen_skips += 1
                                    continue
                            except Exception:
                                pass
                            # Check if already assigned to tracer
                            try:
                                conn_check = get_db()
                                cur = conn_check.cursor()
                                cur.execute("SELECT COUNT(*) as cnt FROM assign_tracer WHERE Agreement_No=? AND IFNULL(Assigned_To,'')!=''", (agr,))
                                if cur.fetchone()['cnt'] > 0:
                                    already_tracer += 1
                                    conn_check.close()
                                    continue
                                conn_check.close()
                            except Exception:
                                pass
                            
                            # Use new assignment system with rotation
                            success, msg = assign_case_to_agent(agr, sel_agent, by)
                            if success:
                                assigned += 1
                            else:
                                # Count specific rejection reasons
                                if "frozen" in msg.lower():
                                    frozen_skips += 1
                                
                        # Audit
                        try:
                            execute(
                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                (u.get('id') if u else None, "AGENT_ASSIGN_FROM_SUP_TABLE", f"Assigned {assigned} to {sel_agent}; frozen: {frozen_skips}; already_tracer: {already_tracer}")
                            )
                        except Exception:
                            pass
                        st.success(f"✅ Berhasil assign: {assigned} case. ❄️ Frozen: {frozen_skips}. 🔍 Sudah di tracer: {already_tracer}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal assign: {e}")

        with c2:
            st.markdown("#### Distribusi Seimbang ke beberapa Agent")
            st.caption("⚖️ Distribusi berdasarkan keseimbangan jumlah case DAN total hutang (Principle Outstanding)")
            sel_agents = st.multiselect("Pilih beberapa agent", options=agents, key="aa_multi_agents")
            btn_multi = st.button("Balanced Distribution (by Outstanding)", key="aa_btn_multi", type="primary")
            if btn_multi:
                if not len(selected_rows):
                    st.warning("Pilih minimal satu baris dahulu.")
                elif not sel_agents:
                    st.warning("Pilih minimal satu agent.")
                else:
                    try:
                        import random as _rand
                        u = current_user() or {}
                        by = (u.get('full_name') or u.get('login_id') or '-')
                        
                        # Prepare case list with principle outstanding
                        cases_with_po = []
                        for _, r in selected_rows.iterrows():
                            agr = str(r.get('Case_ID') or '').strip()
                            if not agr:
                                continue
                            
                            # Get principle outstanding (hutang)
                            po_str = str(r.get('Principle_Outstanding', '0') or '0').strip()
                            try:
                                # Clean string: remove currency symbols, commas, etc.
                                po_clean = ''.join(c for c in po_str if c.isdigit() or c == '.')
                                po_value = float(po_clean) if po_clean else 0.0
                            except Exception:
                                po_value = 0.0
                            
                            cases_with_po.append({
                                'case_id': agr,
                                'po': po_value,
                                'row': r
                            })
                        
                        # Sort cases by principle outstanding DESC (largest first)
                        # This helps balance distribution better
                        cases_with_po.sort(key=lambda x: x['po'], reverse=True)
                        
                        # Initialize agent workload tracking
                        agent_workload = {agent: {'count': 0, 'total_po': 0.0, 'cases': []} for agent in sel_agents}
                        
                        # Statistics tracking
                        assigned = 0
                        frozen_skips = 0
                        already_tracer = 0
                        rotation_blocked = 0
                        
                        # BALANCED DISTRIBUTION ALGORITHM
                        # For each case, assign to agent with LOWEST total_po (greedy algorithm)
                        for case_data in cases_with_po:
                            agr = case_data['case_id']
                            po = case_data['po']
                            
                            # Validation checks
                            try:
                                if is_frozen_by_agreement(agr):
                                    frozen_skips += 1
                                    continue
                            except Exception:
                                pass
                            
                            # Check if already assigned to tracer
                            try:
                                conn_check2 = get_db()
                                cur = conn_check2.cursor()
                                cur.execute("SELECT COUNT(*) as cnt FROM assign_tracer WHERE Agreement_No=? AND IFNULL(Assigned_To,'')!=''", (agr,))
                                if cur.fetchone()['cnt'] > 0:
                                    already_tracer += 1
                                    conn_check2.close()
                                    continue
                                conn_check2.close()
                            except Exception:
                                pass
                            
                            # Find agent with LOWEST total outstanding (greedy balance)
                            target_agent = min(agent_workload.keys(), key=lambda a: agent_workload[a]['total_po'])
                            
                            # Try to assign
                            success, msg = assign_case_to_agent(agr, target_agent, by)
                            if success:
                                assigned += 1
                                agent_workload[target_agent]['count'] += 1
                                agent_workload[target_agent]['total_po'] += po
                                agent_workload[target_agent]['cases'].append(agr)
                            else:
                                if "frozen" in msg.lower():
                                    frozen_skips += 1
                                elif "handle dulu" in msg.lower():
                                    rotation_blocked += 1
                        
                        # Show distribution summary
                        st.markdown("---")
                        st.markdown("### 📊 Hasil Distribusi Seimbang")
                        
                        summary_data = []
                        for agent, wl in agent_workload.items():
                            summary_data.append({
                                'Agent': agent,
                                'Jumlah Case': wl['count'],
                                'Total Hutang (PO)': f"Rp {wl['total_po']:,.0f}",
                                'Rata-rata per Case': f"Rp {(wl['total_po'] / wl['count']):,.0f}" if wl['count'] > 0 else "Rp 0"
                            })
                        
                        summary_df = pd.DataFrame(summary_data)
                        st.dataframe(summary_df, use_container_width=True, hide_index=True)
                        
                        # Calculate balance metrics
                        case_counts = [wl['count'] for wl in agent_workload.values()]
                        po_totals = [wl['total_po'] for wl in agent_workload.values()]
                        
                        if case_counts and max(case_counts) > 0:
                            case_variance = max(case_counts) - min(case_counts)
                            po_variance = max(po_totals) - min(po_totals)
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("📈 Variance Case", f"{case_variance} case", 
                                         help="Selisih jumlah case terbanyak - tersedikit")
                            with col2:
                                st.metric("💰 Variance Hutang", f"Rp {po_variance:,.0f}",
                                         help="Selisih total hutang terbesar - terkecil")
                            with col3:
                                balance_score = "⭐⭐⭐ Excellent" if po_variance < (sum(po_totals)/len(po_totals) * 0.1) else \
                                              "⭐⭐ Good" if po_variance < (sum(po_totals)/len(po_totals) * 0.2) else \
                                              "⭐ Fair"
                                st.metric("⚖️ Balance Score", balance_score,
                                         help="Excellent: variance < 10% avg | Good: < 20% avg")
                        
                        # Audit
                        try:
                            summary_str = " | ".join([f"{a}: {wl['count']} case (Rp {wl['total_po']:,.0f})" 
                                                     for a, wl in agent_workload.items()])
                            execute(
                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                (u.get('id') if u else None, "AGENT_ASSIGN_BALANCED_FROM_SUP_TABLE", 
                                 f"Assigned {assigned} among {len(sel_agents)} agents (BALANCED); frozen: {frozen_skips}; tracer: {already_tracer}; rotation_blocked: {rotation_blocked}; Distribution: {summary_str}")
                            )
                        except Exception:
                            pass
                        st.success(f"✅ Berhasil assign: {assigned} case. ❄️ Frozen: {frozen_skips}. 🔍 Sudah di tracer: {already_tracer}. 🔄 Rotation blocked: {rotation_blocked}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal melakukan distribusi: {e}")
        

    # --- Freeze Manager Tab ---
    with tabs[7]:
        st.markdown("### 🔒 Freeze Manager - Entity Protection System")
        st.caption("Manajemen freeze/unfreeze untuk NIK dan Case ID. Data yang frozen tidak bisa di-assign dan ter-proteksi dari collection activity.")
        
        # Enhanced CSS
        st.markdown("""
        <style>
        .freeze-stat-card {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            padding: 20px;
            border-radius: 16px;
            color: white;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        .freeze-stat-card:hover {
            transform: translateY(-5px);
        }
        .freeze-stat-card h3 {
            margin: 0 0 8px 0;
            font-size: 32px;
            font-weight: 700;
        }
        .freeze-stat-card p {
            margin: 0;
            font-size: 14px;
            opacity: 0.95;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Statistics Dashboard
        st.markdown("#### 📊 Freeze Statistics")
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        
        # Get statistics
        total_frozen = fetchone("SELECT COUNT(*) c FROM frozen_entities WHERE active=1")['c'] or 0
        frozen_niks = fetchone("SELECT COUNT(*) c FROM frozen_entities WHERE active=1 AND NIK_KTP IS NOT NULL AND NIK_KTP != ''")['c'] or 0
        frozen_agrs = fetchone("SELECT COUNT(*) c FROM frozen_entities WHERE active=1 AND Agreement_No IS NOT NULL AND Agreement_No != ''")['c'] or 0
        
        # Count affected cases
        affected_cases = 0
        frozen_rows = fetchall("SELECT NIK_KTP, Agreement_No FROM frozen_entities WHERE active=1")
        for r in frozen_rows:
            nik = (r.get('NIK_KTP') or '').strip()
            agr = (r.get('Agreement_No') or '').strip()
            if nik:
                cnt = (fetchone("SELECT COUNT(*) c FROM assign_tracer WHERE COALESCE(NIK_KTP,'')=?", (nik,)) or {}).get('c', 0)
                affected_cases += cnt
            elif agr:
                cnt = (fetchone("SELECT COUNT(*) c FROM assign_tracer WHERE Agreement_No=?", (agr,)) or {}).get('c', 0)
                affected_cases += cnt
        
        with stat_col1:
            st.markdown(f"""
            <div class='freeze-stat-card'>
                <h3>🔒 {total_frozen}</h3>
                <p>Total Frozen Entities</p>
            </div>
            """, unsafe_allow_html=True)
        
        with stat_col2:
            st.markdown(f"""
            <div class='freeze-stat-card' style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);'>
                <h3>🆔 {frozen_niks}</h3>
                <p>Frozen by NIK</p>
            </div>
            """, unsafe_allow_html=True)
        
        with stat_col3:
            st.markdown(f"""
            <div class='freeze-stat-card' style='background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);'>
                <h3>📄 {frozen_agrs}</h3>
                <p>Frozen by Case ID</p>
            </div>
            """, unsafe_allow_html=True)
        
        with stat_col4:
            st.markdown(f"""
            <div class='freeze-stat-card' style='background: linear-gradient(135deg, #30cfd0 0%, #330867 100%);'>
                <h3>⚠️ {affected_cases}</h3>
                <p>Affected Cases</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Freeze Forms - Enhanced 2-column layout
        st.markdown("#### ➕ Add New Freeze")
        col_fz1, col_fz2 = st.columns(2)
        
        with col_fz1:
            with st.expander("🆔 Freeze by NIK", expanded=True):
                st.caption("Freeze all loans associated with a specific NIK")
                nik_in = st.text_input("NIK KTP (16 digit)", key="freeze_nik", placeholder="e.g., 3201234567890001")
                reason_nik = st.selectbox("Alasan Freeze", [
                    "Deceased Debtor",
                    "Legal Dispute",
                    "Fraud Suspect",
                    "Data Correction Needed",
                    "Management Hold",
                    "Other"
                ], key="freeze_reason_nik")
                note_nik = st.text_area("Catatan Detail (opsional)", key="freeze_note_nik", height=80, placeholder="Tambahkan detail informasi...")
                
                # Show preview if NIK exists
                if nik_in and len(nik_in.strip()) >= 5:
                    preview = fetchall("SELECT Agreement_No, Debtor_Name FROM assign_tracer WHERE NIK_KTP LIKE ? LIMIT 5", (f"%{nik_in.strip()}%",))
                    if preview:
                        st.caption(f"📋 Preview: Found **{len(preview)}** loan(s) with this NIK")
                        for p in preview[:3]:
                            st.caption(f"  • {p.get('Agreement_No')} - {p.get('Debtor_Name')}")
                        if len(preview) > 3:
                            st.caption(f"  ... and {len(preview)-3} more")
                
                if st.button("🔒 Freeze NIK", key="btn_freeze_nik", type="primary", use_container_width=True):
                    nik_val = (nik_in or '').strip()
                    if not nik_val:
                        st.warning("⚠️ Masukkan NIK terlebih dahulu.")
                    elif len(nik_val) < 5:
                        st.warning("⚠️ NIK terlalu pendek. Minimal 5 karakter.")
                    else:
                        try:
                            exists = fetchone("SELECT id FROM frozen_entities WHERE active=1 AND NIK_KTP=? LIMIT 1", (nik_val,))
                            if exists:
                                st.info("ℹ️ NIK ini sudah dalam status Freeze.")
                            else:
                                u = current_user() or {}
                                execute(
                                    "INSERT INTO frozen_entities (NIK_KTP, reason, note, created_by) VALUES (?,?,?,?)",
                                    (nik_val, reason_nik, (note_nik or '').strip() or None, (u.get('full_name') or u.get('login_id') or '-'))
                                )
                                st.success("✅ Berhasil mem-freeze NIK.")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Gagal menyimpan: {e}")

        with col_fz2:
            with st.expander("📄 Freeze by Case ID", expanded=True):
                st.caption("Freeze a specific loan contract")
                agr_in = st.text_input("Case ID", key="freeze_agr", placeholder="e.g., AGR2025001")
                reason_agr = st.selectbox("Alasan Freeze", [
                    "Disputed Amount",
                    "Wrong Debtor Assignment",
                    "Payment Processing",
                    "Legal Hold",
                    "Data Verification",
                    "Other"
                ], key="freeze_reason_agr")
                note_agr = st.text_area("Catatan Detail (opsional)", key="freeze_note_agr", height=80, placeholder="Tambahkan detail informasi...")
                
                # Show quick info lookup
                if (agr_in or '').strip():
                    info = fetchone("SELECT Debtor_Name, NIK_KTP, Assigned_To FROM assign_tracer WHERE Agreement_No LIKE ?", (f"%{agr_in.strip()}%",))
                    if info:
                        st.info(f"📋 **{info.get('Debtor_Name') or '-'}**\n\n🆔 NIK: {info.get('NIK_KTP') or '-'}\n\n👤 Tracer: {info.get('Assigned_To') or '-'}")
                
                if st.button("🔒 Freeze Case ID", key="btn_freeze_agr", type="primary", use_container_width=True):
                    agr_val = (agr_in or '').strip()
                    if not agr_val:
                        st.warning("⚠️ Masukkan Case ID terlebih dahulu.")
                    else:
                        try:
                            exists = fetchone("SELECT id FROM frozen_entities WHERE active=1 AND Agreement_No=? LIMIT 1", (agr_val,))
                            if exists:
                                st.info("ℹ️ Case ID ini sudah dalam status Freeze.")
                            else:
                                u = current_user() or {}
                                execute(
                                    "INSERT INTO frozen_entities (Agreement_No, reason, note, created_by) VALUES (?,?,?,?)",
                                    (agr_val, reason_agr, (note_agr or '').strip() or None, (u.get('full_name') or u.get('login_id') or '-'))
                                )
                                st.success("✅ Berhasil mem-freeze Case ID.")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Gagal menyimpan: {e}")

        st.markdown("---")
        
        # Active Freeze List - Enhanced
        st.markdown("#### 📋 Active Freeze List")
        
        # Enhanced filters
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
        with filter_col1:
            filter_type = st.selectbox("Filter by Type", ["All", "NIK Only", "Case ID Only"], index=0, key="freeze_filter_type")
        with filter_col2:
            filter_reason = st.text_input("Search Reason", key="freeze_filter_reason", placeholder="Search...")
        with filter_col3:
            filter_created_by = st.text_input("Created By", key="freeze_filter_creator", placeholder="Search...")
        with filter_col4:
            sort_by = st.selectbox("Sort By", ["Newest First", "Oldest First", "Most Impacted"], index=0, key="freeze_sort")
        
        # Build query with filters
        q_freeze = "SELECT id, NIK_KTP, Agreement_No, reason, note, created_by, created_at FROM frozen_entities WHERE active=1"
        p_freeze = []
        
        if filter_type == "NIK Only":
            q_freeze += " AND NIK_KTP IS NOT NULL AND NIK_KTP != ''"
        elif filter_type == "Case ID Only":
            q_freeze += " AND Agreement_No IS NOT NULL AND Agreement_No != ''"
        
        if filter_reason:
            q_freeze += " AND COALESCE(reason,'') LIKE ?"
            p_freeze.append(f"%{filter_reason}%")
        
        if filter_created_by:
            q_freeze += " AND COALESCE(created_by,'') LIKE ?"
            p_freeze.append(f"%{filter_created_by}%")
        
        if sort_by == "Oldest First":
            q_freeze += " ORDER BY datetime(created_at) ASC"
        else:
            q_freeze += " ORDER BY datetime(created_at) DESC"
        
        rows = fetchall(q_freeze, tuple(p_freeze))
        
        if not rows:
            st.info("ℹ️ Tidak ada freeze aktif sesuai filter.")
        else:
            # Prepare display data
            disp = []
            for r in rows:
                nik = (r.get('NIK_KTP') or '').strip()
                agr = (r.get('Agreement_No') or '').strip()
                if nik:
                    cnt = (fetchone("SELECT COUNT(*) c FROM assign_tracer WHERE COALESCE(NIK_KTP,'')=?", (nik,)) or {}).get('c', 0)
                    target = f"🆔 NIK: {nik}"
                    entity_type = "NIK"
                elif agr:
                    cnt = (fetchone("SELECT COUNT(*) c FROM assign_tracer WHERE Agreement_No=?", (agr,)) or {}).get('c', 0)
                    target = f"📄 Case ID: {agr}"
                    entity_type = "Case ID"
                else:
                    cnt = 0
                    target = "❓ Unknown"
                    entity_type = "Unknown"
                
                disp.append({
                    "ID": r.get('id'),
                    "Type": entity_type,
                    "Target": target,
                    "Reason": r.get('reason') or '-',
                    "Note": (r.get('note') or '-')[:50] + ('...' if len(r.get('note') or '') > 50 else ''),
                    "Impact": cnt,
                    "Created By": r.get('created_by') or '-',
                    "Created At": r.get('created_at') or '-',
                })
            
            # Sort by impact if requested
            if sort_by == "Most Impacted":
                disp = sorted(disp, key=lambda x: x['Impact'], reverse=True)
            
            df_freeze = pd.DataFrame(disp)
            st.caption(f"📊 Showing **{len(df_freeze)}** active freeze(s)")
            
            st.dataframe(
                df_freeze,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "ID": st.column_config.NumberColumn("ID", width="small"),
                    "Type": st.column_config.TextColumn("Type", width="small"),
                    "Target": st.column_config.TextColumn("Target Entity", width="large"),
                    "Reason": st.column_config.TextColumn("Reason", width="medium"),
                    "Note": st.column_config.TextColumn("Note", width="medium"),
                    "Impact": st.column_config.NumberColumn("Cases Affected", width="small"),
                    "Created By": st.column_config.TextColumn("Created By", width="small"),
                    "Created At": st.column_config.TextColumn("Created At", width="medium"),
                }
            )
            
            # Export
            if st.button("📥 Export Freeze List", key="export_freeze"):
                csv = df_freeze.to_csv(index=False)
                st.download_button(
                    label="Download Freeze List CSV",
                    data=csv,
                    file_name=f"freeze_list_{today_wib().isoformat()}.csv",
                    mime="text/csv"
                )

            # Unfreeze control - Enhanced
            st.markdown("---")
            st.markdown("#### 🔓 Unfreeze Control")
            unf_col1, unf_col2 = st.columns([3, 1])
            with unf_col1:
                unfreeze_id = st.text_input("Enter Freeze ID to Unfreeze", key="unfreeze_id", placeholder="e.g., 123")
                if unfreeze_id and unfreeze_id.strip().isdigit():
                    # Show preview
                    preview = fetchone("SELECT NIK_KTP, Agreement_No, reason FROM frozen_entities WHERE id=? AND active=1", (int(unfreeze_id.strip()),))
                    if preview:
                        target_info = preview.get('NIK_KTP') or preview.get('Agreement_No') or 'Unknown'
                        st.info(f"📋 Preview: **{target_info}** | Reason: {preview.get('reason') or 'N/A'}")
            
            with unf_col2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🔓 Unfreeze", key="btn_unfreeze", type="secondary", use_container_width=True):
                    try:
                        uid = int((unfreeze_id or '0').strip())
                        if uid <= 0:
                            st.warning("⚠️ Invalid ID.")
                        else:
                            execute("UPDATE frozen_entities SET active=0 WHERE id=?", (uid,))
                            st.success("✅ Berhasil unfreeze entity.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Gagal unfreeze: {e}")

        st.markdown("---")


    # --- Trace Results Tab ---
    with tabs[5]:
        st.markdown("### 📝 Trace Results - Touch Activity Logs")
        st.caption("Record dan monitor semua aktivitas tracing. Real-time tracking untuk setiap interaksi dengan debitur.")
        
        # Enhanced CSS for Trace Results - Financial Dashboard Style
        st.markdown("""
        <style>
        /* Financial KPI card matching dashboard style */
        .trace-stat-card { 
            position: relative; 
            overflow: hidden; 
            background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #E5E7EB; 
            border-radius: 20px; 
            padding: 24px 20px; 
            box-shadow: 0 4px 12px rgba(16,24,40,0.08), 0 1px 3px rgba(16,24,40,0.05);
            transition: all 0.3s ease;
            margin-bottom: 10px;
        }
        .trace-stat-card:hover {
            box-shadow: 0 8px 24px rgba(16,24,40,0.12), 0 2px 6px rgba(16,24,40,0.08);
            transform: translateY(-2px);
        }
        
        /* Accent circle (decorative element) */
        .trace-stat-card::after { 
            content:""; 
            position:absolute; 
            right:-40px; 
            top:-50px; 
            width:200px; 
            height:200px; 
            border-radius: 50%; 
            background: radial-gradient(circle at center, var(--accent-light, #EEF4FF), rgba(255,255,255,0) 60%); 
            opacity:.5;
            z-index: 0;
        }
        
        /* Content layer above decoration */
        .trace-stat-card > * { position: relative; z-index: 1; }
        
        .trace-stat-label {
            letter-spacing: .5px; 
            text-transform: uppercase; 
            font-size: 11px; 
            font-weight: 700;
            color: #6B7280; 
            margin-bottom: 12px;
        }
        .trace-stat-value {
            font-size: 32px; 
            font-weight: 800; 
            color: var(--accent, #111827); 
            line-height: 1.1;
            margin: 8px 0;
            letter-spacing: -0.5px;
        }
        
        /* Color variants matching dashboard */
        .accent-teal { --accent: #0D9488; --accent-light: #CCFBF1; }
        .accent-rose { --accent: #E11D48; --accent-light: #FFE4E6; }
        .accent-sky { --accent: #0284C7; --accent-light: #E0F2FE; }
        .accent-emerald { --accent: #059669; --accent-light: #D1FAE5; }
        </style>
        """, unsafe_allow_html=True)
        
        # Quick Stats - Financial Dashboard Style
        stat_cols = st.columns(4)
        today = today_wib()
        
        with stat_cols[0]:
            total_traces = (fetchone("SELECT COUNT(*) as c FROM trace_results") or {}).get('c', 0)
            st.markdown(f"""
            <div class='trace-stat-card accent-teal'>
                <div class='trace-stat-label'>💼 Total Traces</div>
                <div class='trace-stat-value'>{total_traces:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with stat_cols[1]:
            today_traces = (fetchone("SELECT COUNT(*) as c FROM trace_results WHERE DATE(touched_at) = DATE(?)", (today.isoformat(),)) or {}).get('c', 0)
            st.markdown(f"""
            <div class='trace-stat-card accent-rose'>
                <div class='trace-stat-label'>📅 Today's Traces</div>
                <div class='trace-stat-value'>{today_traces:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with stat_cols[2]:
            unique_cases = (fetchone("SELECT COUNT(DISTINCT Agreement_No) as c FROM trace_results") or {}).get('c', 0)
            st.markdown(f"""
            <div class='trace-stat-card accent-sky'>
                <div class='trace-stat-label'>📋 Unique Cases</div>
                <div class='trace-stat-value'>{unique_cases:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with stat_cols[3]:
            active_tracers = (fetchone("SELECT COUNT(DISTINCT tracer) as c FROM trace_results WHERE DATE(touched_at) >= DATE(?, '-7 days')", (today.isoformat(),)) or {}).get('c', 0)
            st.markdown(f"""
            <div class='trace-stat-card accent-emerald'>
                <div class='trace-stat-label'>👥 Active Tracers (7d)</div>
                <div class='trace-stat-value'>{active_tracers:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Add New Trace - Modern Form
        with st.expander("➕ Add New Trace Record", expanded=False):
            with st.form("trace_add_form_enhanced"):
                st.markdown("#### Basic Information")
                c1, c2 = st.columns(2)
                with c1:
                    agr_input = st.text_input("🔖 Case ID *", placeholder="e.g., AGR2025001")
                    tracer_sel = st.text_input("👤 Tracer Name", value=(current_user().get('full_name') if current_user() else ''), disabled=True)
                with c2:
                    status_sel = st.selectbox("📊 Status *", ["", "TRACED", "CONTACTED", "RTP", "PTP", "PAYING", "UNREACHABLE", "REFUSED", "PROMISE_BROKEN", "OTHER"])
                    touch_type = st.selectbox("📞 Contact Method *", ["", "CALL", "WHATSAPP", "SMS", "EMAIL", "VISIT", "VIDEO_CALL", "OTHER"])
                
                st.markdown("#### Contact Details")
                c3, c4 = st.columns(2)
                with c3:
                    party_sel = st.selectbox("🎯 Party Contacted", ["", "DEBTOR_DIRECT", "COMPANY_HR", "COMPANY_FINANCE", "RELATIVE", "NEIGHBOR", "EMERGENCY_CONTACT", "OTHER"])
                with c4:
                    contact_person = st.text_input("👥 Contact Person Name", placeholder="Optional")
                
                st.markdown("#### Notes & Details")
                notes = st.text_area("📝 Notes / Hasil Kontak", height=120, placeholder="Deskripsikan hasil percakapan, kondisi debitur, atau informasi penting lainnya...")
                
                col_submit = st.columns([3, 1])
                with col_submit[1]:
                    submitted = st.form_submit_button("✅ Save Trace", type="primary", use_container_width=True)
                
                if submitted:
                    if not agr_input.strip():
                        st.error("❌ Case ID wajib diisi!")
                    elif not status_sel:
                        st.error("❌ Status wajib dipilih!")
                    elif not touch_type:
                        st.error("❌ Contact Method wajib dipilih!")
                    else:
                        try:
                            u = current_user() or {}
                            # Build detailed notes
                            detail_notes = notes.strip() if notes else ""
                            if contact_person:
                                detail_notes = f"Contact: {contact_person.strip()}\n{detail_notes}"
                            
                            execute(
                                "INSERT INTO trace_results (Agreement_No, tracer, status, notes, touch_type, party, created_by, touched_at) VALUES (?,?,?,?,?,?,?,?)",
                                (agr_input.strip(), tracer_sel.strip() if tracer_sel else None, status_sel or None, detail_notes or None, touch_type or None, party_sel or None, (u.get('full_name') or u.get('login_id') or '-'), now_wib().isoformat())
                            )
                            
                            # Audit log
                            try:
                                execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", 
                                       (u.get('id') if u else None, "TRACE_ADD", f"Added trace for {agr_input.strip()} with status {status_sel}"))
                            except Exception:
                                pass
                            
                            st.success("✅ Trace record berhasil disimpan!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Gagal menyimpan: {e}")

        st.markdown("---")
        
        # View Logs - Enhanced Filter
        st.markdown("### 🔍 Search & Filter Trace Logs")
        
        # Filter row 1
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            f_agr = st.text_input("🔖 Case ID", key="trace_q_agr", placeholder="Search...")
        with fc2:
            f_tracer = st.text_input("👤 Tracer", key="trace_q_tracer", placeholder="Search...")
        with fc3:
            f_status = st.multiselect("📊 Status", ["TRACED", "CONTACTED", "RTP", "PTP", "PAYING", "UNREACHABLE", "REFUSED", "PROMISE_BROKEN", "OTHER"], key="trace_f_status")
        with fc4:
            f_touch = st.multiselect("📞 Method", ["CALL", "WHATSAPP", "SMS", "EMAIL", "VISIT", "VIDEO_CALL", "OTHER"], key="trace_f_touch")
        
        # Filter row 2
        fc5, fc6, fc7, fc8 = st.columns(4)
        with fc5:
            date_from = st.date_input("📅 From Date", value=None, key="trace_from")
        with fc6:
            date_to = st.date_input("📅 To Date", value=None, key="trace_to")
        with fc7:
            f_party = st.multiselect("🎯 Party", ["DEBTOR_DIRECT", "COMPANY_HR", "COMPANY_FINANCE", "RELATIVE", "NEIGHBOR", "EMERGENCY_CONTACT", "OTHER"], key="trace_f_party")
        with fc8:
            limit_rows = st.number_input("📄 Limit", min_value=50, max_value=2000, value=500, step=50, key="trace_limit")

        # Build query
        q = """
        SELECT 
            Agreement_No, 
            tracer, 
            status, 
            party, 
            touch_type, 
            SUBSTR(notes, 1, 100) || CASE WHEN LENGTH(notes) > 100 THEN '...' ELSE '' END as notes_preview,
            touched_at, 
            created_by 
        FROM trace_results 
        WHERE 1=1
        """
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
        if f_touch:
            placeholders = ",".join(["?"] * len(f_touch))
            q += f" AND COALESCE(touch_type,'') IN ({placeholders})"
            params.extend(f_touch)
        if f_party:
            placeholders = ",".join(["?"] * len(f_party))
            q += f" AND COALESCE(party,'') IN ({placeholders})"
            params.extend(f_party)
        if date_from:
            q += " AND date(touched_at) >= date(?)"
            params.append(str(date_from))
        if date_to:
            q += " AND date(touched_at) <= date(?)"
            params.append(str(date_to))
        
        q += " ORDER BY touched_at DESC LIMIT ?"
        params.append(int(limit_rows))

        logs = fetchall(q, tuple(params))
        
        if logs:
            st.caption(f"📊 Menampilkan **{len(logs)}** trace records")
            df_logs = pd.DataFrame(logs)
            
            # Rename columns for better display
            df_logs.columns = ['Case ID', 'Tracer', 'Status', 'Party', 'Method', 'Notes Preview', 'Touched At', 'Created By']
            
            st.dataframe(
                df_logs, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Case ID": st.column_config.TextColumn("Case ID", width="medium"),
                    "Tracer": st.column_config.TextColumn("Tracer", width="medium"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "Party": st.column_config.TextColumn("Party", width="medium"),
                    "Method": st.column_config.TextColumn("Method", width="small"),
                    "Notes Preview": st.column_config.TextColumn("Notes", width="large"),
                    "Touched At": st.column_config.TextColumn("Touched At", width="medium"),
                    "Created By": st.column_config.TextColumn("Created By", width="medium"),
                }
            )
            
            # Export option
            if st.button("📥 Export to CSV", key="trace_export"):
                csv = df_logs.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"trace_results_{today.isoformat()}.csv",
                    mime="text/csv"
                )
        else:
            st.info("ℹ️ Tidak ada data sesuai filter. Coba ubah kriteria pencarian.")


    # --- Enriched & Lookup Tab ---
    with tabs[6]:
        st.markdown("### 🔍 Enriched Monitoring & Global Lookup")
        st.caption("360° view untuk setiap loan: tracking lengkap dari assignment hingga pembayaran")
        
        # Enhanced CSS
        st.markdown("""
        <style>
        .lookup-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 16px;
            color: white;
            margin-bottom: 20px;
        }
        .lookup-card h3 {
            margin: 0 0 10px 0;
            font-size: 18px;
        }
        .lookup-result {
            background: rgba(255,255,255,0.1);
            padding: 12px;
            border-radius: 8px;
            margin-top: 12px;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Main content: 2 columns layout
        left_col, right_col = st.columns([2.5, 1.5])
        
        with left_col:
            st.markdown("#### 📊 Enriched Monitoring")
            st.caption("Gabungan data dari multiple tables untuk monitoring komprehensif")
            
            # Enhanced Filters - Row 1
            fcol1, fcol2, fcol3, fcol4 = st.columns(4)
            with fcol1:
                f_ag = st.text_input("🔖 Case ID", key="en_ag", placeholder="Search...")
            with fcol2:
                f_nik = st.text_input("🆔 NIK", key="en_nik", placeholder="Search...")
            with fcol3:
                # Get tracer list
                tracers = [r['full_name'] for r in fetchall("SELECT DISTINCT COALESCE(full_name,name) AS full_name FROM users WHERE approved=1 AND role IN ('Tracer', 'Superuser', 'Supervisor') ORDER BY 1") if r.get('full_name')]
                f_tracer = st.selectbox("👤 Tracer", options=["(All)"] + tracers, index=0, key="en_tracer")
            with fcol4:
                # Get agent list
                agents = [r['full_name'] for r in fetchall("SELECT DISTINCT COALESCE(full_name,name) AS full_name FROM users WHERE approved=1 AND role IN ('Agent', 'Superuser', 'Supervisor') ORDER BY 1") if r.get('full_name')]
                f_agent = st.selectbox("🎯 Agent", options=["(All)"] + agents, index=0, key="en_agent")

            # Enhanced Filters - Row 2
            fcol5, fcol6, fcol7, fcol8 = st.columns(4)
            with fcol5:
                f_status = st.multiselect("📊 Trace Status", ["TRACED", "CONTACTED", "RTP", "PTP", "PAYING", "UNREACHABLE", "REFUSED", "PROMISE_BROKEN", "OTHER"], key="en_status")
            with fcol6:
                f_pay = st.selectbox("💰 Payment", ["All", "With Payment", "Without Payment", "Paid Off"], index=0, key="en_pay")
            with fcol7:
                ad_from = st.date_input("📅 Assigned From", value=None, key="en_ad_from")
            with fcol8:
                ad_to = st.date_input("📅 Assigned To", value=None, key="en_ad_to")

            # Build comprehensive query
            q_en = """
                SELECT 
                    a.Agreement_No,
                    a.Debtor_Name,
                    a.NIK_KTP,
                    a.Assigned_To AS tracer,
                    a.Masked_Company_Name AS company,
                    ag.Agent_Assigned_To AS agent,
                    ag.assigned_at,
                    ts.status AS latest_status,
                    ts.touched_at AS last_touch,
                    ar.agent_status,
                    ar.agent_ptp_amount,
                    ar.agent_ptp_date,
                    COALESCE(p.amount, 0) AS paid_total,
                    p.last_paid_date,
                    CASE 
                        WHEN COALESCE(p.amount, 0) > 0 THEN '✅ Paid'
                        WHEN ar.agent_status = 'PTP' THEN '⏳ PTP'
                        WHEN ts.status IN ('TRACED', 'CONTACTED') THEN '🔍 Traced'
                        WHEN ag.Agent_Assigned_To IS NOT NULL THEN '👤 Agent Assigned'
                        WHEN a.Assigned_To IS NOT NULL THEN '📋 Tracer Assigned'
                        ELSE '⚪ New'
                    END as pipeline_stage
                FROM assign_tracer a
                LEFT JOIN agent_assignments ag ON ag.Agreement_No = a.Agreement_No AND IFNULL(ag.active,1)=1
                LEFT JOIN (
                    SELECT tr1.Agreement_No, tr1.status, tr1.touched_at
                    FROM trace_results tr1
                    JOIN (SELECT Agreement_No, MAX(touched_at) mt FROM trace_results GROUP BY Agreement_No) t2
                    ON t2.Agreement_No = tr1.Agreement_No AND t2.mt = tr1.touched_at
                ) ts ON ts.Agreement_No = a.Agreement_No
                LEFT JOIN (
                    SELECT Agreement_No, agent_status, agent_ptp_amount, agent_ptp_date
                    FROM agent_results
                    WHERE id IN (SELECT MAX(id) FROM agent_results GROUP BY Agreement_No)
                ) ar ON ar.Agreement_No = a.Agreement_No
                LEFT JOIN (
                    SELECT Agreement_No, SUM(paid_amount) AS amount, MAX(paid_date) AS last_paid_date
                    FROM payments
                    GROUP BY Agreement_No
                ) p ON p.Agreement_No = a.Agreement_No
                WHERE 1=1
            """
            
            p_en = []
            if f_ag:
                q_en += " AND a.Agreement_No LIKE ?"
                p_en.append(f"%{f_ag}%")
            if f_nik:
                q_en += " AND COALESCE(a.NIK_KTP,'') LIKE ?"
                p_en.append(f"%{f_nik}%")
            if f_tracer and f_tracer != "(All)":
                q_en += " AND COALESCE(a.Assigned_To,'') = ?"
                p_en.append(f_tracer)
            if f_agent and f_agent != "(All)":
                q_en += " AND COALESCE(ag.Agent_Assigned_To,'') = ?"
                p_en.append(f_agent)
            if f_status:
                placeholders = ",".join(["?"] * len(f_status))
                q_en += f" AND COALESCE(ts.status,'') IN ({placeholders})"
                p_en.extend(f_status)
            if f_pay == "With Payment":
                q_en += " AND COALESCE(p.amount,0) > 0"
            elif f_pay == "Without Payment":
                q_en += " AND COALESCE(p.amount,0) = 0"
            elif f_pay == "Paid Off":
                q_en += " AND COALESCE(p.amount,0) >= 100000"  # Assume paid off threshold
            if ad_from:
                q_en += " AND DATE(ag.assigned_at) >= DATE(?)"
                p_en.append(str(ad_from))
            if ad_to:
                q_en += " AND DATE(ag.assigned_at) <= DATE(?)"
                p_en.append(str(ad_to))
            
            q_en += " ORDER BY ag.assigned_at DESC, a.id DESC LIMIT 500"

            rows_en = fetchall(q_en, tuple(p_en))
            
            if rows_en:
                df_en = pd.DataFrame(rows_en)
                st.caption(f"📊 Menampilkan **{len(df_en)}** enriched records")
                
                # Rename columns
                df_en.columns = [
                    'Case ID', 'Debtor', 'NIK', 'Tracer', 'Company', 'Agent', 
                    'Assigned At', 'Trace Status', 'Last Touch', 'Agent Status', 
                    'PTP Amount', 'PTP Date', 'Paid Total', 'Last Paid', 'Pipeline Stage'
                ]
                
                st.dataframe(
                    df_en,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Case ID": st.column_config.TextColumn("Case ID", width="medium"),
                        "Debtor": st.column_config.TextColumn("Debtor", width="medium"),
                        "NIK": st.column_config.TextColumn("NIK", width="small"),
                        "Tracer": st.column_config.TextColumn("Tracer", width="small"),
                        "Company": st.column_config.TextColumn("Company", width="medium"),
                        "Agent": st.column_config.TextColumn("Agent", width="small"),
                        "Assigned At": st.column_config.TextColumn("Assigned", width="small"),
                        "Trace Status": st.column_config.TextColumn("T.Status", width="small"),
                        "Last Touch": st.column_config.TextColumn("Last Touch", width="small"),
                        "Agent Status": st.column_config.TextColumn("A.Status", width="small"),
                        "PTP Amount": st.column_config.NumberColumn("PTP Amt", format="Rp %.0f"),
                        "PTP Date": st.column_config.TextColumn("PTP Date", width="small"),
                        "Paid Total": st.column_config.NumberColumn("Paid", format="Rp %.0f"),
                        "Last Paid": st.column_config.TextColumn("Last Paid", width="small"),
                        "Pipeline Stage": st.column_config.TextColumn("Stage", width="medium"),
                    }
                )
                
                # Export
                if st.button("📥 Export to CSV", key="enriched_export"):
                    csv = df_en.to_csv(index=False)
                    st.download_button(
                        label="Download Enriched Data",
                        data=csv,
                        file_name=f"enriched_monitoring_{today_wib().isoformat()}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("ℹ️ Tidak ada data sesuai filter.")

        with right_col:
            st.markdown("#### 🔎 Global Lookup Tools")
            
            # NIK Lookup
            with st.container():
                st.markdown("""
                <div class='lookup-card'>
                    <h3>🆔 NIK Lookup</h3>
                    <p style='font-size:13px; margin:0; opacity:0.9;'>Cari semua loan berdasarkan NIK</p>
                </div>
                """, unsafe_allow_html=True)
                
                nik_q = st.text_input("Masukkan NIK", key="monitor_nik_lookup", placeholder="16 digit NIK...")
                
                if nik_q and len(nik_q.strip()) >= 3:
                    nik_rows = fetchall("""
                        SELECT 
                            Agreement_No, 
                            Debtor_Name, 
                            NIK_KTP, 
                            Assigned_To as Tracer,
                            created_at as Assigned_Date
                        FROM assign_tracer 
                        WHERE NIK_KTP LIKE ? 
                        ORDER BY id DESC 
                        LIMIT 100
                    """, (f"%{nik_q.strip()}%",))
                    
                    if nik_rows:
                        df_nik = pd.DataFrame(nik_rows)
                        st.success(f"✅ Ditemukan **{len(df_nik)}** loan untuk NIK: `{nik_q}`")
                        st.dataframe(df_nik, use_container_width=True, hide_index=True)
                        
                        # Check if frozen
                        frozen_check = fetchone("SELECT id, reason FROM frozen_entities WHERE active=1 AND NIK_KTP LIKE ?", (f"%{nik_q.strip()}%",))
                        if frozen_check:
                            st.warning(f"⚠️ **FROZEN:** NIK ini di-freeze. Reason: {frozen_check.get('reason') or 'N/A'}")
                    else:
                        st.info("ℹ️ Tidak ditemukan loan untuk NIK ini.")
            
            st.markdown("---")
            
            # Case ID Lookup
            with st.container():
                st.markdown("""
                <div class='lookup-card' style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);'>
                    <h3>🔖 Case ID Lookup</h3>
                    <p style='font-size:13px; margin:0; opacity:0.9;'>Detail lengkap per Case ID</p>
                </div>
                """, unsafe_allow_html=True)
                
                agr_q = st.text_input("Masukkan Case ID", key="monitor_agr_lookup", placeholder="e.g., AGR2025001")
                
                if agr_q and len(agr_q.strip()) >= 3:
                    # Get comprehensive data
                    agr_detail = fetchone("""
                        SELECT 
                            a.Agreement_No,
                            a.Debtor_Name,
                            a.NIK_KTP,
                            a.Assigned_To as Tracer,
                            ag.Agent_Assigned_To as Agent,
                            ar.agent_status,
                            COALESCE(p.total_paid, 0) as Total_Paid,
                            (SELECT COUNT(*) FROM trace_results WHERE Agreement_No = a.Agreement_No) as Trace_Count
                        FROM assign_tracer a
                        LEFT JOIN agent_assignments ag ON ag.Agreement_No = a.Agreement_No
                        LEFT JOIN agent_results ar ON ar.Agreement_No = a.Agreement_No
                        LEFT JOIN (SELECT Agreement_No, SUM(paid_amount) as total_paid FROM payments GROUP BY Agreement_No) p 
                            ON p.Agreement_No = a.Agreement_No
                        WHERE a.Agreement_No LIKE ?
                        LIMIT 1
                    """, (f"%{agr_q.strip()}%",))
                    
                    if agr_detail:
                        st.success(f"✅ Case ID: **{agr_detail['Agreement_No']}**")
                        
                        # Display details
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("👤 Debtor", agr_detail['Debtor_Name'] or '-')
                            st.metric("🆔 NIK", agr_detail['NIK_KTP'] or '-')
                        with col2:
                            st.metric("👥 Tracer", agr_detail['Tracer'] or '-')
                            st.metric("🎯 Agent", agr_detail['Agent'] or '-')
                        
                        col3, col4 = st.columns(2)
                        with col3:
                            st.metric("📊 Status", agr_detail['agent_status'] or 'N/A')
                            st.metric("💰 Total Paid", f"Rp {agr_detail['Total_Paid']:,.0f}")
                        with col4:
                            st.metric("🔍 Traces", agr_detail['Trace_Count'])
                        
                        # Check frozen
                        frozen_agr = fetchone("SELECT reason FROM frozen_entities WHERE active=1 AND Agreement_No LIKE ?", (f"%{agr_q.strip()}%",))
                        if frozen_agr:
                            st.error(f"🔒 **FROZEN:** {frozen_agr.get('reason') or 'No reason specified'}")
                    else:
                        st.info("ℹ️ Case ID tidak ditemukan.")
    
    # --- Company Library Tab ---
    with tabs[8]:
        st.subheader("🏢 Company Decode Library")
        st.markdown("""
        <div style='background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%); 
                    padding: 16px; border-radius: 12px; border-left: 4px solid #6366F1; margin-bottom: 20px;'>
            <p style='margin: 0; font-size: 14px;'>
                📚 <b>Library ini digunakan untuk decode nama company yang ter-mask.</b><br>
                Contoh: <code>VI****** CA** IN******* PT</code> → <code>VICTORIA CARE INDONESIA PT</code>
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Sub-tabs for different operations
        lib_tabs = st.tabs(["📋 View Library", "➕ Add/Edit Manual", "📤 Upload CSV/Excel", "🗑️ Delete Entry"])
        
        # --- View Library Tab ---
        with lib_tabs[0]:
            st.markdown("### 📋 Current Library")
            
            # Search filter
            search_company = st.text_input("🔍 Search (Masked or Decoded)", key="lib_search", placeholder="Type to filter...")
            
            # Fetch all entries
            if search_company:
                lib_rows = fetchall("""
                    SELECT id, masked_name, canonical_name, mapping_notes, created_at 
                    FROM masked_companies 
                    WHERE masked_name LIKE ? OR canonical_name LIKE ?
                    ORDER BY masked_name ASC
                """, (f"%{search_company}%", f"%{search_company}%"))
            else:
                lib_rows = fetchall("""
                    SELECT id, masked_name, canonical_name, mapping_notes, created_at 
                    FROM masked_companies 
                    ORDER BY masked_name ASC
                """)
            
            if lib_rows:
                st.success(f"✅ Total entries: **{len(lib_rows)}**")
                df_lib = pd.DataFrame(lib_rows)
                # Rename columns for display
                df_lib_display = df_lib.rename(columns={
                    'id': 'ID',
                    'masked_name': 'Masked Company',
                    'canonical_name': 'Decoded Company',
                    'mapping_notes': 'Notes',
                    'created_at': 'Created At'
                })
                st.dataframe(df_lib_display, use_container_width=True, hide_index=True)
                
                # Download as CSV
                csv = df_lib_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Library as CSV",
                    data=csv,
                    file_name=f"company_library_{today_wib().isoformat()}.csv",
                    mime="text/csv"
                )
            else:
                st.info("📭 Library masih kosong. Tambahkan entry melalui tab 'Add/Edit Manual' atau 'Upload CSV/Excel'.")
        
        # --- Add/Edit Manual Tab ---
        with lib_tabs[1]:
            st.markdown("### ➕ Add or Edit Entry")
            
            # Check if editing existing entry
            edit_mode = st.checkbox("✏️ Edit existing entry", key="lib_edit_mode")
            
            if edit_mode:
                # Select entry to edit
                all_masked = fetchall("SELECT id, masked_name, canonical_name FROM masked_companies ORDER BY masked_name ASC")
                if not all_masked:
                    st.warning("⚠️ No entries to edit. Add new entry first.")
                else:
                    options_dict = {f"{row['masked_name']} → {row['canonical_name']}": row for row in all_masked}
                    selected_entry = st.selectbox("Select entry to edit:", list(options_dict.keys()), key="lib_select_edit")
                    
                    if selected_entry:
                        entry_data = options_dict[selected_entry]
                        st.info(f"Editing ID: {entry_data['id']}")
                        
                        masked_input = st.text_input("Masked Company Name", value=entry_data['masked_name'], key="lib_edit_masked")
                        decoded_input = st.text_input("Decoded Company Name", value=entry_data['canonical_name'], key="lib_edit_decoded")
                        notes_input = st.text_area("Notes (optional)", value="", key="lib_edit_notes")
                        
                        if st.button("💾 Update Entry", type="primary", key="lib_update_btn"):
                            if masked_input.strip() and decoded_input.strip():
                                try:
                                    execute("""
                                        UPDATE masked_companies 
                                        SET masked_name = ?, canonical_name = ?, mapping_notes = ?
                                        WHERE id = ?
                                    """, (masked_input.strip(), decoded_input.strip(), notes_input.strip(), entry_data['id']))
                                    st.success(f"✅ Entry updated successfully!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error updating entry: {e}")
                            else:
                                st.error("❌ Both Masked and Decoded names are required!")
            else:
                # Add new entry
                st.markdown("**Add New Company Mapping:**")
                
                masked_input = st.text_input("Masked Company Name", placeholder="e.g., VI****** CA** IN******* PT", key="lib_add_masked")
                decoded_input = st.text_input("Decoded Company Name", placeholder="e.g., VICTORIA CARE INDONESIA PT", key="lib_add_decoded")
                notes_input = st.text_area("Notes (optional)", placeholder="Any additional information...", key="lib_add_notes")
                
                if st.button("➕ Add to Library", type="primary", key="lib_add_btn"):
                    if masked_input.strip() and decoded_input.strip():
                        try:
                            # Check if masked name already exists
                            existing = fetchone("SELECT id FROM masked_companies WHERE masked_name = ?", (masked_input.strip(),))
                            if existing:
                                st.error(f"❌ Masked name already exists! Use Edit mode to update.")
                            else:
                                execute("""
                                    INSERT INTO masked_companies (masked_name, canonical_name, mapping_notes)
                                    VALUES (?, ?, ?)
                                """, (masked_input.strip(), decoded_input.strip(), notes_input.strip()))
                                st.success(f"✅ Entry added successfully!")
                                st.balloons()
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error adding entry: {e}")
                    else:
                        st.error("❌ Both Masked and Decoded names are required!")
        
        # --- Upload CSV/Excel Tab ---
        with lib_tabs[2]:
            st.markdown("### 📤 Upload CSV/Excel Library")
            st.markdown("""
            <div style='background: #FEF3C7; padding: 12px; border-radius: 8px; border-left: 4px solid #F59E0B; margin-bottom: 16px;'>
                <p style='margin: 0; font-size: 13px;'>
                    ⚠️ <b>Format file:</b> Harus memiliki kolom <code>Masked</code> dan <code>Decoded</code><br>
                    📝 Kolom opsional: <code>Notes</code><br>
                    🔄 Entry yang sudah ada akan di-update (replace), entry baru akan ditambahkan.
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Download Template Button
            st.markdown("#### 📥 Download Template")
            template_df = pd.DataFrame({
                'Masked': ['VI****** CA** IN******* PT', 'AD****** FI******* PT'],
                'Decoded': ['VICTORIA CARE INDONESIA PT', 'ADITAMA FINANSIAL PT'],
                'Notes': ['Example entry 1', 'Example entry 2']
            })
            
            # Convert to Excel
            template_buffer = io.BytesIO()
            with pd.ExcelWriter(template_buffer, engine='openpyxl') as writer:
                template_df.to_excel(writer, index=False, sheet_name='Company Library')
            template_buffer.seek(0)
            
            st.download_button(
                label="📥 Download Template Excel",
                data=template_buffer,
                file_name="company_library_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Download template Excel untuk upload Company Library"
            )
            
            st.markdown("---")
            
            # Display upload result message if any
            _upload_lib_result = st.session_state.pop('lib_upload_result', None)
            if _upload_lib_result:
                st.success(_upload_lib_result)
            
            uploaded_lib = st.file_uploader("Upload CSV/Excel file", type=["csv", "xlsx"], key="lib_upload_file")
            
            if uploaded_lib is not None:
                try:
                    # Read file
                    if uploaded_lib.name.lower().endswith(".csv"):
                        df_upload = pd.read_csv(uploaded_lib)
                    else:
                        df_upload = pd.read_excel(uploaded_lib)
                    
                    # Normalize column names
                    df_upload.columns = [c.strip().lower() for c in df_upload.columns]
                    
                    # Check required columns
                    if 'masked' not in df_upload.columns or 'decoded' not in df_upload.columns:
                        st.error("❌ File harus memiliki kolom 'Masked' dan 'Decoded'!")
                    else:
                        st.info(f"📊 File detected: {uploaded_lib.name} — {len(df_upload):,} rows")
                        
                        # Preview
                        st.dataframe(df_upload.head(10), use_container_width=True, hide_index=True)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            do_upload = st.button("📤 Upload to Library", type="primary", key="lib_commit_upload")
                        with col2:
                            st.button("🚫 Cancel", key="lib_cancel_upload")
                        
                        if do_upload:
                            try:
                                uploaded_lib.seek(0)
                            except Exception:
                                pass
                            
                            # Re-read
                            try:
                                if uploaded_lib.name.lower().endswith(".csv"):
                                    df_full = pd.read_csv(uploaded_lib)
                                else:
                                    df_full = pd.read_excel(uploaded_lib)
                                df_full.columns = [c.strip().lower() for c in df_full.columns]
                            except Exception as e:
                                st.error(f"❌ Error reading file: {e}")
                                df_full = None
                            
                            if df_full is not None:
                                added = 0
                                updated = 0
                                skipped = 0
                                
                                for idx, row in df_full.iterrows():
                                    try:
                                        masked = str(row.get('masked', '')).strip()
                                        decoded = str(row.get('decoded', '')).strip()
                                        notes = str(row.get('notes', '')).strip()
                                        
                                        if not masked or not decoded or masked == 'nan' or decoded == 'nan':
                                            skipped += 1
                                            continue
                                        
                                        # Check if exists
                                        existing = fetchone("SELECT id FROM masked_companies WHERE masked_name = ?", (masked,))
                                        
                                        if existing:
                                            # Update
                                            execute("""
                                                UPDATE masked_companies 
                                                SET canonical_name = ?, mapping_notes = ?
                                                WHERE masked_name = ?
                                            """, (decoded, notes, masked))
                                            updated += 1
                                        else:
                                            # Insert
                                            execute("""
                                                INSERT INTO masked_companies (masked_name, canonical_name, mapping_notes)
                                                VALUES (?, ?, ?)
                                            """, (masked, decoded, notes))
                                            added += 1
                                    except Exception as e:
                                        skipped += 1
                                
                                st.session_state['lib_upload_result'] = f"✅ Upload complete! Added: {added}, Updated: {updated}, Skipped: {skipped}"
                                
                                # Audit log
                                u = current_user() or {}
                                try:
                                    execute(
                                        "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                        (u.get('id') if u else None, "UPLOAD_COMPANY_LIBRARY", 
                                         f"Uploaded company library: {added} added, {updated} updated, {skipped} skipped from '{uploaded_lib.name}'")
                                    )
                                except Exception:
                                    pass
                                
                                st.rerun()
                except Exception as e:
                    st.error(f"❌ Error processing file: {e}")
        
        # --- Delete Entry Tab ---
        with lib_tabs[3]:
            st.markdown("### 🗑️ Delete Entry")
            st.warning("⚠️ **Warning:** Deleting an entry is permanent and cannot be undone!")
            
            # Select entry to delete
            all_entries = fetchall("SELECT id, masked_name, canonical_name FROM masked_companies ORDER BY masked_name ASC")
            
            if not all_entries:
                st.info("📭 No entries to delete.")
            else:
                options_delete = {f"{row['masked_name']} → {row['canonical_name']}": row for row in all_entries}
                selected_delete = st.selectbox("Select entry to delete:", list(options_delete.keys()), key="lib_select_delete")
                
                if selected_delete:
                    entry_to_delete = options_delete[selected_delete]
                    
                    st.error(f"""
                    **You are about to delete:**
                    - ID: {entry_to_delete['id']}
                    - Masked: {entry_to_delete['masked_name']}
                    - Decoded: {entry_to_delete['canonical_name']}
                    """)
                    
                    confirm_delete = st.checkbox("I confirm I want to delete this entry", key="lib_confirm_delete")
                    
                    if confirm_delete:
                        if st.button("🗑️ DELETE ENTRY", type="primary", key="lib_delete_btn"):
                            try:
                                execute("DELETE FROM masked_companies WHERE id = ?", (entry_to_delete['id'],))
                                st.success("✅ Entry deleted successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error deleting entry: {e}")
    
    # --- Data Migration Tab ---
    with tabs[9]:
        st.header("📦 Data Migration - Import Historical Data")
        st.markdown("""
        Gunakan tab ini untuk migrasi data dari Excel lama ke sistem baru.
        
        **PENTING:** `Agreement_No` = `Case_ID` (sama, hanya beda nama kolom)
        
        **Cara menggunakan:**
        1. Download template Excel yang sesuai
        2. Isi data historical Anda ke template (jangan ubah nama kolom!)
        3. Upload file Excel
        4. Preview data dan konfirmasi import
        """)
        
        migration_tabs = st.tabs(["📋 Supervisor Data", "🔍 Tracer Data", "👤 Agent Results", "💰 Payment Data"])
        
        # --- Supervisor Data Migration ---
        with migration_tabs[0]:
            st.subheader("Import Supervisor Data (Master Case List)")
            
            st.markdown("""
            **Template kolom yang diperlukan:**
            - `Case_ID` (wajib, unique identifier untuk case)
            - `Customer_name` (wajib)
            - `Virtual_Account_Number` (wajib)
            - `DPD`, `Principle_Outstanding`, `Phone_Number_1`, dll.
            
            **Kolom wajib minimal:** Case_ID, Customer_name, Virtual_Account_Number
            
            ℹ️ **Note:** `Case_ID` di sini akan menjadi `Agreement_No` di tabel lainnya
            """)
            
            # Generate template button
            if st.button("📥 Download Template Excel - Supervisor Data", key="sup_template"):
                template_cols = [
                    "DT", "Lending_Entity", "Date", "Case_ID", "Task_ID", "Customer_name", 
                    "email", "Gender", "Customer_Occupation", "DPD", "Principle_Outstanding",
                    "Principal_Overdue_CURR", "Interest_Overdue_CURR", "Last_Late_Fee", "Return_Date",
                    "Detail", "Loan_Type", "Third_Uid", "Product", "Home_Address", "Province", "City",
                    "Street", "RoomNumber", "Postcode", "Assignment_Date", "Withdrawal_Date",
                    "Phone_Number_1", "Phone_Number_2", 
                    "Contact_Type_1", "Contact_Name_1", "Contact_Phone_1",
                    "Contact_Type_2", "Contact_Name_2", "Contact_Phone_2",
                    "Contact_Type_3", "Contact_Name_3", "Contact_Phone_3",
                    "Contact_Type_4", "Contact_Name_4", "Contact_Phone_4",
                    "Contact_Type_5", "Contact_Name_5", "Contact_Phone_5",
                    "Contact_Type_6", "Contact_Name_6", "Contact_Phone_6",
                    "Contact_Type_7", "Contact_Name_7", "Contact_Phone_7",
                    "Contact_Type_8", "Contact_Name_8", "Contact_Phone_8",
                    "Total_debt_in_third_party", "Repayment_on_third_Party", "Remaining_Loan_on_third_Party",
                    "Virtual_Account_Number", "NIK_KTP"
                ]
                template_df = pd.DataFrame(columns=template_cols)
                # Add sample row
                template_df.loc[0] = [""] * len(template_cols)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    template_df.to_excel(writer, index=False, sheet_name='Supervisor_Data')
                output.seek(0)
                
                st.download_button(
                    label="💾 Download Template_Supervisor_Data.xlsx",
                    data=output,
                    file_name="Template_Supervisor_Data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="sup_template_download"
                )
            
            st.divider()
            
            # Upload file
            uploaded_sup = st.file_uploader("Upload Excel Supervisor Data", type=["xlsx", "xls"], key="sup_upload_migration")
            
            if uploaded_sup:
                try:
                    df_sup = pd.read_excel(uploaded_sup)
                    st.success(f"✅ File berhasil dibaca! Total baris: {len(df_sup)}")
                    
                    # Validate required columns
                    required_cols = ["Case_ID", "Customer_name", "Virtual_Account_Number"]
                    missing_cols = [col for col in required_cols if col not in df_sup.columns]
                    
                    if missing_cols:
                        st.error(f"❌ Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
                    else:
                        # Preview data
                        st.dataframe(df_sup.head(20), use_container_width=True)
                        
                        # Confirm import
                        st.warning(f"⚠️ Anda akan mengimport {len(df_sup)} baris data ke tabel supervisor_data")
                        
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            if st.button("✅ Konfirmasi Import", type="primary", key="sup_confirm_import"):
                                try:
                                    imported_count = 0
                                    skipped_count = 0
                                    inserted_ids = []
                                    
                                    for idx, row in df_sup.iterrows():
                                        # Check if Case_ID already exists
                                        case_id = str(row.get('Case_ID', '')).strip()
                                        if not case_id:
                                            skipped_count += 1
                                            continue
                                        
                                        existing = fetchone("SELECT id FROM supervisor_data WHERE Case_ID = ?", (case_id,))
                                        if existing:
                                            skipped_count += 1
                                            continue
                                        
                                        # Build insert query dynamically based on available columns
                                        cols_to_insert = []
                                        vals_to_insert = []
                                        
                                        for col in df_sup.columns:
                                            if col in ["DT", "Lending_Entity", "Date", "Case_ID", "Task_ID", "Customer_name", 
                                                      "email", "Gender", "Customer_Occupation", "DPD", "Principle_Outstanding",
                                                      "Principal_Overdue_CURR", "Interest_Overdue_CURR", "Last_Late_Fee", 
                                                      "Return_Date", "Detail", "Loan_Type", "Third_Uid", "Product", 
                                                      "Home_Address", "Province", "City", "Street", "RoomNumber", "Postcode",
                                                      "Assignment_Date", "Withdrawal_Date", "Phone_Number_1", "Phone_Number_2",
                                                      "Contact_Type_1", "Contact_Name_1", "Contact_Phone_1",
                                                      "Contact_Type_2", "Contact_Name_2", "Contact_Phone_2",
                                                      "Contact_Type_3", "Contact_Name_3", "Contact_Phone_3",
                                                      "Contact_Type_4", "Contact_Name_4", "Contact_Phone_4",
                                                      "Contact_Type_5", "Contact_Name_5", "Contact_Phone_5",
                                                      "Contact_Type_6", "Contact_Name_6", "Contact_Phone_6",
                                                      "Contact_Type_7", "Contact_Name_7", "Contact_Phone_7",
                                                      "Contact_Type_8", "Contact_Name_8", "Contact_Phone_8",
                                                      "Total_debt_in_third_party", "Repayment_on_third_Party", 
                                                      "Remaining_Loan_on_third_Party", "Virtual_Account_Number", "NIK_KTP"]:
                                                cols_to_insert.append(col)
                                                val = row.get(col)
                                                # Convert NaN to None
                                                if pd.isna(val):
                                                    vals_to_insert.append(None)
                                                else:
                                                    vals_to_insert.append(str(val).strip() if val else None)
                                        
                                        if cols_to_insert:
                                            query = f"INSERT INTO supervisor_data ({', '.join(cols_to_insert)}) VALUES ({', '.join(['?'] * len(cols_to_insert))})"
                                            last_id = execute(query, tuple(vals_to_insert))
                                            imported_count += 1
                                            # record inserted id for undo
                                            try:
                                                inserted_ids.append(int(last_id))
                                            except Exception:
                                                pass
                                    
                                    st.success(f"✅ Import selesai! Imported: {imported_count}, Skipped (duplicate/empty): {skipped_count}")
                                    st.balloons()
                                    # Record migration history for undo
                                    try:
                                        u = current_user() or {}
                                        if imported_count > 0:
                                            hist_id = execute(
                                                "INSERT INTO migration_history (operation_type, target_table, affected_ids, source_file, user_id) VALUES (?,?,?,?,?)",
                                                (
                                                    'SUPERVISOR_IMPORT',
                                                    'supervisor_data',
                                                    json.dumps(inserted_ids),
                                                    getattr(uploaded_sup, 'name', 'Supervisor_Migration'),
                                                    u.get('id') if u else None,
                                                ),
                                            )
                                            if hist_id:
                                                if st.button("↩️ Undo Import (Supervisor)", key=f"undo_sup_{hist_id}"):
                                                    ok, msg = undo_migration(hist_id)
                                                    if ok:
                                                        st.success(msg)
                                                        st.rerun()
                                                    else:
                                                        st.error(msg)
                                    except Exception:
                                        pass
                                except Exception as e:
                                    st.error(f"❌ Error saat import: {e}")
                        
                        with col2:
                            if st.button("🔄 Reset Upload", key="sup_reset_upload"):
                                st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Error membaca file: {e}")
        
        # --- Tracer Data Migration ---
        with migration_tabs[1]:
            st.subheader("Import Tracer Data (Employment Update)")
            
            st.markdown("""
            **Template kolom yang diperlukan:**
            - `TRC_Code` (opsional, kode tracer)
            - `Agreement_No` (wajib, **sama dengan Case_ID** di Supervisor Data)
            - `Debtor_Name` (wajib)
            - `NIK_KTP`, `EMPLOYMENT_UPDATE`, `EMPLOYER`, dll.
            
            **Kolom wajib minimal:** Agreement_No, Debtor_Name
            
            ℹ️ **Note:** `Agreement_No` = `Case_ID` (pastikan isi sesuai dengan Case_ID di Supervisor Data)
            """)
            
            # Generate template button
            if st.button("📥 Download Template Excel - Tracer Data", key="tracer_template"):
                template_cols = [
                    "TRC_Code", "Agreement_No", "Debtor_Name", "NIK_KTP", 
                    "EMPLOYMENT_UPDATE", "EMPLOYER", "Decoded_Company_Name",
                    "Debtor_Legal_Name", "Employee_Name", "Employee_ID_Number", 
                    "Debtor_Relation_to_Employee", "Assigned_To"
                ]
                template_df = pd.DataFrame(columns=template_cols)
                template_df.loc[0] = [""] * len(template_cols)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    template_df.to_excel(writer, index=False, sheet_name='Tracer_Data')
                output.seek(0)
                
                st.download_button(
                    label="💾 Download Template_Tracer_Data.xlsx",
                    data=output,
                    file_name="Template_Tracer_Data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="tracer_template_download"
                )
            
            st.divider()
            
            # Upload file
            uploaded_tracer = st.file_uploader("Upload Excel Tracer Data", type=["xlsx", "xls"], key="tracer_upload_migration")
            
            if uploaded_tracer:
                try:
                    df_tracer = pd.read_excel(uploaded_tracer)
                    st.success(f"✅ File berhasil dibaca! Total baris: {len(df_tracer)}")
                    
                    # Validate required columns
                    required_cols = ["Agreement_No", "Debtor_Name"]
                    missing_cols = [col for col in required_cols if col not in df_tracer.columns]
                    
                    if missing_cols:
                        st.error(f"❌ Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
                    else:
                        # Preview data
                        st.dataframe(df_tracer.head(20), use_container_width=True)
                        
                        # Confirm import
                        st.warning(f"⚠️ Anda akan mengimport {len(df_tracer)} baris data ke tabel assign_tracer")
                        
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            if st.button("✅ Konfirmasi Import", type="primary", key="tracer_confirm_import"):
                                try:
                                    imported_count = 0
                                    skipped_count = 0
                                    inserted_ids = []
                                    
                                    for idx, row in df_tracer.iterrows():
                                        agreement_no = str(row.get('Agreement_No', '')).strip()
                                        if not agreement_no:
                                            skipped_count += 1
                                            continue
                                        
                                        # Check if already exists
                                        existing = fetchone("SELECT id FROM assign_tracer WHERE Agreement_No = ?", (agreement_no,))
                                        if existing:
                                            skipped_count += 1
                                            continue
                                        
                                        # Auto-decode if EMPLOYER provided but no Decoded_Company_Name
                                        employer = str(row.get('EMPLOYER', '')).strip() if pd.notna(row.get('EMPLOYER')) else None
                                        decoded = str(row.get('Decoded_Company_Name', '')).strip() if pd.notna(row.get('Decoded_Company_Name')) else None
                                        
                                        if employer and not decoded:
                                            decoded = decode_company_name(employer)
                                        
                                        last_id = execute(
                                            """INSERT INTO assign_tracer 
                                            (TRC_Code, Agreement_No, Debtor_Name, NIK_KTP, EMPLOYMENT_UPDATE, EMPLOYER, 
                                             Decoded_Company_Name, Debtor_Legal_Name, Employee_Name, Employee_ID_Number, 
                                             Debtor_Relation_to_Employee, Assigned_To) 
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                            (
                                                str(row.get('TRC_Code', '')).strip() if pd.notna(row.get('TRC_Code')) else None,
                                                agreement_no,
                                                str(row.get('Debtor_Name', '')).strip() if pd.notna(row.get('Debtor_Name')) else None,
                                                str(row.get('NIK_KTP', '')).strip() if pd.notna(row.get('NIK_KTP')) else None,
                                                str(row.get('EMPLOYMENT_UPDATE', '')).strip() if pd.notna(row.get('EMPLOYMENT_UPDATE')) else None,
                                                employer,
                                                decoded,
                                                str(row.get('Debtor_Legal_Name', '')).strip() if pd.notna(row.get('Debtor_Legal_Name')) else None,
                                                str(row.get('Employee_Name', '')).strip() if pd.notna(row.get('Employee_Name')) else None,
                                                str(row.get('Employee_ID_Number', '')).strip() if pd.notna(row.get('Employee_ID_Number')) else None,
                                                str(row.get('Debtor_Relation_to_Employee', '')).strip() if pd.notna(row.get('Debtor_Relation_to_Employee')) else None,
                                                str(row.get('Assigned_To', '')).strip() if pd.notna(row.get('Assigned_To')) else None
                                            )
                                        )
                                        try:
                                            inserted_ids.append(int(last_id))
                                        except Exception:
                                            pass
                                        imported_count += 1
                                    
                                    st.success(f"✅ Import selesai! Imported: {imported_count}, Skipped (duplicate/empty): {skipped_count}")
                                    st.balloons()
                                    # record migration history for undo
                                    try:
                                        u = current_user() or {}
                                        if imported_count > 0:
                                            hist_id = execute(
                                                "INSERT INTO migration_history (operation_type, target_table, affected_ids, source_file, user_id) VALUES (?,?,?,?,?)",
                                                ('TRACER_IMPORT', 'assign_tracer', json.dumps(inserted_ids), getattr(uploaded_tracer, 'name', 'Tracer_Migration'), u.get('id') if u else None),
                                            )
                                            if hist_id:
                                                if st.button("↩️ Undo Import (Tracer)", key=f"undo_tracer_{hist_id}"):
                                                    ok, msg = undo_migration(hist_id)
                                                    if ok:
                                                        st.success(msg)
                                                        st.rerun()
                                                    else:
                                                        st.error(msg)
                                    except Exception:
                                        pass
                                except Exception as e:
                                    st.error(f"❌ Error saat import: {e}")
                        
                        with col2:
                            if st.button("🔄 Reset Upload", key="tracer_reset_upload"):
                                st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Error membaca file: {e}")
        
        # --- Agent Results Migration ---
        with migration_tabs[2]:
            st.subheader("Import Agent Results (Handling Outcome)")
            
            st.markdown("""
            **Template kolom yang diperlukan:**
            - `Agreement_No` (wajib, **sama dengan Case_ID**)
            - `agent` (nama agent)
            - `agent_status` (status handling: PTP, Broken Promise, dll)
            - `agent_ptp_amount`, `agent_ptp_date`, `agent_notes`
            
            **Kolom wajib minimal:** Agreement_No, agent
            
            ℹ️ **Note:** `Agreement_No` = `Case_ID` (gunakan Case_ID yang sama dari Supervisor Data)
            """)
            
            # Generate template button
            if st.button("📥 Download Template Excel - Agent Results", key="agent_template"):
                template_cols = [
                    "Agreement_No", "agent", "agent_status", "agent_ptp_amount", 
                    "agent_ptp_date", "agent_notes", "updated_at"
                ]
                template_df = pd.DataFrame(columns=template_cols)
                template_df.loc[0] = [""] * len(template_cols)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    template_df.to_excel(writer, index=False, sheet_name='Agent_Results')
                output.seek(0)
                
                st.download_button(
                    label="💾 Download Template_Agent_Results.xlsx",
                    data=output,
                    file_name="Template_Agent_Results.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="agent_template_download"
                )
            
            st.divider()
            
            # Upload file
            uploaded_agent = st.file_uploader("Upload Excel Agent Results", type=["xlsx", "xls"], key="agent_upload_migration")
            
            if uploaded_agent:
                try:
                    df_agent = pd.read_excel(uploaded_agent)
                    st.success(f"✅ File berhasil dibaca! Total baris: {len(df_agent)}")
                    
                    # Validate required columns
                    required_cols = ["Agreement_No", "agent"]
                    missing_cols = [col for col in required_cols if col not in df_agent.columns]
                    
                    if missing_cols:
                        st.error(f"❌ Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
                    else:
                        # Preview data
                        st.dataframe(df_agent.head(20), use_container_width=True)
                        
                        # Confirm import
                        st.warning(f"⚠️ Anda akan mengimport {len(df_agent)} baris data ke tabel agent_results")
                        
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            if st.button("✅ Konfirmasi Import", type="primary", key="agent_confirm_import"):
                                try:
                                    imported_count = 0
                                    inserted_ids = []

                                    for idx, row in df_agent.iterrows():
                                        agreement_no = str(row.get('Agreement_No', '')).strip()
                                        agent_name = str(row.get('agent', '')).strip()
                                        
                                        if not agreement_no or not agent_name:
                                            continue
                                        
                                        # Parse PTP amount (handle currency format)
                                        ptp_amount = row.get('agent_ptp_amount')
                                        if pd.notna(ptp_amount):
                                            try:
                                                ptp_amount = float(str(ptp_amount).replace(',', '').replace('Rp', '').strip())
                                            except:
                                                ptp_amount = None
                                        else:
                                            ptp_amount = None
                                        
                                        last_id = execute(
                                            """INSERT INTO agent_results 
                                            (Agreement_No, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes, updated_at) 
                                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                            (
                                                agreement_no,
                                                agent_name,
                                                str(row.get('agent_status', '')).strip() if pd.notna(row.get('agent_status')) else None,
                                                ptp_amount,
                                                str(row.get('agent_ptp_date', '')).strip() if pd.notna(row.get('agent_ptp_date')) else None,
                                                str(row.get('agent_notes', '')).strip() if pd.notna(row.get('agent_notes')) else None,
                                                str(row.get('updated_at', '')).strip() if pd.notna(row.get('updated_at')) else None
                                            )
                                        )
                                        try:
                                            inserted_ids.append(int(last_id))
                                        except Exception:
                                            pass
                                        imported_count += 1
                                    
                                    st.success(f"✅ Import selesai! Total imported: {imported_count}")
                                    st.balloons()
                                    try:
                                        u = current_user() or {}
                                        if imported_count > 0:
                                            hist_id = execute(
                                                "INSERT INTO migration_history (operation_type, target_table, affected_ids, source_file, user_id) VALUES (?,?,?,?,?)",
                                                ('AGENT_RESULTS_IMPORT', 'agent_results', json.dumps(inserted_ids), getattr(uploaded_agent, 'name', 'Agent_Results_Migration'), u.get('id') if u else None),
                                            )
                                            if hist_id:
                                                if st.button("↩️ Undo Import (Agent Results)", key=f"undo_agentres_{hist_id}"):
                                                    ok, msg = undo_migration(hist_id)
                                                    if ok:
                                                        st.success(msg)
                                                        st.rerun()
                                                    else:
                                                        st.error(msg)
                                    except Exception:
                                        pass
                                except Exception as e:
                                    st.error(f"❌ Error saat import: {e}")
                        
                        with col2:
                            if st.button("🔄 Reset Upload", key="agent_reset_upload"):
                                st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Error membaca file: {e}")
        
        # --- Payment Data Migration ---
        with migration_tabs[3]:
            st.subheader("Import Payment Data")
            
            st.markdown("""
            **Template kolom yang diperlukan:**
            - `Agreement_No` (wajib, **sama dengan Case_ID**)
            - `paid_amount` (jumlah pembayaran, wajib)
            - `paid_date` (tanggal bayar, wajib)
            - `status` (opsional: Confirmed/Pending, default: "Confirmed")
            - `source_file` (opsional: nama file sumber, default: "Migration")
            - `uploaded_by` (opsional: nama agent/user yang upload)
            
            **Kolom wajib minimal:** Agreement_No, paid_amount, paid_date
            
            ℹ️ **Note:** `Agreement_No` = `Case_ID` (gunakan Case_ID yang sama dari Supervisor Data)
            
            ---
            
            **📝 Penjelasan Kolom `uploaded_by`:**
            
            Kolom ini untuk tracking siapa yang upload/input data payment.
            
            **Cara mengisi:**
            - Isi dengan **Full Name** agent yang terkait (contoh: "John Doe", "Jane Smith")
            - Atau kosongkan, maka otomatis terisi dengan nama user yang sedang login saat import
            - Jika ada data dari `agent_results`, kolom `agent` akan diprioritaskan di Payment Recap
            
            **Contoh:**
            ```
            Agreement_No | paid_amount | paid_date  | uploaded_by
            45044479     | 5000000     | 2025-11-20 | Budi Santoso
            45044480     | 3000000     | 2025-11-21 | (kosong, akan terisi otomatis)
            ```
            
            ---
            
            **✅ Data yang TERISI OTOMATIS saat tampilan Payment Recap:**
            
            Sistem akan otomatis mengambil data berikut dari tabel lain berdasarkan `Agreement_No`:
            
            1. **Customer Name** → Dari tabel `supervisor_data` (Customer_name)
            2. **Status/Skema Pelunasan** → Dari tabel `agent_results` (agent_status: PTP, Full Payment, dll)
            3. **PAID OFF Status** → Dari tabel `supervisor_data` (Paid_Off: YES/NO)
            4. **Assigned Agent** → Dari tabel `agent_results` (agent) atau `uploaded_by` sebagai fallback
            5. **Product** → Dari tabel `supervisor_data` (Product)
            
            Anda **TIDAK PERLU** mengisi kolom-kolom tersebut di Excel, cukup isi:
            - Agreement_No (Case_ID)
            - paid_amount
            - paid_date
            
            Sistem akan otomatis JOIN dan enrichment data saat ditampilkan! 🎯
            """)
            
            # Generate template button
            if st.button("📥 Download Template Excel - Payment Data", key="payment_template"):
                template_cols = [
                    "Agreement_No", "paid_amount", "paid_date", "status", 
                    "source_file", "uploaded_by"
                ]
                template_df = pd.DataFrame(columns=template_cols)
                template_df.loc[0] = [""] * len(template_cols)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    template_df.to_excel(writer, index=False, sheet_name='Payment_Data')
                output.seek(0)
                
                st.download_button(
                    label="💾 Download Template_Payment_Data.xlsx",
                    data=output,
                    file_name="Template_Payment_Data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="payment_template_download"
                )
            
            st.divider()
            
            # Upload file
            uploaded_payment = st.file_uploader("Upload Excel Payment Data", type=["xlsx", "xls"], key="payment_upload_migration")
            
            if uploaded_payment:
                try:
                    df_payment = pd.read_excel(uploaded_payment)
                    st.success(f"✅ File berhasil dibaca! Total baris: {len(df_payment)}")
                    
                    # Validate required columns
                    required_cols = ["Agreement_No", "paid_amount", "paid_date"]
                    missing_cols = [col for col in required_cols if col not in df_payment.columns]
                    
                    if missing_cols:
                        st.error(f"❌ Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
                    else:
                        # Preview data
                        st.dataframe(df_payment.head(20), use_container_width=True)
                        
                        # Confirm import
                        st.warning(f"⚠️ Anda akan mengimport {len(df_payment)} baris data ke tabel payments")
                        
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            if st.button("✅ Konfirmasi Import", type="primary", key="payment_confirm_import"):
                                try:
                                    imported_count = 0
                                    u = current_user()
                                    uploader_name = u.get('full_name') if u else 'Migration'
                                    
                                    inserted_ids = []
                                    for idx, row in df_payment.iterrows():
                                        agreement_no = str(row.get('Agreement_No', '')).strip()
                                        
                                        if not agreement_no:
                                            continue
                                        
                                        # Parse paid_amount
                                        paid_amt = row.get('paid_amount')
                                        if pd.notna(paid_amt):
                                            try:
                                                paid_amt = float(str(paid_amt).replace(',', '').replace('Rp', '').strip())
                                            except:
                                                continue
                                        else:
                                            continue
                                        
                                        last_id = execute(
                                            """INSERT INTO payments 
                                            (Agreement_No, paid_amount, paid_date, status, source_file, uploaded_by) 
                                            VALUES (?, ?, ?, ?, ?, ?)""",
                                            (
                                                agreement_no,
                                                paid_amt,
                                                str(row.get('paid_date', '')).strip() if pd.notna(row.get('paid_date')) else None,
                                                str(row.get('status', 'Confirmed')).strip() if pd.notna(row.get('status')) else 'Confirmed',
                                                str(row.get('source_file', 'Migration')).strip() if pd.notna(row.get('source_file')) else 'Migration',
                                                str(row.get('uploaded_by', uploader_name)).strip() if pd.notna(row.get('uploaded_by')) else uploader_name
                                            )
                                        )
                                        try:
                                            inserted_ids.append(int(last_id))
                                        except Exception:
                                            pass
                                        imported_count += 1
                                    
                                    st.success(f"✅ Import selesai! Total imported: {imported_count}")
                                    st.balloons()
                                    # record migration history for undo
                                    try:
                                        u = current_user() or {}
                                        if imported_count > 0:
                                            hist_id = execute(
                                                "INSERT INTO migration_history (operation_type, target_table, affected_ids, source_file, user_id) VALUES (?,?,?,?,?)",
                                                ('PAYMENT_IMPORT', 'payments', json.dumps(inserted_ids), getattr(uploaded_payment, 'name', 'Payment_Migration'), u.get('id') if u else None),
                                            )
                                            if hist_id:
                                                if st.button("↩️ Undo Import (Payments)", key=f"undo_pay_{hist_id}"):
                                                    ok, msg = undo_migration(hist_id)
                                                    if ok:
                                                        st.success(msg)
                                                        st.rerun()
                                                    else:
                                                        st.error(msg)
                                    except Exception:
                                        pass
                                except Exception as e:
                                    st.error(f"❌ Error saat import: {e}")
                        
                        with col2:
                            if st.button("🔄 Reset Upload", key="payment_reset_upload"):
                                st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Error membaca file: {e}")
    
    # Close database connection
    conn.close()

def page_tracer():
    require_roles(("Superuser", "Supervisor", "Tracer"))
    u = current_user()
    user_role = u.get('role') if u else None
    tracer_name = (u.get('full_name') or u.get('name')) if u else None
    st.title("Tracer Menu")
    if not tracer_name:
        st.error("Tidak dapat menentukan nama tracer. Silakan login ulang.")
        return
    
    # Jika Supervisor/Superuser: tampilkan semua assignment
    # Jika Tracer: hanya tampilkan assignment untuk dirinya sendiri
    if user_role in ("Superuser", "Supervisor"):
        st.caption(f"Mode: **{user_role}** — Melihat semua assignment tracer")
        rows = fetchall(
            """
            SELECT at.id, at.TRC_Code, at.Agreement_No, at.Debtor_Name, at.NIK_KTP, 
                   at.EMPLOYMENT_UPDATE, at.EMPLOYER, at.Decoded_Company_Name, at.Debtor_Legal_Name, 
                   at.Employee_Name, at.Employee_ID_Number, at.Debtor_Relation_to_Employee, 
                   at.Assigned_To, at.created_at,
                   sd.Remarks_Suggested_NIK_Prospect
            FROM assign_tracer at
            LEFT JOIN supervisor_data sd ON at.Agreement_No = sd.Case_ID
            ORDER BY at.id DESC LIMIT 500
            """
        )
    else:
        st.caption(f"Assignment untuk: {tracer_name}")
        rows = fetchall(
            """
            SELECT at.id, at.TRC_Code, at.Agreement_No, at.Debtor_Name, at.NIK_KTP, 
                   at.EMPLOYMENT_UPDATE, at.EMPLOYER, at.Decoded_Company_Name, at.Debtor_Legal_Name, 
                   at.Employee_Name, at.Employee_ID_Number, at.Debtor_Relation_to_Employee, 
                   at.Assigned_To, at.created_at,
                   sd.Remarks_Suggested_NIK_Prospect
            FROM assign_tracer at
            LEFT JOIN supervisor_data sd ON at.Agreement_No = sd.Case_ID
            WHERE IFNULL(at.Assigned_To,'') = ? 
            ORDER BY at.id DESC LIMIT 500
            """,
            (tracer_name,)
        )
    
    if not rows:
        st.info("Belum ada assignment.")
        return

    st.subheader("Daftar Assignment")
    # Quick search
    qcol1, qcol2 = st.columns([2,1])
    with qcol1:
        q_ag = st.text_input("Cari Case_ID", key="tr_q_ag")
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

    # Selectable table (similar to Agent page)
    data = [
        {
            'ID': r['id'],
            'TRC_Code': r.get('TRC_Code'),
            'Case_ID': r.get('Agreement_No'),
            'Debtor_Name': r.get('Debtor_Name'),
            'NIK_KTP': r.get('NIK_KTP'),
            'Suggested_NIK': r.get('Remarks_Suggested_NIK_Prospect') or '',
            'EMPLOYMENT_UPDATE': r.get('EMPLOYMENT_UPDATE'),
            'EMPLOYER': r.get('EMPLOYER'),
            # Auto-decode for display: use saved value or auto-decode from EMPLOYER
            'Decoded_Company': r.get('Decoded_Company_Name') or (decode_company_name(r.get('EMPLOYER', '')) if r.get('EMPLOYER') else '-'),
            'Debtor_Legal_Name': r.get('Debtor_Legal_Name'),
            'Employee_Name': r.get('Employee_Name'),
            'Employee_ID_Number': r.get('Employee_ID_Number'),
            'Debtor_Relation_to_Employee': r.get('Debtor_Relation_to_Employee'),
            'Assigned_To': r.get('Assigned_To'),
            'Assigned_At': r.get('created_at'),
        } for r in filtered_rows
    ]
    df_view = pd.DataFrame(data)

    prev_selected = set(st.session_state.get('tracer_selected_list', []) or [])
    col_sa, col_cl = st.columns([1,1])
    with col_sa:
        select_all = st.checkbox("Pilih semua", key="tr_select_all")
    with col_cl:
        clear_all = st.checkbox("Kosongkan pilihan", key="tr_clear_all")

    if not df_view.empty:
        if select_all:
            df_view.insert(0, 'Selected', True)
        elif clear_all:
            df_view.insert(0, 'Selected', False)
        else:
            df_view.insert(0, 'Selected', df_view['ID'].apply(lambda x: x in prev_selected))
    else:
        df_view['Selected'] = []

    # Column config dengan tambahan Assigned_To dan Decoded_Company
    col_config = {
        'Selected': st.column_config.CheckboxColumn('Selected', help='Centang untuk memilih assignment'),
        'ID': st.column_config.TextColumn('ID'),
        'TRC_Code': st.column_config.TextColumn('TRC Code'),
        'Case_ID': st.column_config.TextColumn('Case ID'),
        'Debtor_Name': st.column_config.TextColumn('Debtor Name'),
        'NIK_KTP': st.column_config.TextColumn('NIK KTP'),
        'Suggested_NIK': st.column_config.TextColumn('Suggested NIK (by Agent)', help='NIK yang disarankan oleh Agent'),
        'EMPLOYMENT_UPDATE': st.column_config.TextColumn('EMPLOYMENT UPDATE'),
        'EMPLOYER': st.column_config.TextColumn('EMPLOYER (Masked)'),
        'Decoded_Company': st.column_config.TextColumn('EMPLOYER (Decoded)', help='Auto-decoded from library'),
        'Debtor_Legal_Name': st.column_config.TextColumn('Debtor Legal Name'),
        'Employee_Name': st.column_config.TextColumn('Employee Name'),
        'Employee_ID_Number': st.column_config.TextColumn('Employee ID Number'),
        'Debtor_Relation_to_Employee': st.column_config.TextColumn('Debtor Relation to Employee'),
        'Assigned_At': st.column_config.TextColumn('Assigned_At'),
    }
    
    # Tambahkan kolom Assigned_To jika Supervisor/Superuser
    disabled_cols = ['ID','TRC_Code','Case_ID','Debtor_Name','NIK_KTP','Suggested_NIK','EMPLOYMENT_UPDATE','EMPLOYER','Decoded_Company','Debtor_Legal_Name','Employee_Name','Employee_ID_Number','Debtor_Relation_to_Employee','Assigned_At']
    if user_role in ("Superuser", "Supervisor"):
        col_config['Assigned_To'] = st.column_config.TextColumn('Assigned To')
        disabled_cols.append('Assigned_To')
    
    edited = st.data_editor(
        df_view,
        hide_index=True,
        use_container_width=True,
        column_config=col_config,
        disabled=disabled_cols,
    )

    selected_list = []
    if edited is not None and not edited.empty:
        try:
            selected_list = [int(row['ID']) for _, row in edited.iterrows() if bool(row.get('Selected'))]
        except Exception:
            selected_list = []
    st.session_state['tracer_selected_list'] = selected_list
    sel_id = selected_list[0] if selected_list else None

    st.markdown("---")
    st.subheader("Update Detail Employment")
    st.caption("Pilih satu baris kemudian isi data yang diperlukan.")

    # Select a row to update
    sel_row = next((r for r in filtered_rows if r['id'] == sel_id), None)
    if not sel_row:
        st.info("Centang satu baris pada tabel di atas untuk mulai mengedit.")
        return

    # Two sub-tabs: update fields and internal memo
    sub_tabs = st.tabs(["Update Detail Employment", "Internal Memo"]) 

    # --- Update tab ---
    with sub_tabs[0]:
        with st.form("tracer_update_form"):
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("TRC Code", value=sel_row.get('TRC_Code',''), disabled=True, key="tr_v_trc")
                st.text_input("Case ID", value=sel_row.get('Agreement_No',''), disabled=True, key="tr_v_agmt")
                st.text_input("Debtor Name", value=sel_row.get('Debtor_Name',''), disabled=True, key="tr_v_debtor")
                nik_val = st.text_input("NIK KTP", value=sel_row.get('NIK_KTP','') or "", key="tr_v_nik")
            with col2:
                # Employment Update dengan dropdown options
                employment_options = ["", "NIHIL", "PNS", "DEBITUR", "SUAMI", "ISTRI", "AYAH", "IBU", "KERABAT"]
                current_emp_update = sel_row.get('EMPLOYMENT_UPDATE','') or ""
                
                # Jika value saat ini tidak ada di list, tambahkan ke options
                if current_emp_update and current_emp_update not in employment_options:
                    employment_options.insert(1, current_emp_update)
                
                # Find index of current value
                try:
                    current_index = employment_options.index(current_emp_update)
                except ValueError:
                    current_index = 0
                
                emp_update = st.selectbox(
                    "EMPLOYMENT UPDATE", 
                    options=employment_options,
                    index=current_index,
                    key="tr_emp_update",
                    help="Pilih status employment update"
                )
                
                employer = st.text_input("EMPLOYER (Masked)", value=sel_row.get('EMPLOYER',''), key="tr_employer",
                                        help="Masukkan nama company yang ter-mask (e.g., VI****** CA** IN******* PT)")
                
                # Auto-decode preview - calculate decoded value for both new input and existing data
                decoded_value = ""
                employer_value = employer.strip() if employer else ""
                
                if employer_value:
                    decoded_value = decode_company_name(employer_value)
                    if decoded_value != employer_value:
                        st.success(f"🔓 Auto-Decoded: **{decoded_value}**")
                    else:
                        st.info("ℹ️ Belum ada di library. Decoded akan sama dengan Masked.")
                
                # Show decoded field with auto-calculated value
                st.text_input("EMPLOYER (Decoded)", value=decoded_value, key="tr_employer_decoded", disabled=True,
                            help="Otomatis terisi dari library saat save")
                
                debtor_legal = st.text_input("Debtor Legal Name", value=sel_row.get('Debtor_Legal_Name',''), key="tr_debtor_legal")
                employee_name = st.text_input("Employee Name", value=sel_row.get('Employee_Name',''), key="tr_employee_name")
                employee_id = st.text_input("Employee ID Number", value=sel_row.get('Employee_ID_Number',''), key="tr_employee_id")
                relation = st.text_input("Debtor Relation to Employee", value=sel_row.get('Debtor_Relation_to_Employee',''), key="tr_relation")

            submitted = st.form_submit_button("Simpan Perubahan")
            if submitted:
                try:
                    # Normalize NIK (strip spaces); keep empty as NULL
                    nik_new = (nik_val or "").strip()
                    nik_new = nik_new if nik_new != "" else None
                    nik_old = (sel_row.get('NIK_KTP') or '').strip()
                    
                    # Auto-decode company name if EMPLOYER is provided
                    decoded_company = None
                    if employer and employer.strip():
                        decoded_company = decode_company_name(employer.strip())

                    execute(
                        "UPDATE assign_tracer SET NIK_KTP=?, EMPLOYMENT_UPDATE=?, EMPLOYER=?, Debtor_Legal_Name=?, Employee_Name=?, Employee_ID_Number=?, Debtor_Relation_to_Employee=?, Decoded_Company_Name=? WHERE id=?",
                        (
                            nik_new,
                            (emp_update.strip() if emp_update is not None else None),
                            (employer.strip() if employer is not None else None),
                            (debtor_legal.strip() if debtor_legal is not None else None),
                            (employee_name.strip() if employee_name is not None else None),
                            (employee_id.strip() if employee_id is not None else None),
                            (relation.strip() if relation is not None else None),
                            decoded_company,
                            sel_id
                        )
                    )
                    # Optional: propagate updated NIK to supervisor_data for this agreement
                    try:
                        ag_no = sel_row.get('Agreement_No')
                        if ag_no and nik_new is not None:
                            execute(
                                "UPDATE supervisor_data SET NIK_KTP=? WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=?",
                                (nik_new, ag_no, ag_no, ag_no)
                            )
                    except Exception:
                        pass
                    # Audit log tracer update
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id') if u else None, "TRACER_UPDATE", f"Tracer '{tracer_name}' updated assignment ID {sel_id}"))
                    except Exception:
                        pass
                    # If NIK changed, add a specific audit entry
                    try:
                        if (nik_new or '') != (nik_old or ''):
                            execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id') if u else None, "TRACER_UPDATE_NIK", f"ID {sel_id} NIK '{nik_old}' -> '{nik_new or ''}'"))
                    except Exception:
                        pass
                    # Warn if new NIK is frozen
                    if nik_new and is_frozen_by_nik(nik_new):
                        st.toast("⚠️ Perhatian: NIK ini berada dalam daftar freeze!", icon="⚠️")
                    
                    # Show success message with decode info
                    if decoded_company and employer and decoded_company != employer.strip():
                        st.success(f"✅ Data berhasil diperbarui! EMPLOYER decoded: **{decoded_company}**")
                    else:
                        st.success("✅ Data berhasil diperbarui!")
                    
                    st.rerun()
                except Exception as e:
                    st.toast(f"❌ Gagal update: {e}", icon="❌")

    # --- Internal Memo tab ---
    with sub_tabs[1]:
        ag_no = sel_row.get('Agreement_No')
        
        if not ag_no:
            st.info("Pilih case untuk melihat memo internal.")
        else:
            # Enhanced Chat-like CSS (no container, direct render)
            st.markdown(
                """
                <style>
                .tracer-msg { 
                    display: flex;
                    margin: 10px 0;
                    animation: fadeIn 0.3s ease-in;
                }
                @keyframes fadeIn {
                    from { opacity: 0; transform: translateY(10px); }
                    to { opacity: 1; transform: translateY(0); }
                }
                .tracer-msg.left { justify-content: flex-start; }
                .tracer-msg.right { justify-content: flex-end; }
                .tracer-bubble { 
                    max-width: 70%;
                    padding: 10px 14px;
                    border-radius: 16px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                    word-wrap: break-word;
                }
                .tracer-msg.left .tracer-bubble { 
                    background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%);
                    color: #1e293b;
                    border-bottom-left-radius: 4px;
                    border: 1px solid #cbd5e1;
                }
                .tracer-msg.right .tracer-bubble { 
                    background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
                    color: #1e293b;
                    border-bottom-right-radius: 4px;
                    border: 1px solid #fbbf24;
                }
                .tracer-meta { 
                    font-size: 10px;
                    color: #64748b;
                    margin-top: 4px;
                    font-style: italic;
                }
                .tracer-name { 
                    font-weight: 700;
                    font-size: 12px;
                    margin-bottom: 4px;
                    color: #0f172a;
                    letter-spacing: 0.3px;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )

            recent = fetchall(
                "SELECT author_role, author_name, target_role, message, created_at FROM memos WHERE Agreement_No=? ORDER BY id DESC LIMIT 100",
                (ag_no,)
            ) or []
            recent = list(reversed(recent))

            # Render messages directly without container
            if not recent:
                st.info("💬 Belum ada memo untuk case ini")
            else:
                for r in recent:
                    author_role = (r.get('author_role') or '').strip()
                    author_name = (r.get('author_name') or '').strip()
                    msg = (r.get('message') or '').strip()
                    ts = (r.get('created_at') or '').replace('T', ' ')
                    
                    # Tentukan apakah pesan ini dari user saat ini
                    if user_role == 'Tracer':
                        mine = (author_role == 'Tracer' and author_name == tracer_name)
                    else:
                        mine = (author_role in ('Supervisor', 'Superuser') and author_name == tracer_name)
                    
                    side = 'right' if mine else 'left'
                    name = 'Saya' if mine else (author_name or author_role or 'Unknown')
                    safe_msg = msg.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('\n','<br/>')
                    
                    st.markdown(f"""
                        <div class='tracer-msg {side}'>
                            <div class='tracer-bubble'>
                                <div class='tracer-name'>{name}</div>
                                <div class='text'>{safe_msg}</div>
                                <div class='tracer-meta'>{ts}</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)

            # Send message form - label dan role tergantung user
            if user_role in ('Supervisor', 'Superuser'):
                form_label = "Tulis memo untuk Tracer"
                send_as_role = "Supervisor"
                target_role = "Tracer"
            else:
                form_label = "Tulis memo untuk Supervisor"
                send_as_role = "Tracer"
                target_role = "Supervisor"
                
            with st.form("tracer_internal_memo_chat"):
                msg = st.text_area(
                    form_label,
                    value="",
                    placeholder="Ketik pesan Anda di sini...",
                    height=80,
                    help="Tekan Ctrl+Enter atau klik tombol Kirim"
                )
                send = st.form_submit_button("📤 Kirim", use_container_width=True)
                if send:
                    if not msg or not msg.strip():
                        st.toast("⚠️ Pesan tidak boleh kosong!", icon="⚠️")
                    else:
                        try:
                            execute(
                                "INSERT INTO memos (Agreement_No, author_role, author_name, target_role, message) VALUES (?,?,?,?,?)",
                                (ag_no, send_as_role, tracer_name, target_role, msg.strip())
                            )
                            st.toast("✅ Memo berhasil dikirim!", icon="✅")
                            st.rerun()
                        except Exception as e:
                            st.toast(f"❌ Gagal mengirim memo: {e}", icon="❌")
                        st.error(f"❌ Gagal mengirim memo: {e}")

def page_guide():
    """User Guide page with detailed application description and quick how-to."""
    # Accessible to all roles by MENU_ITEMS
    require_roles(ALL_ROLES)
    st.title("📘 Panduan Pengguna — Minama Felonic Solutions")

    st.markdown("""
    ## ⚙️ Gambaran Umum
    Sistem ini dibangun seperti rantai tiga tahap:
    1. Supervisor mengunggah dan menugaskan data kasus (nasabah bermasalah, DPD tinggi, dsb).
    2. Tracer melakukan trace — mencari dan memverifikasi status debitur (apakah masih aktif, bisa dihubungi, dll).
    3. Agent menindaklanjuti hasil trace dengan penagihan atau negosiasi pembayaran.

    Semua aktivitas disimpan dalam database SQLite (`minama.db`), dan secara rutin di-backup ke Google Drive agar tidak ada kehilangan data.
    """, unsafe_allow_html=True)

    st.markdown("---")

    st.header("1) Supervisor — Input Data & Penugasan")
    st.markdown("""
    - Menu: **Supervisor → Upload Excel/CSV Supervisor Data**.
    - File yang diunggah akan diparse ke tabel `supervisor_data`.
    - Setiap baris mewakili 1 pinjaman (Case_ID) — berisi identitas debitur, kontak, DPD, dan outstanding.
    - Aplikasi memeriksa header dan menyimpan ke SQLite (`minama.db`).
    - Supervisor dapat meninjau data, memperbarui baris, dan memilih beberapa Case_ID untuk ditugaskan ke tracer.
    - Penugasan dicatat pada tabel `assign_tracer`. Setelah tracer menyerahkan hasil, supervisor dapat menugaskan ke agent (tabel `agent_assignments`).
    """, unsafe_allow_html=True)

    st.header("2) Tracer — Verifikasi & Investigasi")
    st.markdown("""
    - Login sebagai tracer (misal: `tracer` / password seed `tracer123` jika belum diubah).
    - Buka menu **Tracer** untuk melihat daftar assignment yang ditugaskan kepadamu (dari `assign_tracer`).
    - Lakukan tracing lapangan/telepon: konfirmasi identitas, status pekerjaan, alamat, catat hasil.
    - Setiap interaksi disimpan di `trace_results` dengan kolom: `Case_ID`, `status`, `notes`, `touch_type`, `party`, `created_by`.
    - Sistem menyimpan banyak touch record per Case_ID untuk audit trail.
    """, unsafe_allow_html=True)

    st.header("3) Agent — Penagihan & Pembayaran")
    st.markdown("""
    - Setelah tracer mengonfirmasi debitur, supervisor dapat menugaskan Case_ID ke agent (tabel `agent_assignments`).
    - Agent login melihat penugasan di menu **Agent**.
    - Agent melaporkan hasil ke `agent_results` dengan field: `Case_ID`, `agent_status` (PTP/Paid/Refused), `agent_ptp_amount`, `agent_ptp_date`, `agent_notes`.
    - **Status Cicilan:** Agent dapat melaporkan pembayaran cicilan dengan status:
      - `CICIL OS` - Cicilan Outstanding
      - `CICIL LUNDIS` - Cicilan Lunas Dicicil
      - `CICIL POKOK` - Cicilan Pokok
    - **Approval Workflow:** Semua pengajuan cicilan harus di-approve oleh Supervisor sebelum mengurangi outstanding.
    - **Auto-reduce Outstanding:** Saat Supervisor approve pembayaran, sistem otomatis mengurangi `Principle_Outstanding` di `supervisor_data`.
    - Formula: `New Outstanding = MAX(0, Current Outstanding - Payment Amount)`
    """, unsafe_allow_html=True)
    
    st.header("3.1) Cicilan Approval Process (Supervisor)")
    st.markdown("""
    - Menu: **Agent → Cicilan Approval Tab** (khusus Supervisor/Superuser)
    - Supervisor melihat semua pengajuan cicilan dengan status: Pending / Approved / Rejected
    - Detail yang ditampilkan:
      - Case ID, Customer Name, Agent
      - Status Cicilan (CICIL OS/LUNDIS/POKOK)
      - Outstanding saat ini
      - Jumlah Cicilan yang diajukan
      - **Preview:** Sisa Outstanding setelah approve
    - **Action:**
      - ✅ APPROVE: Update status + Kurangi Principle_Outstanding + Audit log
      - ❌ REJECT: Update status saja (Outstanding tidak berubah)
    - **Auto Paid-Off:** Jika setelah approve Outstanding mencapai 0, sistem otomatis set `Paid_Off = 'Yes'`
    - **Audit Trail:** Semua approval/reject dicatat di `audit_logs` dengan detail amount, Case ID, dan perubahan status paid-off
    """, unsafe_allow_html=True)

    st.header("4) Monitoring & Analytics")
    st.markdown("""
    - Dashboard menampilkan KPI: jumlah pinjaman aktif, sudah lunas, pending, dan metrik per role.
    - Data diambil dari tabel `supervisor_data`, `trace_results`, `agent_results`, dan `payments`.
    - Visualisasi dapat menggunakan Pandas/Altair pada Streamlit.
    """, unsafe_allow_html=True)

    st.header("5) Backup & Auto-Restore")
    st.markdown("""
    **🔒 SISTEM KEAMANAN DATA (Anti-Overwrite Protection)**
    
    Aplikasi ini memiliki mekanisme backup/restore otomatis yang **sangat aman** untuk mencegah kehilangan data:
    
    **A) Auto-Restore (Saat Reboot/Autosleep):**
    - ✅ **WAJIB:** Setiap kali aplikasi terdeteksi fresh (reboot/autosleep), sistem **WAJIB restore dari Google Drive** sebelum melakukan apapun
    - ✅ **Validasi:** File backup divalidasi (format SQLite, ukuran > 50KB)
    - ✅ **Backup Lokal:** Sebelum overwrite, DB lama di-backup lokal (`.before_restore.bak`)
    - ✅ **Grace Period:** Setelah restore, backup diblokir selama 15 menit untuk stabilisasi
    
    **B) Backup Protection (3 Lapis Safeguard):**
    
    **Safeguard #1 - Fresh DB Check:**
    - 🚫 TIDAK AKAN backup jika DB masih fresh (hanya seed users, < 3 data)
    - 🚫 TIDAK AKAN overwrite backup lama dengan DB kosong saat autosleep
    - ✅ Backup hanya dilakukan jika sudah ada data real
    
    **Safeguard #2 - Grace Period:**
    - ⏸️ TIDAK AKAN backup dalam 15 menit pertama setelah restore
    - ⏸️ Mencegah backup loop dan memberikan waktu stabilisasi DB
    
    **Safeguard #3 - Capacity Check:**
    - 📦 Cek ukuran Drive sebelum upload
    - 📦 Tidak akan backup jika melebihi kapasitas (default 2GB)
    
    **C) Kapan Backup Terjadi?**
    - ✅ Saat **Logout** (UTAMA - backup data terakhir setelah user selesai bekerja)
    - ✅ **Daily Backup** (1x per hari, jika belum backup hari ini) - DISABLED by default
    - ✅ **Scheduled Backup** (per slot waktu, jika diaktifkan) - DISABLED by default
    - ✅ **Manual Backup** via menu G Drive
    - ❌ **TIDAK saat Login** (untuk mengurangi beban sistem dan mencegah backup prematur)
    
    **D) Log & Audit:**
    - 📝 Semua aktivitas dicatat di `backup_log` dengan status SUCCESS/FAILED/RESTORED
    - 📝 Audit trail lengkap di `audit_logs`
    - 📝 Setting `auto_restore_last_time` untuk tracking grace period
    
    **E) File Backup:**
    - 📄 **auto_backup.sqlite** - Backup otomatis (overwrite, 1 file)
    - 📄 **scheduled_backup.sqlite** - Backup terjadwal (overwrite, 1 file)
    - 📁 Lokasi: Google Drive folder ID `FOLDER_ID_DEFAULT`
    
    **⚠️ PENTING:**
    - Pastikan `service_account` tersedia di `st.secrets`
    - Jangan hapus file backup di Drive secara manual
    - Monitoring rutin via menu **G Drive → Audit Log**
    """, unsafe_allow_html=True)

    st.header("6) Kontrol Peran & Keamanan")
    st.markdown("""
    - Role yang tersedia: `Superuser`, `Supervisor`, `Tracer`, `Agent`.
    - Akses halaman dikontrol oleh `MENU_ITEMS` dan fungsi `require_roles()`.
    - Autentikasi password di-hash dengan SHA256 (`hash_password()`); akun baru harus disetujui (`approved` flag).
    """, unsafe_allow_html=True)

    st.header("7) Aliran Data (Ringkas)")
    st.markdown("""
    Supervisor Uploads Data
        ↓
      `supervisor_data`
        ↓
    Tracer Assignment → `assign_tracer`
        ↓
    Tracer Updates Status → `trace_results`
        ↓
    Supervisor Assigns to Agent → `agent_assignments`
        ↓
    Agent Collects Payments → `agent_results` & `payments`
        ↓
    Dashboard / Analytics / AI Chat

    Setiap tahap meninggalkan jejak di database dan `audit_logs`, serta disinkronkan ke Google Drive.
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("Dokumentasi Teknis: Ringkasan Tabel Utama")
    st.markdown("""
    - `supervisor_data`: menyimpan semua baris input dari file supervisor (kolom: Customer_name, DPD, Principle_Outstanding, Phone_Number_1, dll.)
    - `assign_tracer`: daftar penugasan tracer (Case_ID, Debtor_Name, NIK_KTP, Assigned_To)
    - `trace_results`: hasil tracing / touch logs (Case_ID, tracer, status, notes, touch_type, party, touched_at)
    - `agent_assignments`: assignment ke agent (Case_ID, Agent_Assigned_To, assigned_at, active)
    - `agent_results`: hasil penagihan (Case_ID, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes)
    - `payments`: rekapan pembayaran (Case_ID, paid_amount, paid_date, status, source_file)
    - `backup_log`, `audit_logs`, `app_settings`, `ai_knowledge` untuk operasional dan audit
    """, unsafe_allow_html=True)

    st.subheader("Contoh Alur Singkat — Kasus Nyata")
    st.markdown("""
    1. Supervisor mengunggah `supervisor_data_dummy.xlsx` berisi 100 baris. Data tersimpan di `supervisor_data`.
    2. Supervisor memilih 20 Case_ID dan menugaskan ke `tracer` — entri dibuat di `assign_tracer`.
    3. Tracer membuka menu Tracer, melihat 20 assignment, dan membuat 1–3 touch per debitur; hasil masuk ke `trace_results`.
    4. Supervisor melihat hasil trace, menugaskan 15 kasus ke `agent` — entri di `agent_assignments`.
    5. Agent mendatangi debitur; 5 kasus menghasilkan pembayaran → disimpan di `payments`; agent juga mengisi `agent_results`.
    6. Dashboard menampilkan metrik: conversion rate agent, success tracer, outstanding reductions.
    """, unsafe_allow_html=True)

    st.subheader("FAQ & Troubleshooting Singkat")
    st.markdown("""
    Q: Bagaimana jika upload CSV gagal?
    A: Periksa header file sesuai contoh, pastikan kolom `Case_ID` ada, dan tidak ada duplikat yang memicu constraint.

    Q: Backup Drive tidak berfungsi?
    A: Pastikan `service_account` tersedia di `st.secrets` dan folder `FOLDER_ID_DEFAULT` benar; cek `backup_log` untuk pesan error.

    Q: Restore otomatis tidak terjadi setelah restart?
    A: Fungsi `attempt_auto_restore_if_seed()` hanya berjalan jika DB terdeteksi fresh (few users, empty backup_log). Periksa `app_settings` dan `auto_restore_enabled`.
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.caption("Panduan ini dibuat otomatis dari struktur aplikasi. Untuk tambahan (contoh CSV/Excel, diagram alir, atau export), minta file contoh dan saya tambahkan.")

if __name__ == '__main__':
    main()