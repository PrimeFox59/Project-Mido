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
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

    # 6) AI Knowledge base (for Chat AI)
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fact TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    except Exception:
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
            'STATUS', 'REGISTERED_PHONE', 'Additional_Contacts', 'Remarks_Suggested_NIK_Prospect', 'Payment', 'Paid_Off_Status'
        ]:
            if col not in cols:
                c.execute(f"ALTER TABLE supervisor_data ADD COLUMN {col} TEXT")
    except Exception:
        pass
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
    # ensure assign_tracer has optional masked company name field
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(assign_tracer)").fetchall()]
        if 'Masked_Company_Name' not in cols:
            c.execute("ALTER TABLE assign_tracer ADD COLUMN Masked_Company_Name TEXT")
        def page_agent():
            require_roles(("Superuser", "Agent"))
            u = current_user()
            agent_name = (u.get('full_name') or u.get('login_id') or '-') if u else '-'
            st.title("Agent Menu")

            # PTP reminder for today
            today_str = date.today().isoformat()
            try:
                ptp_today = fetchone(
                    "SELECT COUNT(*) c FROM agent_results WHERE agent=? AND agent_status='PTP' AND DATE(agent_ptp_date)=?",
                    (agent_name, today_str)
                ) or {"c": 0}
                count_ptp = ptp_today.get('c', 0) or 0
                if count_ptp > 0:
                    st.success(f"Hai {agent_name}, hari ini kamu ada {count_ptp} PTP.")
            except Exception:
                count_ptp = 0

            tabs = st.tabs([
                "Data",
                "Report a Payment/PTP",
                "Internal Memo",
                "My PTP",
                "Monthly Payment Recap",
                "All-time Payment Recap",
            ])

            # Fetch assignments once for reuse
            assignments = fetchall(
                "SELECT Agreement_No, assigned_at FROM agent_assignments WHERE Agent_Assigned_To=? ORDER BY assigned_at DESC LIMIT 1000",
                (agent_name,)
            )

            # --- Data tab ---
            with tabs[0]:
                if not assignments:
                    st.info("Belum ada assignment untuk Anda.")
                else:
                    q_ag = st.text_input("Cari Agreement_No (Loan Number)", key="ag_q_no")
                    filtered = [r for r in assignments if (not q_ag or q_ag.strip() in str(r.get('Agreement_No') or ''))]

                    st.subheader("Assignments")
                    st.dataframe(pd.DataFrame(filtered), use_container_width=True, hide_index=True)

                    sel = st.selectbox("Pilih Loan Number", [r['Agreement_No'] for r in filtered], key="ag_sel")
                    if sel:
                        st.markdown("---")
                        st.subheader(f"Loan Details: {sel}")
                        info = fetchone("SELECT Debtor_Name, NIK_KTP FROM assign_tracer WHERE Agreement_No=?", (sel,)) or {}
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.text_input("Debtor Name", value=info.get('Debtor_Name',''), disabled=True)
                        with c2:
                            st.text_input("NIK", value=info.get('NIK_KTP',''), disabled=True)
                        with c3:
                            sup = fetchone(
                                "SELECT Phone_Number_1 FROM supervisor_data WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=? LIMIT 1",
                                (sel, sel, sel)
                            )
                            phone = (sup.get('Phone_Number_1') if sup else '') or ''
                            st.text_input("Phone", value=phone, disabled=True)
                        if phone:
                            st.markdown(f"[Click to call]({'tel:'+str(phone)})  |  [SIP]({'sip:'+str(phone)})")

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

            # --- Report a Payment/PTP tab ---
            with tabs[1]:
                st.subheader("Report a Payment/PTP")
                if not assignments:
                    st.info("Tidak ada Agreement_No yang ditugaskan.")
                else:
                    sel2 = st.selectbox("Pilih Agreement_No", [r['Agreement_No'] for r in assignments], key="ag_rep_sel")
                    mode = st.radio("Jenis Laporan", ["Payment", "PTP"], horizontal=True)
                    if mode == "Payment":
                        with st.form("form_report_payment"):
                            col1, col2 = st.columns(2)
                            with col1:
                                amount = st.number_input("Paid Amount", min_value=0.0, step=10000.0)
                            with col2:
                                paid_date = st.date_input("Paid Date", value=date.today())
                            status = st.text_input("Status (opsional)", value="PAID")
                            ref = st.text_input("Referensi (opsional)", placeholder="mis. link WA, catatan singkat")
                            submit_p = st.form_submit_button("Simpan Payment")
                            if submit_p:
                                try:
                                    execute(
                                        "INSERT INTO payments (Agreement_No, paid_amount, paid_date, status, source_file, uploaded_by) VALUES (?,?,?,?,?,?)",
                                        (sel2, float(amount or 0), (paid_date.isoformat() if paid_date else None), (status.strip() or None), (ref.strip() or None), agent_name)
                                    )
                                    try:
                                        execute(
                                            "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                            (u.get('id'), 'AGENT_REPORT_PAYMENT', f"{sel2}|{amount}|{paid_date}")
                                        )
                                    except Exception:
                                        pass
                                    st.success("Payment tersimpan.")
                                except Exception as e:
                                    st.error(f"Gagal menyimpan payment: {e}")
                    else:
                        # PTP
                        with st.form("form_report_ptp"):
                            col1, col2 = st.columns(2)
                            with col1:
                                ptp_amount = st.number_input("PTP Amount", min_value=0.0, step=10000.0)
                            with col2:
                                ptp_date = st.date_input("PTP Date", value=date.today())
                            notes = st.text_area("Catatan (opsional)")
                            submit_t = st.form_submit_button("Simpan PTP")
                            if submit_t:
                                try:
                                    execute(
                                        "INSERT INTO agent_results (Agreement_No, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes) VALUES (?,?,?,?,?,?)",
                                        (sel2, agent_name, 'PTP', float(ptp_amount or 0), (ptp_date.isoformat() if ptp_date else None), (notes.strip() or None))
                                    )
                                    try:
                                        execute(
                                            "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                            (u.get('id'), 'AGENT_REPORT_PTP', f"{sel2}|{ptp_amount}|{ptp_date}")
                                        )
                                    except Exception:
                                        pass
                                    st.success("PTP tersimpan.")
                                except Exception as e:
                                    st.error(f"Gagal menyimpan PTP: {e}")

            # --- Internal Memo tab ---
            with tabs[2]:
                st.subheader("Internal Memo")
                if not assignments:
                    st.info("Tidak ada Agreement_No yang ditugaskan.")
                else:
                    # List incoming memos from Supervisor for this agent's loans
                    my_agreements = [r['Agreement_No'] for r in assignments]
                    placeholders = ",".join(["?"] * len(my_agreements)) if my_agreements else "?"
                    try:
                        incoming = fetchall(
                            f"SELECT * FROM memos WHERE target_role='Agent' AND Agreement_No IN ({placeholders}) ORDER BY id DESC LIMIT 200",
                            tuple(my_agreements) if my_agreements else ("",)
                        )
                    except Exception:
                        incoming = []
                    # My memos sent to Supervisor
                    try:
                        mine = fetchall(
                            f"SELECT * FROM memos WHERE author_role='Agent' AND author_name=? AND Agreement_No IN ({placeholders}) ORDER BY id DESC LIMIT 200",
                            (agent_name, *my_agreements) if my_agreements else (agent_name,)
                        )
                    except Exception:
                        mine = []

                    colA, colB = st.columns(2)
                    with colA:
                        st.caption("Dari Supervisor → Agent")
                        if incoming:
                            df_in = pd.DataFrame([
                                {"Agreement_No": r.get('Agreement_No'), "Waktu": r.get('created_at'), "Pesan": r.get('message')}
                                for r in incoming
                            ])
                            st.dataframe(df_in, use_container_width=True, hide_index=True)
                        else:
                            st.info("Belum ada memo dari Supervisor.")
                    with colB:
                        st.caption("Memo Saya ke Supervisor")
                        if mine:
                            df_my = pd.DataFrame([
                                {"Agreement_No": r.get('Agreement_No'), "Waktu": r.get('created_at'), "Pesan": r.get('message')}
                                for r in mine
                            ])
                            st.dataframe(df_my, use_container_width=True, hide_index=True)
                        else:
                            st.info("Belum ada memo yang Anda kirim.")

                    st.markdown("---")
                    st.subheader("Kirim Memo ke Supervisor")
                    with st.form("form_send_memo"):
                        selm = st.selectbox("Agreement_No", [r['Agreement_No'] for r in assignments], key="ag_memo_sel")
                        msg = st.text_area("Pesan", placeholder="Tulis pertanyaan/catatan untuk SPV...")
                        send = st.form_submit_button("Kirim Memo")
                        if send:
                            if not msg or not msg.strip():
                                st.warning("Pesan tidak boleh kosong.")
                            else:
                                try:
                                    execute(
                                        "INSERT INTO memos (Agreement_No, author_role, author_name, target_role, message) VALUES (?,?,?,?,?)",
                                        (selm, 'Agent', agent_name, 'Supervisor', msg.strip())
                                    )
                                    try:
                                        execute(
                                            "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                            (u.get('id'), 'AGENT_MEMO_CREATE', f"{selm} | {msg[:60]}")
                                        )
                                    except Exception:
                                        pass
                                    st.success("Memo terkirim ke Supervisor.")
                                except Exception as e:
                                    st.error(f"Gagal mengirim memo: {e}")

            # --- My PTP tab ---
            with tabs[3]:
                st.subheader("Janji Bayar Saya (PTP)")
                rows = fetchall(
                    "SELECT Agreement_No, agent_ptp_amount, agent_ptp_date, agent_notes FROM agent_results WHERE agent=? AND agent_status='PTP' ORDER BY agent_ptp_date ASC",
                    (agent_name,)
                )
                if not rows:
                    st.info("Belum ada PTP yang tercatat.")
                else:
                    df = pd.DataFrame([
                        {
                            "Agreement_No": r.get('Agreement_No'),
                            "PTP Date": r.get('agent_ptp_date'),
                            "Amount": r.get('agent_ptp_amount'),
                            "Notes": r.get('agent_notes'),
                        }
                        for r in rows
                    ])
                    st.dataframe(df, use_container_width=True, hide_index=True)

            # --- Monthly Payment Recap tab ---
            with tabs[4]:
                st.subheader("Rekap Pembayaran Bulan Ini")
                today = date.today()
                start_of_month = today.replace(day=1).isoformat()
                end_date = today.isoformat()
                rows = fetchall(
                    "SELECT Agreement_No, paid_amount, paid_date, status FROM payments WHERE uploaded_by=? AND DATE(paid_date) BETWEEN DATE(?) AND DATE(?) ORDER BY paid_date DESC",
                    (agent_name, start_of_month, end_date)
                )
                total_amt = sum([float(r.get('paid_amount') or 0) for r in rows]) if rows else 0.0
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Total Amount (This Month)", f"{total_amt:,.0f}")
                with c2:
                    st.metric("Count", len(rows))
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.info("Belum ada laporan pembayaran bulan ini.")

            # --- All-time Payment Recap tab ---
            with tabs[5]:
                st.subheader("Rekap Pembayaran Sepanjang Waktu")
                rows = fetchall(
                    "SELECT Agreement_No, paid_amount, paid_date, status FROM payments WHERE uploaded_by=? ORDER BY paid_date DESC",
                    (agent_name,)
                )
                total_amt = sum([float(r.get('paid_amount') or 0) for r in rows]) if rows else 0.0
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Total Amount (All-time)", f"{total_amt:,.0f}")
                with c2:
                    st.metric("Count", len(rows))
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.info("Belum ada laporan pembayaran.")
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
# Chat AI helpers (Gemini + Memory)
# -------------------------
def get_gemini_api_key():
    """Fetch Gemini API key from Streamlit secrets or environment.
    Priority: st.secrets['gemini']['api_key'] -> st.secrets['GEMINI_API_KEY'] -> env GEMINI_API_KEY
    (No in-app manual entry; configured via Streamlit secrets only.)
    """
    try:
        # Nested object style
        k = st.secrets.get('gemini', {}).get('api_key')
        if k:
            return str(k)
    except Exception:
        pass
    try:
        # Flat style
        if 'GEMINI_API_KEY' in st.secrets:
            return str(st.secrets['GEMINI_API_KEY'])
    except Exception:
        pass
    try:
        if os.environ.get('GEMINI_API_KEY'):
            return os.environ.get('GEMINI_API_KEY')
    except Exception:
        pass
    return None

def ai_add_knowledge(fact: str) -> bool:
    """Insert a new fact into ai_knowledge table in the main app DB."""
    if not fact or not fact.strip():
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("INSERT INTO ai_knowledge (fact) VALUES (?)", (fact.strip(),))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False

def ai_get_all_knowledge() -> str:
    """Return all facts ordered by timestamp ASC, formatted for context."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT fact, timestamp FROM ai_knowledge ORDER BY datetime(timestamp) ASC, id ASC")
        rows = cur.fetchall()
        conn.close()
        if not rows:
            return ""
        out = []
        for fact, ts in rows:
            try:
                # SQLite CURRENT_TIMESTAMP => 'YYYY-MM-DD HH:MM:SS'
                dt = datetime.strptime(str(ts), '%Y-%m-%d %H:%M:%S')
                tag = dt.strftime('%d %b %Y %H:%M')
            except Exception:
                tag = str(ts)
            out.append(f"- (Dicatat pada {tag}) {fact}")
        return "\n".join(out)
    except Exception:
        return ""

def ai_build_system_context() -> str:
    """Small live snapshot of the app to help AI answer about the system."""
    try:
        total_users = (fetchone("SELECT COUNT(*) c FROM users WHERE approved=1") or {}).get('c', 0)
    except Exception:
        total_users = 0
    try:
        pending_approvals = (fetchone("SELECT COUNT(*) c FROM users WHERE approved=0") or {}).get('c', 0)
    except Exception:
        pending_approvals = 0
    try:
        completed_total = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM payments WHERE COALESCE(paid_amount,0) > 0") or {}).get('c', 0)
    except Exception:
        completed_total = 0
    try:
        assigned_total = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM agent_assignments WHERE IFNULL(active,1)=1") or {}).get('c', 0)
    except Exception:
        assigned_total = 0
    pending_total = max(assigned_total - completed_total, 0)
    try:
        last_backup = fetchone("SELECT file_name, status, backup_time FROM backup_log ORDER BY id DESC LIMIT 1") or {}
    except Exception:
        last_backup = {}
    # Short recent activity (5 entries)
    try:
        recent_logs = fetchall("SELECT timestamp, action, details FROM audit_logs ORDER BY id DESC LIMIT 5")
    except Exception:
        recent_logs = []
    logs_lines = []
    for r in (recent_logs or []):
        logs_lines.append(f"{r.get('timestamp','')}: {r.get('action','')} — {r.get('details','')}")
    snapshot = [
        f"Total user aktif (approved): {total_users}",
        f"Pending approval: {pending_approvals}",
        f"Total assignment aktif: {assigned_total}",
        f"Dokumen selesai (punya pembayaran): {completed_total}",
        f"Dokumen pending: {pending_total}",
        f"Backup terakhir: {last_backup.get('file_name','-')} | {last_backup.get('status','-')} @ {last_backup.get('backup_time','-')}",
    ]
    if logs_lines:
        snapshot.append("Aktivitas terakhir:")
        snapshot.extend(["  - "+x for x in logs_lines])
    return "\n".join(snapshot)

# ------- Optional: Summarize DB tables for AI context attachment -------
SAFE_TABLES = {
    # Operational tables (exclude sensitive user auth fields)
    "assign_tracer",
    "supervisor_data",
    "payments",
    "agent_assignments",
    "trace_results",
    "masked_companies",
    "agent_results",
    "backup_log",
    "audit_logs",
    "record_notes",
}

SECRET_COLUMN_BLACKLIST = {"password_hash", "service_account", "email_token"}

def _get_table_columns(table: str) -> list:
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        conn.close()
        return cols
    except Exception:
        return []

def ai_summarize_table(table: str, filter_column: str | None = None, keyword: str | None = None, limit: int = 50) -> str:
    """Return a compact, human-readable summary of a table with optional simple filter.
    Safety: table and column are validated against whitelist and actual schema; values are parameterized.
    """
    table = str(table or "").strip()
    if table not in SAFE_TABLES:
        return "Tabel tidak diizinkan untuk dilampirkan."
    try:
        limit = max(1, min(int(limit or 50), 200))
    except Exception:
        limit = 50

    cols = _get_table_columns(table)
    cols = [c for c in cols if c and c not in SECRET_COLUMN_BLACKLIST]
    if not cols:
        return "Gagal membaca kolom tabel."

    where = ""
    params: list = []
    if filter_column:
        if filter_column not in cols:
            return "Kolom filter tidak valid untuk tabel ini."
        where = f" WHERE COALESCE({filter_column}, '') LIKE ?"
        params.append(f"%{keyword or ''}%")

    # Count rows (with filter)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) AS c FROM {table}{where}", tuple(params))
        total = (cur.fetchone() or {}).get("c", 0)
        # Sample rows
        cur.execute(
            f"SELECT {', '.join(cols)} FROM {table}{where} ORDER BY 1 DESC LIMIT {limit}",
            tuple(params),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        return f"Gagal mengambil data: {e}"

    # Compose text summary
    def _trunc(v):
        s = str(v) if v is not None else ""
        return (s[:120] + "…") if len(s) > 120 else s

    lines = [
        f"=== Lampiran: {table} ===",
        f"Total baris (sesuai filter): {total}",
        f"Kolom: {', '.join(cols)}",
        f"Sampel {min(len(rows), limit)} baris:",
    ]
    for r in rows:
        pair_str = ", ".join([f"{k}={_trunc(r.get(k))}" for k in cols[:8]])
        lines.append(f"- {pair_str}")
    if not rows:
        lines.append("(Tidak ada baris contoh untuk filter ini)")
    return "\n".join(lines)

def ai_build_context_pack(tables: list[str], row_cap_per_table: int = 2000, char_budget: int = 300_000) -> str:
    """Build a large but bounded context pack containing many rows from selected tables.
    Safeguards:
      - Only whitelisted tables
      - Blacklist sensitive columns
      - Row cap per table and total char budget to respect model context limits
    Returns a concatenated text block noting any truncation.
    """
    if not tables:
        return "Tidak ada tabel dipilih."
    # Sanitize inputs
    try:
        row_cap_per_table = max(1, min(int(row_cap_per_table or 2000), 50_000))
    except Exception:
        row_cap_per_table = 2000
    try:
        char_budget = max(10_000, min(int(char_budget or 300_000), 2_000_000))
    except Exception:
        char_budget = 300_000

    parts: list[str] = []
    total_chars = 0
    truncated = False

    def add(text: str):
        nonlocal total_chars, truncated
        if truncated:
            return
        remain = char_budget - total_chars
        if remain <= 0:
            truncated = True
            return
        chunk = text[:remain]
        parts.append(chunk)
        total_chars += len(chunk)
        if len(text) > remain:
            truncated = True

    for tbl in tables:
        if tbl not in SAFE_TABLES:
            continue
        cols = _get_table_columns(tbl)
        cols = [c for c in cols if c and c not in SECRET_COLUMN_BLACKLIST]
        if not cols:
            add(f"\n=== {tbl} (kolom tidak tersedia) ===\n")
            continue
        add(f"\n=== {tbl} (max {row_cap_per_table} baris) ===\n")
        add("|".join(cols) + "\n")
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(f"SELECT {', '.join(cols)} FROM {tbl} ORDER BY 1 DESC LIMIT {row_cap_per_table}")
            for r in cur.fetchall():
                if truncated:
                    break
                vals = []
                for c in cols:
                    v = r.get(c)
                    s = "" if v is None else str(v)
                    # Keep line compact
                    if len(s) > 300:
                        s = s[:300] + "…"
                    # Avoid newlines breaking rows
                    s = s.replace("\n", "\\n")
                    vals.append(s)
                add("|".join(vals) + "\n")
            conn.close()
        except Exception as e:
            add(f"(Gagal membaca {tbl}: {e})\n")
        if truncated:
            break

    if truncated:
        parts.append("\n[Catatan] Lampiran dipotong karena melewati anggaran konteks. Kurangi jumlah tabel/row cap atau tingkatkan anggaran.\n")
    return "".join(parts)

def ai_generate_response(prompt: str, chat_history_for_gemini: list, context_data: str = "") -> str:
    """Call Gemini REST API to generate a response with system + memory context."""
    api_key = get_gemini_api_key()
    if not api_key:
        return "API key Gemini belum dikonfigurasi di Streamlit secrets. Tambahkan [gemini].api_key atau GEMINI_API_KEY di Secrets."
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        headers = {'Content-Type': 'application/json'}
        system_instruction = f"""
Anda adalah Prime AI, sebuah persona digital dari Galih Primananda yang membantu pengguna memahami aplikasi ini.
Fokuskan jawaban pada memori dan snapshot sistem di bawah. Bila ada informasi bertentangan, anggap yang PALING BARU (paling bawah) adalah yang benar.

--- MEMORI & PENGETAHUAN (Kronologis) ---
{context_data if context_data else "Belum ada memori yang tersimpan."}
----------------------------------------

Anda boleh menyimpulkan dan memberi langkah konkret. Jika tidak ada informasi yang relevan di memori, jujur katakan: "Berdasarkan memoriku, aku belum punya informasi tentang itu."
Tanggal hari ini: {datetime.now().strftime("%A, %d %B %Y")} · Waktu: {datetime.now().strftime("%H:%M WIB")}.
"""
        payload_contents = chat_history_for_gemini + [{"role": "user", "parts": [{"text": prompt}]}]
        payload = {
            "contents": payload_contents,
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "generationConfig": {"temperature": 0.7, "topP": 0.95},
        }
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data['candidates'][0]['content']['parts'][0]['text']
    except requests.exceptions.RequestException as e:
        return f"Maaf, terjadi masalah koneksi ke server AI: {e}"
    except Exception:
        try:
            txt = resp.text  # type: ignore[name-defined]
        except Exception:
            txt = ""
        return "Maaf, saya menerima respons yang tidak terduga dari server AI." + (f" Raw: {txt}" if txt else "")

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
    {"label": "Chat AI",    "page": "Chat AI", "roles": ALL_ROLES, "primary": True},
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
    
def page_chat_ai():
    """AI Chat page with memory and system-aware context."""
    require_roles(ALL_ROLES)
    st.title("🤖 Chat AI")
    st.caption("Tanya apa pun terkait sistem ini atau gunakan memori khusus.")

    # Scoped CSS for Chat AI aesthetics (lightweight, page-local intent)
    st.markdown(
        """
        <style>
        /* Chat message bubble styling */
        div[data-testid="stChatMessage"] {
            background: #F8FAFF;
            border: 1px solid #EEF2FF;
            border-radius: 12px;
            padding: 8px 12px;
            margin: 8px 0;
        }
        /* Slightly different background for assistant to improve contrast */
        div[data-testid="stChatMessage"]:has(svg[data-testid="stChatMessageAvatarIcon"]) {
            background: #FDFDFE;
        }
        /* Chat input spacing (default behavior, simple spacing only) */
        div[data-testid="stChatInput"] { margin-top: 8px; }

        /* Pills */
        .pill { display:inline-flex; align-items:center; gap:6px; padding:4px 10px; border-radius:999px; font-size:12px; font-weight:600; }
        .pill-success { background:#ECFDF3; color:#027A48; border:1px solid #A6F4C5; }
        .pill-warning { background:#FFF7ED; color:#C2410C; border:1px solid #FED7AA; }
        .muted { color:#667085; font-size:12px; }

        /* Right column subtle section spacing */
        .section { margin-bottom: 14px; }
        .section h3, .section h4 { margin-bottom: 6px; }

        /* Scrollable chat area: only the inner chat container (without the typing box) */
        /* Avoid styling any ancestor that also contains the input */
        div[data-testid=\"stVerticalBlock\"]:has(.chat-scroll-marker):not(:has(div[data-testid='stChatInput'])) {
            max-height: 1000px; /* adjust as needed */
            overflow-y: auto;
            padding: 6px 8px;
            border: 1px solid #EEF2FF;
            border-radius: 12px;
            background: #FFFFFF;
            scroll-behavior: smooth;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    # (Input follows Streamlit default positioning; no JS injection needed)

    # Layout: chat on left, memory and settings on right
    left, right = st.columns([2, 1])

    # Right: Memory center + AI status (no manual key input)
    with right:
        st.subheader("🧠 Memori")
        with st.form("ai_mem_add_form", clear_on_submit=True):
            new_fact = st.text_area("Tambahkan atau perbarui pengetahuan:", height=120)
            sub = st.form_submit_button("Simpan ke Memori")
            if sub and new_fact.strip():
                if ai_add_knowledge(new_fact.strip()):
                    st.success("Memori baru ditambahkan.")
                    st.rerun()
                else:
                    st.error("Gagal menambahkan memori.")
        st.markdown("")
        st.subheader("🔐 Status Koneksi AI")
        has_key = bool(get_gemini_api_key())
        if has_key:
            st.markdown("<span class='pill pill-success'>Connected</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='pill pill-warning'>Not Configured</span>", unsafe_allow_html=True)
        with st.expander("📎 Lampirkan Data dari Database (opsional)", expanded=False):
            sel_table = st.selectbox("Pilih tabel", sorted(list(SAFE_TABLES)), key="ai_tbl_sel")
            cols = _get_table_columns(sel_table) if sel_table else []
            cols = [c for c in cols if c and c not in SECRET_COLUMN_BLACKLIST]
            col1, col2 = st.columns(2)
            with col1:
                sel_col = st.selectbox("Filter kolom (opsional)", options=["(tanpa filter)"] + cols, index=0, key="ai_tbl_col")
            with col2:
                kw = st.text_input("Kata kunci contains", key="ai_tbl_kw", placeholder="misal: VA nomor / nama / status")
            limit_n = st.number_input("Batas sampel baris", min_value=5, max_value=200, value=30, step=5, key="ai_tbl_lim")
            col_btn1, col_btn2 = st.columns([1,1])
            with col_btn1:
                if st.button("Lampirkan ke konteks", key="ai_attach_btn"):
                    summary = ai_summarize_table(
                        sel_table,
                        None if sel_col == "(tanpa filter)" else sel_col,
                        kw if sel_col != "(tanpa filter)" else None,
                        int(limit_n),
                    )
                    st.session_state["ai_attached_context"] = summary
                    st.success("Lampiran siap dipakai dalam jawaban berikutnya.")
            with col_btn2:
                if st.button("Hapus lampiran", key="ai_clear_attach"):
                    st.session_state.pop("ai_attached_context", None)
                    st.info("Lampiran dihapus.")
            preview = st.session_state.get("ai_attached_context")
            if preview:
                st.text_area("Preview Lampiran", value=preview, height=180, disabled=True)

        # Superuser: big context pack builder (bounded by budget)
        u = current_user() or {}
        if u.get('role') == 'Superuser':
            with st.expander("🛡️ Superuser: Lampirkan seluruh data (dibatasi anggaran)", expanded=False):
                pick_tbls = st.multiselect("Pilih tabel untuk dilampirkan", sorted(list(SAFE_TABLES)), default=sorted(list(SAFE_TABLES))[:5], key="ai_pack_tbls")
                row_cap = st.number_input("Row cap per tabel", min_value=100, max_value=50000, value=2000, step=100, key="ai_pack_rows")
                budget = st.number_input("Anggaran karakter total", min_value=10000, max_value=2000000, value=300000, step=10000, key="ai_pack_budget")
                if st.button("Bangun & Lampirkan Pack", use_container_width=True, key="btn_build_pack"):
                    pack = ai_build_context_pack(pick_tbls, int(row_cap), int(budget))
                    st.session_state["ai_attached_context"] = pack
                    st.success("Context pack dilampirkan.")
                preview_pack = st.session_state.get("ai_attached_context")
                if preview_pack:
                    st.caption(f"Ukuran lampiran: {len(preview_pack):,} karakter")
                    st.text_area("Preview Pack", value=preview_pack[:5000] + ("\n…(dipotong)" if len(preview_pack) > 5000 else ""), height=200, disabled=True)

        st.subheader("📚 Basis Pengetahuan")
        mem = ai_get_all_knowledge()
        st.text_area("Memori Tersimpan (kronologis):", value=(mem if mem else "Memori masih kosong."), height=260, disabled=True)

    # Left: Chat UI
    with left:
        if "ai_messages" not in st.session_state:
            st.session_state.ai_messages = [
                {"role": "assistant", "content": "Halo! Saya Prime AI. Apa yang bisa saya bantu?"}
            ]

        # Wrap chat messages inside a scrollable container (bounded area)
        chat_box = st.container()
        with chat_box:
            # Marker element for CSS :has() selector to apply scroll box styling
            st.markdown("<div class='chat-scroll-marker'></div>", unsafe_allow_html=True)
            for msg in st.session_state.ai_messages:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        user_input = st.chat_input("Tulis pesan Anda…")
        if user_input:
            st.session_state.ai_messages.append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)

            with st.spinner("Prime AI sedang memproses…"):
                # Build combined context: memory + system snapshot
                try:
                    knowledge_context = ai_get_all_knowledge()
                except Exception:
                    knowledge_context = ""
                try:
                    sys_context = ai_build_system_context()
                except Exception:
                    sys_context = ""
                # Combine memory, optional attached DB summary, and live system snapshot
                attached = st.session_state.get("ai_attached_context")
                context_combined = (
                    (knowledge_context or "")
                    + ("\n\n--- Lampiran Data ---\n" + attached if attached else "")
                    + ("\n\n--- Sistem Snapshot ---\n" + sys_context if sys_context else "")
                )

                # Map session messages to Gemini format
                chat_history_for_api = [
                    {"role": m["role"], "parts": [{"text": m["content"]}]} for m in st.session_state.ai_messages
                ]
                reply = ai_generate_response(user_input, chat_history_for_api, context_combined)
                with st.chat_message("assistant"):
                    st.markdown(reply)
                st.session_state.ai_messages.append({"role": "assistant", "content": reply})
                # Rerun to reflow layout so the typing box sits below the latest bubbles
                try:
                    st.rerun()
                except Exception:
                    pass

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
    if st.session_state.page == "Chat AI":
        page_chat_ai()
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
    st.subheader("Update Data untuk Supervisor (Agent fields)")
    st.caption("Kolom-kolom ini berasal dari data upload supervisor dan diupdate oleh Agent.")
    # Load existing values from supervisor_data using Agreement_No equivalence
    sup_agent = fetchone(
        "SELECT id, STATUS, REGISTERED_PHONE, Additional_Contacts, Remarks_Suggested_NIK_Prospect, Payment, Paid_Off_Status "
        "FROM supervisor_data WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=? ORDER BY id DESC LIMIT 1",
        (sel, sel, sel)
    ) or {}
    with st.form("agent_update_supervisor_fields"):
        csa, csb = st.columns(2)
        with csa:
            v_status = st.text_input("STATUS", value=sup_agent.get('STATUS','') or "")
            v_reg_phone = st.text_input("REGISTERED PHONE", value=sup_agent.get('REGISTERED_PHONE','') or "")
            v_payment = st.text_input("Payment", value=str(sup_agent.get('Payment') or ""))
        with csb:
            v_paid_off = st.text_input("Paid Off Status", value=sup_agent.get('Paid_Off_Status','') or "")
            v_add_contacts = st.text_area("Additional Contacts", value=sup_agent.get('Additional_Contacts','') or "", height=80)
            v_remarks = st.text_area("Remarks Suggested NIK Prospect", value=sup_agent.get('Remarks_Suggested_NIK_Prospect','') or "", height=80)
        submit_sup = st.form_submit_button("Simpan ke supervisor_data")
        if submit_sup:
            try:
                if sup_agent.get('id') is not None:
                    execute(
                        "UPDATE supervisor_data SET STATUS=?, REGISTERED_PHONE=?, Additional_Contacts=?, Remarks_Suggested_NIK_Prospect=?, Payment=?, Paid_Off_Status=? WHERE id=?",
                        (v_status.strip(), v_reg_phone.strip(), v_add_contacts.strip(), v_remarks.strip(), v_payment.strip(), v_paid_off.strip(), sup_agent.get('id'))
                    )
                else:
                    # If not found, try a broader update by Case_ID/VA/Third_Uid (may affect multiple rows)
                    execute(
                        "UPDATE supervisor_data SET STATUS=?, REGISTERED_PHONE=?, Additional_Contacts=?, Remarks_Suggested_NIK_Prospect=?, Payment=?, Paid_Off_Status=? WHERE Virtual_Account_Number=? OR Case_ID=? OR Third_Uid=?",
                        (v_status.strip(), v_reg_phone.strip(), v_add_contacts.strip(), v_remarks.strip(), v_payment.strip(), v_paid_off.strip(), sel, sel, sel)
                    )
                # Audit log
                try:
                    u = current_user() or {}
                    execute(
                        "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                        (u.get('id') if u else None, "AGENT_UPDATE_SUP_FIELDS", f"{sel} -> STATUS='{v_status}' REG_PHONE='{v_reg_phone}'")
                    )
                except Exception:
                    pass
                st.success("Data supervisor berhasil diperbarui.")
                st.rerun()
            except Exception as e:
                st.error(f"Gagal memperbarui data supervisor: {e}")

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
    """Clean, compact dashboard inspired by the provided mockup.
    - Top header with logo and title
    - 4 KPI cards: Pending, Selesai, Total User, Bulan Ini
    - Pending approvals banner
    - Recent Activity table and Upcoming PTP deadlines
    """
    require_roles(ALL_ROLES)

    # Header
    top_col1, top_col2 = st.columns([1, 6])
    with top_col1:
        try:
            st.image("logo.png", width=96)
        except Exception:
            st.empty()
    with top_col2:
        st.markdown("<h2 style='margin-bottom:0'>Application Dashboard</h2>", unsafe_allow_html=True)
        st.caption("Ringkasan aktivitas aplikasi dan tenggat terdekat.")

    # -------- KPI calculations --------
    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())
    start_of_month = today.replace(day=1)
    last_month_end = start_of_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Total users (approved)
    total_users = (fetchone("SELECT COUNT(*) c FROM users WHERE approved=1") or {}).get('c', 0)
    # Active (login) today
    active_today = (fetchone("""
        SELECT COUNT(DISTINCT user_id) c
        FROM audit_logs
        WHERE action='LOGIN' AND DATE(timestamp) = DATE(?)
    """, (today.isoformat(),)) or {}).get('c', 0)

    # Completed = agreements with any payment recorded
    completed_total = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM payments WHERE COALESCE(paid_amount,0) > 0") or {}).get('c', 0)
    # Total assigned loans (active)
    assigned_total = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM agent_assignments WHERE IFNULL(active,1)=1") or {}).get('c', 0)
    pending_total = max(assigned_total - completed_total, 0)

    # Completed this week and this month (for tiny captions)
    completed_week = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM payments WHERE DATE(paid_date) >= DATE(?)", (start_of_week.isoformat(),)) or {}).get('c', 0)
    completed_month = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM payments WHERE DATE(paid_date) BETWEEN DATE(?) AND DATE(?)", (start_of_month.isoformat(), today.isoformat())) or {}).get('c', 0)

    # Docs/new assignments this month vs last month
    new_this_month = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM agent_assignments WHERE DATE(assigned_at) BETWEEN DATE(?) AND DATE(?)", (start_of_month.isoformat(), today.isoformat())) or {}).get('c', 0)
    new_last_month = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM agent_assignments WHERE DATE(assigned_at) BETWEEN DATE(?) AND DATE(?)", (last_month_start.isoformat(), last_month_end.isoformat())) or {}).get('c', 0)
    pct_vs_last = 0.0
    try:
        if new_last_month > 0:
            pct_vs_last = (new_this_month / new_last_month) * 100.0
    except Exception:
        pct_vs_last = 0.0

    # Pending delta vs yesterday
    yesterday = today - timedelta(days=1)
    assigned_y = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM agent_assignments WHERE DATE(assigned_at) <= DATE(?)", (yesterday.isoformat(),)) or {}).get('c', 0)
    completed_y = (fetchone("SELECT COUNT(DISTINCT Agreement_No) c FROM payments WHERE DATE(paid_date) <= DATE(?)", (yesterday.isoformat(),)) or {}).get('c', 0)
    pending_y = max(assigned_y - completed_y, 0)
    delta_pending = pending_total - pending_y

    # -------- KPI cards (styled) --------
    st.markdown(
        """
        <style>
        /* KPI cards - row of 4 with soft accent circle like the reference */
        .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 12px 0 4px 0; }
        @media (max-width: 1100px) { .kpi-grid { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 700px) { .kpi-grid { grid-template-columns: 1fr; } }
        .kpi-card { position: relative; overflow: hidden; background: #fff; border: 1px solid #E9ECEF; border-radius: 16px; padding: 18px; box-shadow: 0 2px 6px rgba(16,24,40,0.05); }
        .kpi-card::after { content:""; position:absolute; right:-30px; top:-40px; width:180px; height:180px; border-radius: 50%; background: radial-gradient(circle at center, var(--accent-light, #EEF4FF), rgba(255,255,255,0) 65%); opacity:.8; }
        .kpi-title { letter-spacing: .4px; text-transform: uppercase; font-size: 12px; color: #475467; margin-bottom: 8px; }
        .kpi-value { font-size: 30px; font-weight: 800; color: var(--accent, #1F2937); line-height: 1.1; }
        .kpi-sub { font-size: 12px; color: #667085; margin-top: 6px; }
        /* Accent variants */
        .accent-orange { --accent: #F97316; --accent-light: #FFF7ED; }
        .accent-blue   { --accent: #2563EB; --accent-light: #EEF4FF; }
        .accent-purple { --accent: #9333EA; --accent-light: #F4F3FF; }
        .accent-green  { --accent: #16A34A; --accent-light: #ECFDF3; }
        /* Pills (still used in approvals banner) */
        .pill { display:inline-flex; align-items:center; gap:6px; padding:2px 8px; border-radius: 999px; font-size:12px; }
        .pill-warning { background:#FFF7ED; color:#C2410C; }
        .pill-success { background:#ECFDF3; color:#027A48; }
        .pill-info { background:#EEF4FF; color:#3538CD; }
        .pill-purple { background:#F4F3FF; color:#6941C6; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Render KPI cards side-by-side using Streamlit columns (more reliable than cross-markdown wrappers)
    kpi_cols = st.columns(4)
    with kpi_cols[0]:
        st.markdown(
            f"""
            <div class='kpi-card accent-orange'>
                <div class='kpi-title'>Dokumen Pending</div>
                <div class='kpi-value'>{pending_total:,}</div>
                <div class='kpi-sub'>{{sign}}{abs(delta_pending):,} dari kemarin</div>
            </div>
            """.replace("{sign}", "+" if delta_pending>=0 else "-"),
            unsafe_allow_html=True,
        )
    with kpi_cols[1]:
        st.markdown(
            f"""
            <div class='kpi-card accent-blue'>
                <div class='kpi-title'>Dokumen Selesai</div>
                <div class='kpi-value'>{completed_total:,}</div>
                <div class='kpi-sub'>+{completed_week:,} minggu ini</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with kpi_cols[2]:
        st.markdown(
            f"""
            <div class='kpi-card accent-purple'>
                <div class='kpi-title'>Total User</div>
                <div class='kpi-value'>{total_users:,}</div>
                <div class='kpi-sub'>{active_today:,} aktif hari ini</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with kpi_cols[3]:
        st.markdown(
            f"""
            <div class='kpi-card accent-green'>
                <div class='kpi-title'>Dokumen Bulan Ini</div>
                <div class='kpi-value'>{new_this_month:,}</div>
                <div class='kpi-sub'>{(pct_vs_last or 0):.0f}% dari bulan lalu</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # -------- Pending approvals banner --------
    pending_approvals = get_pending_users_count()
    with st.container():
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class='kpi-card' style='padding:14px;'>
              <div class='kpi-title' style='margin-bottom:2px'>📝 Pending User Approvals</div>
              <div style='display:flex; align-items:center; gap:12px;'>
                <div class='pill pill-info'><strong>{pending_approvals}</strong></div>
                <div class='kpi-sub'>Number of newly registered accounts waiting for admin approval.</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # -------- Bottom tables: Recent logs | Upcoming deadlines --------
    left, right = st.columns([3, 2])

    # Recent Activity Logs
    with left:
        st.subheader("Recent Activity Logs 🧾")
        logs = fetchall(
            """
            SELECT audit_logs.timestamp, COALESCE(users.full_name, users.name, users.login_id) AS user,
                   audit_logs.action, audit_logs.details
            FROM audit_logs
            LEFT JOIN users ON users.id = audit_logs.user_id
            ORDER BY audit_logs.id DESC LIMIT 10
            """
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
    tabs = st.tabs(["Monitoring", "Input", "Trace Assigning", "Agent Assigning", "Trace Results", "Enriched & Lookup", "Freeze Manager"])

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

    # --- Input Tab ---
    with tabs[1]:
        st.subheader("Upload Excel/CSV Supervisor Data")
        field_names = [
            "DT", "Lending_Entity", "Date", "Case_ID", "Task_ID", "Customer_name", "email", "Gender", "Customer_Occupation", "DPD", "Principle_Outstanding", "Principal_Overdue_CURR", "Interest_Overdue_CURR", "Last_Late_Fee", "Return_Date", "Detail", "Loan_Type", "Third_Uid", "Product", "Home_Address", "Province", "City", "Street", "RoomNumber", "Postcode", "Assignment_Date", "Withdrawal_Date", "Phone_Number_1", "Phone_Number_2", "Contact_Type_1", "Contact_Name_1", "Contact_Phone_1", "Contact_Type_2", "Contact_Name_2", "Contact_Phone_2", "Contact_Type_3", "Contact_Name_3", "Contact_Phone_3", "Contact_Type_4", "Contact_Name_4", "Contact_Phone_4", "Contact_Type_5", "Contact_Name_5", "Contact_Phone_5", "Contact_Type_6", "Contact_Name_6", "Contact_Phone_6", "Contact_Type_7", "Contact_Name_7", "Contact_Phone_7", "Contact_Type_8", "Contact_Name_8", "Contact_Phone_8", "Total_debt_in_third_party", "Repayment_on_third_Party", "Remaining_Loan_on_third_Party", "Virtual_Account_Number",
            # Newly added meta fields required by user
            "NIK_KTP", "EMPLOYMENT_UPDATE", "EMPLOYER", "Debtor_Legal_Name", "Employee_Name", "Employee_ID_Number", "Debtor_Relation_to_Employee",
            # Agent-updated fields
            "STATUS", "REGISTERED_PHONE", "Additional_Contacts", "Remarks_Suggested_NIK_Prospect", "Payment", "Paid_Off_Status"
        ]
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
                                for _, row in df_full.iterrows():
                                    try:
                                        vals = [_to_sql_value(row.get(f)) for f in field_names]
                                        execute(
                                            f"INSERT INTO supervisor_data ({','.join(field_names)}) VALUES ({placeholders})",
                                            tuple(vals)
                                        )
                                        saved += 1
                                    except Exception as e:
                                        skipped += 1
                                # Simpan pesan hasil agar tampil sekali setelah rerun
                                st.session_state['sup_upload_result'] = f"Upload selesai. Disimpan: {saved:,}. Dilewati: {skipped:,}."
                                # Audit log
                                u = current_user() or {}
                                try:
                                    execute(
                                        "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                        (u.get('id') if u else None, "UPLOAD_SUPERVISOR", f"Uploaded supervisor data: {saved} rows from '{uploaded.name}' (skipped: {skipped})")
                                    )
                                except Exception:
                                    pass
                                # Clear uploader to prevent re-upload on rerun
                                st.session_state['sup_upload_file'] = None
                                st.rerun()
            except Exception as e:
                st.error(f"Gagal memproses file: {e}")

        st.markdown("---")
        

    with tabs[2]:
        q1, q2, q3, q4 = st.columns([1.2, 1.2, 1.2, 0.6])
        with q1:
            f_case = st.text_input("Filter Case_ID", key="ta_f_case")
        with q2:
            f_name = st.text_input("Filter Customer", key="ta_f_name")
        with q3:
            f_phone = st.text_input("Filter Phone", key="ta_f_phone")
        with q4:
            limit_rows = st.number_input("Limit Row", min_value=10, max_value=2000, value=200, step=10, key="ta_limit")

    # Build SQL with filters
        where = ["Case_ID IS NOT NULL", "TRIM(Case_ID)<>''"]
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
            return f"TRC-{datetime.now().strftime('%y%m%d')}-{prefix}"

        # Process single assign
        if btn_assign_single:
            sel = [r for _, r in (edited or _pd.DataFrame()).iterrows() if bool(r.get("Selected"))]
            if not sel:
                st.warning("Pilih minimal satu baris pada tabel di atas.")
            elif not target_tracer_tbl:
                st.warning("Pilih tracer terlebih dahulu.")
            else:
                try:
                    import sqlite3 as _sql
                    conn = _sql.connect(DB_PATH, timeout=30)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    inserted = 0; updated = 0; frozen = 0
                    for _, r in (edited[edited["Selected"] == True]).iterrows():
                        agr = str(r.get("Case_ID") or "").strip()
                        if not agr:
                            continue
                        nik_val = str(r.get("NIK_KTP") or "").strip() or None
                        # Freeze checks
                        if is_frozen_by_agreement(agr) or (nik_val and is_frozen_by_nik(nik_val)):
                            frozen += 1
                            continue
                        debtor_nm = r.get("Customer_name")
                        # Upsert into assign_tracer
                        trc_code = _gen_trc_code_for(target_tracer_tbl)
                        try:
                            cur.execute(
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
                            # Detect insert vs update by changes? Simpler: try fetch existing before insert
                            # For accuracy, check changes() but SQLite python API may not expose easily; skip granularity here
                            updated += 1
                        except Exception:
                            pass
                    conn.commit(); conn.close()
                    done = (len(sel) - frozen)
                    st.success(f"Assign selesai. Diproses: {done}. Dilewati karena Freeze: {frozen}.")
                    # Audit
                    u = current_user() or {}
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id'), "TRACE_ASSIGN_FROM_SUP_TABLE", f"{done} rows to {target_tracer_tbl}; frozen {frozen}"))
                    except Exception:
                        pass
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal assign: {e}")

        # Process multi assign (random/round-robin)
        if btn_assign_multi:
            sel_df = edited[edited["Selected"] == True] if isinstance(edited, _pd.DataFrame) else _pd.DataFrame()
            if sel_df.empty:
                st.warning("Pilih minimal satu baris pada tabel di atas.")
            elif not tracers_multi:
                st.warning("Pilih minimal satu tracer untuk distribusi.")
            else:
                try:
                    import random, sqlite3 as _sql
                    rows_to_assign = sel_df.to_dict(orient="records")
                    # Shuffle for random distribution
                    random.shuffle(rows_to_assign)
                    conn = _sql.connect(DB_PATH, timeout=30)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    counts = {t: 0 for t in tracers_multi}
                    frozen = 0; done = 0
                    for i, r in enumerate(rows_to_assign):
                        agr = str(r.get("Case_ID") or "").strip()
                        if not agr:
                            continue
                        nik_val = str(r.get("NIK_KTP") or "").strip() or None
                        if is_frozen_by_agreement(agr) or (nik_val and is_frozen_by_nik(nik_val)):
                            frozen += 1
                            continue
                        assignee = tracers_multi[i % len(tracers_multi)]
                        debtor_nm = r.get("Customer_name")
                        trc_code = _gen_trc_code_for(assignee)
                        try:
                            cur.execute(
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
                            counts[assignee] += 1
                            done += 1
                        except Exception:
                            pass
                    conn.commit(); conn.close()
                    # Summary
                    summary = ", ".join([f"{k}:{v}" for k,v in counts.items()])
                    st.success(f"Distribusi selesai. Diproses: {done}. Freeze: {frozen}. Rincian: {summary}")
                    # Audit
                    u = current_user() or {}
                    try:
                        execute("INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)", (u.get('id'), "TRACE_ASSIGN_RANDOM_FROM_SUP_TABLE", f"done {done}; frozen {frozen}; {summary}"))
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
            st.warning(f"{frozen_skipped} baris dilewati karena status Freeze (berdasarkan NIK/Agreement_No).")

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


    # --- Agent Assigning Tab ---
    with tabs[3]:
        st.subheader("Assign ke Agent")
        # Filters similar to Trace Assigning
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
        wh = ["Case_ID IS NOT NULL", "TRIM(Case_ID)<>''"]
        par = []
        if fa_case:
            wh.append("Case_ID LIKE ?")
            par.append(f"%{fa_case.strip()}%")
        if fa_name:
            wh.append("Customer_name LIKE ?")
            par.append(f"%{fa_name.strip()}%")
        if fa_phone:
            wh.append("(Phone_Number_1 LIKE ? OR Phone_Number_2 LIKE ?)")
            par.extend([f"%{fa_phone.strip()}%", f"%{fa_phone.strip()}%"])
        if hide_assigned:
            wh.append("Case_ID NOT IN (SELECT Agreement_No FROM agent_assignments WHERE IFNULL(active,1)=1)")
        wh_sql = " AND ".join(wh) if wh else "1=1"

        # Determine available columns dynamically
        try:
            _sup_cols = fetchall("PRAGMA table_info(supervisor_data)") or []
            sup_cols = {str(r.get('name')) for r in _sup_cols}
        except Exception:
            sup_cols = set()
        base_cols = ["id", "Case_ID", "Customer_name", "NIK_KTP", "DPD", "Phone_Number_1", "Phone_Number_2"]
        extra_cols = [
            # employment details (for context)
            "EMPLOYMENT_UPDATE", "EMPLOYER", "Debtor_Legal_Name", "Employee_Name", "Employee_ID_Number", "Debtor_Relation_to_Employee",
            # agent-editable fields (for visibility)
            "STATUS", "REGISTERED_PHONE", "Additional_Contacts", "Remarks_Suggested_NIK_Prospect", "Payment", "Paid_Off_Status"
        ]
        sel_cols = base_cols + [c for c in extra_cols if c in sup_cols]
        rows_sup = fetchall(
            f"""
            SELECT {', '.join(sel_cols)}
            FROM supervisor_data
            WHERE {wh_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(par + [int(fa_limit)])
        )
        import pandas as _pd
        df = _pd.DataFrame(rows_sup) if rows_sup else _pd.DataFrame(columns=sel_cols)
        for col in extra_cols:
            if col not in df.columns:
                df[col] = ""

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
        try:
            edited = st.data_editor(
                df,
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
                    "STATUS": st.column_config.TextColumn("STATUS", disabled=True),
                    "REGISTERED_PHONE": st.column_config.TextColumn("REGISTERED PHONE", disabled=True),
                    "Additional_Contacts": st.column_config.TextColumn("Additional Contacts", disabled=True),
                    "Remarks_Suggested_NIK_Prospect": st.column_config.TextColumn("Remarks Suggested NIK Prospect", disabled=True),
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
            sel_agent = st.selectbox("Pilih agent", options=["-"] + agents, index=0, key="aa_single_agent")
            btn_single = st.button("Assign ke agent ini", type="primary", key="aa_btn_single")
            if btn_single:
                if not len(selected_rows):
                    st.warning("Pilih minimal satu baris dahulu.")
                elif not sel_agent or sel_agent == "-":
                    st.warning("Pilih agent terlebih dahulu.")
                else:
                    try:
                        u = current_user() or {}
                        by = (u.get('full_name') or u.get('login_id') or '-')
                        frozen_skips = 0
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
                            try:
                                execute(
                                    """
                                    INSERT INTO agent_assignments (Agreement_No, Agent_Assigned_To, assigned_by, active)
                                    VALUES (?,?,?,1)
                                    ON CONFLICT(Agreement_No) DO UPDATE SET
                                        Agent_Assigned_To=excluded.Agent_Assigned_To,
                                        assigned_at=CURRENT_TIMESTAMP,
                                        assigned_by=excluded.assigned_by,
                                        active=1
                                    """,
                                    (agr, sel_agent, by)
                                )
                                assigned += 1
                            except Exception:
                                pass
                        # Audit
                        try:
                            execute(
                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                (u.get('id') if u else None, "AGENT_ASSIGN_FROM_SUP_TABLE", f"Assigned {assigned} to {sel_agent}; frozen skipped {frozen_skips}")
                            )
                        except Exception:
                            pass
                        st.success(f"Berhasil assign {assigned} dokumen. Dilewati (frozen): {frozen_skips}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal assign: {e}")

        with c2:
            st.markdown("#### Distribusi acak ke beberapa Agent")
            sel_agents = st.multiselect("Pilih beberapa agent", options=agents, key="aa_multi_agents")
            btn_multi = st.button("Random distribute", key="aa_btn_multi")
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
                        ids = [str(x).strip() for x in selected_rows['Case_ID'].tolist() if str(x).strip()]
                        _rand.shuffle(ids)
                        assigned = 0
                        frozen_skips = 0
                        for i, agr in enumerate(ids):
                            try:
                                if is_frozen_by_agreement(agr):
                                    frozen_skips += 1
                                    continue
                            except Exception:
                                pass
                            agent = sel_agents[i % len(sel_agents)]
                            try:
                                execute(
                                    """
                                    INSERT INTO agent_assignments (Agreement_No, Agent_Assigned_To, assigned_by, active)
                                    VALUES (?,?,?,1)
                                    ON CONFLICT(Agreement_No) DO UPDATE SET
                                        Agent_Assigned_To=excluded.Agent_Assigned_To,
                                        assigned_at=CURRENT_TIMESTAMP,
                                        assigned_by=excluded.assigned_by,
                                        active=1
                                    """,
                                    (agr, agent, by)
                                )
                                assigned += 1
                            except Exception:
                                pass
                        # Audit
                        try:
                            execute(
                                "INSERT INTO audit_logs (user_id, action, details) VALUES (?,?,?)",
                                (u.get('id') if u else None, "AGENT_ASSIGN_RANDOM_FROM_SUP_TABLE", f"Assigned {assigned} among {len(sel_agents)} agents; frozen skipped {frozen_skips}")
                            )
                        except Exception:
                            pass
                        st.success(f"Distribusi selesai. Berhasil: {assigned}. Dilewati (frozen): {frozen_skips}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal melakukan distribusi: {e}")
        

    # --- Freeze Manager Tab ---
    with tabs[6]:
        st.subheader("Freeze Manager (NIK / Agreement_No)")
        st.caption("Gunakan fitur ini untuk mem-freeze debitur berdasarkan NIK atau kontrak tertentu. Data yang di-freeze tidak akan dapat di-assign ke Tracer maupun Agent.")

        col_fz1, col_fz2 = st.columns(2)
        with col_fz1:
            st.markdown("#### Freeze by NIK")
            nik_in = st.text_input("NIK_KTP", key="freeze_nik")
            reason_nik = st.text_input("Alasan (opsional)", key="freeze_reason_nik")
            note_nik = st.text_area("Catatan (opsional)", key="freeze_note_nik", height=80)
            if st.button("Freeze NIK", key="btn_freeze_nik", type="primary"):
                nik_val = (nik_in or '').strip()
                if not nik_val:
                    st.warning("Masukkan NIK terlebih dahulu.")
                else:
                    try:
                        # If already active, no-op
                        exists = fetchone("SELECT id FROM frozen_entities WHERE active=1 AND NIK_KTP=? LIMIT 1", (nik_val,))
                        if exists:
                            st.info("NIK ini sudah dalam status Freeze.")
                        else:
                            u = current_user() or {}
                            execute(
                                "INSERT INTO frozen_entities (NIK_KTP, reason, note, created_by) VALUES (?,?,?,?)",
                                (nik_val, (reason_nik or '').strip() or None, (note_nik or '').strip() or None, (u.get('full_name') or u.get('login_id') or '-'))
                            )
                            st.success("Berhasil mem-freeze NIK.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Gagal menyimpan: {e}")

        with col_fz2:
            st.markdown("#### Freeze by Contract (Agreement_No)")
            agr_in = st.text_input("Agreement_No", key="freeze_agr")
            reason_agr = st.text_input("Alasan (opsional)", key="freeze_reason_agr")
            note_agr = st.text_area("Catatan (opsional)", key="freeze_note_agr", height=80)
            # Show quick NIK lookup for info
            if (agr_in or '').strip():
                info = fetchone("SELECT Debtor_Name, NIK_KTP FROM assign_tracer WHERE Agreement_No=?", ((agr_in or '').strip(),)) or {}
                if info:
                    st.caption(f"Debtor: {info.get('Debtor_Name') or '-'} | NIK: {info.get('NIK_KTP') or '-'}")
            if st.button("Freeze Agreement_No", key="btn_freeze_agr", type="primary"):
                agr_val = (agr_in or '').strip()
                if not agr_val:
                    st.warning("Masukkan Agreement_No terlebih dahulu.")
                else:
                    try:
                        exists = fetchone("SELECT id FROM frozen_entities WHERE active=1 AND Agreement_No=? LIMIT 1", (agr_val,))
                        if exists:
                            st.info("Agreement_No ini sudah dalam status Freeze.")
                        else:
                            u = current_user() or {}
                            execute(
                                "INSERT INTO frozen_entities (Agreement_No, reason, note, created_by) VALUES (?,?,?,?)",
                                (agr_val, (reason_agr or '').strip() or None, (note_agr or '').strip() or None, (u.get('full_name') or u.get('login_id') or '-'))
                            )
                            st.success("Berhasil mem-freeze Agreement_No.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Gagal menyimpan: {e}")

        st.markdown("---")
        st.markdown("#### Daftar Freeze Aktif")
        rows = fetchall("SELECT id, NIK_KTP, Agreement_No, reason, note, created_by, created_at FROM frozen_entities WHERE active=1 ORDER BY datetime(created_at) DESC")
        if not rows:
            st.info("Belum ada entri Freeze aktif.")
        else:
            # Compute impacted count for display
            disp = []
            for r in rows:
                nik = (r.get('NIK_KTP') or '').strip()
                agr = (r.get('Agreement_No') or '').strip()
                if nik:
                    cnt = (fetchone("SELECT COUNT(*) c FROM assign_tracer WHERE COALESCE(NIK_KTP,'')=?", (nik,)) or {}).get('c', 0)
                    target = f"NIK {nik}"
                elif agr:
                    cnt = (fetchone("SELECT COUNT(*) c FROM assign_tracer WHERE Agreement_No=?", (agr,)) or {}).get('c', 0)
                    target = f"AGR {agr}"
                else:
                    cnt = 0
                    target = "-"
                disp.append({
                    "ID": r.get('id'),
                    "Target": target,
                    "Reason": r.get('reason') or '',
                    "Note": r.get('note') or '',
                    "Impacted rows": cnt,
                    "Created By": r.get('created_by') or '-',
                    "Created At": r.get('created_at') or '-',
                })
            st.dataframe(pd.DataFrame(disp))

            # Unfreeze control
            st.markdown("##### Unfreeze")
            unfreeze_id = st.text_input("Masukkan ID untuk Unfreeze", key="unfreeze_id")
            if st.button("Unfreeze", key="btn_unfreeze"):
                try:
                    uid = int((unfreeze_id or '0').strip())
                    execute("UPDATE frozen_entities SET active=0 WHERE id=?", (uid,))
                    st.success("Berhasil unfreeze.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal unfreeze: {e}")

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
                        # Enforce freeze for Agent assignment upload
                        try:
                            if is_frozen_by_agreement(agr):
                                skip += 1
                                continue
                            info = fetchone("SELECT NIK_KTP FROM assign_tracer WHERE Agreement_No=?", (agr,)) or {}
                            nik = (info.get('NIK_KTP') or '').strip()
                            if nik and is_frozen_by_nik(nik):
                                skip += 1
                                continue
                        except Exception:
                            pass
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

    # --- Enriched & Lookup Tab ---
    with tabs[5]:
        st.title("Enriched Monitoring & Lookup")
        left, right = st.columns([2, 1])
        with left:
            st.subheader("Enriched Monitoring (Loan-centric)")
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

        with right:
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
    - Setiap baris mewakili 1 pinjaman (Agreement_No / Case_ID) — berisi identitas debitur, kontak, DPD, dan outstanding.
    - Aplikasi memeriksa header dan menyimpan ke SQLite (`minama.db`).
    - Supervisor dapat meninjau data, memperbarui baris, dan memilih beberapa Agreement_No untuk ditugaskan ke tracer.
    - Penugasan dicatat pada tabel `assign_tracer`. Setelah tracer menyerahkan hasil, supervisor dapat menugaskan ke agent (tabel `agent_assignments`).
    """, unsafe_allow_html=True)

    st.header("2) Tracer — Verifikasi & Investigasi")
    st.markdown("""
    - Login sebagai tracer (misal: `tracer` / password seed `tracer123` jika belum diubah).
    - Buka menu **Tracer** untuk melihat daftar assignment yang ditugaskan kepadamu (dari `assign_tracer`).
    - Lakukan tracing lapangan/telepon: konfirmasi identitas, status pekerjaan, alamat, catat hasil.
    - Setiap interaksi disimpan di `trace_results` dengan kolom: `Agreement_No`, `status`, `notes`, `touch_type`, `party`, `created_by`.
    - Sistem menyimpan banyak touch record per Agreement_No untuk audit trail.
    """, unsafe_allow_html=True)

    st.header("3) Agent — Penagihan & Pembayaran")
    st.markdown("""
    - Setelah tracer mengonfirmasi debitur, supervisor dapat menugaskan Agreement_No ke agent (tabel `agent_assignments`).
    - Agent login melihat penugasan di menu **Agent**.
    - Agent melaporkan hasil ke `agent_results` dengan field: `Agreement_No`, `agent_status` (PTP/Paid/Refused), `agent_ptp_amount`, `agent_ptp_date`, `agent_notes`.
    - Jika pembayaran terjadi, supervisor bisa menambahkan bukti ke tabel `payments` (`Agreement_No`, `paid_amount`, `paid_date`, `status`, `source_file`, `uploaded_by`).
    """, unsafe_allow_html=True)

    st.header("4) Monitoring & Analytics")
    st.markdown("""
    - Dashboard menampilkan KPI: jumlah pinjaman aktif, sudah lunas, pending, dan metrik per role.
    - Data diambil dari tabel `supervisor_data`, `trace_results`, `agent_results`, dan `payments`.
    - Visualisasi dapat menggunakan Pandas/Altair pada Streamlit.
    """, unsafe_allow_html=True)

    st.header("5) Backup & Auto-Restore")
    st.markdown("""
    - Semua aktivitas penting dicatat di `audit_logs`.
    - Backup otomatis ke Google Drive lewat fungsi `perform_backup()` dan log di `backup_log`.
    - Jika aplikasi restart dan DB terdeteksi fresh (kosong), fungsi `attempt_auto_restore_if_seed()` mencoba restore dari backup Drive terbaru.
    - Pastikan `service_account` disimpan di `st.secrets` untuk mengaktifkan Drive integration.
    """, unsafe_allow_html=True)

    st.header("6) Kontrol Peran & Keamanan")
    st.markdown("""
    - Role yang tersedia: `Superuser`, `Supervisor`, `Tracer`, `Agent`.
    - Akses halaman dikontrol oleh `MENU_ITEMS` dan fungsi `require_roles()`.
    - Autentikasi password di-hash dengan SHA256 (`hash_password()`); akun baru harus disetujui (`approved` flag).
    """, unsafe_allow_html=True)

    st.header("7) Modul Chat AI")
    st.markdown("""
    - Menu **Chat AI** terhubung ke Google Gemini (API key via `st.secrets` atau env var).
    - AI dapat membaca lampiran konteks dari tabel aman (`ai_build_context_pack()`) dan menghasilkan jawaban lewat `ai_generate_response()`.
    - Kolom sensitif (mis. password, service_account, email_token) dikecualikan dari lampiran.
    """, unsafe_allow_html=True)

    st.header("8) Aliran Data (Ringkas)")
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
    - `assign_tracer`: daftar penugasan tracer (Agreement_No, Debtor_Name, NIK_KTP, Assigned_To)
    - `trace_results`: hasil tracing / touch logs (Agreement_No, tracer, status, notes, touch_type, party, touched_at)
    - `agent_assignments`: assignment ke agent (Agreement_No, Agent_Assigned_To, assigned_at, active)
    - `agent_results`: hasil penagihan (Agreement_No, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes)
    - `payments`: rekapan pembayaran (Agreement_No, paid_amount, paid_date, status, source_file)
    - `backup_log`, `audit_logs`, `app_settings`, `ai_knowledge` untuk operasional dan audit
    """, unsafe_allow_html=True)

    st.subheader("Contoh Alur Singkat — Kasus Nyata")
    st.markdown("""
    1. Supervisor mengunggah `supervisor_data_dummy.xlsx` berisi 100 baris. Data tersimpan di `supervisor_data`.
    2. Supervisor memilih 20 Agreement_No dan menugaskan ke `tracer` — entri dibuat di `assign_tracer`.
    3. Tracer membuka menu Tracer, melihat 20 assignment, dan membuat 1–3 touch per debitur; hasil masuk ke `trace_results`.
    4. Supervisor melihat hasil trace, menugaskan 15 kasus ke `agent` — entri di `agent_assignments`.
    5. Agent mendatangi debitur; 5 kasus menghasilkan pembayaran → disimpan di `payments`; agent juga mengisi `agent_results`.
    6. Dashboard menampilkan metrik: conversion rate agent, success tracer, outstanding reductions.
    """, unsafe_allow_html=True)

    st.subheader("FAQ & Troubleshooting Singkat")
    st.markdown("""
    Q: Bagaimana jika upload CSV gagal?
    A: Periksa header file sesuai contoh, pastikan kolom `Agreement_No`/`Case_ID` ada, dan tidak ada duplikat yang memicu constraint.

    Q: Backup Drive tidak berfungsi?
    A: Pastikan `service_account` tersedia di `st.secrets` dan folder `FOLDER_ID_DEFAULT` benar; cek `backup_log` untuk pesan error.

    Q: Restore otomatis tidak terjadi setelah restart?
    A: Fungsi `attempt_auto_restore_if_seed()` hanya berjalan jika DB terdeteksi fresh (few users, empty backup_log). Periksa `app_settings` dan `auto_restore_enabled`.
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.caption("Panduan ini dibuat otomatis dari struktur aplikasi. Untuk tambahan (contoh CSV/Excel, diagram alir, atau export), minta file contoh dan saya tambahkan.")

if __name__ == '__main__':
    main()
