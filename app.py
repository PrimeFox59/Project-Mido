import streamlit as st
import pandas as pd
import gspread
from passlib.context import CryptContext
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Tuple
from PIL import Image

# --- 1. KONFIGURASI APLIKASI ---
# PENTING: Pastikan ID ini berasal dari folder di dalam SHARED DRIVE
GDRIVE_FOLDER_ID = "1Y98WYhpaqWoYZ2Y5RRGW-KJPXo1nBtAp" 
USERS_SHEET_NAME = "users"
SPREADSHEET_URL = st.secrets["connections"]["gsheets"]["spreadsheet"]
ADMIN_EMAIL_RECIPIENT = "primetroyxs@gmail.com"  # Email tujuan notifikasi

# Muat logo aplikasi dan set konfigurasi halaman dengan logo sebagai icon
try:
    logo_image = Image.open("logo.png")
except Exception:
    logo_image = None  # fallback bila logo tidak ditemukan

st.set_page_config(
    page_title="Minama Management System",
    page_icon=logo_image if logo_image else "📁",
    layout="centered"
)


# --- 2. FUNGSI KONEKSI & AUTENTIKASI ---
# Gunakan bcrypt_sha256 agar tidak terkena limit 72 bytes pada bcrypt standar.
# Tetap sertakan 'bcrypt' untuk kompatibilitas hash lama (jika sudah terlanjur tersimpan).
pwd_context = CryptContext(
    schemes=["bcrypt_sha256", "bcrypt"],
    deprecated=["bcrypt"],
)

BCRYPT_HARD_LIMIT = 72  # batas internal bcrypt (bytes) bila masih memakai skema bcrypt lama

def _prepare_password(pw: str) -> Tuple[str, bool]:
    """Normalisasi password & potong bila melampaui limit bcrypt.
    Returns: (possibly_truncated_password, truncated_flag)
    """
    if not isinstance(pw, str):
        pw = str(pw)
    b = pw.encode("utf-8")
    if len(b) > BCRYPT_HARD_LIMIT:
        # Potong hanya untuk kompatibilitas model lama; bcrypt_sha256 tidak butuh ini,
        # tapi kita lakukan agar bila fallback ke bcrypt tidak error.
        return b[:BCRYPT_HARD_LIMIT].decode("utf-8", errors="ignore"), True
    return pw, False

@st.cache_resource
def get_credentials():
    """Membuat object credentials dari secrets."""
    creds_dict = st.secrets["connections"]["gsheets"]
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
    )
    return creds

@st.cache_resource
def get_gsheets_client():
    """Membuat client untuk gspread menggunakan credentials."""
    creds = get_credentials()
    client = gspread.authorize(creds)
    return client

@st.cache_resource
def get_gdrive_service():
    """Membuat service untuk Google Drive API menggunakan credentials."""
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    return service


# --- 3. FUNGSI HELPER & UTILITAS ---
def hash_password(password: str):
    """Mengubah password plain text menjadi hash.
    - Menggunakan bcrypt_sha256 (default) sehingga password panjang tetap aman.
    - Menangani kemungkinan error panjang ketika fallback ke bcrypt.
    """
    pw_prepared, truncated = _prepare_password(password)
    try:
        return pwd_context.hash(pw_prepared)
    except ValueError as e:
        # Jika tetap gagal karena panjang (kasus anomali yg Anda alami), pakai truncation paksa.
        if "longer than" in str(e).lower():
            safe_pw, _ = _prepare_password(pw_prepared)
            return pwd_context.hash(safe_pw)
        raise

def verify_password(plain_password: str, hashed_password: str):
    """Memverifikasi password dengan hash yang tersimpan.
    Jika hash lama memakai bcrypt & password >72 bytes, otomatis dipotong dengan cara sama.
    """
    pw_prepared, _ = _prepare_password(plain_password)
    try:
        return pwd_context.verify(pw_prepared, hashed_password)
    except ValueError as e:
        # Logika tambahan bila terjadi masalah tak terduga.
        st.error(f"Gagal verifikasi password: {e}")
        return False

def send_notification_email(recipient_email, subject, body):
    """Mengirim email notifikasi menggunakan kredensial dari st.secrets."""
    try:
        sender_email = st.secrets["email_credentials"]["username"]
        sender_password = st.secrets["email_credentials"]["app_password"]

        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipient_email
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(message)
        server.quit()
        st.toast(f"📧 Notifikasi email terkirim ke {recipient_email}")
        return True
    except Exception as e:
        st.toast(f" Gagal mengirim email: {e}")
        return False

def initialize_users_sheet():
    """Memastikan sheet 'users' ada dan berisi user default 'admin'."""
    try:
        client = get_gsheets_client()
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        
        try:
            worksheet = spreadsheet.worksheet(USERS_SHEET_NAME)
            df = pd.DataFrame(worksheet.get_all_records())
        except gspread.WorksheetNotFound:
            st.info(f"Sheet '{USERS_SHEET_NAME}' tidak ditemukan. Membuat sheet baru...")
            worksheet = spreadsheet.add_worksheet(title=USERS_SHEET_NAME, rows="100", cols="2")
            headers = ["username", "password_hash"]
            worksheet.append_row(headers)
            st.success(f"Sheet '{USERS_SHEET_NAME}' berhasil dibuat.")
            df = pd.DataFrame(columns=headers)

        if df.empty or 'admin' not in df['username'].values:
            st.info("User default 'admin' tidak ditemukan. Membuat user...")
            hashed_admin_pass = hash_password('admin')  # default password
            worksheet.append_row(['admin', hashed_admin_pass])
            st.success("User default 'admin' dengan password 'admin' berhasil ditambahkan.")
    except Exception as e:
        st.error(f"Gagal inisialisasi Google Sheet: {e}")


# --- 4. MANAJEMEN SESSION STATE ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""


# --- 5. TAMPILAN HALAMAN (UI) ---
def show_login_page():
    """Menampilkan halaman login dan registrasi."""
    st.header("Minama Management System")
    
    with st.sidebar:
        if 'logo_rendered' not in st.session_state:
            # Pastikan logo hanya dirender sekali per sesi untuk konsistensi
            st.session_state.logo_rendered = True
        if logo_image:
            st.image(logo_image, use_container_width=True)
        st.markdown("### Minama Management System")
        st.subheader("Pilih Aksi")
        action = st.radio(" ", ["Login", "Register"])

    try:
        client = get_gsheets_client()
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        worksheet = spreadsheet.worksheet(USERS_SHEET_NAME)
    except Exception as e:
        st.error(f"Tidak dapat terhubung ke Google Sheet. Pastikan file dibagikan dan URL benar. Error: {e}")
        st.stop()

    if action == "Login":
        st.subheader("Login")
        with st.form("login_form"):
            username = st.text_input("Username").lower()
            password = st.text_input("Password", type="password")
            login_button = st.form_submit_button("Login")

            if login_button:
                if not username or not password:
                    st.warning("Username dan Password tidak boleh kosong.")
                    return

                users_df = pd.DataFrame(worksheet.get_all_records())
                user_data = users_df[users_df["username"] == username]

                if not user_data.empty:
                    stored_hash = user_data.iloc[0]["password_hash"]
                    if verify_password(password, stored_hash):
                        
                        # Kirim notifikasi email saat LOGIN
                        email_subject = "Notifikasi: User Login"
                        email_body = f"User '{username}' telah berhasil LOGIN ke aplikasi Anda."
                        send_notification_email(ADMIN_EMAIL_RECIPIENT, email_subject, email_body)
                        
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.rerun()
                    else:
                        st.error("Username atau Password salah.")
                else:
                    st.error("Username atau Password salah.")

    elif action == "Register":
        st.subheader("Buat Akun Baru")
        with st.form("register_form"):
            new_username = st.text_input("Username Baru").lower()
            new_password = st.text_input("Password Baru", type="password")
            confirm_password = st.text_input("Konfirmasi Password", type="password")
            register_button = st.form_submit_button("Register")

            if register_button:
                if not new_username or not new_password or not confirm_password:
                    st.warning("Semua field harus diisi.")
                    return
                if new_password != confirm_password:
                    st.error("Password tidak cocok.")
                    return
                
                users_df = pd.DataFrame(worksheet.get_all_records())
                if new_username in users_df["username"].values:
                    st.error("Username sudah terdaftar. Silakan pilih yang lain.")
                else:
                    hashed_pass = hash_password(new_password)
                    worksheet.append_row([new_username, hashed_pass])
                    st.success("Registrasi berhasil! Silakan login.")

                    # Kirim notifikasi email saat REGISTRASI
                    email_subject = "Notifikasi: User Baru Telah Mendaftar"
                    email_body = f"User baru dengan username '{new_username}' telah berhasil mendaftar di aplikasi Anda."
                    send_notification_email(ADMIN_EMAIL_RECIPIENT, email_subject, email_body)

def show_main_app():
    """Menampilkan aplikasi utama setelah user berhasil login."""
    # Sidebar branding + status user
    with st.sidebar:
        if logo_image:
            st.image(logo_image, use_container_width=True)
        st.markdown("### Minama Management System")
        st.success(f"Login sebagai: **{st.session_state.username}**")
    if st.sidebar.button("Logout"):
        
        # Kirim notifikasi email saat LOGOUT
        email_subject = "Notifikasi: User Logout"
        email_body = f"User '{st.session_state.username}' telah LOGOUT dari aplikasi Anda."
        send_notification_email(ADMIN_EMAIL_RECIPIENT, email_subject, email_body)
        
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.rerun()

    st.title("📂 File Management - Minama Management System")

    st.header("⬆️ Upload File Baru")
    uploaded_file = st.file_uploader("Pilih file untuk diupload ke Google Drive", type=None)
    
    if uploaded_file is not None:
        if st.button(f"Upload '{uploaded_file.name}'"):
            with st.spinner("Mengupload file..."):
                try:
                    drive_service = get_gdrive_service()
                    file_metadata = {'name': uploaded_file.name, 'parents': [GDRIVE_FOLDER_ID]}
                    file_buffer = io.BytesIO(uploaded_file.getvalue())
                    media = MediaIoBaseUpload(file_buffer, mimetype=uploaded_file.type, resumable=True)
                    
                    file = drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id',
                        supportsAllDrives=True
                    ).execute()
                    st.success(f"✅ File '{uploaded_file.name}' berhasil diupload!")
                except Exception as e:
                    st.error(f"Gagal mengupload file: {e}")

    st.header("📋 Daftar File di Drive")
    if st.button("Refresh Daftar File"):
        st.rerun()
        
    try:
        with st.spinner("Memuat daftar file dari Google Drive..."):
            drive_service = get_gdrive_service()
            query = f"'{GDRIVE_FOLDER_ID}' in parents and trashed=false"
            results = drive_service.files().list(
                q=query,
                pageSize=100,
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            items = results.get('files', [])

        if not items:
            st.info("📂 Folder ini masih kosong atau ID salah/belum di-share.")
        else:
            st.write(f"Ditemukan {len(items)} file:")
            for item in items:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"📄 **{item['name']}**")
                with col2:
                    def download_file_from_drive(file_id):
                        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
                        fh = io.BytesIO()
                        fh.write(request.execute())
                        fh.seek(0)
                        return fh.getvalue()

                    file_data = download_file_from_drive(item['id'])
                    st.download_button(
                        label="Download",
                        data=file_data,
                        file_name=item['name'],
                        key=f"dl_{item['id']}"
                    )
    except Exception as e:
        st.error(f"Gagal memuat daftar file: {e}")


# --- 6. LOGIKA UTAMA APLIKASI ---
if __name__ == "__main__":
    initialize_users_sheet()
    
    if not st.session_state.logged_in:
        show_login_page()
    else:
        show_main_app()
