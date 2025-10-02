import streamlit as st
import pandas as pd
import bcrypt
import gspread
from gspread.exceptions import WorksheetNotFound

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Aplikasi Login & Register",
    page_icon="🔑",
    layout="centered"
)

# --- KONEKSI & SETUP GOOGLE SHEETS ---
def get_gsheet_connection():
    """Menghubungkan ke Google Sheets menggunakan kredensial dari st.secrets."""
    try:
        # Menggunakan koneksi gsheets bawaan Streamlit
        creds = st.secrets["connections"]["gsheets"]
        gc = gspread.service_account_from_dict(creds)
        # Buka spreadsheet berdasarkan key yang ada di file secrets.toml
        sh = gc.open_by_key(st.secrets["connections"]["gsheets"]["spreadsheet"].split('/')[-2])
        return sh
    except Exception as e:
        st.error(f"Gagal terhubung ke Google Sheets. Pastikan konfigurasi 'secrets.toml' sudah benar. Error: {e}")
        st.stop() # Hentikan eksekusi jika koneksi gagal

# Panggil fungsi koneksi
sh = get_gsheet_connection()

def get_worksheet(sheet_name):
    """Mendapatkan worksheet berdasarkan nama. Mengembalikan None jika tidak ditemukan."""
    try:
        return sh.worksheet(sheet_name)
    except WorksheetNotFound:
        return None

def check_and_create_user_worksheet():
    """Memeriksa apakah worksheet 'users' ada. Jika tidak, buat worksheet baru dengan header."""
    worksheet_name = "users"
    headers = ['username', 'password_hash', 'role']
    
    existing_worksheets = [ws.title for ws in sh.worksheets()]
    
    if worksheet_name not in existing_worksheets:
        st.info(f"Worksheet '{worksheet_name}' tidak ditemukan. Membuat sekarang...")
        new_ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="10")
        new_ws.append_row(headers)
        st.success(f"Worksheet '{worksheet_name}' berhasil dibuat.")
        # Tambahkan superuser pertama kali
        hashed_password = bcrypt.hashpw("admin123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        new_ws.append_row(["admin", hashed_password, "admin"])
        st.info("Akun admin default ('admin' / 'admin123') telah dibuat.")


# --- FUNGSI DATA & AUTENTIKASI ---
@st.cache_data(ttl=60) # Cache data pengguna selama 60 detik
def get_users_df():
    """Mengambil semua data pengguna dari worksheet 'users' dan mengembalikannya sebagai DataFrame."""
    worksheet = get_worksheet("users")
    if worksheet:
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        return df.astype(str) # Pastikan semua data string untuk menghindari error tipe
    return pd.DataFrame()

def append_user_to_gsheet(username, password_hash, role):
    """Menambahkan baris pengguna baru ke worksheet 'users'."""
    worksheet = get_worksheet("users")
    if worksheet:
        worksheet.append_row([username, password_hash, role])
        st.cache_data.clear() # Hapus cache setelah ada data baru
        return True
    return False

def check_login(username, password):
    """Memverifikasi kredensial login pengguna."""
    users_df = get_users_df()
    if username in users_df['username'].values:
        user_data = users_df[users_df['username'] == username].iloc[0]
        stored_hash = user_data['password_hash'].encode('utf-8')
        
        # Verifikasi password
        if bcrypt.checkpw(password.encode('utf-8'), stored_hash):
            return True, user_data['role']
    return False, None

# --- HALAMAN UI (LOGIN, REGISTER, MAIN APP) ---

def login_page():
    """Menampilkan halaman login."""
    st.title("Masuk ke Akun Anda")
    with st.form("login_form"):
        username = st.text_input("Nama Pengguna", key="login_username")
        password = st.text_input("Kata Sandi", type="password", key="login_password")
        submitted = st.form_submit_button("Login")
        
        if submitted:
            success, role = check_login(username, password)
            if success:
                # Simpan status login di session state
                st.session_state['logged_in'] = True
                st.session_state['username'] = username
                st.session_state['role'] = role
                st.rerun() # Muat ulang halaman untuk menampilkan konten setelah login
            else:
                st.error("Nama pengguna atau kata sandi salah.")

def register_page():
    """Menampilkan halaman registrasi."""
    st.title("Buat Akun Baru")
    with st.form("register_form"):
        new_username = st.text_input("Nama Pengguna Baru", key="reg_username")
        new_password = st.text_input("Kata Sandi Baru", type="password", key="reg_password")
        confirm_password = st.text_input("Konfirmasi Kata Sandi", type="password", key="reg_confirm_password")
        submitted = st.form_submit_button("Register")

        if submitted:
            users_df = get_users_df()
            if not new_username or not new_password:
                st.warning("Nama pengguna dan kata sandi tidak boleh kosong.")
            elif new_password != confirm_password:
                st.error("Konfirmasi kata sandi tidak cocok.")
            elif new_username in users_df['username'].values:
                st.error("Nama pengguna sudah ada. Silakan pilih nama lain.")
            else:
                # Hash password sebelum disimpan
                hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                # Tambahkan pengguna baru ke Google Sheets
                if append_user_to_gsheet(new_username, hashed_password, "user"): # Role default adalah 'user'
                    st.success("Registrasi berhasil! Silakan kembali ke halaman Login untuk masuk.")
                else:
                    st.error("Terjadi kesalahan saat menyimpan data.")

def main():
    """Fungsi utama untuk menjalankan aplikasi."""
    # Inisialisasi session state jika belum ada
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    # Logika untuk menampilkan halaman
    if st.session_state['logged_in']:
        # Tampilan setelah login berhasil
        st.title(f"🎉 Selamat Datang, {st.session_state['username']}!")
        st.markdown("---")
        st.info("Anda telah berhasil masuk ke dalam sistem.")
        
        # Tombol Logout
        if st.button("Logout 🚪"):
            # Hapus semua data dari session state
            for key in st.session_state.keys():
                del st.session_state[key]
            st.rerun() # Muat ulang untuk kembali ke halaman login
    
    else:
        # Tampilan sebelum login
        st.sidebar.title("Navigasi")
        page = st.sidebar.radio("Pilih Halaman", ["Login", "Register"])
        
        if page == "Login":
            login_page()
        elif page == "Register":
            register_page()

if __name__ == "__main__":
    # Pastikan worksheet 'users' ada sebelum aplikasi dijalankan
    check_and_create_user_worksheet()
    main()




