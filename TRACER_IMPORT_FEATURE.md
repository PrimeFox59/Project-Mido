# 📥 Tracer Import to Supervisor Data Feature

## Overview
Fitur ini memungkinkan **Tracer** atau **Supervisor** untuk menarik data yang sudah di-assign ke Tracer (beserta semua update yang sudah dilakukan) dan memasukkannya ke dalam **Supervisor Data** menu.

## Why This Feature?

### Problem Solved:
- 🔄 **Data Sync**: Data yang sudah diupdate oleh Tracer (NIK, Employment Update, Employer, dll) dapat langsung masuk ke sistem supervisor
- 📊 **Centralized Data**: Semua data yang sudah diverifikasi tracer tersentralisasi di supervisor_data
- 🔍 **Better Tracking**: Supervisor dapat melihat progress dan hasil kerja tracer dengan lebih mudah
- ✅ **No Duplicate Entry**: Sistem otomatis mengecek apakah data sudah ada (update) atau baru (insert)

## Features

### 1. Smart Import System
- ✅ **Bulk Import**: Pilih multiple assignments sekaligus untuk import
- ✅ **Auto Update**: Jika Case_ID sudah ada di supervisor_data, sistem akan UPDATE data yang ada
- ✅ **Auto Insert**: Jika Case_ID belum ada, sistem akan INSERT sebagai data baru
- ✅ **COALESCE Strategy**: Hanya update field yang memiliki nilai, tidak menghapus data existing yang kosong

### 2. Data Yang Di-Import
Data dari `assign_tracer` yang akan masuk ke `supervisor_data`:
- 📌 **Case_ID** / Agreement_No
- 👤 **Customer_name** / Debtor_Name
- 🆔 **NIK_KTP**
- 💼 **EMPLOYMENT_UPDATE** (NIHIL, PNS, DEBITUR, SUAMI, ISTRI, dll)
- 🏢 **EMPLOYER** (Masked company name)
- 🔓 **Decoded_Company_Name** (Auto-decoded from library)
- 📝 **Debtor_Legal_Name**
- 👨‍💼 **Employee_Name**
- 🔢 **Employee_ID_Number**
- 👥 **Debtor_Relation_to_Employee**
- 🏷️ **TRC_Code**

### 3. Audit Trail
- 📋 Setiap import dicatat di `audit_logs` dengan action `TRACER_IMPORT_TO_SUPERVISOR`
- 🔍 Detail mencakup: user, assignment ID, Case_ID, timestamp

## How to Use

### For Tracer:

1. **Login** sebagai Tracer
2. Buka menu **Tracer**
3. Lihat **Daftar Assignment** Anda
4. **Centang** satu atau beberapa assignment yang ingin di-import
5. Scroll ke bagian **📥 Import Data ke Supervisor Menu**
6. Klik tombol **"📥 Import ke Supervisor Data"**
7. Tunggu proses import selesai
8. Lihat summary: berapa data baru ditambahkan, berapa diupdate

### For Supervisor:

1. **Login** sebagai Supervisor/Superuser
2. Buka menu **Tracer**
3. Lihat **semua assignment** dari semua tracer
4. Filter berdasarkan Case_ID atau NIK jika perlu
5. **Centang** assignment yang ingin di-import ke supervisor data
6. Klik **"📥 Import ke Supervisor Data"**
7. Data akan masuk ke menu **Supervisor** → tab **Monitoring**

## Use Cases

### Scenario 1: Tracer Selesai Update Data
```
1. Tracer mendapat 20 assignments
2. Tracer update NIK, EMPLOYER, Employment Update untuk semua
3. Tracer centang semua (atau select all)
4. Import ke Supervisor Data
5. Supervisor langsung dapat melihat hasil update di menu Monitoring
```

### Scenario 2: Progressive Import
```
1. Tracer update 5 data dari 20
2. Import 5 data tersebut dulu
3. Lanjut update 5 data lagi
4. Import lagi
5. Repeat until done
```

### Scenario 3: Supervisor Review & Import
```
1. Supervisor cek hasil kerja tracer di menu Tracer
2. Review data yang sudah diupdate
3. Pilih data yang sudah valid
4. Import yang sudah valid saja ke supervisor data
5. Minta tracer perbaiki sisanya
```

## Technical Details

### Database Tables
- **Source**: `assign_tracer` (data yang sudah diassign ke tracer)
- **Destination**: `supervisor_data` (data utama sistem)
- **Audit**: `audit_logs` (tracking semua import activity)

### Logic Flow
```
1. User pilih assignment(s) → selected_list
2. For each assignment:
   a. Fetch data from assign_tracer
   b. Check if Case_ID exists in supervisor_data
   c. If exists: UPDATE with COALESCE (keep existing non-null values)
   d. If not exists: INSERT new row
   e. Log to audit_logs
3. Show summary: X inserted, Y updated, Z failed
```

### COALESCE Strategy
```sql
UPDATE supervisor_data 
SET Customer_name = COALESCE(?, Customer_name),
    NIK_KTP = COALESCE(?, NIK_KTP),
    -- dst...
WHERE Case_ID = ? OR Virtual_Account_Number = ?
```
- **Behavior**: Only update jika value baru tidak NULL
- **Benefit**: Tidak menghapus data existing yang sudah ada

## UI/UX

### Visual Feedback
- ℹ️ **Info**: Menampilkan jumlah assignment yang dipilih
- ✅ **Success**: "X data baru ditambahkan, Y data diupdate"
- ⚠️ **Warning**: "Z data gagal" (jika ada error)
- ❌ **Error**: Detail error per assignment (toast notification)
- 🎈 **Balloons**: Animation saat import berhasil

### Button States
- **Disabled**: Jika tidak ada assignment yang dipilih
- **Primary**: Button "Import ke Supervisor Data" dengan icon 📥
- **Auto Rerun**: Setelah import berhasil, page auto-refresh

## Version History

### v1.0 (January 2026)
- ✅ Initial release
- ✅ Basic import functionality
- ✅ Auto update/insert logic
- ✅ Audit logging
- ✅ Support for Decoded_Company_Name field
- ✅ TRC_Code tracking

## Future Enhancements (Planned)
- 🔄 Auto-import on tracer submission (optional toggle)
- 📊 Import history viewer
- 🔍 Advanced filtering before import
- 📧 Email notification to supervisor when tracer imports data
- 📈 Import statistics dashboard

## Related Documentation
- [ASSIGNMENT_ROTATION_SYSTEM.md](ASSIGNMENT_ROTATION_SYSTEM.md)
- [BALANCED_DISTRIBUTION.md](BALANCED_DISTRIBUTION.md)
- [QUICK_START.md](QUICK_START.md)

## Support
Jika ada pertanyaan atau masalah, hubungi tim developer atau buka menu **Guide** di aplikasi.
