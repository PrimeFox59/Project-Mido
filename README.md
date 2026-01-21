# Project Mido – Field Collection & Tracing App

## Ringkas
- Stack: Streamlit + SQLite (local `minama.db`) + Google Drive backup/export.
- Peran: Superuser, Supervisor, Tracer, Agent (seed login: admin/supervisor/tracer/agent dengan password default di [app.py](app.py#L347-L357)).
- Fitur utama: upload kasus (Supervisor Data), assign tracer/agent dengan rotasi & auto-return, distribusi seimbang berdasarkan outstanding, hasil trace & payment recap, auto-screenshot kontrak + WhatsApp, backup/restore DB via Excel/Drive.

## Cara Jalan
1) `pip install -r requirements.txt`
2) `streamlit run app.py`
3) Login pakai akun seed, ganti password setelah login.

## Alur Data (tinggi-level)
```
Upload Supervisor Data (Excel/CSV)
    → tabel supervisor_data (pool kasus)
        → Assign Tracer (assign_tracer + assignment_history)
        → Trace Results dicatat di trace_results
        → Jika perlu, Assign Agent (agent_assignments + assignment_history)
            → Agent update hasil ke agent_results
            → Pembayaran di-upload ke payments
Auto-return 7 hari (tanpa payment) mengembalikan case ke pool agent
Freeze list (frozen_entities) memblokir assignment tertentu
Audit log mencatat aksi user
Backup/Restore: export/import seluruh tabel via Excel/Drive
```

## Menu Utama & Kegunaan
- Dashboard: KPI + auto-return check.
- Supervisor: Monitoring, Upload Supervisor Data, Assign Tracer/Agent, distribusi seimbang, hapus data.
- Tracer: Kerjakan case tracer, simpan trace results.
- Agent: Ambil case (dengan aturan rotasi), lihat detail kontrak, auto-screenshot→WhatsApp, simpan hasil agent & pembayaran.
- G Drive: Backup/restore DB ke Google Drive (Excel / Google Sheet).
- Audit Log: Riwayat aksi user.

## Modul & Fungsi Singkat
- Dashboard: ringkas KPI, trigger auto-return agent (7 hari), overview data.
- Supervisor: unggah/monitor kasus, assign tracer/agent, distribusi seimbang, hapus/replace data, set DT restriction & batch priority.
- Tracer: ambil dan kerjakan kasus tracer, catat status/notes di trace_results.
- Agent: ambil kasus dengan aturan rotasi + auto-return, lihat kontrak, kirim WA dengan auto-screenshot, catat hasil agent dan payment.
- Utility / G Drive: export DB ke Excel, upload & replace DB dari Excel, pull/replace dari Google Drive, backup log.
- Audit Log: lacak aksi (LOGIN/UPLOAD/ASSIGN/DELETE/RESTORE) untuk compliance.
- User Setting: kelola profil/password user (peran sesuai hak akses).

## Role & Authority
| Role | Akses Menu | Kewenangan Utama |
| --- | --- | --- |
| Superuser | Dashboard, Supervisor, Tracer, Agent, Utility/G Drive, Audit Log, User Setting | Upload/replace DB, assign tracer/agent, hapus data, set DT restriction, set prioritized batch, approval/monitoring penuh, lihat semua audit log |
| Supervisor | Dashboard, Supervisor, Tracer, Agent, Utility/G Drive, Audit Log, User Setting | Upload Supervisor Data, assign tracer/agent, distribusi seimbang, hapus data, export/import Excel/Drive, set DT restriction & batch priority, lihat audit log penuh |
| Tracer | Dashboard, Tracer, Audit Log (hanya aktivitas sendiri), User Setting | Ambil & kerjakan kasus tracer, isi trace_results, lihat status assignment tracer; tidak bisa assign agent |
| Agent | Dashboard, Agent, Audit Log (hanya aktivitas sendiri), User Setting | Ambil kasus agent (patuh rotasi/auto-return/freeze), lihat kontrak, auto-screenshot + WhatsApp, isi agent_results, upload payment; tidak bisa assign/hapus |

## Panduan Upload & Replace Database (Excel)
- Tombol “Upload & Replace Database dari Excel” di menu Utility (lihat [app.py](app.py#L11884-L11910)).
- File harus hasil export aplikasi (sheet nama sesuai). Isi sheet akan **mengganti** isi tabel target.
- Kolom `id` boleh diisi manual; jika kosong akan diisi otomatis oleh SQLite. Pastikan unik dan konsisten dengan relasi.

## Aturan Rotasi & Auto-Return (Agent)
- Mutual exclusion: case aktif di agent tidak bisa di tracer, dan sebaliknya.
- Rotasi fairness: agent hanya boleh ambil lagi setelah semua agent lain pernah handle case tersebut (lihat [Assignment Rotation System](ASSIGNMENT_ROTATION_SYSTEM.md)).
- Auto-return: 7 hari tanpa pembayaran → aktif=0, case kembali ke pool agent (lihat `auto_return_date` di agent_assignments).

## Distribusi Seimbang
- Opsi Balanced Distribution membagi case ke beberapa agent dengan greedy balance: meratakan jumlah case dan total Principle Outstanding (lihat [BALANCED_DISTRIBUTION.md](BALANCED_DISTRIBUTION.md)).

## Fitur Auto-Screenshot + WhatsApp
- Di halaman Agent: “Show Contract Detail” → tombol hijau “Copy Screenshot to Clipboard” (html2canvas + Clipboard API) → “Open WhatsApp” → Ctrl+V di WA (lihat [SCREENSHOT_WHATSAPP_FEATURE.md](SCREENSHOT_WHATSAPP_FEATURE.md)).

## Tabel & Kolom (fungsi bisnis)

### users
- id (PK, auto): identitas user.
- name/full_name/login_id/email/password_hash/role/approved/created_at.
- Profil: division, nik, dob, phone_number, alamat, work_email, join_date, nomor_rekening_bca, nama_rekening_bca, sertifikasi_drive_id, sertifikasi_filename.
- Peran: Superuser, Supervisor, Tracer, Agent.

### supervisor_data (pool kasus utama)
- id (PK, auto): contoh 1,2,3…
- DT: bucket/DT source internal.
- Lending_Entity: contoh "AkuLaku"; dipakai filter, tiering, pembatasan DT agent.
- Date: tanggal data.
- Case_ID: ID pinjaman, kunci utama case di seluruh modul.
- Task_ID: ID task eksternal.
- Customer_name, email, Gender, Customer_Occupation.
- DPD: days past due.
- Principle_Outstanding, Principal_Overdue_CURR, Interest_Overdue_CURR, Last_Late_Fee.
- Return_Date, Detail, Loan_Type, Third_Uid, Product.
- Alamat: Home_Address, Province, City, Street, RoomNumber, Postcode.
- Assignment_Date, Withdrawal_Date: batch/penugasan awal lender.
- Phone_Number_1/2.
- Contact_Type_1..8 + Contact_Name_1..8 + Contact_Phone_1..8: kontak tambahan.
- Total_debt_in_third_party / Repayment_on_third_Party / Remaining_Loan_on_third_Party.
- Virtual_Account_Number: VA untuk payment map.
- Tambahan KYC/HR: NIK_KTP, EMPLOYMENT_UPDATE, EMPLOYER, Decoded_Company_Name, Debtor_Legal_Name, Employee_Name, Employee_ID_Number, Debtor_Relation_to_Employee.
- Field agent/tracer: STATUS, REGISTERED_PHONE, Additional_Contacts, Remarks_Suggested_NIK_Prospect, Payment, Paid_Off_Status, Paid_Off.
- Approval: approval_status, approved_by, approved_at, TRC_Code.
- created_at otomatis.

### assign_tracer (legacy kompatibilitas tracer)
- id (PK, auto), TRC_Code, Agreement_No (alias Case_ID), Debtor_Name, NIK_KTP, EMPLOYMENT_UPDATE, EMPLOYER, Debtor_Legal_Name, Employee_Name, Employee_ID_Number, Debtor_Relation_to_Employee, Decoded_Company_Name, Assigned_To, returned_to_supervisor, returned_at, created_at.
- Unik per Agreement_No (index), jadi satu tracer aktif per case.

### agent_assignments (assignment aktif)
- id (PK, auto), Agreement_No, Agent_Assigned_To, assigned_at, assigned_by.
- active (1/0), assignment_type ('agent' atau 'tracer' untuk kompatibilitas), auto_return_date (YYYY-MM-DD), completed_at, completion_reason.
- Index Agreement_No + active untuk lookup cepat.

### assignment_history (jejak siapa pernah pegang)
- Agreement_No, assigned_to, assignment_type, assigned_at, completed_at, assigned_by, completion_notes.
- Dipakai rotasi fairness dan histori UI.

### trace_results
- Agreement_No, tracer, status, notes, touch_type, party, touched_at, created_by.
- Menyimpan setiap touch tracer.

### agent_results
- Agreement_No, agent, agent_status, agent_ptp_amount, agent_ptp_date, agent_notes, updated_at.
- Approval: approval_status (pending/approved/rejected), approval_by, approval_at, rejection_notes.

### payments
- Agreement_No, paid_amount, paid_date, status, source_file, uploaded_by, uploaded_at.
- Lampiran bukti: proof_image_drive_id, proof_image_filename.
- Approval: approval_status/by/at, rejection_notes.

### memos
- Agreement_No, author_role/name, target_role, message, created_at. Komunikasi Agent↔Supervisor.

### audit_logs
- user_id, action, details, timestamp. Semua aksi penting terekam.

### app_settings
- key/value konfigurasi (mis. prioritized batch AkuLaku).

### backup_log
- catatan backup DB ke Drive: file_name, drive_file_id, status, message, backup_time.

### record_notes
- catatan manual terkait restore/checkpoint.

### migration_history
- operasi upload/migrasi: operation_type, target_table, affected_ids, source_file, user_id, created_at, undone flag.

### masked_companies
- kamus nama perusahaan yang didekode: masked_name → canonical_name, mapping_notes.

### frozen_entities
- NIK_KTP atau Agreement_No dibekukan: reason, note, active, created_by, created_at. Mencegah assignment.

### agent_tiers
- user_id → tier (Priority_1/2/3/4). Dipakai kontrol akses batch prioritas AkuLaku.

## Catatan Praktis Kolom Kunci
- Case_ID/Agreement_No/Virtual_Account_Number: identitas case; dipakai lintas tabel (assign, trace, payment, memo).
- Lending_Entity: penting untuk filter, pembatasan DT agent, dan tiering batch AkuLaku.
- auto_return_date: deadline auto-return agent (7 hari).
- approval_status (agent_results/payments/supervisor_data): alur approval internal.
- TRC_Code: kode tracer assignment (format TRC-YYMMDD-XXX).

### Mapping Kunci Utama antar Tabel
| Key | Sumber Utama | Dipakai di | Fungsi |
| --- | --- | --- | --- |
| Case_ID | supervisor_data.Case_ID | assign_tracer (Agreement_No), agent_assignments (Agreement_No), assignment_history.Agreement_No, trace_results.Agreement_No, agent_results.Agreement_No, payments.Agreement_No, memos.Agreement_No | Identitas kasus utama lintas modul (tracer, agent, payment, memo). |
| Agreement_No | agent_assignments.Agreement_No | assignment_history, trace_results, agent_results, payments, memos, lookup ke supervisor_data (Case_ID/Virtual_Account_Number/Third_Uid) | Alias Case_ID pada modul assignment/agent/payment. |
| Virtual_Account_Number | supervisor_data.Virtual_Account_Number | lookup Lending_Entity/payment matching, helper get lending entity | Nomor VA untuk pemetaan payment & identifikasi kasus. |
| TRC_Code | assign_tracer.TRC_Code | supervisor_data (approval/TRC_Code), monitoring tracer | Kode penugasan tracer (TRC-YYMMDD-XXX) sebagai jejak assign tracer. |
| Lending_Entity | supervisor_data.Lending_Entity | filter Monitoring, DT restriction agent, tiering batch AkuLaku | Menentukan lender/DT, mempengaruhi akses agent dan prioritas batch. |
| auto_return_date | agent_assignments.auto_return_date | Status Assignment (Monitoring), auto-return 7 hari | Tanggal batas auto-return kasus agent jika belum ada payment. |
| approval_status | supervisor_data / agent_results / payments | Approval flow di supervisor/agent/payment | Status persetujuan internal (pending/approved/rejected). |
| id | Semua tabel (PK auto) | Referensi internal; bisa diisi manual saat replace DB Excel bila konsisten | Primary key per tabel; jika kosong saat upload, dibuat otomatis oleh SQLite. |

## Backup/Restore & Export/Import
- Export seluruh DB ke Excel: tombol "Generate & Download Excel DB".
- Replace DB dari Excel lokal: "Upload & Replace Database dari Excel" (isi sheet akan menggantikan tabel terkait).
- Pull/Replace dari Google Drive (Excel / Google Sheet) dengan tombol "Pull & Replace DB".
- Backup log dicatat di backup_log; proses memakai Google Drive Service Account (lihat `SCOPES` di [app.py](app.py#L17-L24)).

## Alur Upload Supervisor Data (tab Supervisor → Input)
1) Download template (kolom sudah lengkap).
2) Isi data; wajib: Case_ID, Customer_name, Virtual_Account_Number (lainnya opsional kecuali kebutuhan bisnis).
3) Upload CSV/XLSX.
4) Aplikasi preview; kalau OK, klik "Upload ke sistem".
5) Jika Case_ID sudah ada, baris lama dihapus lalu di-insert baru (id baru otomatis jika tidak diisi).

## Kontrol Pembatasan
- Freeze: daftar di frozen_entities mencegah assign berdasarkan NIK atau Agreement_No.
- DT restriction per agent: Supervisor dapat set daftar Lending_Entity yang boleh diambil agent.
- Prioritized batch AkuLaku: hanya Priority_1 yang boleh ambil batch tertentu (lihat pengaturan di Agent Assigning tab).

## Contoh Istilah
- id: angka auto-increment unik per tabel (contoh: 1, 2, 3) kecuali di-replace manual saat import Excel.
- Lending_Entity: nama lender/DT (contoh: AkuLaku, MNTMIN); dipakai filter kasus, tiering, dan pembatasan agent.
- DPD: usia tunggakan (hari) untuk prioritas penagihan.
- TRC_Code: kode penugasan tracer, format TRC-YYMMDD-XXX.
- auto_return_date: tanggal case otomatis kembali ke pool agent bila belum ada payment.

## Audit & Keamanan
- Semua aksi kritikal tercatat di audit_logs (LOGIN/UPLOAD/ASSIGN/DELETE/RESTORE).
- Seed password sebaiknya segera diganti.
- Clipboard/WhatsApp hanya terjadi di sisi browser; tidak ada data dikirim keluar selain ke WhatsApp saat Anda membuka link.

## Troubleshooting Singkat
- Upload gagal: cek header sesuai template; kolom wajib lengkap; nilai id unik jika diisi manual.
- Tidak bisa assign agent: cek frozen_entities, mutual exclusion dengan tracer, aturan rotasi, atau payment sudah masuk.
- Auto-screenshot gagal: gunakan Chrome/Edge; izinkan clipboard; fallback Windows+Shift+S.
- Drive import gagal: periksa kredensial service account dan akses folder Google Drive.

---
Dokumen lain: [QUICK_START.md](QUICK_START.md) · [ASSIGNMENT_ROTATION_SYSTEM.md](ASSIGNMENT_ROTATION_SYSTEM.md) · [BALANCED_DISTRIBUTION.md](BALANCED_DISTRIBUTION.md) · [SCREENSHOT_WHATSAPP_FEATURE.md](SCREENSHOT_WHATSAPP_FEATURE.md) · [AUTO_SCREENSHOT_UPDATE.md](AUTO_SCREENSHOT_UPDATE.md)
