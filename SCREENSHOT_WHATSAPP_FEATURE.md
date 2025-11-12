# 📸 Contract Detail Auto-Screenshot & WhatsApp Feature

## Overview
Fitur ini memungkinkan Agent untuk dengan mudah menampilkan Contract Detail dalam format yang menarik dan mengirimnya ke WhatsApp debtor menggunakan **AUTO-SCREENSHOT** ke clipboard (tidak perlu Windows + Shift + S lagi!).

## ✨ NEW! Auto-Screenshot Feature

### Fitur Utama

### 1. **Show Contract Detail**
- Menampilkan detail kontrak dalam format HTML yang indah
- Style sesuai dengan desain yang diminta (gradient header, tabel detail yang rapi)
- Informasi yang ditampilkan:
  - Debtor Name
  - Phone Number
  - Gender
  - Legal Address
  - Date of Birth (DOB)
  - Email
  - Last Known Office Name
  - Last Known Job Position
  - Last Known Work Phone
  - Debtor Phone Number II
  - Debtor Other Phone Numbers
  - Date of Contract
  - DPD (Days Past Due)

### 2. **Open WhatsApp**
- Membuka WhatsApp Web dengan nomor telepon debtor yang sudah terformat
- Otomatis mengkonversi nomor lokal (08xx) ke format internasional (628xx)
- Membuka di tab baru browser

### 3. **Auto-Screenshot to Clipboard** ⭐ NEW!
- **Tombol auto-screenshot** langsung di dalam Contract Detail HTML
- Menggunakan **html2canvas** library untuk capture HTML ke image
- Screenshot otomatis masuk ke clipboard menggunakan **Clipboard API**
- Tidak perlu download file
- Tidak perlu tekan Windows + Shift + S manual!
- Siap paste (Ctrl+V) langsung ke WhatsApp

## Cara Penggunaan

### Langkah-langkah (Updated - No Manual Screenshot Needed!):

1. **Pilih Case ID** dari tabel Assignment
2. **Klik tombol "📋 Show Contract Detail"**
   - Contract Detail akan muncul di bawah dengan desain yang menarik
3. **Scroll ke bawah** Contract Detail yang muncul
4. **Klik tombol "� Copy Screenshot to Clipboard"** yang ada di bawah contract detail
   - Tombol ini akan mengambil screenshot dan menyalin ke clipboard secara otomatis
   - Status akan muncul: "✅ Screenshot copied to clipboard!"
5. **Klik tombol "�💬 Open WhatsApp"**
   - WhatsApp Web akan terbuka di tab baru dengan nomor debtor
6. **Paste di WhatsApp:**
   - Kembali ke tab WhatsApp yang sudah terbuka
   - Tekan **Ctrl + V** untuk paste screenshot
   - Kirim ke debtor

### Alternative (Manual - If Auto-Screenshot Fails):
Jika auto-screenshot tidak bekerja di browser Anda:
1. Tekan **Windows + Shift + S** (Snipping Tool)
2. Pilih area Contract Detail untuk di-screenshot
3. Screenshot otomatis masuk ke Clipboard
4. Paste dengan **Ctrl + V** di WhatsApp

## Technical Details

### Dependencies Baru
```
pillow        # Untuk image processing
pyperclip     # Untuk clipboard operations (optional, pakai Windows Snipping Tool)
```

### Fungsi Utama

#### `generate_contract_detail_html(case_data: dict) -> str`
- Generate HTML contract detail dengan styling yang menarik
- Input: Dictionary dengan data case
- Output: String HTML yang siap di-render

#### `open_whatsapp_with_clipboard_instruction(phone_number: str) -> str`
- Clean dan format nomor telepon
- Generate WhatsApp Web URL
- Return: URL untuk WhatsApp Web

### Database Query
Mengambil data dari tabel:
- `assign_tracer`: Debtor Name, NIK_KTP
- `supervisor_data`: Phone numbers, Gender, Address, Email, Occupation, DPD, Assignment Date

## UI/UX Enhancements

### Design Features:
- ✅ **Gradient Header**: Purple gradient yang menarik (seperti gambar referensi)
- ✅ **Clean Layout**: Tabel detail yang rapi dan mudah dibaca
- ✅ **Color Coding**: 
  - Highlight values (blue) untuk nama dan nomor penting
  - Red untuk data yang tidak tersedia (#N/A)
- ✅ **Responsive**: Menyesuaikan ukuran browser
- ✅ **Professional**: Box shadow, border-radius untuk tampilan modern

### Buttons:
- **Show Contract Detail**: Primary button dengan icon 📋
- **Open WhatsApp**: Primary button (type="primary") dengan icon 💬
- **Hide Contract Detail**: Close button dengan icon ❌

## Screenshot Instructions (Inline Help)
```
📸 Cara Screenshot ke Clipboard:
1. Tekan Windows + Shift + S untuk membuka Snipping Tool
2. Pilih area Contract Detail di atas untuk di-screenshot
3. Screenshot otomatis masuk ke Clipboard
4. Buka WhatsApp (klik tombol 'Open WhatsApp' di atas)
5. Ctrl + V untuk paste screenshot di chat
```

## Keunggulan Fitur Ini

1. **No Download Required**: Screenshot langsung ke clipboard, tidak perlu simpan file
2. **One-Click WhatsApp**: Otomatis buka WhatsApp dengan nomor yang benar
3. **Beautiful Design**: Contract detail tampil profesional dan menarik
4. **Easy to Use**: Hanya 3 klik untuk kirim contract detail ke debtor
5. **Fast Workflow**: Menghemat waktu agent dalam komunikasi dengan debtor

## Browser Compatibility
- ✅ Chrome (Recommended)
- ✅ Edge
- ✅ Firefox
- ⚠️ Safari (WhatsApp Web might have limitations)

## Future Enhancements (Optional)
- [ ] Auto-screenshot tanpa manual Snipping Tool
- [ ] Template pesan WhatsApp otomatis
- [ ] Bulk screenshot untuk multiple cases
- [ ] Export to PDF option
- [ ] Custom branding/logo di contract detail

## Troubleshooting

### WhatsApp tidak terbuka?
- Pastikan popup blocker tidak aktif
- Coba refresh halaman
- Gunakan browser Chrome/Edge

### Screenshot tidak masuk ke clipboard?
- Pastikan menggunakan Windows 10/11
- Coba Windows + Shift + S lagi
- Alternatif: Gunakan Snipping Tool app langsung

### Nomor WhatsApp salah?
- Cek format nomor di database
- Pastikan nomor diawali 08 atau 628
- Update nomor di supervisor_data jika perlu

## Contact
Untuk pertanyaan atau issue, hubungi developer atau buat ticket di repository.

---
**Version**: 1.0  
**Last Updated**: November 2025  
**Developer**: Project Mido Team
