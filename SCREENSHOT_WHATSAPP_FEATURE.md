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
pillow        # Untuk image processing (optional, for future enhancements)
```

### JavaScript Libraries (CDN)
```
html2canvas v1.4.1  # Auto-screenshot HTML to canvas
Clipboard API       # Native browser API for clipboard access
```

### Fungsi Utama

#### `generate_contract_detail_html(case_data: dict, include_screenshot_js: bool = False) -> str`
- Generate HTML contract detail dengan styling yang menarik
- **Parameter baru:** `include_screenshot_js` - jika True, menyertakan JavaScript auto-screenshot
- **JavaScript Features:**
  - `html2canvas` - Capture HTML element ke canvas
  - `captureAndCopyToClipboard()` - Function untuk screenshot dan copy ke clipboard
  - Automatic status updates (capturing, success, error)
  - Button hide/show logic
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

1. **No Manual Screenshot Required** ⭐ - Satu klik untuk screenshot, tidak perlu Windows Snipping Tool!
2. **Auto Copy to Clipboard** - Screenshot langsung masuk clipboard, siap paste
3. **No Download Required** - Tidak ada file yang disimpan, lebih cepat dan bersih
4. **One-Click WhatsApp** - Otomatis buka WhatsApp dengan nomor yang benar
5. **Beautiful Design** - Contract detail tampil profesional dan menarik
6. **Easy to Use** - Hanya 3 klik untuk kirim contract detail ke debtor
7. **Fast Workflow** - Menghemat waktu agent dalam komunikasi dengan debtor
8. **Visual Feedback** - Status message real-time untuk setiap action

## Browser Compatibility

### Auto-Screenshot Feature:
- ✅ **Chrome (Recommended)** - Full support for Clipboard API & html2canvas
- ✅ **Edge** - Full support for Clipboard API & html2canvas
- ⚠️ **Firefox** - May require clipboard permissions prompt
- ⚠️ **Safari** - Limited Clipboard API support, use manual screenshot fallback

### WhatsApp Web:
- ✅ Chrome, Edge, Firefox
- ⚠️ Safari (may have limitations)

## Future Enhancements (Optional)
- [x] Auto-screenshot tanpa manual Snipping Tool ✅ DONE!
- [ ] Template pesan WhatsApp otomatis
- [ ] Bulk screenshot untuk multiple cases
- [ ] Export to PDF option
- [ ] Custom branding/logo di contract detail
- [ ] Watermark on screenshot

## Troubleshooting

### Auto-Screenshot tidak bekerja?
**Possible Causes:**
1. Browser tidak support Clipboard API (Safari/older browsers)
2. HTTPS required untuk Clipboard API
3. User belum memberikan clipboard permission

**Solutions:**
- Gunakan Chrome atau Edge browser (recommended)
- Pastikan akses via HTTPS (bukan HTTP)
- Klik "Allow" saat browser meminta clipboard permission
- Fallback: Gunakan Windows + Shift + S manual

### Screenshot quality rendah?
- html2canvas menggunakan scale: 2 untuk high quality
- Pastikan zoom browser 100%
- Gunakan browser dengan hardware acceleration enabled

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
