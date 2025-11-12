# 🚀 Quick Start - Auto-Screenshot & WhatsApp Feature

## Installation

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

New packages added:
- `pillow` - Image processing (optional for future enhancements)

### 2. Run Application
```bash
streamlit run app.py
```

## Quick Usage Guide

### For Agents (Updated - Auto-Screenshot!):

1. **Login** sebagai Agent
2. Buka menu **Agent**
3. Pilih **Case ID** dari tabel
4. Scroll ke bagian **📸 Contract Detail Screenshot & WhatsApp**
5. Klik **"📋 Show Contract Detail"**
6. **Scroll ke bawah** contract detail
7. Klik **"📸 Copy Screenshot to Clipboard"** (di dalam contract detail)
8. Klik **"💬 Open WhatsApp"** (buka di tab baru)
9. **Ctrl + V** di WhatsApp untuk paste screenshot

### ⭐ NEW! No Manual Screenshot Needed:
```
❌ OLD: Windows + Shift + S → Select Area → Ctrl + V
✅ NEW: One Click Button → Auto Copy → Ctrl + V
```

## Features Checklist
- ✅ Beautiful HTML Contract Detail rendering
- ✅ Auto-format phone numbers (08xx → 628xx)
- ✅ One-click WhatsApp Web opener
- ✅ **Auto-screenshot to clipboard (html2canvas + Clipboard API)** ⭐ NEW!
- ✅ No manual Windows Snipping Tool needed
- ✅ Professional gradient design matching reference image
- ✅ Inline instructions for users
- ✅ Session state management for show/hide
- ✅ Visual feedback for screenshot status

## What's New in This Version (v2.0)

### Added:
1. **Auto-Screenshot Button** - One-click screenshot directly in HTML ⭐
2. `html2canvas` library integration (CDN) for HTML-to-image conversion
3. `Clipboard API` integration for automatic clipboard copy
4. Visual status feedback (capturing, success, error)
5. Screenshot button styling with green gradient
6. Enhanced HTML generator with `include_screenshot_js` parameter
7. Fallback instructions for manual screenshot if needed

### Enhanced:
- Contract Detail height increased to 1000px (accommodate screenshot button)
- Better user instructions with step-by-step guide
- Success message with clear workflow
- Info message before showing contract detail

## File Structure
```
Project-Mido/
├── app.py                              # Main application (UPDATED)
├── requirements.txt                    # Dependencies (UPDATED)
├── SCREENSHOT_WHATSAPP_FEATURE.md     # Feature documentation (NEW)
├── QUICK_START.md                     # This file (NEW)
└── minama.db                          # Database
```

## Key Functions Location

### In `app.py`:
- **Line ~750-970**: Helper functions
  - `generate_contract_detail_html(include_screenshot_js=False)` - HTML generator with optional auto-screenshot JS
  - JavaScript `captureAndCopyToClipboard()` - Auto-screenshot function
  - `open_whatsapp_with_clipboard_instruction()` - WhatsApp URL maker
  
- **Line ~3900-3980**: Agent page contract detail section
  - Show Contract Detail button
  - Open WhatsApp button (st.link_button)
  - Auto-screenshot info message
  - HTML rendering with st.components.v1.html() (height=1000)
  - Hide button logic
  - Success message with workflow instructions

## Testing Checklist

### Before Production:
- [ ] Test auto-screenshot button in Chrome
- [ ] Test auto-screenshot button in Edge
- [ ] Test clipboard permission prompt
- [ ] Verify screenshot quality (scale: 2)
- [ ] Test with real phone numbers (08xx format)
- [ ] Test with international format (628xx)
- [ ] Verify HTML rendering in different browsers
- [ ] Test WhatsApp Web opening
- [ ] Test status messages (capturing, success, error)
- [ ] Check responsive design on different screen sizes
- [ ] Verify data accuracy in contract detail
- [ ] Test fallback to manual screenshot if auto fails

### Browser Testing Priority:
- [x] Chrome (Primary - Full Support)
- [x] Edge (Full Support)
- [ ] Firefox (Clipboard permission may be required)
- [ ] Safari (May need fallback to manual)

## Common Issues & Solutions

### Issue: Auto-screenshot button tidak bekerja
**Solution 1**: Gunakan Chrome atau Edge browser (recommended)
**Solution 2**: Pastikan browser meminta clipboard permission dan klik "Allow"
**Solution 3**: Pastikan akses via HTTPS (bukan HTTP lokal)
**Fallback**: Gunakan Windows + Shift + S manual

### Issue: Screenshot quality poor
**Solution**: html2canvas sudah menggunakan scale: 2 untuk high quality. Pastikan zoom browser 100%

### Issue: "Clipboard API not supported" error
**Solution**: Browser terlalu lama atau tidak support. Upgrade browser atau gunakan Chrome/Edge

### Issue: WhatsApp button doesn't work
**Solution**: Check browser popup blocker settings, gunakan st.link_button

### Issue: Phone number format wrong
**Solution**: Update phone number in supervisor_data table with correct format

### Issue: Contract detail not showing
**Solution**: Check if case has data in supervisor_data table

## Performance Notes
- HTML rendering is instant
- Auto-screenshot using html2canvas (~1-2 seconds for capture)
- Clipboard API is non-blocking
- WhatsApp opens in new tab (non-blocking)
- Minimal impact on app performance
- CDN-loaded html2canvas (no server load)

## Security Notes
- No sensitive data sent to external services
- html2canvas runs client-side only
- Clipboard API requires user permission (secure)
- Phone numbers formatted on client-side
- WhatsApp uses official wa.me URL
- No clipboard access from backend

## Browser Permissions Required
- ✅ Popup windows (for WhatsApp Web)
- ✅ Clipboard write access (for auto-screenshot) - Browser will prompt on first use

## Support
For issues or questions:
1. Check SCREENSHOT_WHATSAPP_FEATURE.md for detailed documentation
2. Review inline help text in the application
3. Contact development team

---
**Ready to use!** 🎉

Last updated: November 2025
