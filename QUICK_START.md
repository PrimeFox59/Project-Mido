# 🚀 Quick Start - Screenshot & WhatsApp Feature

## Installation

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

New packages added:
- `pillow` - Image processing
- `pyperclip` - Clipboard operations (backup, Windows Snipping Tool is primary)

### 2. Run Application
```bash
streamlit run app.py
```

## Quick Usage Guide

### For Agents:

1. **Login** sebagai Agent
2. Buka menu **Agent**
3. Pilih **Case ID** dari tabel
4. Scroll ke bagian **📸 Contract Detail Screenshot & WhatsApp**
5. Klik **"📋 Show Contract Detail"**
6. Klik **"💬 Open WhatsApp"**
7. Screenshot menggunakan **Windows + Shift + S**
8. Paste di WhatsApp dengan **Ctrl + V**

### Screenshot Instructions:
```
Windows + Shift + S → Select Area → Auto Copy to Clipboard → Ctrl + V in WhatsApp
```

## Features Checklist
- ✅ Beautiful HTML Contract Detail rendering
- ✅ Auto-format phone numbers (08xx → 628xx)
- ✅ One-click WhatsApp Web opener
- ✅ Manual screenshot to clipboard (Windows Snipping Tool)
- ✅ Professional gradient design matching reference image
- ✅ Inline instructions for users
- ✅ Session state management for show/hide

## What's New in This Version

### Added:
1. `generate_contract_detail_html()` - Generate beautiful contract detail HTML
2. `open_whatsapp_with_clipboard_instruction()` - WhatsApp URL generator
3. New imports: `PIL`, `base64`, `webbrowser`, `urllib.parse`
4. Contract Detail display section in Agent page
5. Show/Hide toggle for contract detail
6. WhatsApp button with auto-open
7. Comprehensive inline instructions

### Modified:
- `requirements.txt` - Added pillow, pyperclip
- `app.py` - Enhanced page_agent() function
- Enhanced supervisor_data query to fetch more fields

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
- **Line ~740-840**: Helper functions
  - `generate_contract_detail_html()` - HTML generator
  - `open_whatsapp_with_clipboard_instruction()` - WhatsApp URL maker
  
- **Line ~3880-3980**: Agent page contract detail section
  - Show Contract Detail button
  - Open WhatsApp button
  - HTML rendering with st.components.v1.html()
  - Hide button logic

## Testing Checklist

### Before Production:
- [ ] Test with real phone numbers (08xx format)
- [ ] Test with international format (628xx)
- [ ] Verify HTML rendering in different browsers
- [ ] Test WhatsApp Web opening
- [ ] Verify screenshot quality
- [ ] Test show/hide toggle
- [ ] Check responsive design on different screen sizes
- [ ] Verify data accuracy in contract detail

### Browser Testing:
- [ ] Chrome
- [ ] Edge
- [ ] Firefox

## Common Issues & Solutions

### Issue: WhatsApp button doesn't work
**Solution**: Check browser popup blocker settings

### Issue: Screenshot quality poor
**Solution**: Use Windows Snipping Tool with higher zoom level

### Issue: Phone number format wrong
**Solution**: Update phone number in supervisor_data table with correct format

### Issue: Contract detail not showing
**Solution**: Check if case has data in supervisor_data table

## Performance Notes
- HTML rendering is instant
- No server-side screenshot (client-side only)
- WhatsApp opens in new tab (non-blocking)
- Minimal impact on app performance

## Security Notes
- No sensitive data sent to external services
- Phone numbers formatted on client-side
- WhatsApp uses official wa.me URL
- No clipboard access from backend (uses OS-level snipping tool)

## Browser Permissions Required
- ✅ Popup windows (for WhatsApp Web)
- ✅ Clipboard access (handled by Windows Snipping Tool)

## Support
For issues or questions:
1. Check SCREENSHOT_WHATSAPP_FEATURE.md for detailed documentation
2. Review inline help text in the application
3. Contact development team

---
**Ready to use!** 🎉

Last updated: November 2025
