# 📋 Assignment Rotation System Documentation

## Overview
Sistem rotasi assignment yang comprehensive untuk mengelola distribusi case antara Agent dan Tracer dengan aturan mutual exclusion dan rotation fairness.

## 🎯 Key Features

### 1. **Mutual Exclusion**
- ✅ Case yang sedang di-assign ke **Agent** tidak bisa di-assign ke **Tracer** (dan sebaliknya)
- ✅ Hanya satu assignment aktif per case pada satu waktu
- ✅ Validasi otomatis saat assignment untuk mencegah konflik

### 2. **Status Tracking**
#### Active Assignment
- Setiap case menampilkan status assignment aktif di kolom **"Status Assignment"**
- Format status:
  - 🎯 **Agent Active** (Return: YYYY-MM-DD) - Case sedang ditangani Agent
  - 🔍 **Tracer Active** - Case sedang ditangani Tracer
  - 📂 **Available (Fresh)** - Case belum pernah di-assign
  - 📂 **Available (Returned)** - Case sudah pernah di-assign, sekarang kembali ke database

#### Assignment History
- Kolom **"Assignment History"** menampilkan histori lengkap siapa saja yang pernah handle case
- Format: `Agent A (agent) → Agent B (agent) → Tracer C (tracer)`
- Memudahkan tracking untuk rotation fairness

### 3. **Rotation Rules (Khusus Agent)**
#### Prinsip Fairness
Case hanya bisa kembali ke Agent A jika **SEMUA agent lain** sudah pernah handle case tersebut.

#### Contoh Scenario
**Setup:** 5 Agent aktif (A, B, C, D, E)

**Scenario 1: Fresh Case**
- Case baru → Agent A bisa ambil ✅
- Histori: Empty → A bisa ambil (fresh case)

**Scenario 2: Agent A sudah pernah handle**
- Histori: A (completed)
- Agent A mau ambil lagi → ❌ BLOCKED
- Reason: "Agent lain harus handle dulu: B, C, D, E"

**Scenario 3: Rotation Complete**
- Histori: A → B → C → D → E (semua sudah touch)
- Agent A mau ambil lagi → ✅ ALLOWED
- Reason: "OK (rotation complete)"

**Scenario 4: Partial Rotation**
- Histori: A → B → C
- Agent A mau ambil lagi → ❌ BLOCKED
- Reason: "Agent lain harus handle dulu: D, E"

### 4. **Auto-Return System (7 Hari)**
#### Aturan Auto-Return
- Case yang di-assign ke Agent akan **otomatis kembali ke database** setelah 7 hari
- **HANYA** jika belum ada pembayaran masuk
- Jika sudah ada pembayaran → case tetap assigned

#### Flow
```
Day 1: Supervisor assign case ke Agent A
Day 2-7: Agent A bekerja pada case
Day 8: 
  - Jika belum ada payment → ✅ Auto-return ke database
  - Jika sudah ada payment → ❌ Case tetap di Agent A
```

#### Notifikasi
- Dashboard akan menampilkan toast notification saat ada auto-return:
  ```
  🔄 [X] case otomatis kembali ke database (7 hari habis)
  ```

#### Field auto_return_date
- Setiap assignment agent memiliki field `auto_return_date`
- Format: YYYY-MM-DD
- Tampil di kolom "Status Assignment": 
  ```
  🎯 Agent Active (Return: 2025-11-18)
  ```

## 🗄️ Database Schema

### Table: agent_assignments (Enhanced)
```sql
CREATE TABLE agent_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Agreement_No TEXT,
    Agent_Assigned_To TEXT,
    assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
    assigned_by TEXT,
    active INTEGER DEFAULT 1,
    assignment_type TEXT DEFAULT 'agent',  -- NEW: 'agent' or 'tracer'
    auto_return_date TEXT,                 -- NEW: YYYY-MM-DD for agent assignments
    completed_at TEXT,                     -- NEW: When assignment completed
    completion_reason TEXT                 -- NEW: Why assignment ended
);
```

### Table: assignment_history (New)
```sql
CREATE TABLE assignment_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Agreement_No TEXT NOT NULL,
    assigned_to TEXT NOT NULL,
    assignment_type TEXT DEFAULT 'agent',
    assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    assigned_by TEXT,
    completion_notes TEXT
);
```

## 🔧 Helper Functions

### 1. get_active_assignment(agreement_no)
Mendapatkan assignment aktif untuk suatu case.

**Returns:**
```python
{
    'Agreement_No': '12345',
    'assigned_to': 'Agent A',
    'assignment_type': 'agent',
    'assigned_at': '2025-11-11T10:00:00',
    'auto_return_date': '2025-11-18'
}
```

### 2. get_assignment_history(agreement_no, assignment_type=None)
Mendapatkan histori assignment untuk suatu case.

**Returns:**
```python
[
    {'assigned_to': 'Agent E', 'assignment_type': 'agent', 'assigned_at': '...'},
    {'assigned_to': 'Agent D', 'assignment_type': 'agent', 'assigned_at': '...'},
    {'assigned_to': 'Agent C', 'assignment_type': 'agent', 'assigned_at': '...'},
]
```

### 3. can_agent_take_case(agent_name, agreement_no)
Validasi apakah agent bisa mengambil case (rotation check).

**Returns:**
```python
(True, "OK (rotation complete)")
# atau
(False, "Agent lain harus handle dulu: B, C, D")
```

### 4. assign_case_to_agent(agreement_no, agent_name, assigned_by)
Assign case ke agent dengan rotation validation.

**Features:**
- ✅ Validasi rotation rules
- ✅ Validasi freeze status
- ✅ Validasi payment status
- ✅ Auto-set auto_return_date (7 hari)
- ✅ Log ke assignment_history

### 5. assign_case_to_tracer(agreement_no, tracer_name, assigned_by)
Assign case ke tracer (no rotation rules, no time limit).

**Features:**
- ✅ Validasi mutual exclusion
- ✅ Validasi freeze status
- ✅ Log ke assignment_history

### 6. unassign_case(agreement_no, reason)
Remove active assignment (return to database pool).

### 7. check_and_auto_return_expired_assignments()
Background task untuk auto-return expired assignments.

**Triggered:**
- Saat dashboard page load (once per day)
- Check di `page_dashboard()` function

## 📊 UI/UX Changes

### Monitoring Tab (Supervisor)
**New Columns:**
1. **Status Assignment** - Status assignment saat ini dengan emoji visual
2. **Currently Assigned To** - Nama agent/tracer yang sedang handle
3. **Assignment History** - Chain histori assignment

### Agent Assigning Tab (Supervisor)
**Enhanced Feedback:**
```
✅ Berhasil assign: 10 case
❄️ Frozen: 2 case
🔍 Sudah di tracer: 1 case
🔄 Rotation blocked: 3 case
```

### Trace Assigning Tab (Supervisor)
**Enhanced Feedback:**
```
✅ Berhasil assign: 8 case
❄️ Frozen: 1 case
🔒 Already assigned: 2 case
```

## 🚀 Usage Examples

### Example 1: Assign Fresh Case
```python
# Case baru, belum pernah di-assign
success, msg = assign_case_to_agent("CASE-001", "Agent A", "Supervisor X")
# Result: (True, "Case berhasil di-assign ke Agent A (auto-return: 2025-11-18)")
```

### Example 2: Rotation Block
```python
# Case sudah pernah di-handle Agent A, tapi Agent B-E belum semua touch
success, msg = assign_case_to_agent("CASE-001", "Agent A", "Supervisor X")
# Result: (False, "Agent lain harus handle dulu: B, C")
```

### Example 3: Rotation Complete
```python
# Semua agent sudah pernah touch, Agent A bisa ambil lagi
success, msg = assign_case_to_agent("CASE-001", "Agent A", "Supervisor X")
# Result: (True, "Case berhasil di-assign ke Agent A (auto-return: 2025-11-25)")
```

### Example 4: Mutual Exclusion
```python
# Case sedang di-handle tracer
success, msg = assign_case_to_agent("CASE-002", "Agent A", "Supervisor X")
# Result: (False, "Case sedang di-assign ke tracer: Tracer X")
```

### Example 5: Auto-Return
```python
# Hari ke-8, case otomatis kembali ke database (if no payment)
# Triggered automatically by check_and_auto_return_expired_assignments()
# User sees: "🔄 5 case otomatis kembali ke database (7 hari habis)"
```

## 🔐 Business Rules Summary

### ✅ Agent Assignment Rules
1. Case tidak boleh frozen
2. Case tidak boleh sudah ada pembayaran
3. Case tidak boleh sedang assigned ke orang lain (agent/tracer)
4. **ROTATION:** Agent hanya bisa handle case jika semua agent lain sudah pernah handle
5. **AUTO-RETURN:** Case otomatis kembali setelah 7 hari tanpa pembayaran

### ✅ Tracer Assignment Rules
1. Case tidak boleh frozen
2. Case tidak boleh sedang assigned ke orang lain (agent/tracer)
3. **NO ROTATION RULE** - Tracer bisa handle case kapan saja
4. **NO TIME LIMIT** - Tidak ada auto-return untuk tracer

### ✅ Mutual Exclusion
- Case yang sedang di-assign ke Agent → Tidak bisa di-assign ke Tracer
- Case yang sedang di-assign ke Tracer → Tidak bisa di-assign ke Agent
- Hanya setelah unassign/complete, case bisa di-assign ulang ke role lain

## 🎨 Visual Indicators

### Status Emoji Guide
- 🎯 **Agent Active** - Sedang dikerjakan agent (ada deadline)
- 🔍 **Tracer Active** - Sedang dikerjakan tracer (no deadline)
- 📂 **Available (Fresh)** - Case baru, belum pernah di-assign
- 📂 **Available (Returned)** - Case kembali ke pool setelah completed/expired
- ❄️ **Frozen** - Case diblokir dari assignment
- 🔒 **Already Assigned** - Case sudah assigned ke agent/tracer lain
- 🔄 **Rotation Blocked** - Agent belum boleh ambil (waiting for others)

## 📝 Migration Notes

### Existing Data
- Existing `agent_assignments` records akan tetap berfungsi
- New columns akan terisi NULL untuk old records
- Recommended: Run migration script untuk populate `assignment_history` dari existing data

### Backward Compatibility
- ✅ Old assignment logic masih berfungsi via `agent_assignments.active=1`
- ✅ assign_tracer table tetap digunakan untuk compatibility
- ✅ Existing queries tidak akan break

## 🐛 Troubleshooting

### Issue: Agent tidak bisa assign case
**Check:**
1. Apakah case frozen? → Check kolom "Status Assignment"
2. Apakah rotation complete? → Check "Assignment History"
3. Apakah sudah ada payment? → Check payments table

### Issue: Case tidak auto-return
**Check:**
1. Apakah sudah hari ke-8? → Check `auto_return_date`
2. Apakah ada payment? → Case tidak auto-return jika ada payment
3. Apakah dashboard sudah di-load hari ini? → Auto-return trigger saat page load

### Issue: Rotation tidak fair
**Check:**
1. Berapa jumlah agent aktif? → `get_all_active_agents()`
2. Siapa saja yang sudah touch? → `get_agents_who_touched_case()`
3. Apakah semua agent di list aktif (approved=1)? → Check users table

## 🔮 Future Enhancements

### Potential Features
1. **Manual Override** - Supervisor bisa force assign bypass rotation
2. **Rotation Reset** - Reset rotation counter per case bila perlu
3. **Assignment Metrics** - Dashboard untuk track rotation fairness per agent
4. **Auto-Reassign** - Otomatis assign ke next agent in rotation setelah auto-return
5. **Workload Balancing** - Smart distribution based on current agent workload

---

**Version:** 1.0  
**Last Updated:** 2025-11-11  
**Author:** System Architecture Team
