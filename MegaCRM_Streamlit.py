# MegaCRM_Streamlit_App.py — CRM + Finance (MB/Bizerte) + InterNotes + Reassign Log + Payouts + Monthly Stats + Payment Edit
# =================================================================================================
# إضافات جديدة:
# - 📅 إحصائيات شهرية (اختيار شهر والاطلاع على الأداء العام + حسب الموظّف + حسب التكوين)
# - ✏️ تعديل/تكملة دفعة موجودة: بحث على جميع الأشهر لنفس Libellé وتعديل نفس الصف داخل ورقة الشهر
# - Payouts تبقى كما هي، و Reassign_Log يسجّل "شكون حرّك" العميل

import json, time, urllib.parse, base64, uuid
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta, timezone
from PIL import Image

# ---------------- Page config ----------------
st.set_page_config(page_title="MegaCRM", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <div style='text-align:center;'>
        <h1 style='color:#333; margin-top: 8px;'>📊 CRM MEGA FORMATION - إدارة العملاء</h1>
    </div>
    <hr>
    """,
    unsafe_allow_html=True
)

# ---------------- Google Sheets Auth ----------------
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client_and_sheet_id():
    try:
        sa = st.secrets["gcp_service_account"]
        sa_info = dict(sa) if hasattr(sa, "keys") else (json.loads(sa) if isinstance(sa, str) else {})
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = st.secrets["SPREADSHEET_ID"]
        return client, sheet_id
    except Exception:
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"  # بدّلها إذا يلزم
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ======================================================================
#                               CONSTANTS
# ======================================================================
INTER_NOTES_SHEET = "InterNotes"
INTER_NOTES_HEADERS = ["timestamp","sender","receiver","message","status","note_id"]

REASSIGN_LOG_SHEET   = "Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

EXPECTED_HEADERS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

FIN_REV_COLUMNS = [
    "Date", "Libellé", "Prix",
    "Montant_Admin", "Montant_Structure", "Montant_PreInscription", "Montant_Total",
    "Echeance", "Reste",
    "Mode", "Employé", "Catégorie", "Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]

# 🆕 Payouts (خلاص الإدارة/المكوّنين)
PAYOUTS_COLUMNS = [
    "Date", "Type", "Personne", "Libellé", "Montant",
    "Caisse_Source", "Mode", "Employé", "Note"
]

FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind == "Revenus" else ("Dépense " if kind == "Dépenses" else "Payout ")
    short = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB", "MB_2025!")), "Bizerte": str(b.get("BZ", "BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba": "MB_2025!", "Bizerte": "BZ_2025!"}

# ======================================================================
#                               HELPERS
# ======================================================================
def safe_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = pd.Index(df.columns).astype(str)
    return df.loc[:, ~df.columns.duplicated(keep="first")]

def fmt_date(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def format_display_phone(s: str) -> str:
    d = "".join(ch for ch in str(s) if s is not None and ch.isdigit())
    return f"+{d}" if d else ""

def color_tag(val):
    if isinstance(val, str) and val.strip().startswith("#") and len(val.strip()) == 7:
        return f"background-color: {val}; color: white;"
    return ""

def mark_alert_cell(val: str):
    s = str(val).strip()
    if not s: return ''
    if "متأخر" in s: return 'background-color: #ffe6b3; color: #7a4e00'
    return 'background-color: #ffcccc; color: #7a0000'

def highlight_inscrit_row(row: pd.Series):
    insc = str(row.get("Inscription", "")).strip().lower()
    return ['background-color: #d6f5e8' if insc in ("inscrit","oui") else '' for _ in row.index]

def _to_num_series_any(s):
    return (
        pd.Series(s).astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )

def ensure_ws(title: str, columns: list[str]):
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        ws.update("1:1", [columns])
        return ws
    rows = ws.get_all_values()
    if not rows:
        ws.update("1:1", [columns])
    else:
        header = rows[0]
        if not header or header[:len(columns)] != columns:
            ws.update("1:1", [columns])
    return ws

# ======================================================================
#                               InterNotes
# ======================================================================
def inter_notes_open_ws():
    return ensure_ws(INTER_NOTES_SHEET, INTER_NOTES_HEADERS)

def inter_notes_append(sender: str, receiver: str, message: str):
    if not message.strip():
        return False, "النص فارغ"
    ws = inter_notes_open_ws()
    ts = datetime.now(timezone.utc).isoformat()
    note_id = str(uuid.uuid4())
    ws.append_row([ts, sender, receiver, message.strip(), "unread", note_id])
    return True, note_id

def inter_notes_fetch_all_df() -> pd.DataFrame:
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1:
        return pd.DataFrame(columns=INTER_NOTES_HEADERS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for c in INTER_NOTES_HEADERS:
        if c not in df.columns:
            df[c] = ""
    return df

def inter_notes_fetch_unread(receiver: str) -> pd.DataFrame:
    df = inter_notes_fetch_all_df()
    return df[(df["receiver"] == receiver) & (df["status"] == "unread")].copy()

def inter_notes_mark_read(note_ids: list[str]):
    if not note_ids:
        return
    ws = inter_notes_open_ws()
    values = ws.get_all_values()
    if not values or len(values) <= 1:
        return
    header = values[0]
    try:
        idx_note = header.index("note_id")
        idx_status = header.index("status")
    except ValueError:
        return
    for r, row in enumerate(values[1:], start=2):
        if len(row) > idx_note and row[idx_note] in note_ids:
            ws.update_cell(r, idx_status + 1, "read")

def play_sound_mp3(path="notification.mp3"):
    try:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        st.markdown(
            f"""
            <audio autoplay>
              <source src="data:audio/mp3;base64,{b64}" type="audio/mp3">
            </audio>
            """,
            unsafe_allow_html=True,
        )
    except FileNotFoundError:
        pass

def inter_notes_ui(current_employee: str, all_employees: list[str], is_admin: bool=False):
    st.subheader("📝 النوط الداخلية")

    with st.expander("✍️ إرسال نوط لموظف آخر", expanded=True):
        col1, col2 = st.columns([1,2])
        with col1:
            receivers = [e for e in all_employees if e != current_employee] if all_employees else []
            receiver = st.selectbox("الموظّف المستلم", receivers)
        with col2:
            message = st.text_area("الملاحظة", placeholder="اكتب ملاحظة قصيرة...")

        if st.button("إرسال ✅", use_container_width=True):
            ok, info = inter_notes_append(current_employee, receiver, message)
            st.success("تم الإرسال 👌") if ok else st.error(f"تعذّر الإرسال: {info}")

    st.divider()
    _autorefresh = getattr(st, "autorefresh", None) or getattr(st, "experimental_autorefresh", None)
    if callable(_autorefresh):
        _autorefresh(interval=10_000, key="inter_notes_poll")

    if "prev_unread_count" not in st.session_state:
        st.session_state.prev_unread_count = 0

    unread_df = inter_notes_fetch_unread(current_employee)
    unread_count = len(unread_df)

    try:
        if unread_count > st.session_state.prev_unread_count:
            st.toast("📩 نوط جديدة وصْلتك!", icon="✉️")
            play_sound_mp3()
    finally:
        st.session_state.prev_unread_count = unread_count

    st.markdown(f"### 📥 غير المقروء: **{unread_count}**")
    if unread_count == 0:
        st.info("ما فماش نوط غير مقروءة حاليا.")
    else:
        st.dataframe(
            unread_df[["timestamp","sender","message","note_id"]].sort_values("timestamp", ascending=False),
            use_container_width=True, height=220
        )
        colA, colB = st.columns(2)
        with colA:
            if st.button("اعتبر الكل مقروء ✅", use_container_width=True):
                inter_notes_mark_read(unread_df["note_id"].tolist())
                st.success("تم التعليم كمقروء."); st.rerun()
        with colB:
            selected_to_read = st.multiselect(
                "اختار رسائل لتعليمها كمقروء",
                options=unread_df["note_id"].tolist(),
                format_func=lambda nid: f"من {unread_df[unread_df['note_id']==nid]['sender'].iloc[0]} — {unread_df[unread_df['note_id']==nid]['message'].iloc[0][:30]}..."
            )
            if st.button("تعليم المحدد كمقروء", disabled=not selected_to_read, use_container_width=True):
                inter_notes_mark_read(selected_to_read)
                st.success("تم التعليم كمقروء."); st.rerun()

    st.divider()
    df_all_notes = inter_notes_fetch_all_df()
    mine = df_all_notes[(df_all_notes["receiver"] == current_employee) | (df_all_notes["sender"] == current_employee)].copy()
    st.markdown("### 🗂️ مراسلاتي")
    if mine.empty:
        st.caption("ما عندكش مراسلات مسجلة بعد.")
    else:
        def _fmt_ts(x):
            try:
                return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
            except:
                return x
        mine["وقت"] = mine["timestamp"].apply(_fmt_ts)
        mine = mine[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False)
        st.dataframe(mine, use_container_width=True, height=280)

    if is_admin:
        st.divider(); st.markdown("### 🛡️ لوحة مراقبة الأدمِن (كل المراسلات)")
        if df_all_notes.empty:
            st.caption("لا توجد مراسلات بعد.")
        else:
            def _fmt_ts2(x):
                try:
                    return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except:
                    return x
            df_all_notes["وقت"] = df_all_notes["timestamp"].apply(_fmt_ts2)
            disp = df_all_notes[["وقت","sender","receiver","message","status","note_id"]].sort_values("وقت", ascending=False)
            st.dataframe(disp, use_container_width=True, height=320)

# ======================================================================
#                       Load all CRM (employee sheets only)
# ======================================================================
@st.cache_data(ttl=600)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    worksheets = sh.worksheets()
    all_dfs, all_employes = [], []
    for ws in worksheets:
        title = ws.title.strip()
        if title.endswith("_PAIEMENTS"):    continue
        if title.startswith("_"):           continue
        if title.startswith("Revenue ") or title.startswith("Dépense ") or title.startswith("Payout "): continue
        if title in (INTER_NOTES_SHEET, REASSIGN_LOG_SHEET): continue

        all_employes.append(title)
        rows = ws.get_all_values()
        if not rows:
            ws.update("1:1", [EXPECTED_HEADERS]); rows = ws.get_all_values()

        data_rows = rows[1:] if len(rows) > 1 else []
        fixed_rows = []
        for r in data_rows:
            r = list(r or [])
            if len(r) < len(EXPECTED_HEADERS): r += [""] * (len(EXPECTED_HEADERS) - len(r))
            else: r = r[:len(EXPECTED_HEADERS)]
            fixed_rows.append(r)

        df = pd.DataFrame(fixed_rows, columns=EXPECTED_HEADERS)
        df["__sheet_name"] = title
        all_dfs.append(df)

    big = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=EXPECTED_HEADERS + ["__sheet_name"])
    return big, all_employes

df_all, all_employes = load_all_data()

# ======================================================================
#                               Sidebar
# ======================================================================
try:
    st.sidebar.image(Image.open("assets/logo.png"), use_container_width=True)
except Exception:
    try:
        st.sidebar.image(Image.open("logo.png"), use_container_width=True)
    except Exception:
        pass

tab_choice = st.sidebar.radio(
    "📑 اختر تبويب:",
    ["CRM", "مداخيل (MB/Bizerte)", "💼 خلاص الإدارة/المكوّنين", "📝 نوط داخلية"],
    index=0
)
role = st.sidebar.radio("الدور", ["موظف", "أدمن"], horizontal=True)
employee = None
if role == "موظف":
    employee = st.sidebar.selectbox("👨‍💼 اختر الموظّف (ورقة Google Sheets)", all_employes) if all_employes else None

# ======================================================================
#                            Admin lock
# ======================================================================
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at", None)
    return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(role=="أدمن" and not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"):
                st.session_state["admin_ok"] = False
                st.session_state["admin_ok_at"] = None
                st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password", key="admin_pwd_inp")
            if st.button("فتح صفحة الأدمِن"):
                conf = str(st.secrets.get("admin_password", "admin123"))
                if admin_pwd and admin_pwd == conf:
                    st.session_state["admin_ok"] = True
                    st.session_state["admin_ok_at"] = datetime.now()
                    st.success("تم فتح صفحة الأدمِن لمدة 30 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

if role == "أدمن":
    admin_lock_ui()

# ======================================================================
#                    Finance Readers/Writers (Revenus/Dépenses)
# ======================================================================
def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind == "Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            if c in df.columns:
                df[c] = _to_num_series_any(df[c])
        if "Alert" not in df.columns:
            df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce")
            reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            df.loc[ech.notna() & (ech < today_ts) & (reste > 0), "Alert"] = "⚠️ متأخر"
            df.loc[ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0), "Alert"] = "⏰ اليوم"
    else:
        if "Montant" in df.columns:
            df["Montant"] = _to_num_series_any(df["Montant"])
    return safe_unique_columns(df)

def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ======================================================================
#                  Payouts (خلاص الإدارة/المكوّنين) Readers/Writers
# ======================================================================
def payouts_read_df(title: str) -> pd.DataFrame:
    ws = ensure_ws(title, PAYOUTS_COLUMNS)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=PAYOUTS_COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if "Montant" in df.columns:
        df["Montant"] = _to_num_series_any(df["Montant"])
    return safe_unique_columns(df)

def payouts_append_row(title: str, row: dict):
    ws = ensure_ws(title, PAYOUTS_COLUMNS)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)

# ======================================================================
#      🆕 Revenus helpers لقراءة/تجميع دفعات نفس Libellé عبر كل الأشهر
# ======================================================================
def find_revenus_across_months_for_libelle(branch: str, libelle: str) -> pd.DataFrame:
    """يرجع كل الأسطر (Revenus) في السنة الحالية لنفس Libellé عبر جميع الأشهر للفرع."""
    out = []
    for m in FIN_MONTHS_FR:
        title = fin_month_title(m, "Revenus", branch)
        try:
            df = fin_read_df(title, "Revenus")
        except Exception:
            df = pd.DataFrame(columns=FIN_REV_COLUMNS)
        if not df.empty and "Libellé" in df.columns:
            sub = df[df["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()].copy()
            if not sub.empty:
                sub["__sheet_title"] = title
                sub["__mois"] = m
                out.append(sub)
    if out:
        return pd.concat(out, ignore_index=True)
    return pd.DataFrame(columns=FIN_REV_COLUMNS + ["__sheet_title","__mois"])

def find_revenus_row_index(ws, libelle: str, date_str: str) -> int | None:
    """نلقى رقم الصف عبر مطابقة Libellé + Date (مكتوبة dd/mm/YYYY)"""
    rows = ws.get_all_values()
    if not rows: return None
    header = rows[0]
    try:
        idx_lib = header.index("Libellé")
        idx_dt  = header.index("Date")
    except ValueError:
        return None
    for i, r in enumerate(rows[1:], start=2):
        if len(r) > max(idx_lib, idx_dt):
            if r[idx_lib].strip().lower() == libelle.strip().lower() and r[idx_dt].strip() == date_str.strip():
                return i
    return None

# ======================================================================
#                                   CRM مشتقّات + لوحة
# ======================================================================
df_all = df_all.copy()
if not df_all.empty:
    df_all["DateAjout_dt"] = pd.to_datetime(df_all["Date ajout"], dayfirst=True, errors="coerce")
    df_all["DateSuivi_dt"] = pd.to_datetime(df_all["Date de suivi"], dayfirst=True, errors="coerce")
    df_all["Mois"] = df_all["DateAjout_dt"].dt.strftime("%m-%Y")
    today = datetime.now().date()
    base_alert = df_all["Alerte"].fillna("").astype(str).str.strip()
    dsv_date = df_all["DateSuivi_dt"].dt.date
    due_today = dsv_date.eq(today).fillna(False)
    overdue  = dsv_date.lt(today).fillna(False)
    df_all["Alerte_view"] = base_alert
    df_all.loc[base_alert.eq("") & overdue, "Alerte_view"] = "⚠️ متابعة متأخرة"
    df_all.loc[base_alert.eq("") & due_today, "Alerte_view"] = "⏰ متابعة اليوم"
    df_all["Téléphone_norm"] = df_all["Téléphone"].apply(normalize_tn_phone)
    ALL_PHONES = set(df_all["Téléphone_norm"].dropna().astype(str))
    df_all["Inscription_norm"] = df_all["Inscription"].fillna("").astype(str).str.strip().str.lower()
    inscrit_mask = df_all["Inscription_norm"].isin(["oui", "inscrit"])
    df_all.loc[inscrit_mask, "Date de suivi"] = ""
    df_all.loc[inscrit_mask, "Alerte_view"] = ""
else:
    df_all["Alerte_view"] = ""; df_all["Mois"] = ""; df_all["Téléphone_norm"] = ""; ALL_PHONES = set()

# ---------------- Dashboard سريع ----------------
st.subheader("لوحة إحصائيات سريعة")
df_dash = df_all.copy()
if df_dash.empty:
    st.info("ما فماش داتا للعرض.")
else:
    df_dash["DateAjout_dt"] = pd.to_datetime(df_dash.get("Date ajout"), dayfirst=True, errors="coerce")
    df_dash["DateSuivi_dt"] = pd.to_datetime(df_dash.get("Date de suivi"), dayfirst=True, errors="coerce")
    today = datetime.now().date()
    df_dash["Inscription_norm"] = df_dash["Inscription"].fillna("").astype(str).str.strip().str.lower()
    df_dash["Alerte_norm"]      = df_dash["Alerte_view"].fillna("").astype(str).str.strip()
    added_today_mask      = df_dash["DateAjout_dt"].dt.date.eq(today)
    registered_today_mask = df_dash["Inscription_norm"].isin(["oui", "inscrit"]) & added_today_mask
    alert_now_mask        = df_dash["Alerte_norm"].ne("")
    total_clients    = int(len(df_dash))
    added_today      = int(added_today_mask.sum())
    registered_today = int(registered_today_mask.sum())
    alerts_now       = int(alert_now_mask.sum())
    registered_total = int((df_dash["Inscription_norm"] == "oui").sum())
    rate = round((registered_total / total_clients) * 100, 2) if total_clients else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("👥 إجمالي العملاء", f"{total_clients}")
    with c2: st.metric("🆕 المضافون اليوم", f"{added_today}")
    with c3: st.metric("✅ المسجّلون اليوم", f"{registered_today}")
    with c4: st.metric("🚨 التنبيهات الحالية", f"{alerts_now}")
    with c5: st.metric("📈 نسبة التسجيل الإجمالية", f"{rate}%")

# ---------------- ملخّص حسب الموظف ----------------
df_stats = df_all.copy()
if not df_stats.empty:
    df_stats["Inscription_norm"] = df_stats["Inscription"].fillna("").astype(str).str.strip().str.lower()
    df_stats["Alerte_norm"]      = df_stats["Alerte_view"].fillna("").astype(str).str.strip()
    df_stats["DateAjout_dt"]     = pd.to_datetime(df_stats.get("Date ajout"), dayfirst=True, errors="coerce")
    df_stats["DateSuivi_dt"]     = pd.to_datetime(df_stats.get("Date de suivi"), dayfirst=True, errors="coerce")
    today = datetime.now().date()
    added_today_mask      = df_stats["DateAjout_dt"].dt.date.eq(today)
    registered_today_mask = df_stats["Inscription_norm"].isin(["oui","inscrit"]) & added_today_mask
    alert_now_mask        = df_stats["Alerte_norm"].ne("")
    df_stats["__added_today"] = added_today_mask
    df_stats["__reg_today"]   = registered_today_mask
    df_stats["__has_alert"]   = alert_now_mask

    grp_base = (
        df_stats.groupby("__sheet_name", dropna=False)
        .agg(
            Clients   = ("Nom & Prénom", "count"),
            Inscrits  = ("Inscription_norm", lambda x: (x == "oui").sum()),
            تنبيهات     = ("__has_alert", "sum"),
            مضافون_اليوم = ("__added_today", "sum"),
            مسجلون_اليوم = ("__reg_today", "sum"),
        )
        .reset_index().rename(columns={"__sheet_name": "الموظف"})
    )
    grp_base["% تسجيل"] = ((grp_base["Inscrits"] / grp_base["Clients"]).replace([float("inf"), float("nan")], 0) * 100).round(2)
    grp_base = grp_base.sort_values(by=["تنبيهات", "Clients"], ascending=[False, False])
    st.markdown("#### حسب الموظّف")
    st.dataframe(grp_base, use_container_width=True)

# ======================= 🆕 📅 إحصائيات شهرية (اختيار شهر) =======================
st.markdown("---")
st.subheader("📅 إحصائيات شهرية (العملاء)")
if not df_all.empty and "DateAjout_dt" in df_all.columns:
    df_all["MonthStr"] = df_all["DateAjout_dt"].dt.strftime("%Y-%m")
    months_avail = sorted(df_all["MonthStr"].dropna().unique(), reverse=True)
    month_pick = st.selectbox("اختر شهر", months_avail, index=0 if months_avail else None, key="stats_month_pick")
    if month_pick:
        y, m = month_pick.split("-")
        month_mask = (df_all["DateAjout_dt"].dt.strftime("%Y-%m") == month_pick)
        df_month = df_all[month_mask].copy()

        total_clients_m = len(df_month)
        total_inscrits_m = int((df_month["Inscription_norm"] == "oui").sum())
        alerts_m = int(df_month["Alerte_view"].fillna("").astype(str).str.strip().ne("").sum())
        rate_m = round((total_inscrits_m / total_clients_m) * 100, 2) if total_clients_m else 0.0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("👥 عملاء هذا الشهر", f"{total_clients_m}")
        c2.metric("✅ مسجّلون", f"{total_inscrits_m}")
        c3.metric("🚨 تنبيهات", f"{alerts_m}")
        c4.metric("📈 نسبة التسجيل", f"{rate_m}%")

        st.markdown("#### 👨‍💼 حسب الموظّف (هذا الشهر)")
        grp_emp = (
            df_month.groupby("__sheet_name", dropna=False)
            .agg(
                Clients=("Nom & Prénom","count"),
                Inscrits=("Inscription_norm", lambda x: (x=="oui").sum()),
                Alerts=("Alerte_view", lambda x: (x.fillna("").astype(str).str.strip()!="").sum()),
            )
            .reset_index().rename(columns={"__sheet_name":"الموظف"})
        )
        grp_emp["% تسجيل"] = ((grp_emp["Inscrits"]/grp_emp["Clients"]).replace([float("inf"), float("nan")],0)*100).round(2)
        st.dataframe(grp_emp.sort_values(["Inscrits","Clients"], ascending=False), use_container_width=True)

        st.markdown("#### 📚 حسب التكوين (هذا الشهر)")
        grp_form = (
            df_month.groupby("Formation", dropna=False)
            .agg(
                Clients=("Nom & Prénom","count"),
                Inscrits=("Inscription_norm", lambda x: (x=="oui").sum()),
            )
            .reset_index().rename(columns={"Formation":"التكوين"})
        )
        grp_form["% تسجيل"] = ((grp_form["Inscrits"]/grp_form["Clients"]).replace([float("inf"), float("nan")],0)*100).round(2)
        st.dataframe(grp_form.sort_values(["Inscrits","Clients"], ascending=False), use_container_width=True)
else:
    st.caption("لا توجد بيانات كافية لإظهار الإحصائيات الشهرية.")

# ======================================================================
#                تبويب "مداخيل (MB/Bizerte)" (Revenus/Dépenses)
# ======================================================================
if tab_choice == "مداخيل (MB/Bizerte)":
    st.title("💸 المداخيل والمصاريف — (منزل بورقيبة & بنزرت)")

    with st.sidebar:
        st.markdown("---"); st.subheader("🔧 إعدادات المداخيل/المصاريف")
        branch = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="fin_branch")
        kind_ar = st.radio("النوع", ["مداخيل","مصاريف"], horizontal=True, key="fin_kind_ar")
        kind = "Revenus" if kind_ar == "مداخيل" else "Dépenses"
        mois   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="fin_month")

        BRANCH_PASSWORDS = _branch_passwords()
        key_pw = f"finance_pw_ok::{branch}"
        if key_pw not in st.session_state: st.session_state[key_pw] = False
        if not st.session_state[key_pw]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"fin_pw_{branch}")
            if st.button("دخول الفرع", key=f"fin_enter_{branch}"):
                if pw_try and pw_try == BRANCH_PASSWORDS.get(branch, ""):
                    st.session_state[key_pw] = True; st.success("تم الدخول ✅")
                else:
                    st.error("كلمة سرّ غير صحيحة ❌")

    if not st.session_state.get(f"finance_pw_ok::{branch}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    fin_title = fin_month_title(mois, kind, branch)
    df_fin = fin_read_df(fin_title, kind)
    df_view = df_fin.copy()

    if role == "موظف" and employee and "Employé" in df_view.columns:
        df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == (employee or "").strip().lower()]

    with st.expander("🔎 فلاتر"):
        c1, c2, c3 = st.columns(3)
        date_from = c1.date_input("من تاريخ", value=None, key="fin_from")
        date_to   = c2.date_input("إلى تاريخ", value=None, key="fin_to")
        search    = c3.text_input("بحث (Libellé/Catégorie/Mode/Note)", key="fin_search")
        if "Date" in df_view.columns:
            if date_from: df_view = df_view[df_view["Date"] >= pd.to_datetime(date_from)]
            if date_to:   df_view = df_view[df_view["Date"] <= pd.to_datetime(date_to)]
        if search and not df_view.empty:
            m = pd.Series([False]*len(df_view))
            for col in [c for c in ["Libellé","Catégorie","Mode","Employé","Note","Caisse_Source","Montant_PreInscription"] if c in df_view.columns]:
                m |= df_view[col].fillna("").astype(str).str.contains(search, case=False, na=False)
            df_view = df_view[m]

    st.subheader(f"📄 {fin_title}")
    df_view = safe_unique_columns(df_view)
    if kind == "Revenus":
        cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    else:
        cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
    st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

    # ملخّص شهري للأدمِن
    if role == "أدمن" and admin_unlocked():
        with st.expander("📊 ملخّص الفرع للشهر (حسب الصنف) — Admin Only"):
            rev_df = fin_read_df(fin_month_title(mois, "Revenus", branch), "Revenus")
            dep_df = fin_read_df(fin_month_title(mois, "Dépenses", branch), "Dépenses")

            sum_admin    = rev_df["Montant_Admin"].sum()           if ("Montant_Admin" in rev_df.columns and not rev_df.empty) else 0.0
            sum_struct   = rev_df["Montant_Structure"].sum()       if ("Montant_Structure" in rev_df.columns and not rev_df.empty) else 0.0
            sum_preins   = rev_df["Montant_PreInscription"].sum()  if ("Montant_PreInscription" in rev_df.columns and not rev_df.empty) else 0.0
            sum_total_as = rev_df["Montant_Total"].sum()           if ("Montant_Total" in rev_df.columns and not rev_df.empty) else (sum_admin + sum_struct)
            sum_reste_due= rev_df["Reste"].sum()                   if ("Reste" in rev_df.columns and not rev_df.empty) else 0.0

            if not dep_df.empty and "Caisse_Source" in dep_df.columns and "Montant" in dep_df.columns:
                dep_admin  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin",        "Montant"].sum()
                dep_struct = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure",    "Montant"].sum()
                dep_inscr  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Inscription",  "Montant"].sum()
            else:
                dep_admin = dep_struct = dep_inscr = 0.0

            reste_admin    = float(sum_admin)  - float(dep_admin)
            reste_struct   = float(sum_struct) - float(dep_struct)
            reste_inscr    = float(sum_preins) - float(dep_inscr)

            st.markdown("#### 🔹 Admin")
            a1, a2, a3 = st.columns(3)
            a1.metric("مداخيل Admin",   f"{sum_admin:,.2f}")
            a2.metric("مصاريف Admin",   f"{dep_admin:,.2f}")
            a3.metric("Reste Admin",     f"{reste_admin:,.2f}")

            st.markdown("#### 🔹 Structure")
            s1, s2, s3 = st.columns(3)
            s1.metric("مداخيل Structure", f"{sum_struct:,.2f}")
            s2.metric("مصاريف Structure", f"{dep_struct:,.2f}")
            s3.metric("Reste Structure",   f"{reste_struct:,.2f}")

            st.markdown("#### 🔹 Inscription (Pré-Inscription)")
            i1, i2, i3 = st.columns(3)
            i1.metric("مداخيل Inscription", f"{sum_preins:,.2f}")
            i2.metric("مصاريف Inscription", f"{dep_inscr:,.2f}")
            i3.metric("Reste Inscription",   f"{reste_inscr:,.2f}")

            st.markdown("#### 🔸 معلومات إضافية")
            x1, x2, x3 = st.columns(3)
            x1.metric("Total Admin+Structure (مداخيل فقط)", f"{sum_total_as:,.2f}")
            x2.metric("Total مصاريف", f"{(dep_admin + dep_struct + dep_inscr):,.2f}")
            x3.metric("إجمالي المتبقّي بالدروس (Reste Due)", f"{sum_reste_due:,.2f}")

    st.markdown("---")
    st.markdown("### ➕ إضافة عملية جديدة / ✏️ تعديل دفعة موجودة")

    # ---------- اختيار عميل ----------
    selected_client_info = None
    client_default_lib = ""
    emp_default = (employee or "")

    if kind == "Revenus":
        st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
        reg_df = df_all.copy()
        reg_df["Inscription_norm"] = reg_df["Inscription"].fillna("").astype(str).str.strip().str.lower()
        reg_df = reg_df[reg_df["Inscription_norm"].isin(["oui","inscrit"])]
        if role == "موظف" and employee:
            reg_df = reg_df[reg_df["__sheet_name"] == employee]
        pick = None
        options = []
        if not reg_df.empty:
            def _opt(row):
                phone = format_display_phone(row.get("Téléphone",""))
                return f"{row.get('Nom & Prénom','')} — {phone} — {row.get('Formation','')}  [{row.get('__sheet_name','')}]"
            options = [_opt(r) for _, r in reg_df.iterrows()]
            pick = st.selectbox("اختر عميلًا مُسجَّلًا", ["— بدون اختيار —"] + options, key="fin_client_pick")

        if pick and pick != "— بدون اختيار —":
            idx = options.index(pick); row = reg_df.iloc[idx]
            selected_client_info = {
                "name": str(row.get("Nom & Prénom","")).strip(),
                "tel":  str(row.get("Téléphone","")).strip(),
                "formation": str(row.get("Formation","")).strip(),
                "emp": str(row.get("__sheet_name","")).strip()
            }
            client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}".strip()
            if not emp_default: emp_default = selected_client_info["emp"]

            # 🆕 عرض الدفعات السابقة لنفس Libellé
            prev_df = find_revenus_across_months_for_libelle(branch, client_default_lib)
            st.markdown("#### 💾 دفعات سابقة لنفس Libellé (جميع الأشهر)")
            if prev_df.empty:
                st.caption("لا توجد دفعات سابقة مسجّلة لنفس العنوان.")
                paid_so_far_all = 0.0
                last_reste = 0.0
            else:
                show_cols_prev = ["__mois","Date","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste","Mode","Employé","Catégorie","Note"]
                show_cols_prev = [c for c in show_cols_prev if c in prev_df.columns]
                st.dataframe(prev_df[show_cols_prev], use_container_width=True)
                paid_so_far_all = float(prev_df["Montant_Total"].sum()) if "Montant_Total" in prev_df else 0.0
                last_reste = float(prev_df["Reste"].iloc[-1]) if "Reste" in prev_df and not prev_df["Reste"].isna().all() else 0.0
            st.info(f"🔎 مجموع المدفوع سابقًا لنفس Libellé عبر السنة: **{paid_so_far_all:,.2f}** — آخر Reste مسجّل: **{last_reste:,.2f}**")

            # 🆕 ✏️ تعديل دفعة موجودة
            st.markdown("### ✏️ تعديل دفعة موجودة (نفس Libellé)")
            if 'edit_pick_idx' not in st.session_state:
                st.session_state['edit_pick_idx'] = 0

            if not prev_df.empty:
                def _label_row(r):
                    dt = r["Date"].strftime("%d/%m/%Y") if isinstance(r["Date"], pd.Timestamp) and not pd.isna(r["Date"]) else str(r["Date"])
                    return f"[{r['__mois']}] {dt} — Admin:{r.get('Montant_Admin',0)} / Struct:{r.get('Montant_Structure',0)} / PréIns:{r.get('Montant_PreInscription',0)} / Total:{r.get('Montant_Total',0)} (Reste:{r.get('Reste',0)})"
                choices = [_label_row(r) for _, r in prev_df.iterrows()]
                pick_old = st.selectbox("اختر الدفعة للتعديل", choices, index=0)
                sel_row = prev_df.iloc[choices.index(pick_old)]

                orig_date = sel_row["Date"].date() if isinstance(sel_row["Date"], pd.Timestamp) and not pd.isna(sel_row["Date"]) else date.today()
                orig_admin = float(sel_row.get("Montant_Admin", 0.0) or 0.0)
                orig_struct= float(sel_row.get("Montant_Structure", 0.0) or 0.0)
                orig_preins= float(sel_row.get("Montant_PreInscription", 0.0) or 0.0)
                orig_total = float(sel_row.get("Montant_Total", 0.0) or 0.0)
                orig_reste = float(sel_row.get("Reste", 0.0) or 0.0)
                orig_mode  = str(sel_row.get("Mode","") or "")
                orig_emp   = str(sel_row.get("Employé","") or "")
                orig_cat   = str(sel_row.get("Catégorie","") or "")
                orig_note  = str(sel_row.get("Note","") or "")

                with st.form("edit_existing_payment"):
                    e1, e2, e3 = st.columns(3)
                    new_date    = e1.date_input("Date", value=orig_date)
                    new_mode    = e2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"], index=(["Espèces","Virement","Carte","Chèque","Autre"].index(orig_mode) if orig_mode in ["Espèces","Virement","Carte","Chèque","Autre"] else 0))
                    new_emp     = e3.selectbox("Employé", all_employes if all_employes else [""], index=(all_employes.index(orig_emp) if (orig_emp in all_employes) else 0) if all_employes else 0)

                    n1, n2, n3 = st.columns(3)
                    new_admin  = n1.number_input("Montant Admin", min_value=0.0, value=float(orig_admin), step=10.0)
                    new_struct = n2.number_input("Montant Structure", min_value=0.0, value=float(orig_struct), step=10.0)
                    new_preins = n3.number_input("Montant Pré-Inscription", min_value=0.0, value=float(orig_preins), step=10.0)

                    new_total  = float(new_admin) + float(new_struct)
                    r1, r2 = st.columns(2)
                    new_reste  = r1.number_input("Reste", min_value=0.0, value=float(orig_reste), step=10.0)
                    new_cat    = r2.text_input("Catégorie", value=orig_cat or "Revenus")
                    new_note   = st.text_area("Note", value=orig_note or "")

                    if st.form_submit_button("💾 حفظ التعديل على نفس الصف"):
                        try:
                            target_title = str(sel_row["__sheet_title"])
                            ws = ensure_ws(target_title, FIN_REV_COLUMNS)
                            row_idx = find_revenus_row_index(ws, client_default_lib, fmt_date(new_date if new_date else orig_date))
                            if not row_idx:
                                row_idx = find_revenus_row_index(ws, client_default_lib, fmt_date(orig_date))
                            if not row_idx:
                                st.error("❌ تعذّر تحديد الصف؛ راجع Libellé/Date.")
                            else:
                                header = ws.row_values(1)
                                col_map = {h: (header.index(h)+1) for h in FIN_REV_COLUMNS if h in header}
                                def _upd(h, val):
                                    if h in col_map: ws.update_cell(row_idx, col_map[h], val)

                                _upd("Date", fmt_date(new_date))
                                _upd("Libellé", client_default_lib)
                                _upd("Montant_Admin", f"{float(new_admin):.2f}")
                                _upd("Montant_Structure", f"{float(new_struct):.2f}")
                                _upd("Montant_PreInscription", f"{float(new_preins):.2f}")
                                _upd("Montant_Total", f"{float(new_total):.2f}")
                                _upd("Reste", f"{float(new_reste):.2f}")
                                _upd("Mode", new_mode)
                                _upd("Employé", new_emp)
                                _upd("Catégorie", new_cat)
                                _upd("Note", new_note)

                                st.success("✅ تمّ تعديل الدفعة على نفس الصف")
                                st.cache_data.clear(); st.rerun()
                        except Exception as e:
                            st.error(f"❌ خطأ أثناء التعديل: {e}")

    # ---------- إضافة جديدة ----------
    with st.form("fin_add_row"):
        d1, d2, d3 = st.columns(3)
        date_val = d1.date_input("Date", value=datetime.today())
        libelle  = d2.text_input("Libellé", value=(client_default_lib if (kind=="Revenus" and client_default_lib) else ""))
        employe  = d3.selectbox("Employé", all_employes if all_employes else [""],
                                index=(all_employes.index(emp_default) if (all_employes and emp_default in all_employes) else 0))

        if kind == "Revenus":
            r1, r2, r3 = st.columns(3)
            prix            = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
            montant_admin   = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
            montant_struct  = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)

            r4, r5 = st.columns(2)
            montant_preins  = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0, help="اختياري")
            montant_total   = float(montant_admin) + float(montant_struct)

            e1, e2, e3 = st.columns(3)
            echeance   = e1.date_input("⏰ تاريخ الاستحقاق", value=date.today())
            mode       = e2.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
            categorie  = e3.text_input("Catégorie", value="Revenus")

            note_default = f"Client: {selected_client_info['name']} / {selected_client_info['formation']}" if selected_client_info else ""
            note = st.text_area("Note", value=note_default)

            if kind == "Revenus" and libelle.strip():
                prev_df = find_revenus_across_months_for_libelle(branch, libelle)
                paid_so_far = float(prev_df["Montant_Total"].sum()) if not prev_df.empty else 0.0
            else:
                paid_so_far = 0.0

            reste_calc = max(float(prix) - (paid_so_far + float(montant_total)), 0.0)
            reste_input = st.number_input("💳 الباقي للدفع (Reste)", min_value=0.0, value=float(round(reste_calc,2)), step=10.0,
                                          help="يتحسب آليًا حسب جميع الدفعات لنفس Libellé خلال السنة")

            st.caption(
                f"💡 Total (Admin+Structure): {montant_total:.2f} — مدفوع سابقًا لنفس Libellé (كل الأشهر): {paid_so_far:.2f} — "
                f"Reste المقترح: {reste_calc:.2f} — Pré-Inscription منفصل: {montant_preins:.2f}"
            )

            if st.form_submit_button("✅ حفظ العملية (إضافة جديدة)"):
                if not libelle.strip():
                    st.error("Libellé مطلوب.")
                elif prix <= 0:
                    st.error("Prix مطلوب.")
                elif montant_total <= 0 and montant_preins <= 0:
                    st.error("المبلغ لازم > 0 (Admin/Structure أو Pré-Inscription).")
                else:
                    fin_append_row(
                        fin_month_title(mois, "Revenus", branch),
                        {
                            "Date": fmt_date(date_val),
                            "Libellé": libelle.strip(),
                            "Prix": f"{float(prix):.2f}",
                            "Montant_Admin": f"{float(montant_admin):.2f}",
                            "Montant_Structure": f"{float(montant_struct):.2f}",
                            "Montant_PreInscription": f"{float(montant_preins):.2f}",
                            "Montant_Total": f"{float(montant_total):.2f}",
                            "Echeance": fmt_date(echeance),
                            "Reste": f"{float(reste_input):.2f}",
                            "Mode": mode,
                            "Employé": employe.strip(),
                            "Catégorie": categorie.strip(),
                            "Note": note.strip(),
                        },
                        "Revenus"
                    )
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

        else:
            r1, r2, r3 = st.columns(3)
            montant   = r1.number_input("Montant", min_value=0.0, step=10.0)
            caisse    = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
            mode      = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])

            c2, c3 = st.columns(2)
            categorie = c2.text_input("Catégorie", value="Achat")
            note      = c3.text_area("Note (اختياري)")

            if st.form_submit_button("✅ حفظ العملية"):
                if not libelle.strip():
                    st.error("Libellé مطلوب.")
                elif montant <= 0:
                    st.error("المبلغ لازم > 0.")
                else:
                    fin_append_row(
                        fin_month_title(mois, "Dépenses", branch),
                        {
                            "Date": fmt_date(date_val),
                            "Libellé": libelle.strip(),
                            "Montant": f"{float(montant):.2f}",
                            "Caisse_Source": caisse,
                            "Mode": mode,
                            "Employé": employe.strip(),
                            "Catégorie": categorie.strip(),
                            "Note": note.strip(),
                        },
                        "Dépenses"
                    )
                    st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

# ======================================================================
#                      💼 تبويب خلاص الإدارة/المكوّنين (Payouts)
# ======================================================================
if tab_choice == "💼 خلاص الإدارة/المكوّنين":
    st.title("💼 خلاص الإدارة والمكوّنين — (MB & Bizerte)")

    with st.sidebar:
        st.markdown("---"); st.subheader("🔧 إعدادات الخلاص")
        branch_p = st.selectbox("الفرع", ["Menzel Bourguiba", "Bizerte"], key="payout_branch")
        mois_p   = st.selectbox("الشهر", FIN_MONTHS_FR, index=datetime.now().month-1, key="payout_month")
        BRANCH_PASSWORDS = _branch_passwords()
        key_pw_p = f"payout_pw_ok::{branch_p}"
        if key_pw_p not in st.session_state: st.session_state[key_pw_p] = False
        if not st.session_state[key_pw_p]:
            pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"payout_pw_{branch_p}")
            if st.button("دخول تبويب الخلاص", key=f"payout_enter_{branch_p}"):
                if pw_try and pw_try == BRANCH_PASSWORDS.get(branch_p, ""):
                    st.session_state[key_pw_p] = True; st.success("تم الدخول ✅")
                else:
                    st.error("كلمة سرّ غير صحيحة ❌")

    if not st.session_state.get(f"payout_pw_ok::{branch_p}", False):
        st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة."); st.stop()

    title_payout = fin_month_title(mois_p, "Payouts", branch_p)
    df_payout = payouts_read_df(title_payout)

    st.subheader(f"📄 {title_payout}")
    if df_payout.empty:
        st.caption("لا توجد مدفوعات لهذا الشهر بعد.")
    else:
        st.dataframe(df_payout[[
            "Date","Type","Personne","Libellé","Montant","Caisse_Source","Mode","Employé","Note"
        ]], use_container_width=True)

        with st.expander("📊 تلخيص شهري"):
            tot_admin  = float(df_payout.loc[df_payout["Caisse_Source"]=="Caisse_Admin","Montant"].sum()) if "Montant" in df_payout else 0.0
            tot_struct = float(df_payout.loc[df_payout["Caisse_Source"]=="Caisse_Structure","Montant"].sum()) if "Montant" in df_payout else 0.0
            tot_inscr  = float(df_payout.loc[df_payout["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()) if "Montant" in df_payout else 0.0
            c1, c2, c3 = st.columns(3)
            c1.metric("Admin (مجموع الخلاص)", f"{tot_admin:,.2f}")
            c2.metric("Structure (مجموع الخلاص)", f"{tot_struct:,.2f}")
            c3.metric("Inscription (مجموع الخلاص)", f"{tot_inscr:,.2f}")

    st.markdown("---")
    st.markdown("### ➕ إضافة خلاص جديد")
    with st.form("payout_add_form"):
        a1, a2, a3 = st.columns(3)
        date_p   = a1.date_input("Date", value=datetime.today())
        type_p   = a2.selectbox("Type", ["Administration","Formateur"])  # إدارة / مكوّن
        person_p = a3.text_input("Personne (الاسم)")

        b1, b2, b3 = st.columns(3)
        lib_p      = b1.text_input("Libellé", value=f"خلاص {type_p}")
        caisse_p   = b2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
        mode_p     = b3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])

        c1, c2 = st.columns(2)
        montant_p = c1.number_input("Montant", min_value=0.0, step=10.0)
        employe_p = c2.selectbox("Employé (المسجّل)", all_employes if all_employes else [""])

        note_p = st.text_area("Note (اختياري)")

        if st.form_submit_button("✅ حفظ الخلاص"):
            if not person_p.strip():
                st.error("❌ اسم الشخص مطلوب.")
            elif montant_p <= 0:
                st.error("❌ المبلغ يجب أن يكون أكبر من 0.")
            else:
                payouts_append_row(
                    title_payout,
                    {
                        "Date": fmt_date(date_p),
                        "Type": type_p,
                        "Personne": person_p.strip(),
                        "Libellé": lib_p.strip(),
                        "Montant": f"{float(montant_p):.2f}",
                        "Caisse_Source": caisse_p,
                        "Mode": mode_p,
                        "Employé": employe_p.strip(),
                        "Note": note_p.strip(),
                    }
                )
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()

# ======================================================================
#                                   CRM: منطقة الموظف + نقل + واتساب
# ======================================================================

def render_table(df_disp: pd.DataFrame):
    if df_disp.empty:
        st.info("لا توجد بيانات.")
        return
    _df = df_disp.copy()
    _df["Alerte"] = _df.get("Alerte_view", "")
    display_cols = [c for c in EXPECTED_HEADERS if c in _df.columns]
    styled = (
        _df[display_cols]
        .style.apply(highlight_inscrit_row, axis=1)
        .applymap(mark_alert_cell, subset=["Alerte"])
        .applymap(color_tag, subset=["Tag"])
    )
    st.dataframe(styled, use_container_width=True)

if role == "موظف" and employee:
    # ---------- حماية ورقة الموظف ----------
    def _get_emp_password(emp_name: str) -> str:
        try:
            mp = st.secrets["employee_passwords"]
            return str(mp.get(emp_name, mp.get("_default", "1234")))
        except Exception:
            return "1234"

    def _emp_unlocked(emp_name: str) -> bool:
        ok = st.session_state.get(f"emp_ok::{emp_name}", False)
        ts = st.session_state.get(f"emp_ok_at::{emp_name}")
        return bool(ok and ts and (datetime.now() - ts) <= timedelta(minutes=15))

    def _emp_lock_ui(emp_name: str):
        with st.expander(f"🔐 حماية ورقة الموظّف: {emp_name}", expanded=not _emp_unlocked(emp_name)):
            if _emp_unlocked(emp_name):
                c1, c2 = st.columns(2)
                with c1:
                    st.success("مفتوح (15 دقيقة).")
                with c2:
                    if st.button("قفل الآن"):
                        st.session_state[f"emp_ok::{emp_name}"] = False
                        st.session_state[f"emp_ok_at::{emp_name}"] = None
                        st.info("تم القفل.")
            else:
                pwd_try = st.text_input("أدخل كلمة السرّ", type="password", key=f"emp_pwd_{emp_name}")
                if st.button("فتح"):
                    if pwd_try and pwd_try == _get_emp_password(emp_name):
                        st.session_state[f"emp_ok::{emp_name}"] = True
                        st.session_state[f"emp_ok_at::{emp_name}"] = datetime.now()
                        st.success("تم الفتح لمدة 15 دقيقة.")
                    else:
                        st.error("كلمة سرّ غير صحيحة.")

    _emp_lock_ui(employee)
    if not _emp_unlocked(employee):
        st.info("🔒 أدخل كلمة سرّ الموظّف لفتح الورقة.")
        st.stop()

    # ---------- بيانات الموظّف ----------
    st.subheader(f"📁 لوحة {employee}")
    df_emp = df_all[df_all["__sheet_name"] == employee].copy()

    if df_emp.empty:
        st.warning("⚠️ لا يوجد أي عملاء لهذا الموظف بعد.")
        st.stop()

    df_emp["DateAjout_dt"] = pd.to_datetime(df_emp["Date ajout"], dayfirst=True, errors="coerce")
    df_emp = df_emp.dropna(subset=["DateAjout_dt"])
    df_emp["Mois"] = df_emp["DateAjout_dt"].dt.strftime("%m-%Y")
    month_options = sorted(df_emp["Mois"].dropna().unique(), reverse=True)
    month_filter = st.selectbox("🗓️ اختر شهر الإضافة", month_options)
    filtered_df = df_emp[df_emp["Mois"] == month_filter].copy()

    st.markdown("### 📋 قائمة العملاء")
    render_table(filtered_df)

    # (A) تنبيهات
    if not filtered_df.empty and st.checkbox("🔴 عرض العملاء الذين لديهم تنبيهات"):
        _df_alerts = filtered_df.copy()
        _df_alerts["Alerte"] = _df_alerts.get("Alerte_view", "")
        alerts_df = _df_alerts[_df_alerts["Alerte"].fillna("").astype(str).str.strip() != ""]
        st.markdown("### 🚨 عملاء مع تنبيهات")
        if alerts_df.empty:
            st.info("لا توجد تنبيهات حاليًا ضمن الفلترة.")
        else:
            render_table(alerts_df)

    st.divider()

    # (B) ملاحظات سريعة
    st.markdown("### 📝 أضف ملاحظة (سريعة)")
    scope_df = filtered_df if not filtered_df.empty else df_emp
    scope_df = scope_df.copy()
    scope_df["Téléphone_norm"] = scope_df["Téléphone"].apply(normalize_tn_phone)

    tel_to_update_key = st.selectbox(
        "اختر العميل",
        [
            f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}"
            for _, r in scope_df.iterrows()
            if str(r.get('Téléphone','')).strip() != ""
        ],
        key="note_quick_pick"
    )
    tel_to_update = normalize_tn_phone(tel_to_update_key.split("—")[-1]) if tel_to_update_key else ""
    new_note_quick = st.text_area("🗒️ ملاحظة جديدة (سيضاف لها طابع زمني)", key="note_quick_txt")

    if st.button("📌 أضف الملاحظة", key="note_quick_btn"):
        try:
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
            values = ws.get_all_values()
            header = values[0] if values else []
            if "Téléphone" in header:
                tel_idx = header.index("Téléphone")
                row_idx = None
                for i, r in enumerate(values[1:], start=2):
                    if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == tel_to_update:
                        row_idx = i
                        break
                if not row_idx:
                    st.error("❌ الهاتف غير موجود.")
                else:
                    rem_col = EXPECTED_HEADERS.index("Remarque") + 1
                    old_remark = ws.cell(row_idx, rem_col).value or ""
                    stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                    updated = (old_remark + "\n" if old_remark else "") + f"[{stamp}] {new_note_quick.strip()}"
                    ws.update_cell(row_idx, rem_col, updated)
                    st.success("✅ تمت إضافة الملاحظة")
                    st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    st.divider()

    # (C) تعديل بيانات عميل
    st.markdown("### ✏️ تعديل بيانات عميل")
    df_emp_edit = df_emp.copy()
    df_emp_edit["Téléphone_norm"] = df_emp_edit["Téléphone"].apply(normalize_tn_phone)

    phone_choices = {
        f"[{i}] {row['Nom & Prénom']} — {format_display_phone(row['Téléphone_norm'])}": row["Téléphone_norm"]
        for i, row in df_emp_edit.iterrows()
        if str(row.get("Téléphone","")).strip() != ""
    }

    if phone_choices:
        chosen_key   = st.selectbox("اختر العميل (بالاسم/الهاتف)", list(phone_choices.keys()), key="edit_pick")
        chosen_phone = phone_choices.get(chosen_key, "")
        cur_row = df_emp_edit[df_emp_edit["Téléphone_norm"] == chosen_phone].iloc[0] if chosen_phone else None

        cur_name      = str(cur_row.get("Nom & Prénom","")) if cur_row is not None else ""
        cur_tel_raw   = str(cur_row.get("Téléphone",""))    if cur_row is not None else ""
        cur_formation = str(cur_row.get("Formation",""))    if cur_row is not None else ""
        cur_remark    = str(cur_row.get("Remarque",""))     if cur_row is not None else ""
        cur_ajout = (
            pd.to_datetime(cur_row.get("Date ajout",""), dayfirst=True, errors="coerce").date()
            if cur_row is not None else date.today()
        )
        cur_suivi = (
            pd.to_datetime(cur_row.get("Date de suivi",""), dayfirst=True, errors="coerce").date()
            if cur_row is not None and str(cur_row.get("Date de suivi","")).strip()
            else date.today()
        )
        cur_insc  = str(cur_row.get("Inscription","")).strip().lower() if cur_row is not None else ""

        name_key   = f"edit_name_txt::{chosen_phone}"
        phone_key  = f"edit_phone_txt::{chosen_phone}"
        form_key   = f"edit_formation_txt::{chosen_phone}"
        ajout_key  = f"edit_ajout_dt::{chosen_phone}"
        suivi_key  = f"edit_suivi_dt::{chosen_phone}"
        insc_key   = f"edit_insc_sel::{chosen_phone}"
        remark_key = f"edit_remark_txt::{chosen_phone}"
        note_key   = f"append_note_txt::{chosen_phone}"

        col1, col2 = st.columns(2)
        with col1:
            new_name      = st.text_input("👤 الاسم و اللقب", value=cur_name, key=name_key)
            new_phone_raw = st.text_input("📞 رقم الهاتف", value=cur_tel_raw, key=phone_key)
            new_formation = st.text_input("📚 التكوين", value=cur_formation, key=form_key)
        with col2:
            new_ajout = st.date_input("🕓 تاريخ الإضافة", value=cur_ajout, key=ajout_key)
            new_suivi = st.date_input("📆 تاريخ المتابعة", value=cur_suivi, key=suivi_key)
            new_insc  = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"], index=(1 if cur_insc == "oui" else 0), key=insc_key)

        new_remark_full = st.text_area("🗒️ ملاحظة (استبدال كامل)", value=cur_remark, key=remark_key)
        extra_note      = st.text_area("➕ أضف ملاحظة جديدة (طابع زمني)", placeholder="اكتب ملاحظة لإلحاقها…", key=note_key)

        def _find_row_by_phone(ws, phone_digits: str) -> int | None:
            values = ws.get_all_values()
            if not values:
                return None
            header = values[0]
            if "Téléphone" not in header:
                return None
            tel_idx = header.index("Téléphone")
            for i, r in enumerate(values[1:], start=2):
                if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_digits:
                    return i
            return None

        if st.button("💾 حفظ التعديلات", key="save_all_edits"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                row_idx = _find_row_by_phone(ws, normalize_tn_phone(chosen_phone))
                if not row_idx:
                    st.error("❌ تعذّر إيجاد الصف لهذا الهاتف.")
                else:
                    col_map = {h: (EXPECTED_HEADERS.index(h) + 1) for h in [
                        "Nom & Prénom","Téléphone","Formation","Date ajout","Date de suivi","Inscription","Remarque"
                    ]}
                    new_phone_norm = normalize_tn_phone(new_phone_raw)
                    if not new_name.strip():
                        st.error("❌ الاسم و اللقب إجباري."); st.stop()
                    if not new_phone_norm.strip():
                        st.error("❌ رقم الهاتف إجباري."); st.stop()

                    phones_except_current = (set(df_all["Téléphone_norm"].astype(str)) - {normalize_tn_phone(chosen_phone)})
                    if new_phone_norm in phones_except_current:
                        st.error("⚠️ الرقم موجود مسبقًا لعميل آخر."); st.stop()

                    ws.update_cell(row_idx, col_map["Nom & Prénom"], new_name.strip())
                    ws.update_cell(row_idx, col_map["Téléphone"],   new_phone_norm)
                    ws.update_cell(row_idx, col_map["Formation"],   new_formation.strip())
                    ws.update_cell(row_idx, col_map["Date ajout"],  fmt_date(new_ajout))
                    ws.update_cell(row_idx, col_map["Date de suivi"], fmt_date(new_suivi))
                    ws.update_cell(row_idx, col_map["Inscription"], "Oui" if new_insc == "Inscrit" else "Pas encore")

                    if new_remark_full.strip() != cur_remark.strip():
                        ws.update_cell(row_idx, col_map["Remarque"], new_remark_full.strip())

                    if extra_note.strip():
                        old_rem = ws.cell(row_idx, col_map["Remarque"]).value or ""
                        stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
                        appended = (old_rem + "\n" if old_rem else "") + f"[{stamp}] {extra_note.strip()}"
                        ws.update_cell(row_idx, col_map["Remarque"], appended)

                    st.success("✅ تم حفظ التعديلات")
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء التعديل: {e}")

    st.divider()

    # (D) Tag/لون
    st.markdown("### 🎨 اختر لون/Tag للعميل")
    scope_df2 = filtered_df if not filtered_df.empty else df_emp
    scope_df2 = scope_df2.copy()
    scope_df2["Téléphone_norm"] = scope_df2["Téléphone"].apply(normalize_tn_phone)

    tel_color_key = st.selectbox(
        "اختر العميل",
        [
            f"{r['Nom & Prénom']} — {format_display_phone(normalize_tn_phone(r['Téléphone']))}"
            for _, r in scope_df2.iterrows()
            if str(r.get('Téléphone','')).strip() != ""
        ],
        key="tag_select"
    )
    tel_color = normalize_tn_phone(tel_color_key.split("—")[-1]) if tel_color_key else ""
    hex_color = st.color_picker("اختر اللون", value=st.session_state.get("last_color", "#00AA88"))
    if st.button("🖌️ تلوين", key="tag_apply_btn"):
        try:
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
            values = ws.get_all_values()
            header = values[0] if values else []
            row_idx = None
            if "Téléphone" in header:
                tel_idx = header.index("Téléphone")
                for i, r in enumerate(values[1:], start=2):
                    if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == tel_color:
                        row_idx = i
                        break
            if not row_idx:
                st.error("❌ لم يتم إيجاد العميل.")
            else:
                st.session_state["last_color"] = hex_color
                color_col = EXPECTED_HEADERS.index("Tag") + 1
                ws.update_cell(row_idx, color_col, hex_color)
                st.success("✅ تم التلوين")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ خطأ: {e}")

    st.divider()

    # (E) نقل عميل + Log
    st.markdown("### 🔁 نقل عميل بين الموظفين")
    if all_employes:
        colRA, colRB = st.columns(2)
        with colRA:
            src_emp = st.selectbox("من موظّف", all_employes, key="reassign_src")
        with colRB:
            dst_emp = st.selectbox("إلى موظّف", [e for e in all_employes if e != src_emp], key="reassign_dst")

        df_src = df_all[df_all["__sheet_name"] == src_emp].copy()
        if df_src.empty:
            st.info("❕ لا يوجد عملاء عند هذا الموظّف.")
        else:
            pick = st.selectbox(
                "اختر العميل للنقل",
                [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in df_src.iterrows()],
                key="reassign_pick"
            )
            phone_pick = normalize_tn_phone(pick.split("—")[-1])
            mover = employee  # شكون عامل النقل (الموظف الحالي)
            if st.button("🚚 نقل الآن"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    ws_src, ws_dst = sh.worksheet(src_emp), sh.worksheet(dst_emp)
                    values = ws_src.get_all_values()
                    header = values[0] if values else []
                    row_idx = None
                    if "Téléphone" in header:
                        tel_idx = header.index("Téléphone")
                        for i, r in enumerate(values[1:], start=2):
                            if len(r) > tel_idx and normalize_tn_phone(r[tel_idx]) == phone_pick:
                                row_idx = i
                                break
                    if not row_idx:
                        st.error("❌ لم يتم العثور على هذا العميل.")
                    else:
                        row_values = ws_src.row_values(row_idx)
                        if len(row_values) < len(EXPECTED_HEADERS):
                            row_values += [""] * (len(EXPECTED_HEADERS) - len(row_values))
                        row_values = row_values[:len(EXPECTED_HEADERS)]
                        row_values[EXPECTED_HEADERS.index("Employe")] = dst_emp
                        ws_dst.append_row(row_values)
                        ws_src.delete_rows(row_idx)

                        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
                        wslog.append_row([
                            datetime.now(timezone.utc).isoformat(),
                            mover, src_emp, dst_emp,
                            row_values[0],
                            normalize_tn_phone(row_values[1])
                        ])

                        st.success(f"✅ نقل ({row_values[0]}) من {src_emp} إلى {dst_emp}")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ أثناء النقل: {e}")

    st.divider()

    # (F) WhatsApp
    st.markdown("### 💬 تواصل WhatsApp")
    wa_pick = st.selectbox(
        "اختر العميل لفتح واتساب",
        [f"{r['Nom & Prénom']} — {format_display_phone(r['Téléphone'])}" for _, r in (filtered_df if not filtered_df.empty else df_emp).iterrows()],
        key="wa_pick"
    )
    default_msg = "سلام! معاك Mega Formation. بخصوص التكوين، نحبّوا ننسّقو معاك موعد المتابعة. 👍"
    wa_msg = st.text_area("الرسالة (WhatsApp)", value=default_msg, key="wa_msg")
    if st.button("📲 فتح WhatsApp"):
        try:
            raw_tel = wa_pick.split("—")[-1]
            tel_norm = normalize_tn_phone(raw_tel)
            url = f"https://wa.me/{tel_norm}?text={urllib.parse.quote(wa_msg)}"
            st.markdown(f"[افتح المحادثة الآن]({url})")
            st.info("اضغط على الرابط لفتح واتساب.")
        except Exception as e:
            st.error(f"❌ تعذّر إنشاء رابط واتساب: {e}")

    st.divider()

    # (G) إضافة عميل جديد
    st.markdown("### ➕ أضف عميل جديد")
    with st.form("emp_add_client"):
        col1, col2 = st.columns(2)
        with col1:
            nom = st.text_input("👤 الاسم و اللقب")
            tel_raw = st.text_input("📞 رقم الهاتف")
            formation = st.text_input("📚 التكوين")
            inscription = st.selectbox("🟢 التسجيل", ["Pas encore", "Inscrit"])
        with col2:
            type_contact = st.selectbox("📞 نوع الاتصال", ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"])
            date_ajout_in = st.date_input("🕓 تاريخ الإضافة", value=date.today())
            date_suivi_in = st.date_input("📆 تاريخ المتابعة", value=date.today())

        if st.form_submit_button("📥 أضف العميل"):
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(employee)
                tel = normalize_tn_phone(tel_raw)
                if not (nom and tel and formation):
                    st.error("❌ حقول أساسية ناقصة."); st.stop()
                if tel in ALL_PHONES:
                    st.warning("⚠️ الرقم موجود مسبقًا."); st.stop()
                insc_val = "Oui" if inscription == "Inscrit" else "Pas encore"
                ws.append_row([
                    nom, tel, type_contact, formation, "",
                    fmt_date(date_ajout_in), fmt_date(date_suivi_in),
                    "", insc_val, employee, ""
                ])
                st.success("✅ تم إضافة العميل")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ خطأ أثناء الإضافة: {e}")

# ======================================================================
#                                   📝 نوط داخلية
# ======================================================================
if tab_choice == "📝 نوط داخلية":
    current_emp_name = (employee if (role == "موظف" and employee) else "Admin")
    is_admin_user = (role == "أدمن")
    inter_notes_ui(
        current_employee=current_emp_name,
        all_employees=all_employes,
        is_admin=is_admin_user
    )

# ======================================================================
#                                   Admin Page
# ======================================================================
if role == "أدمن":
    st.markdown("## 👑 لوحة الأدمِن")
    if not admin_unlocked():
        st.info("🔐 أدخل كلمة سرّ الأدمِن من اليسار لفتح الصفحة.")
    else:
        colA, colB, colC = st.columns(3)

        with colA:
            st.subheader("➕ إضافة موظّف")
            new_emp = st.text_input("اسم الموظّف الجديد")
            if st.button("إنشاء ورقة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    titles = [w.title for w in sh.worksheets()]
                    if not new_emp or new_emp in titles:
                        st.warning("⚠️ الاسم فارغ أو موجود.")
                    else:
                        sh.add_worksheet(title=new_emp, rows="1000", cols="20")
                        sh.worksheet(new_emp).update("1:1", [EXPECTED_HEADERS])
                        st.success("✔️ تم الإنشاء"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        with colB:
            st.subheader("➕ إضافة عميل (لأي موظّف)")
            sh = client.open_by_key(SPREADSHEET_ID)
            target_emp = st.selectbox("اختر الموظّف", all_employes, key="admin_add_emp")
            nom_a = st.text_input("👤 الاسم و اللقب", key="admin_nom")
            tel_a_raw = st.text_input("📞 الهاتف", key="admin_tel")
            formation_a = st.text_input("📚 التكوين", key="admin_form")
            type_contact_a = st.selectbox("نوع التواصل", ["Visiteur","Appel téléphonique","WhatsApp","Social media"], key="admin_type")
            inscription_a = st.selectbox("التسجيل", ["Pas encore","Inscrit"], key="admin_insc")
            date_ajout_a = st.date_input("تاريخ الإضافة", value=date.today(), key="admin_dt_add")
            suivi_date_a = st.date_input("تاريخ المتابعة", value=date.today(), key="admin_dt_suivi")
            if st.button("📥 أضف"):
                try:
                    if not (nom_a and tel_a_raw and formation_a and target_emp): st.error("❌ حقول ناقصة."); st.stop()
                    tel_a = normalize_tn_phone(tel_a_raw)
                    if tel_a in set(df_all["Téléphone_norm"]): st.warning("⚠️ الرقم موجود.")
                    else:
                        insc_val = "Oui" if inscription_a=="Inscrit" else "Pas encore"
                        ws = sh.worksheet(target_emp)
                        ws.append_row([nom_a, tel_a, type_contact_a, formation_a, "", fmt_date(date_ajout_a), fmt_date(suivi_date_a), "", insc_val, target_emp, ""])
                        st.success("✅ تمت الإضافة"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        with colC:
            st.subheader("🗑️ حذف موظّف")
            emp_to_delete = st.selectbox("اختر الموظّف", all_employes, key="admin_del_emp")
            if st.button("❗ حذف الورقة كاملة"):
                try:
                    sh = client.open_by_key(SPREADSHEET_ID)
                    sh.del_worksheet(sh.worksheet(emp_to_delete))
                    st.success("تم الحذف"); st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ خطأ: {e}")

        st.markdown("---")
        st.subheader("📜 سجلّ نقل العملاء (Reassign Log)")
        wslog = ensure_ws(REASSIGN_LOG_SHEET, REASSIGN_LOG_HEADERS)
        vals = wslog.get_all_values()
        if vals and len(vals) > 1:
            df_log = pd.DataFrame(vals[1:], columns=vals[0])
            def _fmt_ts3(x):
                try:
                    return datetime.fromisoformat(x).astimezone().strftime("%Y-%m-%d %H:%M")
                except:
                    return x
            if "timestamp" in df_log.columns:
                df_log["وقت"] = df_log["timestamp"].apply(_fmt_ts3)
            show_cols = ["وقت","moved_by","src_employee","dst_employee","client_name","phone"]
            show_cols = [c for c in show_cols if c in df_log.columns]
            st.dataframe(df_log[show_cols].sort_values(show_cols[0], ascending=False), use_container_width=True)
        else:
            st.caption("لا يوجد سجلّ نقل إلى حدّ الآن.")

        st.caption("صفحة الأدمِن مفتوحة لمدّة 30 دقيقة من وقت الفتح.")
