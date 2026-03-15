import os
import re
import csv
import tempfile
from datetime import datetime, date
from io import BytesIO
from typing import Dict, List, Tuple, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from openpyxl import load_workbook, Workbook
from openpyxl.utils.datetime import from_excel

# ========= ENV =========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

# ✅ multi admin (comma/space separated)
# contoh Railway: ADMIN_IDS = 5397964203,123456789,987654321
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

if not BOT_TOKEN:
    raise SystemExit("Missing BOT_TOKEN env")
if not ADMIN_IDS_RAW:
    raise SystemExit("Missing ADMIN_IDS env (comma separated digits)")

ADMIN_IDS: set[int] = set()
for x in re.split(r"[,\s]+", ADMIN_IDS_RAW):
    x = x.strip()
    if not x:
        continue
    if not x.isdigit():
        raise SystemExit(f"Invalid ADMIN_IDS value: {x} (must be digits)")
    ADMIN_IDS.add(int(x))

if not ADMIN_IDS:
    raise SystemExit("ADMIN_IDS is empty")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# awaiting: None | "idsfile" | "file"
SESS: Dict[int, Dict] = {}

RE_SPLIT = re.compile(r"[,\s]+")

def is_admin(m: Message) -> bool:
    u = m.from_user
    return bool(u and u.id in ADMIN_IDS)

async def deny_if_not_admin(m: Message) -> bool:
    if not is_admin(m):
        await m.answer("⛔ Akses ditolak. Bot ini khusus admin.")
        return True
    return False

def norm_id(s: str) -> str:
    return (s or "").strip().lower()

def fmt(n: int) -> str:
    """Format angka pakai koma: 1252 -> 1,252"""
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)

def parse_date_any(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None

    fmts = [
        "%d/%m/%y %H.%M",
        "%d/%m/%y %H:%M",
        "%d/%m/%Y %H.%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d/%m/%y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            pass

    s2 = s.replace(".", ":")
    if s2 != s:
        for f in ["%d/%m/%y %H:%M", "%d/%m/%Y %H:%M"]:
            try:
                return datetime.strptime(s2, f)
            except Exception:
                pass
    return None

def to_int_coin(x) -> int:
    try:
        if isinstance(x, str):
            xs = x.strip().replace(",", "")
            if not xs:
                return 0
            if "e" in xs.lower():
                return int(float(xs))
            return int(float(xs))
        if isinstance(x, (int, float)):
            return int(x)
    except Exception:
        return 0
    return 0

# =======================
# Load ID file (TXT/CSV/XLSX)
# =======================

def load_ids_from_txt(path: str) -> set[str]:
    # TXT bebas: 1 ID per baris / spasi / koma
    ids = set()
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            parts = RE_SPLIT.split(line.strip())
            for p in parts:
                s = norm_id(p)
                if s and s not in {"to", "id"}:
                    ids.add(s)
    return ids

def load_ids_from_csv(path: str) -> set[str]:
    # streaming, bisa ada header TO, kalau tidak ada ambil kolom pertama
    ids = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        except Exception:
            dialect = csv.excel

        reader = csv.reader(f, dialect=dialect)
        first = next(reader, None)
        if not first:
            return ids

        header = [str(c).strip().lower() for c in first]
        to_idx = header.index("to") if "to" in header else None

        if to_idx is None:
            s = norm_id(str(first[0]) if first else "")
            if s and s not in {"to", "id"}:
                ids.add(s)

        for r in reader:
            if not r:
                continue
            val = r[to_idx] if (to_idx is not None and to_idx < len(r)) else r[0]
            s = norm_id(str(val))
            if s and s not in {"to", "id"}:
                ids.add(s)
    return ids

def load_ids_from_xlsx(path: str) -> set[str]:
    ids = set()
    wb = load_workbook(path, data_only=True)
    ws = wb.active

    first_row = next(ws.iter_rows(values_only=True), [])
    headers = [str(c).strip().lower() if c is not None else "" for c in first_row]
    to_col = headers.index("to") + 1 if "to" in headers else None

    for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if i == 1:
            continue
        if not row:
            continue
        if to_col is not None and to_col <= len(row):
            val = row[to_col - 1]
        else:
            val = row[0]
        s = norm_id(str(val) if val is not None else "")
        if s and s not in {"to", "id", "none"}:
            ids.add(s)
    return ids

def load_id_file(path: str) -> set[str]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".txt":
        return load_ids_from_txt(path)
    if ext == ".csv":
        return load_ids_from_csv(path)
    if ext == ".xlsx":
        return load_ids_from_xlsx(path)
    raise ValueError("File ID harus .txt / .csv / .xlsx")

# =======================
# Load history (CSV/XLSX)
# =======================

def load_rows_from_csv(path: str) -> List[dict]:
    """
    auto detect delimiter + header lower+strip
    hasil dict key semuanya lowercase
    """
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        except Exception:
            dialect = csv.excel

        reader = csv.DictReader(f, dialect=dialect)
        if reader.fieldnames:
            reader.fieldnames = [str(fn).strip().lower() for fn in reader.fieldnames]

        for r in reader:
            rr = {}
            for k, v in r.items():
                if k is None:
                    continue
                kk = str(k).strip().lower()
                rr[kk] = v.strip() if isinstance(v, str) else v
            rows.append(rr)
    return rows

def load_rows_from_xlsx(path: str) -> List[dict]:
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    headers: List[str] = []
    rows: List[dict] = []
    for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if i == 1:
            headers = [str(c).strip().lower() if c is not None else "" for c in row]
            continue
        if not any(cell is not None and str(cell).strip() != "" for cell in row):
            continue
        rr = {}
        for h, cell in zip(headers, row):
            if not h:
                continue
            rr[h] = cell.strip() if isinstance(cell, str) else cell
        rows.append(rr)
    return rows

def load_history(path: str) -> List[dict]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return load_rows_from_csv(path)
    if ext == ".xlsx":
        return load_rows_from_xlsx(path)
    raise ValueError("History harus CSV atau XLSX")

def compute_report(rows: List[dict], ids: set[str], requested_day: Optional[date]) -> Tuple[date, List[Tuple[str,int,int]]]:
    required = {"date", "info", "to", "coin"}
    keys = set(rows[0].keys()) if rows else set()
    missing = required - keys
    if missing:
        raise ValueError(
            f"Kolom wajib tidak ditemukan: {', '.join(sorted(missing))}\n"
            f"Kolom yang terbaca di file: {', '.join(sorted(keys))}"
        )

    parsed = []
    for r in rows:
        info = str(r.get("info", "")).lower()
        if "deposit" not in info:
            continue

        dt = r.get("date")
        dtp: Optional[datetime] = None

        if isinstance(dt, datetime):
            dtp = dt
        elif isinstance(dt, (int, float)):
            try:
                dtp = from_excel(dt)
            except Exception:
                dtp = None
        else:
            dtp = parse_date_any(str(dt))

        if not dtp:
            continue

        to_id = norm_id(str(r.get("to", "")))
        if not to_id:
            continue

        if to_id not in ids:
            continue

        coin = to_int_coin(r.get("coin", 0))
        parsed.append((dtp.date(), to_id, coin))

    if not parsed:
        raise ValueError("Tidak ada data deposit yang cocok dengan ID new member di file history.")

    available_days = sorted({d for d, _, _ in parsed})
    target_day = requested_day if (requested_day in available_days) else available_days[-1]

    agg: Dict[str, Dict[str,int]] = {}
    for d, to_id, coin in parsed:
        if d != target_day:
            continue
        if to_id not in agg:
            agg[to_id] = {"count": 0, "sum": 0}
        agg[to_id]["count"] += 1
        agg[to_id]["sum"] += coin

    out = [(k, v["count"], v["sum"]) for k, v in agg.items()]
    out.sort(key=lambda x: (x[2], x[1]), reverse=True)
    return target_day, out

def make_excel_bytes(target_day: date, items: List[Tuple[str,int,int]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Report"
    ws.append(["TO", "Deposit Count", "Total Coin", "Tanggal"])
    for to_id, cnt, total in items:
        ws.append([to_id, cnt, total, str(target_day)])
    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

# ================== COMMANDS ==================

@dp.message(Command("start"))
async def start(m: Message):
    if await deny_if_not_admin(m):
        return
    SESS[m.from_user.id] = {"ids": set(), "target_date": None, "awaiting": None}
    await m.answer(
        "✅ Bot Cek New Member siap (OMTOGEL)\n\n"
        "Perintah:\n"
        "/ceknew = mulai (upload file ID dulu)\n"
        "/tanggal YYYY-MM-DD = set tanggal (opsional)\n"
        "/reset = hapus session\n\n"
        "Alur:\n"
        "1) /ceknew\n"
        "2) upload ID file (.txt/.csv/.xlsx)\n"
        "3) upload history koin (.csv/.xlsx)\n"
    )

@dp.message(Command("reset"))
async def reset(m: Message):
    if await deny_if_not_admin(m):
        return
    SESS[m.from_user.id] = {"ids": set(), "target_date": None, "awaiting": None}
    await m.answer("♻️ Session direset. Ketik /ceknew untuk mulai lagi.")

@dp.message(Command("tanggal"))
async def tanggal(m: Message):
    if await deny_if_not_admin(m):
        return
    parts = (m.text or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Format: /tanggal 2026-01-13")
        return
    try:
        d = datetime.strptime(parts[1].strip(), "%Y-%m-%d").date()
    except Exception:
        await m.answer("Format tanggal salah. Contoh: /tanggal 2026-01-13")
        return
    sess = SESS.setdefault(m.from_user.id, {"ids": set(), "target_date": None, "awaiting": None})
    sess["target_date"] = d
    await m.answer(f"📅 OK. Target tanggal diset: {d} (kalau tidak ada di file, bot pakai tanggal terbaru).")

@dp.message(Command("ceknew"))
async def ceknew(m: Message):
    if await deny_if_not_admin(m):
        return
    sess = SESS.setdefault(m.from_user.id, {"ids": set(), "target_date": None, "awaiting": None})
    sess["ids"] = set()
    sess["awaiting"] = "idsfile"
    await m.answer(
        "Upload file ID new member dulu ✅\n"
        "Format: .txt / .csv / .xlsx\n"
        "- TXT: boleh 1 baris 1 ID atau dipisah spasi/koma (bebas)\n"
        "- CSV/XLSX: kolom TO (atau kolom A)\n\n"
        "Setelah itu bot akan minta upload history koin."
    )

@dp.message(Command("adminlist"))
async def adminlist(m: Message):
    if await deny_if_not_admin(m):
        return
    await m.answer("✅ Admin IDs:\n" + "\n".join(str(i) for i in sorted(ADMIN_IDS)))

# ================== DOCUMENT HANDLER ==================

@dp.message(F.document)
async def handle_doc(m: Message):
    if await deny_if_not_admin(m):
        return

    sess = SESS.setdefault(m.from_user.id, {"ids": set(), "target_date": None, "awaiting": None})
    doc = m.document
    fname = (doc.file_name or "file").strip()
    ext = os.path.splitext(fname)[1].lower()

    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, fname)
        file = await bot.get_file(doc.file_id)
        await bot.download_file(file.file_path, destination=path)

        # 1) upload ID file
        if sess.get("awaiting") == "idsfile":
            if ext not in [".txt", ".csv", ".xlsx"]:
                await m.answer("❌ File ID harus .txt / .csv / .xlsx")
                return
            try:
                ids = load_id_file(path)
            except Exception as e:
                await m.answer(f"❌ Gagal baca file ID: {e}")
                return

            if not ids:
                await m.answer("❌ ID kosong. Pastikan file berisi ID.")
                return

            sess["ids"] = ids
            sess["awaiting"] = "file"
            await m.answer(f"✅ ID terkumpul: {fmt(len(ids))}\nSekarang upload history koin (CSV/XLSX).")
            return

        # 2) upload history file
        if sess.get("awaiting") == "file":
            if ext not in [".csv", ".xlsx"]:
                await m.answer("❌ History harus CSV atau XLSX")
                return
            if not sess.get("ids"):
                await m.answer("❌ ID belum ada. Ketik /ceknew lalu upload file ID dulu.")
                return

            try:
                rows = load_history(path)
                target_day, items = compute_report(rows, sess["ids"], sess.get("target_date"))
            except Exception as e:
                await m.answer(f"❌ Gagal proses history: {e}")
                return

            total_member_dp = len(items)
            total_dp_count = sum(cnt for _, cnt, _ in items)
            total_coin = sum(total for _, _, total in items)

            lines = []
            for i, (to_id, cnt, total) in enumerate(items[:15], start=1):
                lines.append(f"{i}. {to_id} | {fmt(cnt)}x | {fmt(total)}")

            msg = (
                f"📊 Report New Member (Tanggal {target_day})\n\n"
                f"📌 Total ID input: {fmt(len(sess['ids']))}\n"
                f"✅ Member yang deposit: {fmt(total_member_dp)}\n"
                f"🧾 Total deposit count (all new member): {fmt(total_dp_count)}\n"
                f"🪙 Total coin deposit (all new member): {fmt(total_coin)}\n\n"
                f"🏆 Top 15:\n" + "\n".join(lines)
            )
            await m.answer(msg)

            xbytes = make_excel_bytes(target_day, items)
            outname = f"report_new_member_{target_day}.xlsx"
            await m.answer_document(BufferedInputFile(xbytes, filename=outname))

            sess["awaiting"] = None
            return

    await m.answer("Ketik /ceknew dulu, lalu upload file ID (.txt/.csv/.xlsx).")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
