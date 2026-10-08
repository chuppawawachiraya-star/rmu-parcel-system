import os
import csv
import io
import json
import time
import uuid
import random
import secrets
import sqlite3
import threading
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, request, jsonify, g, Response, send_from_directory
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
CORS(app)  # รองรับการเรียกใช้จาก Web Frontend ต่างโดเมน

# ----------------------------------------------------
# การตั้งค่า
# ----------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'rmu_parcel.db')          # ไฟล์ฐานข้อมูล SQLite
LEGACY_JSON = os.path.join(BASE_DIR, 'packages.json')      # ข้อมูลเวอร์ชันเก่า (ย้ายเข้าฐานข้อมูลอัตโนมัติ)

TOKEN_TTL_SECONDS = 8 * 60 * 60        # login อยู่ได้ 8 ชั่วโมง
MAX_PIN_ATTEMPTS = 5                   # กรอก PIN ผิดได้กี่ครั้งก่อนล็อก
PIN_LOCK_SECONDS = 10 * 60             # ล็อกพัสดุชิ้นนั้นกี่วินาที
LOOKUP_MAX_FAILS = 10                  # ขอดู PIN ผิดได้กี่ครั้งต่อ IP
LOOKUP_WINDOW_SECONDS = 10 * 60        # ในช่วงเวลากี่วินาที
MIN_QUERY_LENGTH = 6
OVERDUE_DAYS = 7                       # รอรับเกินกี่วันถือว่าค้าง

# URL ของ Make.com Webhook สำหรับแจ้งเตือน LINE (อ่านจาก environment variable)
MAKE_WEBHOOK_URL = os.environ.get('MAKE_WEBHOOK_URL', '').strip()

# สถานะพัสดุ 3 ขั้น ตามขอบเขตโครงการ: รับเข้า → กำลังจัดส่ง → ส่งสำเร็จ
STATUS_RECEIVED = 'ARRIVED_AT_RMU'
STATUS_IN_TRANSIT = 'IN_TRANSIT'
STATUS_DELIVERED = 'DELIVERED'
STATUS_LABELS = {
    STATUS_RECEIVED: 'รับเข้าแล้ว',
    STATUS_IN_TRANSIT: 'กำลังจัดส่ง',
    STATUS_DELIVERED: 'ส่งสำเร็จ',
}

# ----------------------------------------------------
# โครงสร้างฐานข้อมูล (3 ตาราง)
# ----------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS staff_users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL UNIQUE,
    full_name     TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS packages (
    id               TEXT PRIMARY KEY,
    tracking_number  TEXT NOT NULL,
    carrier          TEXT NOT NULL,
    student_id       TEXT NOT NULL,
    recipient_name   TEXT NOT NULL,
    storage_location TEXT NOT NULL,
    shipping_status  TEXT NOT NULL
        CHECK (shipping_status IN ('ARRIVED_AT_RMU', 'IN_TRANSIT', 'DELIVERED')),
    pickup_pin       TEXT NOT NULL,
    failed_attempts  INTEGER NOT NULL DEFAULT 0,
    locked_until     REAL NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    dispatched_at    TEXT,
    delivered_at     TEXT,
    registered_by    INTEGER REFERENCES staff_users(id),
    dispatched_by    INTEGER REFERENCES staff_users(id),
    delivered_by     INTEGER REFERENCES staff_users(id),
    cancelled_at     TEXT,
    cancel_reason    TEXT,
    cancelled_by     INTEGER REFERENCES staff_users(id)
);

CREATE INDEX IF NOT EXISTS idx_packages_tracking ON packages(tracking_number);
CREATE INDEX IF NOT EXISTS idx_packages_student  ON packages(student_id);
CREATE INDEX IF NOT EXISTS idx_packages_created  ON packages(created_at);

CREATE TABLE IF NOT EXISTS package_status_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id  TEXT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    status      TEXT NOT NULL,
    changed_at  TEXT NOT NULL,
    changed_by  INTEGER REFERENCES staff_users(id),
    note        TEXT
);
"""

# ข้อมูลพัสดุพร้อมชื่อผู้ใช้ของเจ้าหน้าที่แต่ละขั้น
PACKAGE_SELECT = """
SELECT p.*,
       r.username AS registered_by_username,
       d.username AS dispatched_by_username,
       v.username AS delivered_by_username
FROM packages p
LEFT JOIN staff_users r ON r.id = p.registered_by
LEFT JOIN staff_users d ON d.id = p.dispatched_by
LEFT JOIN staff_users v ON v.id = p.delivered_by
"""

WAIT_HOURS_SQL = "(julianday(delivered_at) - julianday(created_at)) * 24"

# การดำเนินการที่บันทึกในประวัติ (นอกเหนือจาก 3 สถานะหลัก)
ACTION_EDITED = 'EDITED'
ACTION_CANCELLED = 'CANCELLED'
ACTION_LABELS = {**STATUS_LABELS, ACTION_EDITED: 'แก้ไขข้อมูล', ACTION_CANCELLED: 'ยกเลิกรายการ'}

# ฟิลด์ที่เจ้าหน้าที่แก้ไขได้
EDITABLE_FIELDS = {
    'tracking_number': 'เลข Tracking',
    'carrier': 'บริษัทขนส่ง',
    'student_id': 'รหัสนักศึกษา/บุคลากร',
    'recipient_name': 'ชื่อผู้รับ',
    'storage_location': 'จุดรับพัสดุ',
}

write_lock = threading.Lock()
active_tokens = {}      # token -> {user_id, username, name, expires}
lookup_failures = {}    # ip -> [timestamp, ...]


# ----------------------------------------------------
# ตัวช่วยฐานข้อมูล
# ----------------------------------------------------
def connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def get_db():
    if 'db' not in g:
        g.db = connect()
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def now_iso():
    return datetime.now().isoformat(timespec='seconds')


def clean_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '')).isoformat(timespec='seconds')
    except ValueError:
        return None


def normalize(value):
    return str(value or '').replace(' ', '').strip().lower()


def log_status(db, package_id, status, staff_id, note=None, at=None):
    db.execute(
        'INSERT INTO package_status_logs (package_id, status, changed_at, changed_by, note) VALUES (?, ?, ?, ?, ?)',
        (package_id, status, at or now_iso(), staff_id, note)
    )


def init_db():
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        ensure_columns(conn)
        if conn.execute('SELECT COUNT(*) FROM staff_users').fetchone()[0] == 0:
            conn.execute(
                'INSERT INTO staff_users (username, full_name, password_hash, created_at) VALUES (?, ?, ?, ?)',
                ('admin', 'เจ้าหน้าที่พัสดุ มรภ.', generate_password_hash('1234'), now_iso())
            )
        migrate_legacy_json(conn)
        conn.commit()
    finally:
        conn.close()


def ensure_columns(conn):
    """เพิ่มคอลัมน์ใหม่ให้ฐานข้อมูลเดิม โดยไม่กระทบข้อมูลที่มีอยู่"""
    existing = {row['name'] for row in conn.execute('PRAGMA table_info(packages)')}
    for name, ddl in [('cancelled_at', 'TEXT'), ('cancel_reason', 'TEXT'),
                      ('cancelled_by', 'INTEGER REFERENCES staff_users(id)')]:
        if name not in existing:
            conn.execute(f'ALTER TABLE packages ADD COLUMN {name} {ddl}')


def migrate_legacy_json(conn):
    """ย้ายข้อมูลจาก packages.json (เวอร์ชันก่อน) เข้าฐานข้อมูลครั้งเดียว"""
    if not os.path.exists(LEGACY_JSON):
        return
    if conn.execute('SELECT COUNT(*) FROM packages').fetchone()[0] > 0:
        return
    try:
        with open(LEGACY_JSON, 'r', encoding='utf-8') as f:
            items = json.load(f)
    except (json.JSONDecodeError, OSError):
        return

    users = {row['username']: row['id'] for row in conn.execute('SELECT id, username FROM staff_users')}
    count = 0
    for p in items:
        status = p.get('shipping_status')
        if status not in STATUS_LABELS:
            status = STATUS_RECEIVED
        created = clean_iso(p.get('created_at')) or now_iso()
        delivered = clean_iso(p.get('delivered_at')) if status == STATUS_DELIVERED else None
        registered_by = users.get(p.get('registered_by'))
        delivered_by = users.get(p.get('delivered_by'))
        pin = str(p.get('pickup_pin') or f"{random.SystemRandom().randint(0, 9999):04d}")
        pkg_id = p.get('id') or f"pkg_{uuid.uuid4().hex[:12]}"

        cur = conn.execute(
            """INSERT OR IGNORE INTO packages
               (id, tracking_number, carrier, student_id, recipient_name, storage_location,
                shipping_status, pickup_pin, created_at, delivered_at, registered_by, delivered_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pkg_id, p.get('tracking_number', ''), p.get('carrier', 'อื่นๆ'),
             p.get('student_id', ''), p.get('recipient_name', ''), p.get('storage_location', ''),
             status, pin, created, delivered, registered_by, delivered_by)
        )
        if cur.rowcount:
            log_status(conn, pkg_id, STATUS_RECEIVED, registered_by, 'ย้ายจากข้อมูลเดิม', created)
            if delivered:
                log_status(conn, pkg_id, STATUS_DELIVERED, delivered_by, 'ย้ายจากข้อมูลเดิม', delivered)
            count += 1

    conn.commit()
    os.replace(LEGACY_JSON, LEGACY_JSON + '.bak')
    print(f'📦 ย้ายข้อมูลเดิม {count} รายการจาก packages.json เข้าฐานข้อมูลแล้ว (ไฟล์เดิมเปลี่ยนชื่อเป็น packages.json.bak)')


# ----------------------------------------------------
# แจ้งเตือน LINE ผ่าน Make.com (server เป็นคนเรียก หน้าเว็บไม่เห็น URL)
# ----------------------------------------------------
def _post_webhook(payload):
    try:
        req = urllib.request.Request(
            MAKE_WEBHOOK_URL,
            data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urllib.request.urlopen(req, timeout=10) as res:
            app.logger.info('ส่งแจ้งเตือน Make.com สำเร็จ (HTTP %s)', res.status)
    except (urllib.error.URLError, OSError) as e:
        app.logger.warning('ส่งแจ้งเตือน Make.com ไม่สำเร็จ: %s', e)


def notify_package_registered(pkg):
    if not MAKE_WEBHOOK_URL:
        return False
    # ชื่อ field เหมือนเดิม scenario ใน Make ไม่ต้องแก้ / ไม่ส่ง PIN
    # Make.com ใช้ student_id หา LINE ของเจ้าของพัสดุ แล้วส่งหาเจ้าของโดยตรง
    # ข้อความถึงผู้รับควรใช้ tracking_masked เพื่อไม่ให้เลขเต็มปรากฏบนหน้าจอแจ้งเตือน
    payload = {
        'event': 'package_registered',
        'tracking_number': pkg['tracking_number'],
        'tracking_masked': mask_middle(pkg['tracking_number'], 4, 3),
        'carrier': pkg['carrier'],
        'student_id': pkg['student_id'],
        'recipient_name': pkg['recipient_name'],
        'storage_location': pkg['storage_location'],
        'status': STATUS_LABELS[pkg['shipping_status']],
        'created_at': pkg['created_at'],
    }
    threading.Thread(target=_post_webhook, args=(payload,), daemon=True).start()
    return True


# ----------------------------------------------------
# รูปแบบข้อมูลที่ส่งกลับ
# ----------------------------------------------------
def mask_middle(value, keep_start, keep_end):
    s = str(value or '')
    if len(s) <= keep_start + keep_end:
        return s[:1] + '*' * max(len(s) - 1, 0)
    return s[:keep_start] + '*' * (len(s) - keep_start - keep_end) + s[-keep_end:]


def mask_name(name):
    parts = str(name or '').split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1][0]}."
    return (parts[0][:2] + '***') if parts else ''


def public_view(row):
    """ข้อมูลที่คนทั่วไปเห็นได้: ไม่มี PIN และปิดบังข้อมูลส่วนตัวบางส่วน"""
    return {
        'id': row['id'],
        'tracking_number': mask_middle(row['tracking_number'], 4, 3),
        'carrier': row['carrier'],
        'student_id': mask_middle(row['student_id'], 4, 2),
        'recipient_name': mask_name(row['recipient_name']),
        'storage_location': row['storage_location'],
        'shipping_status': row['shipping_status'],
        'status': STATUS_LABELS.get(row['shipping_status']),
        'created_at': row['created_at'],
        'dispatched_at': row['dispatched_at'],
        'delivered_at': row['delivered_at'],
    }


def staff_view(row):
    """ข้อมูลสำหรับเจ้าหน้าที่: เห็นครบ ยกเว้น PIN และข้อมูลภายใน"""
    hidden = {'pickup_pin', 'failed_attempts', 'locked_until'}
    data = {k: row[k] for k in row.keys() if k not in hidden}
    data['status'] = STATUS_LABELS.get(row['shipping_status'])
    return data


def fetch_package(db, package_id):
    return db.execute(PACKAGE_SELECT + ' WHERE p.id = ?', (package_id,)).fetchone()


# ----------------------------------------------------
# ระบบยืนยันตัวตนเจ้าหน้าที่ (Bearer Token)
# ----------------------------------------------------
def get_token():
    auth = request.headers.get('Authorization', '')
    return auth[7:] if auth.startswith('Bearer ') else ''


def require_staff(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        token = get_token()
        session = active_tokens.get(token)
        if not session or session['expires'] < time.time():
            active_tokens.pop(token, None)
            return jsonify({"success": False, "message": "กรุณาเข้าสู่ระบบเจ้าหน้าที่"}), 401
        request.staff = session
        return func(*args, **kwargs)
    return wrapper


def too_many_lookup_failures(ip):
    cutoff = time.time() - LOOKUP_WINDOW_SECONDS
    recent = [t for t in lookup_failures.get(ip, []) if t > cutoff]
    lookup_failures[ip] = recent
    return len(recent) >= LOOKUP_MAX_FAILS


def record_lookup_failure(ip):
    lookup_failures.setdefault(ip, []).append(time.time())


# ----------------------------------------------------
# API Endpoints
# ----------------------------------------------------
@app.route('/', methods=['GET'])
def index_page():
    # เปิดหน้าเว็บผ่าน http://127.0.0.1:5000 ได้เลย (ส่งเฉพาะ index.html ไม่เปิดให้เข้าถึงไฟล์อื่นในโฟลเดอร์)
    return send_from_directory(BASE_DIR, 'index.html')


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "online",
        "system": "RMU Parcel Tracking Backend API",
        "version": "2.1.0",
        "database": "SQLite"
    })


# 0. เข้าสู่ระบบ / ออกจากระบบเจ้าหน้าที่
@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json(silent=True) or {}
    username = str(data.get('username', '')).strip()
    password = str(data.get('password', ''))

    user = get_db().execute('SELECT * FROM staff_users WHERE username = ?', (username,)).fetchone()
    if not user or not check_password_hash(user['password_hash'], password):
        return jsonify({"success": False, "message": "❌ ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง!"}), 401

    token = secrets.token_urlsafe(32)
    active_tokens[token] = {
        'user_id': user['id'],
        'username': user['username'],
        'name': user['full_name'],
        'expires': time.time() + TOKEN_TTL_SECONDS,
    }
    return jsonify({
        "success": True,
        "token": token,
        "user": {"username": user['username'], "name": user['full_name'], "role": "staff"}
    })


@app.route('/api/logout', methods=['POST'])
def logout():
    active_tokens.pop(get_token(), None)
    return jsonify({"success": True})


# 1. ค้นหาพัสดุ (สาธารณะ) — ต้องตรงตัวทั้งเลข ไม่ค้นด้วยชื่อ และไม่ส่ง PIN
@app.route('/api/packages/search', methods=['GET'])
def search_packages():
    query = normalize(request.args.get('q'))
    if len(query) < MIN_QUERY_LENGTH:
        return jsonify({
            "success": False,
            "message": "กรุณากรอกเลข Tracking หรือรหัสนักศึกษา/บุคลากรให้ครบ"
        }), 400

    rows = get_db().execute(
        """SELECT * FROM packages
           WHERE (REPLACE(LOWER(tracking_number), ' ', '') = ?
                  OR REPLACE(LOWER(student_id), ' ', '') = ?)
             AND cancelled_at IS NULL
           ORDER BY created_at DESC""",
        (query, query)
    ).fetchall()
    results = [public_view(r) for r in rows]
    return jsonify({"success": True, "count": len(results), "data": results})


# 1.1 ขอดู PIN / QR ของตัวเอง (สาธารณะ) — ต้องรู้ทั้งเลข Tracking และรหัสนักศึกษาแบบเต็ม
@app.route('/api/packages/pickup-code', methods=['POST'])
def get_pickup_code():
    ip = request.remote_addr or 'unknown'
    if too_many_lookup_failures(ip):
        return jsonify({"success": False, "message": "ลองผิดหลายครั้งเกินไป กรุณารอ 10 นาทีแล้วลองใหม่"}), 429

    data = request.get_json(silent=True) or {}
    row = get_db().execute('SELECT * FROM packages WHERE id = ? AND cancelled_at IS NULL', (data.get('package_id'),)).fetchone()

    if (row
            and normalize(row['tracking_number']) == normalize(data.get('tracking_number'))
            and normalize(row['student_id']) == normalize(data.get('student_id'))):
        if row['shipping_status'] == STATUS_DELIVERED:
            return jsonify({"success": False, "message": "พัสดุชิ้นนี้ส่งสำเร็จแล้ว"}), 409
        return jsonify({"success": True, "data": {"id": row['id'], "pickup_pin": row['pickup_pin']}})

    record_lookup_failure(ip)
    return jsonify({"success": False, "message": "เลข Tracking หรือรหัสนักศึกษาไม่ตรงกับพัสดุชิ้นนี้"}), 403


# 2. ลงทะเบียนพัสดุใหม่ (สถานะ: รับเข้าแล้ว) + สุ่ม PIN (เฉพาะเจ้าหน้าที่)
@app.route('/api/packages', methods=['POST'])
@require_staff
def create_package():
    data = request.get_json(silent=True) or {}

    required_fields = ['tracking_number', 'carrier', 'student_id', 'recipient_name', 'storage_location']
    for field in required_fields:
        if not str(data.get(field, '')).strip():
            return jsonify({"success": False, "message": f"ขาดข้อมูล {field}"}), 400

    tracking = data['tracking_number'].strip()
    db = get_db()

    with write_lock:
        duplicate = db.execute(
            """SELECT 1 FROM packages
               WHERE REPLACE(LOWER(tracking_number), ' ', '') = ? AND shipping_status != ?
                 AND cancelled_at IS NULL""",
            (normalize(tracking), STATUS_DELIVERED)
        ).fetchone()
        if duplicate:
            return jsonify({
                "success": False,
                "message": f"เลข Tracking {tracking} ลงทะเบียนไว้แล้วและยังส่งไม่สำเร็จ"
            }), 409

        pkg_id = f"pkg_{uuid.uuid4().hex[:12]}"
        pickup_pin = f"{random.SystemRandom().randint(0, 9999):04d}"
        created_at = now_iso()
        staff_id = request.staff['user_id']

        db.execute(
            """INSERT INTO packages
               (id, tracking_number, carrier, student_id, recipient_name, storage_location,
                shipping_status, pickup_pin, created_at, registered_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pkg_id, tracking, data['carrier'], data['student_id'].strip(), data['recipient_name'].strip(),
             data['storage_location'], STATUS_RECEIVED, pickup_pin, created_at, staff_id)
        )
        log_status(db, pkg_id, STATUS_RECEIVED, staff_id, 'ลงทะเบียนรับเข้า', created_at)
        db.commit()

    row = fetch_package(db, pkg_id)
    notified = notify_package_registered(row)

    response_data = staff_view(row)
    response_data['pickup_pin'] = pickup_pin  # แสดงให้เจ้าหน้าที่เห็นครั้งเดียว
    return jsonify({
        "success": True,
        "message": "บันทึกพัสดุสำเร็จ",
        "data": response_data,
        "line_notification": notified
    }), 201


# 3. อัปเดตสถานะเป็น "กำลังจัดส่ง" ไปยังจุดรับ (เฉพาะเจ้าหน้าที่)
@app.route('/api/packages/<package_id>/dispatch', methods=['POST'])
@require_staff
def dispatch_package(package_id):
    db = get_db()
    with write_lock:
        row = db.execute('SELECT * FROM packages WHERE id = ? AND cancelled_at IS NULL', (package_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "ไม่พบรายการพัสดุนี้"}), 404
        if row['shipping_status'] != STATUS_RECEIVED:
            return jsonify({
                "success": False,
                "message": f"พัสดุนี้อยู่ในสถานะ \"{STATUS_LABELS[row['shipping_status']]}\" แล้ว"
            }), 409

        at = now_iso()
        staff_id = request.staff['user_id']
        db.execute(
            'UPDATE packages SET shipping_status = ?, dispatched_at = ?, dispatched_by = ? WHERE id = ?',
            (STATUS_IN_TRANSIT, at, staff_id, package_id)
        )
        log_status(db, package_id, STATUS_IN_TRANSIT, staff_id, f"จัดส่งไปยัง {row['storage_location']}", at)
        db.commit()

    return jsonify({"success": True, "message": "🚚 อัปเดตสถานะเป็นกำลังจัดส่งแล้ว",
                    "data": staff_view(fetch_package(db, package_id))})


# 4. ตรวจสอบ PIN แล้วอัปเดตเป็น "ส่งสำเร็จ" (เฉพาะเจ้าหน้าที่) + ล็อกเมื่อผิดหลายครั้ง
@app.route('/api/packages/verify-pin', methods=['POST'])
@require_staff
def verify_pin():
    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    input_pin = str(data.get('pin', '')).strip()
    db = get_db()

    with write_lock:
        row = db.execute('SELECT * FROM packages WHERE id = ? AND cancelled_at IS NULL', (package_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "ไม่พบรายการพัสดุนี้"}), 404
        if row['shipping_status'] == STATUS_DELIVERED:
            return jsonify({"success": False, "message": "พัสดุชิ้นนี้ส่งสำเร็จแล้ว"}), 409

        remaining_lock = row['locked_until'] - time.time()
        if remaining_lock > 0:
            minutes = int(remaining_lock // 60) + 1
            return jsonify({
                "success": False,
                "message": f"🔒 กรอก PIN ผิดหลายครั้ง พัสดุชิ้นนี้ถูกล็อก กรุณารออีก {minutes} นาที"
            }), 423

        if secrets.compare_digest(row['pickup_pin'], input_pin):
            at = now_iso()
            staff_id = request.staff['user_id']
            db.execute(
                """UPDATE packages SET shipping_status = ?, delivered_at = ?, delivered_by = ?,
                   failed_attempts = 0, locked_until = 0 WHERE id = ?""",
                (STATUS_DELIVERED, at, staff_id, package_id)
            )
            log_status(db, package_id, STATUS_DELIVERED, staff_id, 'ยืนยัน PIN ส่งมอบให้ผู้รับ', at)
            db.commit()
            return jsonify({"success": True, "message": "✅ รหัสถูกต้อง! ยืนยันการรับพัสดุเรียบร้อย"})

        attempts = row['failed_attempts'] + 1
        attempts_left = MAX_PIN_ATTEMPTS - attempts
        if attempts_left <= 0:
            db.execute('UPDATE packages SET failed_attempts = 0, locked_until = ? WHERE id = ?',
                       (time.time() + PIN_LOCK_SECONDS, package_id))
            message = f"❌ PIN ผิดครบ {MAX_PIN_ATTEMPTS} ครั้ง พัสดุชิ้นนี้ถูกล็อก {PIN_LOCK_SECONDS // 60} นาที"
        else:
            db.execute('UPDATE packages SET failed_attempts = ? WHERE id = ?', (attempts, package_id))
            message = f"❌ รหัส PIN ไม่ถูกต้อง! (เหลือโอกาสอีก {attempts_left} ครั้ง)"
        db.commit()
        return jsonify({"success": False, "message": message}), 403


# 4.1 ส่งมอบโดยตรวจบัตร กรณีผู้รับไม่มี PIN / QR (เฉพาะเจ้าหน้าที่)
@app.route('/api/packages/<package_id>/deliver-manual', methods=['POST'])
@require_staff
def deliver_manual(package_id):
    data = request.get_json(silent=True) or {}
    card_number = normalize(data.get('id_card_number'))
    reason = str(data.get('reason', '')).strip()
    if not card_number or not reason:
        return jsonify({"success": False, "message": "กรุณากรอกเลขบัตรที่ตรวจสอบและเหตุผล"}), 400

    db = get_db()
    with write_lock:
        row = db.execute('SELECT * FROM packages WHERE id = ? AND cancelled_at IS NULL', (package_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "ไม่พบรายการพัสดุนี้"}), 404
        if row['shipping_status'] == STATUS_DELIVERED:
            return jsonify({"success": False, "message": "พัสดุชิ้นนี้ส่งสำเร็จแล้ว"}), 409
        if normalize(row['student_id']) != card_number:
            return jsonify({
                "success": False,
                "message": "❌ เลขบัตรไม่ตรงกับรหัสผู้รับของพัสดุชิ้นนี้ ห้ามส่งมอบ"
            }), 403

        at = now_iso()
        staff_id = request.staff['user_id']
        db.execute(
            """UPDATE packages SET shipping_status = ?, delivered_at = ?, delivered_by = ?,
               failed_attempts = 0, locked_until = 0 WHERE id = ?""",
            (STATUS_DELIVERED, at, staff_id, package_id)
        )
        log_status(db, package_id, STATUS_DELIVERED, staff_id, f'ส่งมอบโดยตรวจบัตร (ไม่ใช้ PIN) เหตุผล: {reason}', at)
        db.commit()
    return jsonify({"success": True, "message": "✅ ส่งมอบพัสดุโดยตรวจบัตรเรียบร้อย"})


# 4.2 แก้ไขข้อมูลพัสดุ (เฉพาะเจ้าหน้าที่ และพัสดุที่ยังไม่ส่งสำเร็จ)
@app.route('/api/packages/<package_id>', methods=['PUT'])
@require_staff
def update_package(package_id):
    data = request.get_json(silent=True) or {}
    db = get_db()
    with write_lock:
        row = db.execute('SELECT * FROM packages WHERE id = ? AND cancelled_at IS NULL', (package_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "ไม่พบรายการพัสดุนี้"}), 404
        if row['shipping_status'] == STATUS_DELIVERED:
            return jsonify({"success": False, "message": "พัสดุที่ส่งสำเร็จแล้วแก้ไขไม่ได้"}), 409

        changes = {}
        for field, label in EDITABLE_FIELDS.items():
            if field in data:
                value = str(data[field] or '').strip()
                if not value:
                    return jsonify({"success": False, "message": f"{label} ห้ามว่าง"}), 400
                if value != row[field]:
                    changes[field] = value
        if not changes:
            return jsonify({"success": True, "message": "ไม่มีข้อมูลที่เปลี่ยนแปลง"})

        if 'tracking_number' in changes:
            duplicate = db.execute(
                """SELECT 1 FROM packages
                   WHERE REPLACE(LOWER(tracking_number), ' ', '') = ? AND shipping_status != ?
                     AND cancelled_at IS NULL AND id != ?""",
                (normalize(changes['tracking_number']), STATUS_DELIVERED, package_id)
            ).fetchone()
            if duplicate:
                return jsonify({"success": False, "message": "เลข Tracking นี้มีอยู่ในระบบแล้วและยังส่งไม่สำเร็จ"}), 409

        # ชื่อคอลัมน์มาจาก EDITABLE_FIELDS เท่านั้น จึงปลอดภัยจาก SQL Injection
        sets = ', '.join(f'{field} = ?' for field in changes)
        db.execute(f'UPDATE packages SET {sets} WHERE id = ?', (*changes.values(), package_id))
        note = 'แก้ไข ' + '; '.join(f'{EDITABLE_FIELDS[f]}: {row[f]} → {v}' for f, v in changes.items())
        log_status(db, package_id, ACTION_EDITED, request.staff['user_id'], note)
        db.commit()

    return jsonify({"success": True, "message": "✅ บันทึกการแก้ไขแล้ว",
                    "data": staff_view(fetch_package(db, package_id))})


# 4.3 ยกเลิกรายการพัสดุ (ไม่ลบข้อมูลจริง เก็บไว้ตรวจสอบย้อนหลัง)
@app.route('/api/packages/<package_id>/cancel', methods=['POST'])
@require_staff
def cancel_package(package_id):
    reason = str((request.get_json(silent=True) or {}).get('reason', '')).strip()
    if not reason:
        return jsonify({"success": False, "message": "กรุณาระบุเหตุผลที่ยกเลิก"}), 400

    db = get_db()
    with write_lock:
        row = db.execute('SELECT * FROM packages WHERE id = ? AND cancelled_at IS NULL', (package_id,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "ไม่พบรายการพัสดุนี้"}), 404
        if row['shipping_status'] == STATUS_DELIVERED:
            return jsonify({"success": False, "message": "พัสดุที่ส่งสำเร็จแล้วยกเลิกไม่ได้"}), 409

        at = now_iso()
        staff_id = request.staff['user_id']
        db.execute('UPDATE packages SET cancelled_at = ?, cancel_reason = ?, cancelled_by = ? WHERE id = ?',
                   (at, reason, staff_id, package_id))
        log_status(db, package_id, ACTION_CANCELLED, staff_id, f'เหตุผล: {reason}', at)
        db.commit()
    return jsonify({"success": True, "message": "🗑️ ยกเลิกรายการพัสดุแล้ว"})


# 4.4 ประวัติการดำเนินการของพัสดุ (เฉพาะเจ้าหน้าที่)
@app.route('/api/packages/<package_id>/history', methods=['GET'])
@require_staff
def package_history(package_id):
    db = get_db()
    pkg = fetch_package(db, package_id)
    if not pkg:
        return jsonify({"success": False, "message": "ไม่พบรายการพัสดุนี้"}), 404
    logs = db.execute(
        """SELECT l.status, l.changed_at, l.note, s.username, s.full_name
           FROM package_status_logs l
           LEFT JOIN staff_users s ON s.id = l.changed_by
           WHERE l.package_id = ?
           ORDER BY l.changed_at, l.id""",
        (package_id,)
    ).fetchall()
    return jsonify({"success": True, "data": {
        "package": staff_view(pkg),
        "logs": [{
            "status": l['status'],
            "label": ACTION_LABELS.get(l['status'], l['status']),
            "changed_at": l['changed_at'],
            "by": l['full_name'] or l['username'] or '-',
            "note": l['note'],
        } for l in logs]
    }})


# 5. รายการพัสดุทั้งหมดสำหรับ Dashboard (เฉพาะเจ้าหน้าที่ และไม่ส่ง PIN)
@app.route('/api/packages', methods=['GET'])
@require_staff
def get_all_packages():
    rows = get_db().execute(PACKAGE_SELECT + ' WHERE p.cancelled_at IS NULL ORDER BY p.created_at DESC').fetchall()
    return jsonify({"success": True, "data": [staff_view(r) for r in rows]})


# ----------------------------------------------------
# รายงานสรุปและสถิติ
# ----------------------------------------------------
def parse_report_range():
    today = datetime.now().date()
    try:
        to_date = datetime.strptime(request.args.get('to') or today.isoformat(), '%Y-%m-%d').date()
        default_from = (to_date - timedelta(days=29)).isoformat()
        from_date = datetime.strptime(request.args.get('from') or default_from, '%Y-%m-%d').date()
    except ValueError:
        return None, None, 'รูปแบบวันที่ไม่ถูกต้อง (ต้องเป็น YYYY-MM-DD)'
    if from_date > to_date:
        from_date, to_date = to_date, from_date
    if (to_date - from_date).days > 366:
        return None, None, 'เลือกช่วงเวลาได้ไม่เกิน 1 ปี'
    return from_date, to_date, None


def range_bounds(from_date, to_date):
    return from_date.isoformat() + 'T00:00:00', (to_date + timedelta(days=1)).isoformat() + 'T00:00:00'


def round_or_none(value, digits=1):
    return round(value, digits) if value is not None else None


@app.route('/api/reports', methods=['GET'])
@require_staff
def get_report():
    from_date, to_date, error = parse_report_range()
    if error:
        return jsonify({"success": False, "message": error}), 400

    start, end = range_bounds(from_date, to_date)
    db = get_db()
    in_range = 'created_at >= ? AND created_at < ? AND cancelled_at IS NULL'

    s = db.execute(f"""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(shipping_status = 'ARRIVED_AT_RMU'), 0) AS received,
               COALESCE(SUM(shipping_status = 'IN_TRANSIT'), 0)     AS in_transit,
               COALESCE(SUM(shipping_status = 'DELIVERED'), 0)      AS delivered,
               AVG(CASE WHEN delivered_at IS NOT NULL THEN {WAIT_HOURS_SQL} END) AS avg_wait_hours
        FROM packages WHERE {in_range}
    """, (start, end)).fetchone()

    overdue_cutoff = (datetime.now() - timedelta(days=OVERDUE_DAYS)).isoformat(timespec='seconds')
    overdue_now = db.execute(
        'SELECT COUNT(*) FROM packages WHERE shipping_status != ? AND created_at <= ? AND cancelled_at IS NULL',
        (STATUS_DELIVERED, overdue_cutoff)
    ).fetchone()[0]

    # ปริมาณรายวัน (เติมวันที่ไม่มีข้อมูลเป็น 0)
    received_by_day = dict(db.execute(
        f'SELECT substr(created_at, 1, 10), COUNT(*) FROM packages WHERE {in_range} GROUP BY 1',
        (start, end)
    ).fetchall())
    delivered_by_day = dict(db.execute(
        'SELECT substr(delivered_at, 1, 10), COUNT(*) FROM packages '
        'WHERE delivered_at >= ? AND delivered_at < ? AND cancelled_at IS NULL GROUP BY 1',
        (start, end)
    ).fetchall())
    daily = []
    day = from_date
    while day <= to_date:
        key = day.isoformat()
        daily.append({"date": key, "received": received_by_day.get(key, 0), "delivered": delivered_by_day.get(key, 0)})
        day += timedelta(days=1)

    # ช่วงเวลาที่รับพัสดุเข้า (รายชั่วโมง 0-23)
    by_hour = [0] * 24
    for hour, count in db.execute(
        f'SELECT CAST(substr(created_at, 12, 2) AS INTEGER), COUNT(*) FROM packages WHERE {in_range} GROUP BY 1',
        (start, end)
    ).fetchall():
        if hour is not None and 0 <= hour < 24:
            by_hour[hour] = count

    by_location = [
        {
            "location": r['storage_location'],
            "total": r['total'],
            "waiting": r['waiting'],
            "delivered": r['delivered'],
            "avg_wait_hours": round_or_none(r['avg_wait_hours']),
        }
        for r in db.execute(f"""
            SELECT storage_location,
                   COUNT(*) AS total,
                   SUM(shipping_status != 'DELIVERED') AS waiting,
                   SUM(shipping_status = 'DELIVERED')  AS delivered,
                   AVG(CASE WHEN delivered_at IS NOT NULL THEN {WAIT_HOURS_SQL} END) AS avg_wait_hours
            FROM packages WHERE {in_range}
            GROUP BY storage_location ORDER BY total DESC
        """, (start, end)).fetchall()
    ]

    by_carrier = [
        {"carrier": r['carrier'], "total": r['total']}
        for r in db.execute(
            f'SELECT carrier, COUNT(*) AS total FROM packages WHERE {in_range} GROUP BY carrier ORDER BY total DESC',
            (start, end)
        ).fetchall()
    ]

    return jsonify({
        "success": True,
        "range": {"from": from_date.isoformat(), "to": to_date.isoformat()},
        "summary": {
            "total": s['total'],
            "received": s['received'],
            "in_transit": s['in_transit'],
            "delivered": s['delivered'],
            "waiting": s['received'] + s['in_transit'],
            "overdue_now": overdue_now,
            "avg_wait_hours": round_or_none(s['avg_wait_hours']),
        },
        "daily": daily,
        "by_hour": by_hour,
        "by_location": by_location,
        "by_carrier": by_carrier,
    })


@app.route('/api/reports/export', methods=['GET'])
@require_staff
def export_report_csv():
    from_date, to_date, error = parse_report_range()
    if error:
        return jsonify({"success": False, "message": error}), 400

    start, end = range_bounds(from_date, to_date)
    rows = get_db().execute(
        PACKAGE_SELECT + ' WHERE p.created_at >= ? AND p.created_at < ? AND p.cancelled_at IS NULL ORDER BY p.created_at',
        (start, end)
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'เลข Tracking', 'บริษัทขนส่ง', 'รหัสนักศึกษา/บุคลากร', 'ชื่อผู้รับ', 'จุดรับพัสดุ', 'สถานะ',
        'รับเข้าเมื่อ', 'จัดส่งเมื่อ', 'ส่งสำเร็จเมื่อ', 'ระยะเวลารอรับ (ชั่วโมง)',
        'ลงทะเบียนโดย', 'จัดส่งโดย', 'ส่งมอบโดย'
    ])
    for r in rows:
        wait_hours = ''
        if r['delivered_at']:
            delta = datetime.fromisoformat(r['delivered_at']) - datetime.fromisoformat(r['created_at'])
            wait_hours = round(delta.total_seconds() / 3600, 1)
        writer.writerow([
            r['tracking_number'], r['carrier'], r['student_id'], r['recipient_name'], r['storage_location'],
            STATUS_LABELS.get(r['shipping_status'], r['shipping_status']),
            r['created_at'], r['dispatched_at'] or '', r['delivered_at'] or '', wait_hours,
            r['registered_by_username'] or '', r['dispatched_by_username'] or '', r['delivered_by_username'] or ''
        ])

    filename = f"rmu-parcel-report_{from_date.isoformat()}_{to_date.isoformat()}.csv"
    # ใส่ BOM เพื่อให้ Excel อ่านภาษาไทยถูกต้อง
    return Response(
        '\ufeff' + output.getvalue(),
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )


init_db()


def warn_default_password():
    conn = connect()
    try:
        row = conn.execute("SELECT password_hash FROM staff_users WHERE username = 'admin'").fetchone()
        if row and check_password_hash(row['password_hash'], '1234'):
            print('⚠️  บัญชี admin ยังใช้รหัสผ่านเริ่มต้น 1234 เปลี่ยนด้วยคำสั่ง: py -3.14 manage_staff.py passwd admin')
    finally:
        conn.close()


warn_default_password()

if MAKE_WEBHOOK_URL:
    print('🔔 เปิดการแจ้งเตือน LINE ผ่าน Make.com แล้ว')
else:
    print('🔕 ยังไม่ได้ตั้งค่า MAKE_WEBHOOK_URL ระบบจะไม่ส่งแจ้งเตือน LINE')

def lan_ip():
    """หาเลข IP ของเครื่องในวง Wi-Fi (ไม่ได้ส่งข้อมูลออกไปจริง)"""
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(('8.8.8.8', 80))
        return sock.getsockname()[0]
    except OSError:
        return None
    finally:
        sock.close()


if __name__ == '__main__':
    host = os.environ.get('RMU_HOST', '0.0.0.0')
    debug = os.environ.get('RMU_DEBUG', '1') == '1' and host == '127.0.0.1'
    print('🌐 เปิดระบบในเครื่องนี้ที่ http://127.0.0.1:5000')
    if host != '127.0.0.1':
        ip = lan_ip()
        if ip and os.environ.get('WERKZEUG_RUN_MAIN') is None:
            print(f'📱 เปิดในมือถือ (ต่อ Wi-Fi เดียวกับเครื่องนี้): http://{ip}:5000')
        elif not ip:
            print('⚠️ หาเลข IP ของเครื่องไม่เจอ ตรวจสอบว่าเชื่อมต่อ Wi-Fi อยู่')
    app.run(host=host, port=5000, debug=debug)