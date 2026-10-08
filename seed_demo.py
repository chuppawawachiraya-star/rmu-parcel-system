"""
สคริปต์สร้างข้อมูลพัสดุตัวอย่างสำหรับสาธิตระบบ RMU Parcel Tracking

วิธีใช้ (รันใน terminal ที่โฟลเดอร์ queue-system):
    py -3.14 seed_demo.py              เพิ่มพัสดุตัวอย่าง 40 ชิ้น
    py -3.14 seed_demo.py --count 60   เพิ่มตามจำนวนที่ต้องการ
    py -3.14 seed_demo.py --clear      ลบเฉพาะข้อมูลตัวอย่างทั้งหมด (ข้อมูลจริงไม่หาย)

หมายเหตุ
- ต้องรัน app.py อย่างน้อยหนึ่งครั้งก่อน เพื่อให้มีไฟล์ rmu_parcel.db
- เปิด server ค้างไว้ระหว่างรันได้ ไม่ต้องปิด
- ข้อมูลตัวอย่างเขียนลงฐานข้อมูลตรง จึงไม่ส่งแจ้งเตือน LINE
- ชื่อ รหัส และเลข Tracking สุ่มขึ้นมาทั้งหมด ไม่ใช่ข้อมูลของบุคคลจริง
"""
import os
import sys
import random
import sqlite3
import argparse
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'rmu_parcel.db')
DEMO_PREFIX = 'demo_'

RECEIVED, IN_TRANSIT, DELIVERED = 'ARRIVED_AT_RMU', 'IN_TRANSIT', 'DELIVERED'

# ต้องตรงกับตัวเลือกจุดรับใน index.html ทุกตัวอักษร
LOCATIONS = [
    ('R2 - อาคารพัสดุ (ศูนย์รับพัสดุกลาง)', 30),
    ('J - อาคารหอพักนักศึกษา (RMU Dormitory)', 15),
    ('อาคาร 38 - คณะเทคโนโลยีสารสนเทศ', 8),
    ('อาคาร 1 - คณะครุศาสตร์', 6),
    ('อาคาร 34 - คณะวิทยาการจัดการ', 6),
    ('อาคาร 6 - คณะวิทยาศาสตร์และเทคโนโลยี', 5),
    ('อาคาร 19 - คณะวิศวกรรมศาสตร์', 5),
    ('อาคาร 3 - คณะมนุษยศาสตร์และสังคมศาสตร์', 4),
    ('อาคาร 33 - คณะนิติศาสตร์', 3),
    ('อาคาร 16 - สำนักวิทยบริการและเทคโนโลยีสารสนเทศ (หอสมุด)', 3),
    ('G - อาคารโรงอาหาร', 2),
]

CARRIERS = [
    ('Shopee Express', 30),
    ('Flash Express', 22),
    ('ไปรษณีย์ไทย', 18),
    ('J&T Express', 15),
    ('Kerry Express', 10),
    ('Lazada Logistics', 5),
]

FIRST_NAMES = ['กมลชนก', 'ธนพล', 'ณัฐธิดา', 'พีรพัฒน์', 'สุภาวดี', 'อนุชา', 'ศิริพร', 'กิตติศักดิ์',
               'วรรณภา', 'ปิยะพงษ์', 'นันทนา', 'ชยพล', 'อรอุมา', 'ภานุวัฒน์', 'จิราพร', 'สิทธิชัย']
LAST_NAMES = ['ศรีสุข', 'บุญมา', 'แก้วมณี', 'ทองดี', 'พรหมวงศ์', 'สายสุวรรณ',
              'จันทร์เพ็ญ', 'ภูมิพันธ์', 'อินทร์แก้ว', 'ศรีวงษ์', 'มณีวงศ์', 'คำภา']

# ช่วงเวลาที่พัสดุเข้ามาบ่อย (เช้าและบ่าย)
WORK_HOURS = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
HOUR_WEIGHTS = [2, 5, 6, 4, 2, 5, 6, 4, 3, 1]


def pick(rng, weighted):
    items, weights = zip(*weighted)
    return rng.choices(items, weights=weights, k=1)[0]


def make_tracking(rng, carrier):
    digits = lambda n: ''.join(rng.choice('0123456789') for _ in range(n))
    if carrier == 'ไปรษณีย์ไทย':
        return f"E{rng.choice('BFKLMR')}{digits(9)}TH"
    if carrier == 'Flash Express':
        return f"TH{digits(10)}"
    if carrier == 'Kerry Express':
        return f"KEX{digits(10)}"
    if carrier == 'J&T Express':
        return f"8{digits(11)}"
    if carrier == 'Shopee Express':
        return f"SPXTH{digits(12)}"
    return f"LEXPU{digits(10)}"


def make_student_id(rng):
    return f"6{rng.randint(4, 8)}{rng.randint(10**9, 10**10 - 1)}"


def random_created_at(rng, now, days_ago):
    day = (now - timedelta(days=days_ago)).date()
    created = datetime(day.year, day.month, day.day,
                       rng.choices(WORK_HOURS, weights=HOUR_WEIGHTS, k=1)[0],
                       rng.randint(0, 59), rng.randint(0, 59))
    while created > now:  # วันนี้ยังไม่ถึงเวลานั้น ให้ย้อนไปวันก่อนหน้า
        created -= timedelta(days=1)
    return created


def iso(dt):
    return dt.isoformat(timespec='seconds') if dt else None


def connect():
    if not os.path.exists(DB_PATH):
        sys.exit('❌ ไม่พบไฟล์ rmu_parcel.db กรุณารัน app.py หนึ่งครั้งก่อน แล้วค่อยรันสคริปต์นี้')
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def clear_demo(conn):
    cur = conn.execute('DELETE FROM packages WHERE id LIKE ?', (DEMO_PREFIX + '%',))
    conn.commit()
    print(f'🧹 ลบข้อมูลตัวอย่าง {cur.rowcount} รายการแล้ว (ข้อมูลจริงยังอยู่ครบ)')


def seed(conn, count):
    rng = random.Random()
    now = datetime.now()
    admin = conn.execute("SELECT id FROM staff_users WHERE username = 'admin'").fetchone()
    staff_id = admin['id'] if admin else None

    # แบ่งกลุ่มให้แดชบอร์ดมีตัวเลขครบทุกการ์ด
    n_overdue = max(2, count // 12)   # ค้างเกิน 7 วัน
    n_today = max(3, count // 10)     # รับเข้าวันนี้
    plan = (['overdue'] * n_overdue) + (['today'] * n_today) + (['normal'] * max(0, count - n_overdue - n_today))

    stats = {RECEIVED: 0, IN_TRANSIT: 0, DELIVERED: 0}
    example = None

    for group in plan:
        carrier = pick(rng, CARRIERS)
        location = pick(rng, LOCATIONS)

        if group == 'overdue':
            created = random_created_at(rng, now, rng.randint(8, 14))
        elif group == 'today':
            created = random_created_at(rng, now, 0)
        else:
            created = random_created_at(rng, now, rng.randint(1, 29))

        age_days = (now - created).days

        # กำหนดสถานะตามอายุของพัสดุ
        if group == 'overdue':
            status = rng.choice([RECEIVED, IN_TRANSIT])
        elif age_days >= 7:
            status = DELIVERED  # พัสดุเก่าที่ไม่ได้ตั้งใจให้ค้าง ถือว่ารับไปแล้ว
        elif age_days >= 2:
            status = DELIVERED if rng.random() < 0.8 else IN_TRANSIT
        else:
            status = rng.choices([RECEIVED, IN_TRANSIT, DELIVERED], weights=[4, 3, 3], k=1)[0]

        dispatched = delivered = None
        if status in (IN_TRANSIT, DELIVERED):
            dispatched = created + timedelta(minutes=rng.randint(30, 300))
            if dispatched > now:
                dispatched, status = None, RECEIVED
        if status == DELIVERED:
            delivered = dispatched + timedelta(minutes=rng.randint(60, 72 * 60))
            if delivered > now:
                delivered, status = None, IN_TRANSIT

        pkg_id = f"{DEMO_PREFIX}{rng.getrandbits(48):012x}"
        tracking = make_tracking(rng, carrier)
        student_id = make_student_id(rng)
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        pin = f"{rng.randint(0, 9999):04d}"

        conn.execute(
            """INSERT INTO packages
               (id, tracking_number, carrier, student_id, recipient_name, storage_location,
                shipping_status, pickup_pin, created_at, dispatched_at, delivered_at,
                registered_by, dispatched_by, delivered_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pkg_id, tracking, carrier, student_id, name, location, status, pin,
             iso(created), iso(dispatched), iso(delivered),
             staff_id, staff_id if dispatched else None, staff_id if delivered else None)
        )

        logs = [(RECEIVED, created, 'ลงทะเบียนรับเข้า (ข้อมูลตัวอย่าง)')]
        if dispatched:
            logs.append((IN_TRANSIT, dispatched, f'จัดส่งไปยัง {location} (ข้อมูลตัวอย่าง)'))
        if delivered:
            logs.append((DELIVERED, delivered, 'ยืนยัน PIN ส่งมอบให้ผู้รับ (ข้อมูลตัวอย่าง)'))
        for log_status, at, note in logs:
            conn.execute(
                'INSERT INTO package_status_logs (package_id, status, changed_at, changed_by, note) '
                'VALUES (?, ?, ?, ?, ?)',
                (pkg_id, log_status, iso(at), staff_id, note)
            )

        stats[status] += 1
        if example is None and status != DELIVERED and group != 'overdue':
            example = (tracking, student_id, pin)

    conn.commit()

    print(f'✅ เพิ่มพัสดุตัวอย่าง {len(plan)} ชิ้นแล้ว')
    print(f'   รับเข้าแล้ว {stats[RECEIVED]} | กำลังจัดส่ง {stats[IN_TRANSIT]} | ส่งสำเร็จ {stats[DELIVERED]}')
    print(f'   ในจำนวนนี้ค้างเกิน 7 วัน {n_overdue} ชิ้น และรับเข้าวันนี้ประมาณ {n_today} ชิ้น')
    if example:
        print('\n🔎 ตัวอย่างสำหรับสาธิตการค้นหาและแสดง PIN:')
        print(f'   เลข Tracking : {example[0]}')
        print(f'   รหัสนักศึกษา : {example[1]}')
        print(f'   PIN          : {example[2]}')
    print('\nรีเฟรชหน้าเว็บ (Ctrl+F5) เพื่อดูข้อมูลใหม่')


def main():
    parser = argparse.ArgumentParser(description='สร้างหรือลบข้อมูลพัสดุตัวอย่าง')
    parser.add_argument('--count', type=int, default=40, help='จำนวนพัสดุตัวอย่างที่จะเพิ่ม (ค่าเริ่มต้น 40)')
    parser.add_argument('--clear', action='store_true', help='ลบข้อมูลตัวอย่างทั้งหมด')
    args = parser.parse_args()

    conn = connect()
    try:
        if args.clear:
            clear_demo(conn)
        else:
            seed(conn, max(1, args.count))
    finally:
        conn.close()


if __name__ == '__main__':
    main()
