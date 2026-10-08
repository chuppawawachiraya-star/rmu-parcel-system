"""
จัดการบัญชีเจ้าหน้าที่ของระบบ RMU Parcel Tracking

วิธีใช้ (รันใน terminal ที่โฟลเดอร์ queue-system):
    py -3.14 manage_staff.py list                          ดูรายชื่อบัญชีทั้งหมด
    py -3.14 manage_staff.py add somchai "สมชาย ใจดี"      เพิ่มบัญชีใหม่ (ระบบจะถามรหัสผ่าน)
    py -3.14 manage_staff.py passwd admin                  เปลี่ยนรหัสผ่าน

หมายเหตุ
- ต้องรัน app.py อย่างน้อยหนึ่งครั้งก่อน เพื่อให้มีไฟล์ rmu_parcel.db
- ตอนพิมพ์รหัสผ่านจะไม่มีตัวอักษรขึ้นบนหน้าจอ เป็นเรื่องปกติ พิมพ์แล้วกด Enter ได้เลย
- เปลี่ยนรหัสผ่านแล้ว คนที่ login ค้างไว้ยังใช้ต่อได้จนกว่าจะออกจากระบบหรือรีสตาร์ท server
"""
import os
import sys
import sqlite3
import argparse
from datetime import datetime
from getpass import getpass

from werkzeug.security import generate_password_hash

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'rmu_parcel.db')
MIN_PASSWORD_LENGTH = 6


def connect():
    if not os.path.exists(DB_PATH):
        sys.exit('❌ ไม่พบไฟล์ rmu_parcel.db กรุณารัน app.py หนึ่งครั้งก่อน')
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ask_password():
    while True:
        pw = getpass(f'ตั้งรหัสผ่าน (อย่างน้อย {MIN_PASSWORD_LENGTH} ตัวอักษร): ')
        if len(pw) < MIN_PASSWORD_LENGTH:
            print(f'⚠️  รหัสผ่านสั้นเกินไป ต้องมีอย่างน้อย {MIN_PASSWORD_LENGTH} ตัวอักษร')
            continue
        if pw == '1234':
            print('⚠️  ห้ามใช้รหัสผ่านเริ่มต้น')
            continue
        if getpass('พิมพ์รหัสผ่านอีกครั้ง: ') != pw:
            print('⚠️  รหัสผ่านสองครั้งไม่ตรงกัน ลองใหม่')
            continue
        return pw


def cmd_list(conn, _args):
    rows = conn.execute('SELECT username, full_name, created_at FROM staff_users ORDER BY id').fetchall()
    print(f'บัญชีเจ้าหน้าที่ทั้งหมด {len(rows)} บัญชี')
    for r in rows:
        print(f"  • {r['username']:<15} {r['full_name']}  (สร้างเมื่อ {r['created_at']})")


def cmd_add(conn, args):
    username = args.username.strip()
    if not username or ' ' in username:
        sys.exit('❌ ชื่อผู้ใช้ห้ามว่างและห้ามมีช่องว่าง')
    if conn.execute('SELECT 1 FROM staff_users WHERE username = ?', (username,)).fetchone():
        sys.exit(f'❌ มีชื่อผู้ใช้ "{username}" อยู่แล้ว')
    pw = ask_password()
    conn.execute(
        'INSERT INTO staff_users (username, full_name, password_hash, created_at) VALUES (?, ?, ?, ?)',
        (username, args.full_name.strip(), generate_password_hash(pw), datetime.now().isoformat(timespec='seconds'))
    )
    conn.commit()
    print(f'✅ เพิ่มบัญชี "{username}" ({args.full_name}) เรียบร้อย')


def cmd_passwd(conn, args):
    if not conn.execute('SELECT 1 FROM staff_users WHERE username = ?', (args.username,)).fetchone():
        sys.exit(f'❌ ไม่พบชื่อผู้ใช้ "{args.username}"')
    pw = ask_password()
    conn.execute('UPDATE staff_users SET password_hash = ? WHERE username = ?',
                 (generate_password_hash(pw), args.username))
    conn.commit()
    print(f'✅ เปลี่ยนรหัสผ่านของ "{args.username}" เรียบร้อย')


def main():
    parser = argparse.ArgumentParser(description='จัดการบัญชีเจ้าหน้าที่')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('list', help='ดูรายชื่อบัญชีทั้งหมด')
    p_add = sub.add_parser('add', help='เพิ่มบัญชีเจ้าหน้าที่')
    p_add.add_argument('username')
    p_add.add_argument('full_name', help='ชื่อที่แสดงในระบบ ถ้ามีช่องว่างให้ใส่ในเครื่องหมายคำพูด')
    p_pw = sub.add_parser('passwd', help='เปลี่ยนรหัสผ่าน')
    p_pw.add_argument('username')
    args = parser.parse_args()

    conn = connect()
    try:
        {'list': cmd_list, 'add': cmd_add, 'passwd': cmd_passwd}[args.command](conn, args)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
