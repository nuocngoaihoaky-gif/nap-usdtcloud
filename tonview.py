import os
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, firestore, db

# ==========================================
# CẤU HÌNH LẤY TỪ BIẾN MÔI TRƯỜNG
# ==========================================
ADMIN_WALLET = os.environ.get("ADMIN_WALLET")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
# Lưu ý: ADMIN_TELEGRAM_ID phải có định dạng -100... (VD: -1003442716824)
ADMIN_TELEGRAM_ID = os.environ.get("ADMIN_TELEGRAM_ID")
ADMIN_TOPIC_ID = os.environ.get("ADMIN_TOPIC_ID") # Điền số 4 vào Github Secrets
DATABASE_URL = os.environ.get("DATABASE_URL")
FIREBASE_SERVICE_ACCOUNT = os.environ.get("FIREBASE_SERVICE_ACCOUNT")

PRICE_SPREAD = 0.03

if not all([ADMIN_WALLET, BOT_TOKEN, ADMIN_TELEGRAM_ID, DATABASE_URL, FIREBASE_SERVICE_ACCOUNT]):
    print("❌ LỖI: Thiếu biến môi trường. Hãy kiểm tra lại Github Secrets!")
    exit()

try:
    cred_dict = json.loads(FIREBASE_SERVICE_ACCOUNT)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
    db_fs = firestore.client()
    db_rt = db
    print("✅ Kết nối Firebase thành công!")
except Exception as e:
    print(f"❌ Lỗi kết nối Firebase: {e}")
    exit()

# Cập nhật hàm gửi tin nhắn hỗ trợ Topic (message_thread_id)
def send_telegram_msg(chat_id, text, thread_id=None):
    try:
        payload = {
            "chat_id": chat_id, 
            "text": text, 
            "parse_mode": "HTML"
        }
        if thread_id:
            payload["message_thread_id"] = thread_id
            
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", 
                      json=payload, timeout=5)
    except Exception as e:
        print(f"Lỗi gửi Tele: {e}")

# Hàm làm tròn chuẩn 10 số thập phân
def round10(num):
    return float(f"{num:.10f}")

# ==========================================
# CHƯƠNG TRÌNH CHÍNH (WORKER)
# ==========================================
def main():
    print("⏳ Đang khởi động Bot Quét TON...")
    
    last_processed_time = 0
    try:
        init_res = requests.get(f"https://tonapi.io/v2/accounts/{ADMIN_WALLET}/events?limit=1", timeout=10).json()
        if 'events' in init_res and len(init_res['events']) > 0:
            last_processed_time = init_res['events'][0]['timestamp']
            print(f"🎯 Mốc bắt đầu quét: {last_processed_time}")
        else:
            last_processed_time = int(time.time())
    except Exception as e:
        print(f"⚠️ Lỗi lấy mốc thời gian: {e}. Dùng thời gian máy.")
        last_processed_time = int(time.time())

    print("🚀 BOT BẮT ĐẦU HOẠT ĐỘNG (Quét 5s/lần)...\n")
    start_time = time.time()
    MAX_RUN_TIME = 5.5 * 3600 

    # 🔥 KHỞI TẠO HÀNG ĐỢI RAM LƯU ĐƠN TREO
    pending_orders_ram = []

    while True:
        if time.time() - start_time > MAX_RUN_TIME:
            print("⏳ Tự động tắt chờ Cronjob lượt sau gọi dậy!")
            break

        try:
            time.sleep(5)
            
            # 1. QUÉT API LẤY GIAO DỊCH
            res = requests.get(f"https://tonapi.io/v2/accounts/{ADMIN_WALLET}/events?limit=20", timeout=10)
            if res.status_code != 200: continue
            
            events = res.json().get('events', [])
            new_events = [e for e in events if e['timestamp'] > last_processed_time]
            new_events.reverse() 

            # Nếu không có đơn treo và không có đơn mới thì bỏ qua luôn
            if not new_events and not pending_orders_ram: 
                continue

            # 2. LẤY GIÁ TON (CÓ API DỰ PHÒNG CHỐNG BAN IP)
            ton_price_usd = 0
            is_price_alive = False

            try:
                binance_res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT", timeout=5)
                if binance_res.status_code == 200:
                    ton_price_usd = float(binance_res.json()['price'])
                    is_price_alive = True
            except Exception:
                pass

            # NẾU BINANCE LỖI -> CHUYỂN SANG DÙNG SÀN KUCOIN CHỮA CHÁY
            if not is_price_alive:
                try:
                    kucoin_res = requests.get("https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=TON-USDT", timeout=5)
                    if kucoin_res.status_code == 200:
                        ton_price_usd = float(kucoin_res.json()['data']['price'])
                        is_price_alive = True
                except Exception as e:
                    print(f"⚠️ Lỗi kết nối Kucoin: {e}")

            ton_deposit_rate_usd = ton_price_usd * (1 - PRICE_SPREAD)

            # 3. ƯU TIÊN XỬ LÝ ĐƠN TREO TỪ RAM (NẾU API GIÁ ĐÃ SỐNG LẠI)
            if is_price_alive and pending_orders_ram:
                print(f"🛠️ Đang quét lại {len(pending_orders_ram)} đơn treo từ RAM...")
                for pending in pending_orders_ram[:]: 
                    uid, ton_received, tx_hash = pending['uid'], pending['ton_received'], pending['tx_hash']
                    
                    wallet_ref = db_rt.reference(f"user_wallets/{uid}")
                    wallet_snap = wallet_ref.get()
                    if not wallet_snap: continue

                    # Tính toán USDT (Làm tròn 6 số để an toàn, tránh trôi số thập phân)
                    raw_usd_value = ton_received * ton_deposit_rate_usd
                    safe_usd_value = round(raw_usd_value, 6)

                    # Lấy số dư hiện tại
                    current_balance = round10(float(wallet_snap.get('balance', 0)))
                    current_locked = round10(float(wallet_snap.get('lockedBalance', 0)))
                    current_deposited = float(wallet_snap.get('totalDepositedUSD', 0))

                    batch = db_fs.batch()
                    tx_ref = db_fs.collection('transactions').document(tx_hash)
                    user_ref = db_fs.collection('users').document(uid)

                    # Ghi đè trạng thái pending thành success
                    batch.set(tx_ref, {
                        'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 'amountUSD': safe_usd_value,
                        'txHash': tx_hash, 'status': 'success', 'createdAt': int(time.time() * 1000)
                    })

                    user_doc = user_ref.get()
                    current_history = user_doc.to_dict().get('transactions', []) if user_doc.exists else []
                    current_history.insert(0, {'type': 'deposit', 'tonAmount': f"{ton_received:.4f}", 'status': 'success', 'time': int(time.time() * 1000)})
                    batch.set(user_ref, {'transactions': current_history[:50], 'hasDeposited3USD': (current_deposited + safe_usd_value) >= 3}, merge=True)
                    batch.commit()

                    # CỘNG VÀO TỔNG SỐ DƯ & SỐ DƯ KHÓA
                    wallet_ref.update({
                        'balance': round10(current_balance + safe_usd_value), 
                        'lockedBalance': round10(current_locked + safe_usd_value),
                        'totalDepositedUSD': current_deposited + safe_usd_value
                    })
                    
                    print(f"✅ [GỠ TREO RAM] +{safe_usd_value} USDT cho ID {uid}")
                    send_telegram_msg(uid, f"🎉 <b>Deposit Processed!</b>\n\nYour delayed deposit of <b>{ton_received:.4f} TON</b> has been processed.\n<b>+{safe_usd_value} USDT</b> has been added!")
                    
                    pending_orders_ram.remove(pending)

            # 4. XỬ LÝ CÁC GIAO DỊCH MỚI
            for event in new_events:
                if event['timestamp'] > last_processed_time:
                    last_processed_time = event['timestamp']

                actions = event.get('actions', [])
                ton_transfer = next((a for a in actions if a.get('type') == 'TonTransfer' and a.get('status') == 'ok'), None)
                if not ton_transfer: continue

                ton_data = ton_transfer.get('TonTransfer', {})
                receiver = ton_data.get('recipient', {}).get('address', '')
                memo = ton_data.get('comment', '')

                if receiver.lower() == ADMIN_WALLET.lower() and memo:
                    ton_received = int(ton_data.get('amount', 0)) / 1e9
                    uid = str(memo).strip()
                    tx_hash = event['event_id']

                    # Đảm bảo memo gửi lên là 1 dãy số (ID Telegram)
                    if not uid.isdigit():
                        print(f"⚠️ Bỏ qua giao dịch {tx_hash} vì memo không phải ID hợp lệ: {uid}")
                        continue

                    tx_ref = db_fs.collection('transactions').document(tx_hash)
                    if tx_ref.get().exists: continue

                    user_ref = db_fs.collection('users').document(uid)
                    wallet_ref = db_rt.reference(f"user_wallets/{uid}")
                    wallet_snap = wallet_ref.get()

                    if not wallet_snap: continue 

                    if is_price_alive:
                        # KỊCH BẢN 1: API Giá sống -> Xử lý mượt mà
                        raw_usd_value = ton_received * ton_deposit_rate_usd
                        safe_usd_value = round(raw_usd_value, 6)

                        current_balance = round10(float(wallet_snap.get('balance', 0)))
                        current_locked = round10(float(wallet_snap.get('lockedBalance', 0)))
                        current_deposited = float(wallet_snap.get('totalDepositedUSD', 0))

                        batch = db_fs.batch()
                        batch.set(tx_ref, {'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 'amountUSD': safe_usd_value, 'txHash': tx_hash, 'status': 'success', 'createdAt': int(time.time() * 1000)})
                        
                        user_doc = user_ref.get()
                        current_history = user_doc.to_dict().get('transactions', []) if user_doc.exists else []
                        current_history.insert(0, {'type': 'deposit', 'tonAmount': f"{ton_received:.4f}", 'status': 'success', 'time': int(time.time() * 1000)})
                        batch.set(user_ref, {'transactions': current_history[:50], 'hasDeposited3USD': (current_deposited + safe_usd_value) >= 3}, merge=True)
                        batch.commit()

                        # CỘNG VÀO TỔNG SỐ DƯ & SỐ DƯ KHÓA
                        wallet_ref.update({
                            'balance': round10(current_balance + safe_usd_value), 
                            'lockedBalance': round10(current_locked + safe_usd_value),
                            'totalDepositedUSD': current_deposited + safe_usd_value
                        })
                        print(f"✅ [NẠP AUTO] +{safe_usd_value} USDT cho ID {uid}")

                        # Gửi tin nhắn cho người dùng
                        send_telegram_msg(uid, f"🎉 <b>Deposit Successful!</b>\n\nYou have successfully deposited <b>{ton_received:.4f} TON</b>.\n<b>+{safe_usd_value} USDT</b> added.")
                        
                        # Gửi tin nhắn báo cáo vào Group Chat (Topic số 4)
                        admin_msg = f"🔔 <b>NẠP AUTO!</b>\n👤 <b>ID:</b> <code>{uid}</code>\n💎 <b>USDT:</b> +{safe_usd_value}\n💰 <b>TON:</b> {ton_received:.4f} TON"
                        send_telegram_msg(ADMIN_TELEGRAM_ID, admin_msg, thread_id=ADMIN_TOPIC_ID)

                    else:
                        # KỊCH BẢN 2: API Giá Lỗi -> Ném vào Hàng đợi RAM
                        print(f"⚠️ [TREO RAM] Tạm lưu đơn {ton_received} TON của ID {uid} vào RAM đợi giá phục hồi.")
                        
                        batch = db_fs.batch()
                        batch.set(tx_ref, {'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 'amountUSD': 0, 'txHash': tx_hash, 'status': 'pending_manual', 'createdAt': int(time.time() * 1000)})
                        user_doc = user_ref.get()
                        current_history = user_doc.to_dict().get('transactions', []) if user_doc.exists else []
                        current_history.insert(0, {'type': 'deposit', 'tonAmount': f"{ton_received:.4f}", 'status': 'pending', 'time': int(time.time() * 1000)})
                        batch.set(user_ref, {'transactions': current_history[:50]}, merge=True)
                        batch.commit()

                        pending_orders_ram.append({'uid': uid, 'ton_received': ton_received, 'tx_hash': tx_hash})
                        
                        # Cảnh báo lỗi vào Group Chat
                        err_msg = f"⚠️ Đang kẹt 1 đơn <b>{ton_received:.4f} TON</b> của <code>{uid}</code> trong RAM do lỗi mạng Binance."
                        send_telegram_msg(ADMIN_TELEGRAM_ID, err_msg, thread_id=ADMIN_TOPIC_ID)

        except requests.exceptions.RequestException:
            pass 
        except Exception as e:
            print(f"❌ Lỗi vòng lặp: {e}")

if __name__ == "__main__":
    main()
