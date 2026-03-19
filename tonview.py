import os
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, firestore, db

# ==========================================
# CẤU HÌNH CỨNG
# ==========================================
ADMIN_WALLET = "0:54efc445116ebc8fd644b5a2e88728ebff91aebf14d2245de1ec76190c60997e" 
ADMIN_TELEGRAM_ID = "-1003442716824"
ADMIN_TOPIC_ID = 4
PRICE_SPREAD = 0.03

# ==========================================
# CẤU HÌNH NHẠY CẢM LẤY TỪ BIẾN MÔI TRƯỜNG
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
FIREBASE_SERVICE_ACCOUNT = os.environ.get("FIREBASE_SERVICE_ACCOUNT")

if not all([BOT_TOKEN, DATABASE_URL, FIREBASE_SERVICE_ACCOUNT]):
    print("❌ LỖI: Thiếu biến môi trường nhạy cảm.")
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

def send_telegram_msg(chat_id, text, thread_id=None):
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if thread_id:
            payload["message_thread_id"] = thread_id
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload, timeout=5)
    except Exception as e:
        print(f"Lỗi gửi Tele: {e}")

def round10(num):
    return float(f"{num:.10f}")

# ==========================================
# CHƯƠNG TRÌNH CHÍNH (WORKER)
# ==========================================
def main():
    print("⏳ Đang khởi động Bot Quét TON...")
    
    last_processed_time = 0
    # LỚP KHIÊN 1: LƯU TRỮ ID GIAO DỊCH VÀO RAM
    processed_txs = set() 
    
    try:
        init_res = requests.get(f"https://tonapi.io/v2/accounts/{ADMIN_WALLET}/events?limit=1", timeout=10).json()
        if 'events' in init_res and len(init_res['events']) > 0:
            last_processed_time = init_res['events'][0]['timestamp']
            # Đưa luôn ID đơn gần nhất vào RAM để không bị lặp lúc vừa bật Bot
            processed_txs.add(init_res['events'][0]['event_id'])
            print(f"🎯 Mốc bắt đầu quét: {last_processed_time}")
        else:
            last_processed_time = int(time.time())
    except Exception as e:
        print(f"⚠️ Lỗi lấy mốc thời gian: {e}. Dùng thời gian máy.")
        last_processed_time = int(time.time())

    print("🚀 BOT BẮT ĐẦU HOẠT ĐỘNG (Quét 5s/lần)...\n")
    start_time = time.time()
    MAX_RUN_TIME = 5.5 * 3600 

    pending_orders_ram = []

    while True:
        if time.time() - start_time > MAX_RUN_TIME:
            print("⏳ Tự động tắt chờ Cronjob lượt sau gọi dậy!")
            break

        try:
            time.sleep(5)
            
            # Reset RAM Cache nếu đầy (tránh tràn RAM server)
            if len(processed_txs) > 1000:
                processed_txs.clear()

            # 1. QUÉT API LẤY GIAO DỊCH
            res = requests.get(f"https://tonapi.io/v2/accounts/{ADMIN_WALLET}/events?limit=20", timeout=10)
            if res.status_code != 200: continue
            
            events = res.json().get('events', [])
            new_events = [e for e in events if e['timestamp'] >= last_processed_time]
            new_events.reverse() 

            if not new_events and not pending_orders_ram: 
                continue

            # 2. LẤY GIÁ TON 
            ton_price_usd = 0
            is_price_alive = False

            try:
                binance_res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT", timeout=5)
                if binance_res.status_code == 200:
                    ton_price_usd = float(binance_res.json()['price'])
                    is_price_alive = True
            except Exception:
                pass

            if not is_price_alive:
                try:
                    kucoin_res = requests.get("https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=TON-USDT", timeout=5)
                    if kucoin_res.status_code == 200:
                        ton_price_usd = float(kucoin_res.json()['data']['price'])
                        is_price_alive = True
                except Exception:
                    pass

            ton_deposit_rate_usd = ton_price_usd * (1 - PRICE_SPREAD)

            # 3. ƯU TIÊN XỬ LÝ ĐƠN TREO TỪ RAM 
            if is_price_alive and pending_orders_ram:
                for pending in pending_orders_ram[:]: 
                    uid, ton_received, tx_hash = pending['uid'], pending['ton_received'], pending['tx_hash']
                    
                    tx_ref = db_fs.collection('transactions').document(tx_hash)
                    tx_doc = tx_ref.get()
                    if not tx_doc.exists or tx_doc.to_dict().get('status') == 'success':
                        pending_orders_ram.remove(pending)
                        continue
                    
                    wallet_ref = db_rt.reference(f"user_wallets/{uid}")
                    wallet_snap = wallet_ref.get()
                    if not wallet_snap: continue

                    safe_usd_value = round(ton_received * ton_deposit_rate_usd, 6)
                    current_balance = round10(float(wallet_snap.get('balance', 0)))
                    current_locked = round10(float(wallet_snap.get('lockedBalance', 0)))
                    current_deposited = float(wallet_snap.get('totalDepositedUSD', 0))

                    batch = db_fs.batch()
                    user_ref = db_fs.collection('users').document(uid)

                    batch.update(tx_ref, {'amountUSD': safe_usd_value, 'status': 'success'})

                    user_doc = user_ref.get()
                    current_history = user_doc.to_dict().get('transactions', []) if user_doc.exists else []
                    current_history.insert(0, {'type': 'deposit', 'tonAmount': f"{ton_received:.4f}", 'status': 'success', 'time': int(time.time() * 1000)})
                    batch.set(user_ref, {'transactions': current_history[:50], 'hasDeposited3USD': (current_deposited + safe_usd_value) >= 3}, merge=True)
                    
                    try:
                        batch.commit()
                        wallet_ref.update({
                            'balance': round10(current_balance + safe_usd_value), 
                            'lockedBalance': round10(current_locked + safe_usd_value),
                            'totalDepositedUSD': current_deposited + safe_usd_value
                        })
                        print(f"✅ [GỠ TREO RAM] +{safe_usd_value} USDT cho ID {uid}")
                        send_telegram_msg(uid, f"🎉 <b>Deposit Processed!</b>\n\nYour delayed deposit of <b>{ton_received:.4f} TON</b> has been processed.\n<b>+{safe_usd_value} USDT</b> has been added!")
                        pending_orders_ram.remove(pending)
                    except Exception:
                        pass

            # 4. XỬ LÝ CÁC GIAO DỊCH MỚI
            for event in new_events:
                if event['timestamp'] > last_processed_time:
                    last_processed_time = event['timestamp']

                tx_hash = event['event_id'] # Lấy ID của giao dịch từ chuỗi khối TON
                
                # 🛡️ KIỂM TRA LỚP KHIÊN 1: ĐÃ XỬ LÝ TRONG RAM CHƯA?
                if tx_hash in processed_txs:
                    continue
                
                actions = event.get('actions', [])
                ton_transfer = next((a for a in actions if a.get('type') == 'TonTransfer' and a.get('status') == 'ok'), None)
                if not ton_transfer: continue

                ton_data = ton_transfer.get('TonTransfer', {})
                receiver = ton_data.get('recipient', {}).get('address', '')
                memo = ton_data.get('comment', '')

                if receiver.lower() == ADMIN_WALLET.lower() and memo:
                    ton_received = int(ton_data.get('amount', 0)) / 1e9
                    uid = str(memo).strip()

                    if not uid.isdigit():
                        processed_txs.add(tx_hash) # Nhét vào RAM luôn cho rảnh nợ
                        continue

                    # 🛡️ KIỂM TRA LỚP KHIÊN 2: LỊCH SỬ ĐÃ CÓ TRONG FIRESTORE CHƯA?
                    tx_ref = db_fs.collection('transactions').document(tx_hash)
                    if tx_ref.get().exists: 
                        processed_txs.add(tx_hash)
                        continue

                    user_ref = db_fs.collection('users').document(uid)
                    wallet_ref = db_rt.reference(f"user_wallets/{uid}")
                    wallet_snap = wallet_ref.get()

                    if not wallet_snap: 
                        processed_txs.add(tx_hash)
                        continue 

                    if is_price_alive:
                        safe_usd_value = round(ton_received * ton_deposit_rate_usd, 6)

                        current_balance = round10(float(wallet_snap.get('balance', 0)))
                        current_locked = round10(float(wallet_snap.get('lockedBalance', 0)))
                        current_deposited = float(wallet_snap.get('totalDepositedUSD', 0))

                        batch = db_fs.batch()
                        # Dùng create() để khóa chặt 100%, chống đè dữ liệu DB
                        batch.create(tx_ref, {'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 'amountUSD': safe_usd_value, 'txHash': tx_hash, 'status': 'success', 'createdAt': int(time.time() * 1000)})
                        
                        user_doc = user_ref.get()
                        current_history = user_doc.to_dict().get('transactions', []) if user_doc.exists else []
                        current_history.insert(0, {'type': 'deposit', 'tonAmount': f"{ton_received:.4f}", 'status': 'success', 'time': int(time.time() * 1000)})
                        batch.set(user_ref, {'transactions': current_history[:50], 'hasDeposited3USD': (current_deposited + safe_usd_value) >= 3}, merge=True)
                        
                        try:
                            batch.commit()
                            
                            # CỘNG TIỀN VÀ NHẮN TIN - CHỈ CHẠY KHI GHI BATCH THÀNH CÔNG!
                            wallet_ref.update({
                                'balance': round10(current_balance + safe_usd_value), 
                                'lockedBalance': round10(current_locked + safe_usd_value),
                                'totalDepositedUSD': current_deposited + safe_usd_value
                            })
                            print(f"✅ [NẠP AUTO] +{safe_usd_value} USDT cho ID {uid}")
                            
                            # ĐÁNH DẤU GIAO DỊCH ĐÃ XỬ LÝ VÀO RAM NGAY LẬP TỨC
                            processed_txs.add(tx_hash)

                            send_telegram_msg(uid, f"🎉 <b>Deposit Successful!</b>\n\nYou have successfully deposited <b>{ton_received:.4f} TON</b>.\n<b>+{safe_usd_value} USDT</b> added.")
                            admin_msg = f"🔔 <b>NẠP AUTO!</b>\n👤 <b>ID:</b> <code>{uid}</code>\n💎 <b>USDT:</b> +{safe_usd_value}\n💰 <b>TON:</b> {ton_received:.4f} TON"
                            send_telegram_msg(ADMIN_TELEGRAM_ID, admin_msg, thread_id=ADMIN_TOPIC_ID)
                        except Exception as e:
                            print(f"⚠️ [TRÙNG LẶP] Đơn {tx_hash} đã được xử lý (Bắt tại Batch).")
                            processed_txs.add(tx_hash)

                    else:
                        batch = db_fs.batch()
                        batch.create(tx_ref, {'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 'amountUSD': 0, 'txHash': tx_hash, 'status': 'pending_manual', 'createdAt': int(time.time() * 1000)})
                        user_doc = user_ref.get()
                        current_history = user_doc.to_dict().get('transactions', []) if user_doc.exists else []
                        current_history.insert(0, {'type': 'deposit', 'tonAmount': f"{ton_received:.4f}", 'status': 'pending', 'time': int(time.time() * 1000)})
                        batch.set(user_ref, {'transactions': current_history[:50]}, merge=True)
                        
                        try:
                            batch.commit()
                            pending_orders_ram.append({'uid': uid, 'ton_received': ton_received, 'tx_hash': tx_hash})
                            processed_txs.add(tx_hash) # Đã nhét RAM chờ xử lý thì không quét lại nữa
                            
                            err_msg = f"⚠️ Đang kẹt 1 đơn <b>{ton_received:.4f} TON</b> của <code>{uid}</code> trong RAM do lỗi mạng Binance."
                            send_telegram_msg(ADMIN_TELEGRAM_ID, err_msg, thread_id=ADMIN_TOPIC_ID)
                        except Exception:
                            processed_txs.add(tx_hash)

        except requests.exceptions.RequestException:
            pass 
        except Exception as e:
            print(f"❌ Lỗi vòng lặp: {e}")

if __name__ == "__main__":
    main()
