import admin from 'firebase-admin';
import { TonClient, WalletContractV4, internal, external, beginCell, storeMessage } from '@ton/ton';
import { mnemonicToPrivateKey } from 'ton-crypto';

// ==========================================
// CẤU HÌNH BIẾN MÔI TRƯỜNG & HẰNG SỐ
// ==========================================
const ADMIN_CHAT_ID = '-1003848712775'; 
const ADMIN_TOPIC_ID = 137; 
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TONCENTER_API_KEY = process.env.TONCENTER_API_KEY || '';

// Khởi tạo Firebase Admin SDK (Đọc từ biến môi trường của GitHub)
if (!admin.apps.length) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: process.env.FIREBASE_DATABASE_URL // VD: https://app-cua-ban-default-rtdb.firebaseio.com
    });
}

const db = admin.firestore();
const rtdb = admin.database();

async function sendTelegramMsg(chatId, text, threadId = null) {
    if (!BOT_TOKEN) return;
    try {
        const payload = { chat_id: chatId, text: text, parse_mode: 'HTML', disable_web_page_preview: true };
        if (threadId) payload.message_thread_id = threadId;

        await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
    } catch (error) {
        console.error(`Lỗi gửi tin nhắn Telegram cho ${chatId}:`, error);
    }
}

async function main() {
    console.log("🚀 BẮT ĐẦU WORKER XẢ TIỀN TỰ ĐỘNG...");
    const queueRef = rtdb.ref('withdraw_queue');
    const lockRef = rtdb.ref('system_locks/is_git_running');

    try {
        // 1. ĐỌC DB ĐÚNG 1 LẦN DUY NHẤT VÀ NẠP VÀO RAM
        const queueSnap = await queueRef.orderByChild('status').equalTo('pending').once('value');
        const queueData = queueSnap.val() || {};
        let ramOrders = Object.keys(queueData);
        
        console.log(`📦 Đã nạp vào RAM ${ramOrders.length} đơn đang chờ.`);

        let tonClient, wallet, keyPair;
        let isBlockchainInit = false;

        // 2. VÒNG LẶP VÉT CẠN (XẢ 4 ĐƠN/MẺ)
        while (ramOrders.length >= 4) {
            const batchToProcess = ramOrders.splice(0, 4);
            console.log(`⚡ Đang xả mẻ 4 đơn: ${batchToProcess.join(', ')}`);

            // Chỉ khởi tạo kết nối Blockchain nếu có đơn để xả (Tiết kiệm tài nguyên)
            if (!isBlockchainInit) {
                tonClient = new TonClient({ endpoint: 'https://toncenter.com/api/v2/jsonRPC', apiKey: TONCENTER_API_KEY });
                const mnemonic = process.env.ADMIN_SEED_PHRASE.split(' ');
                keyPair = await mnemonicToPrivateKey(mnemonic);
                wallet = WalletContractV4.create({ workchain: 0, publicKey: keyPair.publicKey });
                isBlockchainInit = true;
            }

            const contract = tonClient.open(wallet);
            const seqno = await contract.getSeqno();

            const messages = [];
            let totalTonSent = 0;

            // Gom data của 4 đơn
            for (const txId of batchToProcess) {
                const order = queueData[txId];
                totalTonSent += order.tonAmount;
                const nanoTonAmount = BigInt(Math.round(order.tonAmount * 1e9));

                messages.push(internal({
                    to: order.walletAddress,
                    value: nanoTonAmount, 
                    bounce: false, 
                    body: String(order.uid)
                }));
            }

            // TÍNH TOÁN MÃ TXHASH CHUẨN XÁC
            const transferBody = wallet.createTransfer({ seqno, secretKey: keyPair.secretKey, messages });
            const extMsg = external({ to: wallet.address, init: seqno === 0 ? wallet.init : undefined, body: transferBody });
            const extMsgCell = beginCell().store(storeMessage(extMsg)).endCell();
            const txHash = extMsgCell.hash().toString('hex');
            const txLink = `https://tonviewer.com/transaction/${txHash}`;

            try {
                // BẮN GIAO DỊCH LÊN MẠNG
                await contract.sendTransfer({ seqno, secretKey: keyPair.secretKey, messages });
                console.log(`✅ Đã đẩy lệnh lên Blockchain. Hash: ${txHash}`);
                
                // Nghỉ 3 giây tránh bị TON chặn do gọi API quá nhanh
                await new Promise(resolve => setTimeout(resolve, 3000));
                
                let adminMsgDetails = "";

                // GHI NHẬN KẾT QUẢ VÀO DB VÀ BÁO TELEGRAM
                for (const txId of batchToProcess) {
                    const order = queueData[txId];
                    
                    // 2.1 Xóa khỏi hàng đợi RTDB
                    await queueRef.child(txId).remove(); 
                    
                    // 2.2 Cập nhật trạng thái Firestore thành 'completed'
                    try {
                        const userRef = db.collection('users').doc(order.uid);
                        const userSnap = await userRef.get();
                        
                        if (userSnap.exists) {
                            const userData = userSnap.data();
                            let history = userData.transactionHistory || [];
                            let isUpdated = false;
                            
                            for (let i = 0; i < history.length; i++) {
                                if (history[i].id === txId) {
                                    history[i].status = 'completed'; 
                                    history[i].txHash = txHash;      
                                    isUpdated = true;
                                    break;
                                }
                            }
                            if (isUpdated) await userRef.update({ transactionHistory: history });
                        }
                    } catch (err) {
                        console.error(`Lỗi update lịch sử Firestore cho ID ${txId}:`, err);
                    }
                    
                    // 2.3 Gửi tin nhắn cho từng khách
                    const userMsg = `✅ <b>WITHDRAWAL SUCCESSFUL</b> ✅\n\n💎 <b>Token:</b> <code>TON</code>\n💰 <b>Amount Received:</b> <code>${order.tonAmount.toFixed(4)} TON</code>\n💵 <b>USDT Deducted:</b> <code>${order.usdtAmount.toFixed(2)} USDT</code>\n⚡️ <b>Status:</b> <code>Completed</code>\n\n🔍 <a href="${txLink}">View Transaction on Explorer</a>`;
                    await sendTelegramMsg(order.uid, userMsg);

                    adminMsgDetails += `- ID <code>${order.uid}</code> (${order.username}): ${order.usdtAmount} USDT ➡️ <b>${order.tonAmount.toFixed(4)} TON</b>\n`;
                }

                // 2.4 Báo cáo tổng kết cho Admin
                const adminMsg = `🔔 <b>XẢ TIỀN TỰ ĐỘNG THÀNH CÔNG</b>\n\nĐã thanh toán <b>${batchToProcess.length} đơn</b>!\n💰 Tổng chi mạng: <b>${totalTonSent.toFixed(5)} TON</b>\n\n📜 Chi tiết:\n${adminMsgDetails}\n\n🔍 <a href="${txLink}">Check lô giao dịch trên Explorer</a>`;
                await sendTelegramMsg(ADMIN_CHAT_ID, adminMsg, ADMIN_TOPIC_ID);

                console.log(`✅ Hoàn thành mẻ 4 đơn. Nghỉ 2s trước khi xả mẻ tiếp theo...`);
                await new Promise(resolve => setTimeout(resolve, 2000));

            } catch (err) {
                console.error("❌ Lỗi xả tiền mạng TON. Giữ nguyên các đơn còn lại trong RAM để mẻ sau xả:", err);
                break; // Văng khỏi vòng lặp while, tắt máy. Không xóa DB.
            }
        }

        console.log(`🏁 KẾT THÚC WORKER. Còn dư ${ramOrders.length} đơn chờ mẻ sau.`);
        
    } catch (error) {
        console.error("❌ Lỗi nghiêm trọng của hệ thống Worker:", error);
    } finally {
        // CHỐT CHẶN CUỐI CÙNG: LUÔN LUÔN TRẢ LẠI CHÌA KHÓA KHI TẮT MÁY
        await lockRef.set(false);
        console.log("🔒 Đã nhả khóa is_git_running = false. Đóng chương trình.");
        process.exit(0);
    }
}

// Bắt đầu chạy
main();
