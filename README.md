# Spot Listing Telegram Bot

Railway uzerinde calisacak Telegram haber okuyucu.

Bot sadece spot listing haberlerini yakalar ve Turkce alarm yollar. Futures, perpetual, margin, earn, campaign, airdrop ve benzeri duyurular filtre disi kalir.

## Kanallar

- `@CoinMarketCapAnnouncements`
- `@CoinDeskGlobal`
- `@coinlistofficialchannel`
- `@OKXAnnouncements`
- `@dwflabs`
- `@crypto_fundraising`
- `@Bloomberg`
- `@the_block_crypto`
- `@binance_announcements`

## Railway environment variables

`.env.example` icindeki degerleri Railway Variables alanina gir.

Zorunlu degiskenler:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_SESSION_STRING`
- `ALERT_BOT_TOKEN`
- `ALERT_CHAT_ID`

Onerilen:

- `DATABASE_URL`

`DATABASE_URL` Railway Postgres ile verilmeli. Bu sayede bot hata aldiginda veya Railway yeniden basladiginda kaldigi `last_message_id` noktasindan devam eder.

## Telegram session olusturma

Lokal bilgisayarda:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_API_ID=123456
export TELEGRAM_API_HASH=your_api_hash
python session_gen.py
```

Ekrana gelen uzun metni Railway'de `TELEGRAM_SESSION_STRING` olarak ekle.

## Calisma mantigi

1. Kanallardaki yeni mesajlari okur.
2. Sadece spot listing kelimelerini arar.
3. Futures/perp/margin/earn/campaign gibi haberleri eler.
4. Turkce alarm yollar.
5. Her kanal icin `last_message_id` kaydeder.
6. Hata olursa 20 saniye bekleyip kaldigi yerden devam eder.

## GitHub push

```bash
git init
git add .
git commit -m "Initial spot listing Telegram bot"
git branch -M main
git remote add origin https://github.com/KULLANICI_ADI/spot-listing-telegram-bot.git
git push -u origin main
```
