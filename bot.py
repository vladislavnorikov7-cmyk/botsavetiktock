import asyncio
import glob
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import yt_dlp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.error import BadRequest, Forbidden, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# ================== НАСТРОЙКИ ==================
TOKEN = "8563911034:AAFbRK884nawrhuUunXuKs_pnY80XQRRdmg"
ADMIN_IDS = {5218071279}  # твой user_id (узнать через @userinfobot)

CAPTION = "⬇️ скачано в @savetiktock_bot, делись со своими братиками"
CHANNELS_FILE = Path("channels.json")

# Если каналов нет — пускать всех (True) или запрещать всем (False)
ALLOW_IF_NO_CHANNELS = True

# Автопроверка подписки (сек)
AUTO_CHECK_SECONDS = 5

# Кэш подписки (сек) — "проверка раз в сутки"
SUB_CACHE_SECONDS = 24 * 60 * 60

# Очередь: сколько скачиваний одновременно
MAX_WORKERS = 3

# Оценка времени одной задачи (для ETA в очереди)
AVG_VIDEO_SECONDS = 35
AVG_AUDIO_SECONDS = 25

# Таймауты Telegram API (если сеть шалит)
TG_TIMEOUT = 30
# ===============================================

TIKTOK_RE = re.compile(r"(https?://\S*tiktok\.com/\S+)", re.IGNORECASE)


# ================== STORAGE ==================
def load_channels() -> List[Dict[str, str]]:
    if not CHANNELS_FILE.exists():
        return []
    try:
        with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            out = []
            for item in data:
                if isinstance(item, dict) and "id" in item and "link" in item:
                    cid = str(item["id"]).strip()
                    link = str(item["link"]).strip()
                    if cid and link:
                        out.append({"id": cid, "link": link})
            return out
    except Exception:
        pass
    return []


def save_channels(channels: List[Dict[str, str]]) -> None:
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ================== HELPERS ==================
def cleanup_downloads(prefix: str = "download") -> None:
    for f in glob.glob(f"{prefix}.*"):
        try:
            os.remove(f)
        except OSError:
            pass


def newest_file(prefix: str = "download") -> Optional[str]:
    files = glob.glob(f"{prefix}.*")
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def extract_tiktok_url(text: str) -> Optional[str]:
    m = TIKTOK_RE.search(text or "")
    return m.group(1) if m else None


async def safe_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int) -> None:
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def download_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🎬 Видео", callback_data="dl_video"),
                InlineKeyboardButton("🎵 Аудио (MP3)", callback_data="dl_audio"),
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
        ]
    )


def subscribe_keyboard(channels: List[Dict[str, str]]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    if channels:
        for ch in channels:
            buttons.append([InlineKeyboardButton(f"📢 Подписаться: {ch['id']}", url=ch["link"])])
    else:
        buttons.append([InlineKeyboardButton("⚠️ Каналы не настроены", callback_data="noop")])

    buttons.append([InlineKeyboardButton("✅ я подписался", callback_data="check_sub")])
    buttons.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(buttons)


def queue_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Выйти из очереди", callback_data="q_cancel")]])


async def spinner_animate(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, stop: asyncio.Event) -> None:
    frames = ["⏳", "⌛", "⏳", "⌛"]
    i = 0
    while not stop.is_set():
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"{frames[i % len(frames)]} скачиваю, жди братик…",
            )
        except Exception:
            return

        try:
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        except Exception:
            pass

        i += 1
        await asyncio.sleep(0.6)


# ================== YT-DLP ==================
_COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.tiktok.com/",
}


def _ydl_download_video(url: str) -> None:
    cleanup_downloads("download")
    ydl_opts = {
        "outtmpl": "download.%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "format": "best",
        "http_headers": _COMMON_HEADERS,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


def _ydl_download_audio_mp3(url: str) -> None:
    cleanup_downloads("download")
    ydl_opts = {
        "outtmpl": "download.%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "format": "bestaudio/best",
        "http_headers": _COMMON_HEADERS,
        "postprocessors": [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
        ],
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# ================== SUBSCRIPTION (ALL + PROGRESS + CACHE + AUTO) ==================
def _cache_ok(user_data: dict) -> bool:
    until = user_data.get("sub_ok_until", 0)
    return isinstance(until, (int, float)) and time.time() < float(until)


def _set_cache_ok(user_data: dict) -> None:
    user_data["sub_ok_until"] = time.time() + SUB_CACHE_SECONDS


async def subscription_progress(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> Tuple[int, int, List[str]]:
    """
    Требование: подписка НА ВСЕ каналы.
    Возвращает: (ok_count, total, missing_ids)
    """
    channels = load_channels()
    total = len(channels)

    if total == 0:
        return 0, 0, []

    ok = 0
    missing: List[str] = []
    for ch in channels:
        try:
            member = await context.bot.get_chat_member(chat_id=ch["id"], user_id=user_id)
            if member.status in ("creator", "administrator", "member"):
                ok += 1
            else:
                missing.append(ch["id"])
        except (BadRequest, Forbidden, TimedOut):
            missing.append(ch["id"])
        except Exception:
            missing.append(ch["id"])

    return ok, total, missing


def progress_text(ok: int, total: int, missing: List[str]) -> str:
    if total == 0:
        return "⚠️ Каналы не настроены."

    if ok >= total:
        return f"✅ Подписка подтверждена: {ok} из {total}\nОткрываю меню…"

    miss_line = ""
    if missing:
        show = missing[:8]
        miss_line = "\n\nНе хватает подписки на:\n" + "\n".join([f"• {x}" for x in show])
        if len(missing) > 8:
            miss_line += f"\n…и ещё {len(missing) - 8}"

    bar_len = 10
    filled = int((ok / total) * bar_len)
    bar = "█" * filled + "░" * (bar_len - filled)

    return (
        "🔒 Чтобы скачать, подпишись на **ВСЕ** каналы ниже.\n"
        f"📊 Прогресс: **{ok} из {total}**  `{bar}`"
        f"{miss_line}\n\n"
        "После подписки нажми «✅ Я подписался» — или просто подожди, я проверю сам."
    )


def sub_job_name(chat_id: int, user_id: int) -> str:
    return f"subcheck:{chat_id}:{user_id}"


def cancel_sub_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    jq = context.job_queue
    if not jq:
        return
    name = sub_job_name(chat_id, user_id)
    for job in jq.get_jobs_by_name(name):
        job.schedule_removal()


async def subcheck_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    user_id = data.get("user_id")
    msg_id = data.get("message_id")

    if not isinstance(chat_id, int) or not isinstance(user_id, int) or not isinstance(msg_id, int):
        return

    ud = context.application.user_data.get(user_id, {})
    if _cache_ok(ud):
        cancel_sub_job(context, chat_id, user_id)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text="✅ Подписка уже подтверждена (кэш на сутки).\nЧто скачать?",
                reply_markup=download_menu_keyboard(),
            )
        except Exception:
            pass
        return

    channels = load_channels()
    if not channels:
        cancel_sub_job(context, chat_id, user_id)
        if ALLOW_IF_NO_CHANNELS:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text="⚠️ Каналы не настроены, доступ открыт.\nЧто скачать?",
                    reply_markup=download_menu_keyboard(),
                )
            except Exception:
                pass
        else:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text="⛔ Каналы не настроены. Доступ закрыт администратором.",
                )
            except Exception:
                pass
        return

    ok, total, missing = await subscription_progress(context, user_id)
    if ok >= total:
        _set_cache_ok(ud)
        context.application.user_data[user_id] = ud
        cancel_sub_job(context, chat_id, user_id)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text="✅ Подписка подтверждена!\nЧто скачать?",
                reply_markup=download_menu_keyboard(),
            )
        except Exception:
            pass
        return

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=progress_text(ok, total, missing),
            reply_markup=subscribe_keyboard(channels),
            parse_mode="Markdown",
        )
    except Exception:
        pass


def start_sub_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message_id: int) -> None:
    if not context.job_queue:
        return
    cancel_sub_job(context, chat_id, user_id)
    context.job_queue.run_repeating(
        subcheck_job,
        interval=AUTO_CHECK_SECONDS,
        first=AUTO_CHECK_SECONDS,
        name=sub_job_name(chat_id, user_id),
        data={"chat_id": chat_id, "user_id": user_id, "message_id": message_id},
    )


# ================== КРАСИВАЯ ОЧЕРЕДЬ ==================
@dataclass
class Job:
    chat_id: int
    user_id: int
    url: str
    kind: str  # "video" or "audio"
    user_msg_id: int
    status_msg_id: int
    created_ts: float


class QueueManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._event = asyncio.Event()
        self._queue: List[Job] = []
        self._running: Dict[int, Job] = {}  # user_id -> job

    async def has_active(self, user_id: int) -> bool:
        async with self._lock:
            if user_id in self._running:
                return True
            return any(j.user_id == user_id for j in self._queue)

    async def enqueue(self, job: Job) -> Tuple[int, int]:
        """
        Возвращает: (позиция, всего_в_очереди)
        """
        async with self._lock:
            self._queue.append(job)
            pos = len(self._queue)
            total = len(self._queue)
            self._event.set()
            return pos, total

    async def cancel(self, user_id: int) -> bool:
        """
        Отменяет, если job в очереди (не в running). Возвращает True если удалили.
        """
        async with self._lock:
            for i, j in enumerate(self._queue):
                if j.user_id == user_id:
                    self._queue.pop(i)
                    return True
            return False

    async def pop_next(self) -> Job:
        while True:
            async with self._lock:
                if self._queue:
                    job = self._queue.pop(0)
                    self._running[job.user_id] = job
                    return job
                self._event.clear()
            await self._event.wait()

    async def done(self, user_id: int) -> None:
        async with self._lock:
            self._running.pop(user_id, None)

    async def position_and_eta(self, user_id: int) -> Tuple[Optional[int], Optional[int], int]:
        """
        Возвращает (pos, eta_seconds, queue_len).
        pos — позиция в очереди (1..N), eta — оценка ожидания до старта.
        """
        async with self._lock:
            queue_len = len(self._queue)
            # если уже исполняется
            if user_id in self._running:
                return None, 0, queue_len

            pos = None
            kind = None
            for i, j in enumerate(self._queue, start=1):
                if j.user_id == user_id:
                    pos = i
                    kind = j.kind
                    break

            if pos is None:
                return None, None, queue_len

            avg = AVG_VIDEO_SECONDS if kind == "video" else AVG_AUDIO_SECONDS
            waves = (pos - 1) // MAX_WORKERS  # сколько "пакетов" перед ним
            eta = max(0, waves * avg)
            return pos, eta, queue_len


QUEUE = QueueManager()


def queue_text(pos: int, eta: int, qlen: int) -> str:
    eta_min = eta // 60
    eta_sec = eta % 60
    if eta_min > 0:
        eta_str = f"≈ {eta_min} мин {eta_sec} сек"
    else:
        eta_str = f"≈ {eta_sec} сек"

    return (
        "🧾 Заявка добавлена в очередь ✅\n"
        f"📌 Твоя позиция: **#{pos}** из **{qlen}**\n"
        f"⏱ Ожидание до старта: **{eta_str}**\n\n"
        "Можно подождать или выйти из очереди кнопкой ниже."
    )


def qjobname(chat_id: int, user_id: int) -> str:
    return f"qstatus:{chat_id}:{user_id}"


def cancel_qstatus_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    if not context.job_queue:
        return
    name = qjobname(chat_id, user_id)
    for job in context.job_queue.get_jobs_by_name(name):
        job.schedule_removal()


async def qstatus_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    user_id = data.get("user_id")
    msg_id = data.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(user_id, int) or not isinstance(msg_id, int):
        return

    pos, eta, qlen = await QUEUE.position_and_eta(user_id)
    # если уже не в очереди — остановим
    if pos is None and eta is None:
        cancel_qstatus_job(context, chat_id, user_id)
        return

    # если started (eta==0 but pos None) — worker уже взял
    if pos is None and eta == 0:
        cancel_qstatus_job(context, chat_id, user_id)
        return

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=queue_text(pos, eta or 0, qlen),
            reply_markup=queue_keyboard(),
            parse_mode="Markdown",
        )
    except Exception:
        pass


def start_qstatus_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message_id: int) -> None:
    if not context.job_queue:
        return
    cancel_qstatus_job(context, chat_id, user_id)
    context.job_queue.run_repeating(
        qstatus_job,
        interval=5,
        first=5,
        name=qjobname(chat_id, user_id),
        data={"chat_id": chat_id, "user_id": user_id, "message_id": message_id},
    )


async def worker_loop(app: Application) -> None:
    # бесконечный воркер
    while True:
        job = await QUEUE.pop_next()
        try:
            # “анимация” на статус-сообщении
            stop = asyncio.Event()
            spinner_task = asyncio.create_task(
                spinner_animate(app.bot, job.chat_id, job.status_msg_id, stop)  # type: ignore
            )
        except Exception:
            stop = asyncio.Event()
            spinner_task = None

        try:
            # здесь реально скачиваем
            if job.kind == "video":
                await asyncio.to_thread(_ydl_download_video, job.url)
                path = newest_file("download")
                if not path:
                    try:
                        await app.bot.send_message(job.chat_id, "Не удалось скачать видео 😢")
                    except Exception:
                        pass
                    continue

                # удалить служебные сообщения
                try:
                    await app.bot.delete_message(job.chat_id, job.user_msg_id)
                except Exception:
                    pass
                try:
                    await app.bot.delete_message(job.chat_id, job.status_msg_id)
                except Exception:
                    pass

                with open(path, "rb") as f:
                    await app.bot.send_video(chat_id=job.chat_id, video=f, caption=CAPTION)
                cleanup_downloads("download")

            else:  # audio
                await asyncio.to_thread(_ydl_download_audio_mp3, job.url)
                path = "download.mp3" if os.path.exists("download.mp3") else newest_file("download")
                if not path or not os.path.exists(path):
                    try:
                        await app.bot.send_message(job.chat_id, "Не удалось скачать аудио 😢 (нужен FFmpeg)")
                    except Exception:
                        pass
                    continue

                try:
                    await app.bot.delete_message(job.chat_id, job.user_msg_id)
                except Exception:
                    pass
                try:
                    await app.bot.delete_message(job.chat_id, job.status_msg_id)
                except Exception:
                    pass

                with open(path, "rb") as f:
                    await app.bot.send_audio(chat_id=job.chat_id, audio=f, caption=CAPTION)
                cleanup_downloads("download")

        except Exception:
            try:
                await app.bot.send_message(job.chat_id, "⚠️ Ошибка скачивания. Попробуй ещё раз позже.")
            except Exception:
                pass
        finally:
            if spinner_task:
                stop.set()
                try:
                    await spinner_task
                except Exception:
                    pass
            await QUEUE.done(job.user_id)


# небольшой хак: spinner_animate выше ожидает ContextTypes, но worker использует app.bot.
# Сделаем мини-обертку, совместимую по сигнатуре.
async def spinner_animate(bot, chat_id: int, message_id: int, stop: asyncio.Event) -> None:
    frames = ["⏳", "⌛", "⏳", "⌛"]
    i = 0
    while not stop.is_set():
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=f"{frames[i % len(frames)]} скачиваю, жди братик…",
            )
        except Exception:
            return

        try:
            await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        except Exception:
            pass

        i += 1
        await asyncio.sleep(0.6)


# ================== ADMIN PANEL (BUTTONS) ==================
ADMIN_STATE_KEY = "admin_state"
ADMIN_TMP_ID = "admin_tmp_channel_id"

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📋 Список каналов", callback_data="adm_list")],
            [InlineKeyboardButton("➕ Добавить канал", callback_data="adm_add")],
            [InlineKeyboardButton("➖ Удалить канал", callback_data="adm_remove")],
            [InlineKeyboardButton("🗑 Очистить список", callback_data="adm_clear")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="adm_close")],
        ]
    )

def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm_back")]])

def admin_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("❌ Отмена", callback_data="adm_cancel")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="adm_back")],
        ]
    )

def normalize_link(link: str) -> Optional[str]:
    link = (link or "").strip()
    if not link:
        return None
    if link.startswith("t.me/"):
        link = "https://" + link
    if link.startswith("http://t.me/"):
        link = "https://t.me/" + link[len("http://t.me/"):]
    if not link.startswith("https://t.me/"):
        return None
    return link

def normalize_channel_id(cid: str) -> Optional[str]:
    cid = (cid or "").strip()
    if not cid:
        return None
    if cid.startswith("@") and len(cid) > 1:
        return cid
    if cid.startswith("-100") and cid[4:].isdigit():
        return cid
    return None

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        # можно молча игнорировать (как сейчас)
        return
    context.user_data.pop(ADMIN_STATE_KEY, None)
    context.user_data.pop(ADMIN_TMP_ID, None)
    await update.message.reply_text("👑 Админ-панель:", reply_markup=admin_menu_kb())

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    try:
        await q.answer()
    except Exception:
        pass

    if not is_admin(q.from_user.id):
        return

    data = q.data

    if data == "adm_close":
        try:
            await q.edit_message_text("Админ-панель закрыта.")
        except Exception:
            pass
        context.user_data.pop(ADMIN_STATE_KEY, None)
        context.user_data.pop(ADMIN_TMP_ID, None)
        return

    if data == "adm_back":
        context.user_data.pop(ADMIN_STATE_KEY, None)
        context.user_data.pop(ADMIN_TMP_ID, None)
        try:
            await q.edit_message_text("👑 Админ-панель:", reply_markup=admin_menu_kb())
        except Exception:
            pass
        return

    if data == "adm_cancel":
        context.user_data.pop(ADMIN_STATE_KEY, None)
        context.user_data.pop(ADMIN_TMP_ID, None)
        try:
            await q.edit_message_text("Отменено.", reply_markup=admin_menu_kb())
        except Exception:
            pass
        return

    if data == "adm_list":
        channels = load_channels()
        if not channels:
            text = "📋 Список каналов пуст."
        else:
            text = "📋 Каналы:\n\n" + "\n".join([f"• {c['id']} → {c['link']}" for c in channels])
            text += "\n\nВажно: бот должен быть админом в каждом канале, иначе проверка подписки будет 'не видна'."
        try:
            await q.edit_message_text(text, reply_markup=admin_back_kb())
        except Exception:
            await q.message.reply_text(text, reply_markup=admin_back_kb())
        return

    if data == "adm_clear":
        save_channels([])
        context.user_data.pop(ADMIN_STATE_KEY, None)
        context.user_data.pop(ADMIN_TMP_ID, None)
        try:
            await q.edit_message_text("🗑 Список каналов очищен.", reply_markup=admin_menu_kb())
        except Exception:
            pass
        return

    if data == "adm_add":
        context.user_data[ADMIN_STATE_KEY] = "await_add_id"
        context.user_data.pop(ADMIN_TMP_ID, None)
        try:
            await q.edit_message_text(
                "➕ Добавление канала\n\n"
                "Отправь *ID канала*:\n"
                "• публичный: `@my_channel`\n"
                "• приватный: `-1001234567890`\n",
                reply_markup=admin_cancel_kb(),
                parse_mode="Markdown",
            )
        except Exception:
            pass
        return

    if data == "adm_remove":
        context.user_data[ADMIN_STATE_KEY] = "await_remove_id"
        context.user_data.pop(ADMIN_TMP_ID, None)
        try:
            await q.edit_message_text(
                "➖ Удаление канала\n\n"
                "Отправь ID канала (`@my_channel` или `-100...`).",
                reply_markup=admin_cancel_kb(),
                parse_mode="Markdown",
            )
        except Exception:
            pass
        return

async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return

    state = context.user_data.get(ADMIN_STATE_KEY)
    if not state:
        return

    text = (update.message.text or "").strip()

    if state == "await_add_id":
        cid = normalize_channel_id(text)
        if not cid:
            await update.message.reply_text("❌ Неверный ID. Пришли `@username` или `-100...`",
                                            parse_mode="Markdown", reply_markup=admin_cancel_kb())
            return

        channels = load_channels()
        if any(c["id"] == cid for c in channels):
            context.user_data.pop(ADMIN_STATE_KEY, None)
            context.user_data.pop(ADMIN_TMP_ID, None)
            await update.message.reply_text("⚠️ Такой канал уже есть.", reply_markup=admin_menu_kb())
            return

        context.user_data[ADMIN_TMP_ID] = cid
        context.user_data[ADMIN_STATE_KEY] = "await_add_link"
        await update.message.reply_text(
            "Теперь пришли *ссылку* на канал:\n`https://t.me/my_channel` или `https://t.me/+AbCd...`",
            parse_mode="Markdown",
            reply_markup=admin_cancel_kb(),
        )
        return

    if state == "await_add_link":
        link = normalize_link(text)
        if not link:
            await update.message.reply_text("❌ Неверная ссылка. Нужна `https://t.me/...`",
                                            parse_mode="Markdown", reply_markup=admin_cancel_kb())
            return

        cid = context.user_data.get(ADMIN_TMP_ID)
        if not cid:
            context.user_data.pop(ADMIN_STATE_KEY, None)
            await update.message.reply_text("Что-то пошло не так. Открой /admin заново.")
            return

        channels = load_channels()
        channels.append({"id": cid, "link": link})
        save_channels(channels)

        context.user_data.pop(ADMIN_STATE_KEY, None)
        context.user_data.pop(ADMIN_TMP_ID, None)
        await update.message.reply_text(
            f"✅ Канал добавлен: {cid}\n\n"
            "Важно: добавь бота админом в этот канал, иначе проверка подписки может не работать.",
            reply_markup=admin_menu_kb(),
        )
        return

    if state == "await_remove_id":
        cid = normalize_channel_id(text)
        if not cid:
            await update.message.reply_text("❌ Неверный ID. Пришли `@username` или `-100...`",
                                            parse_mode="Markdown", reply_markup=admin_cancel_kb())
            return

        channels = load_channels()
        new_channels = [c for c in channels if c["id"] != cid]
        if len(new_channels) == len(channels):
            context.user_data.pop(ADMIN_STATE_KEY, None)
            await update.message.reply_text("⚠️ Такого канала нет в списке.", reply_markup=admin_menu_kb())
            return

        save_channels(new_channels)
        context.user_data.pop(ADMIN_STATE_KEY, None)
        context.user_data.pop(ADMIN_TMP_ID, None)
        await update.message.reply_text(f"❌ Канал удалён: {cid}", reply_markup=admin_menu_kb())
        return


# ================== USER FLOW ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Отправь ссылку на TikTok — я пришлю кнопки скачать.\nАдмину: /admin")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # если админ в режиме добавления/удаления
    if is_admin(update.effective_user.id) and context.user_data.get(ADMIN_STATE_KEY):
        await admin_text_handler(update, context)
        return

    text = (update.message.text or "").strip()
    url = extract_tiktok_url(text)

    if not url:
        await update.message.reply_text("Пришли ссылку на TikTok-видео 🙂")
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # 1 активная задача на человека
    if await QUEUE.has_active(user_id):
        await update.message.reply_text("⏳ У тебя уже есть заявка в очереди/в работе. Дождись результата 🙂")
        return

    context.user_data["tiktok_url"] = url
    context.user_data["user_msg_id"] = update.message.message_id

    # если кэш подписки ок — сразу меню
    if _cache_ok(context.user_data):
        msg = await update.message.reply_text("✅ Подписка подтверждена (кэш на сутки).\nЧто скачать?",
                                              reply_markup=download_menu_keyboard())
        context.user_data["bot_menu_msg_id"] = msg.message_id
        return

    channels = load_channels()
    if not channels:
        if ALLOW_IF_NO_CHANNELS:
            msg = await update.message.reply_text("⚠️ Каналы не настроены, доступ открыт.\nЧто скачать?",
                                                  reply_markup=download_menu_keyboard())
            context.user_data["bot_menu_msg_id"] = msg.message_id
        else:
            await update.message.reply_text("⛔ Каналы не настроены. Доступ закрыт.")
        return

    ok, total, missing = await subscription_progress(context, user_id)
    if ok >= total:
        _set_cache_ok(context.user_data)
        msg = await update.message.reply_text("✅ Подписка подтверждена!\nЧто скачать?",
                                              reply_markup=download_menu_keyboard())
        context.user_data["bot_menu_msg_id"] = msg.message_id
        return

    msg = await update.message.reply_text(
        progress_text(ok, total, missing),
        reply_markup=subscribe_keyboard(channels),
        parse_mode="Markdown",
    )
    context.user_data["bot_menu_msg_id"] = msg.message_id
    start_sub_job(context, chat_id, user_id, msg.message_id)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass

    action = query.data
    chat_id = query.message.chat_id
    user_id = query.from_user.id

    # админ-кнопки
    if action.startswith("adm_"):
        await admin_callback(update, context)
        return

    if action == "noop":
        return

    # выйти из очереди
    if action == "q_cancel":
        removed = await QUEUE.cancel(user_id)
        cancel_qstatus_job(context, chat_id, user_id)
        if removed:
            try:
                await query.edit_message_text("❌ Убрал тебя из очереди.")
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text("⚠️ Ты уже не в очереди.")
            except Exception:
                pass
        return

    if action == "cancel":
        cancel_sub_job(context, chat_id, user_id)
        await safe_delete(context, chat_id, query.message.message_id)
        context.user_data.pop("tiktok_url", None)
        return

    if action == "check_sub":
        channels = load_channels()
        if not channels:
            if ALLOW_IF_NO_CHANNELS:
                try:
                    await query.edit_message_text("⚠️ Каналы не настроены, доступ открыт.\nЧто скачать?",
                                                  reply_markup=download_menu_keyboard())
                except Exception:
                    pass
            else:
                try:
                    await query.edit_message_text("⛔ Каналы не настроены. Доступ закрыт.")
                except Exception:
                    pass
            cancel_sub_job(context, chat_id, user_id)
            return

        ok, total, missing = await subscription_progress(context, user_id)
        if ok >= total:
            _set_cache_ok(context.user_data)
            cancel_sub_job(context, chat_id, user_id)
            try:
                await query.edit_message_text("✅ Подписка подтверждена!\nЧто скачать?",
                                              reply_markup=download_menu_keyboard())
            except Exception:
                pass
            return

        # не прошёл — обновим
        try:
            await query.edit_message_text(
                progress_text(ok, total, missing),
                reply_markup=subscribe_keyboard(channels),
                parse_mode="Markdown",
            )
        except Exception:
            pass
        start_sub_job(context, chat_id, user_id, query.message.message_id)
        return

    # ==== постановка в очередь скачивания ====
    if action in ("dl_video", "dl_audio"):
        # повторная проверка подписки (или кэш)
        if not _cache_ok(context.user_data):
            ok, total, missing = await subscription_progress(context, user_id)
            if total > 0 and ok < total:
                channels = load_channels()
                try:
                    await query.edit_message_text(
                        progress_text(ok, total, missing),
                        reply_markup=subscribe_keyboard(channels),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
                start_sub_job(context, chat_id, user_id, query.message.message_id)
                return
            if total > 0 and ok >= total:
                _set_cache_ok(context.user_data)

        url = context.user_data.get("tiktok_url")
        user_msg_id = context.user_data.get("user_msg_id")
        if not url or not isinstance(user_msg_id, int):
            try:
                await query.edit_message_text("ссылка потерялась 😅 Пришли её ещё раз.")
            except Exception:
                pass
            return

        # 1 активная задача на человека
        if await QUEUE.has_active(user_id):
            try:
                await query.edit_message_text("⏳ у тебя уже есть заявка в очереди/в работе. Дождись результата 🙂")
            except Exception:
                pass
            return

        kind = "video" if action == "dl_video" else "audio"
        job = Job(
            chat_id=chat_id,
            user_id=user_id,
            url=url,
            kind=kind,
            user_msg_id=user_msg_id,
            status_msg_id=query.message.message_id,  # будем редактировать это сообщение
            created_ts=time.time(),
        )

        pos, qlen = await QUEUE.enqueue(job)
        avg = AVG_VIDEO_SECONDS if kind == "video" else AVG_AUDIO_SECONDS
        waves = (pos - 1) // MAX_WORKERS
        eta = max(0, waves * avg)

        # показываем красивый статус очереди + кнопку отмены
        try:
            await query.edit_message_text(
                queue_text(pos, eta, qlen),
                reply_markup=queue_keyboard(),
                parse_mode="Markdown",
            )
        except Exception:
            pass

        # автообновление позиции/ETA
        start_qstatus_job(context, chat_id, user_id, query.message.message_id)
        return


# ================== ERROR HANDLER ==================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        print("Ошибка:", repr(context.error))
    except Exception:
        pass
    try:
        if hasattr(update, "effective_message") and update.effective_message:
            await update.effective_message.reply_text("⚠️ проблема с соединением, щас подожди, скину.")
    except Exception:
        pass


# ================== APP INIT ==================
async def post_init(application: Application) -> None:
    # запускаем воркеры
    for _ in range(MAX_WORKERS):
        application.create_task(worker_loop(application))


def main() -> None:
    # FIX для Windows/Python 3.10+
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    request = HTTPXRequest(
        connect_timeout=TG_TIMEOUT,
        read_timeout=TG_TIMEOUT,
        write_timeout=TG_TIMEOUT,
        pool_timeout=TG_TIMEOUT,
    )

    app = Application.builder().token(TOKEN).request(request).post_init(post_init).build()
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))

    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Бот запущен.")
    app.run_polling()


if __name__ == "__main__":
    main()
