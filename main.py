import asyncio
from contextlib import asynccontextmanager
import os
import sys
import logging
import subprocess
import signal
import psutil
import aiosqlite
import hashlib
import json
import zipfile
import shutil
import shlex
import ast
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Union, Set, List, Tuple

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.types import LabeledPrice

from aiohttp import web

# ---------- Python 3.8 Compatibility ----------
if sys.version_info < (3, 9):
    try:
        from typing import AsyncIterator
    except ImportError:
        pass

async def to_thread(func, *args, **kwargs):
    """asyncio.to_thread fallback for Python < 3.9"""
    if hasattr(asyncio, "to_thread"):
        return await asyncio.to_thread(func, *args, **kwargs)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------- Config (with .env support) ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("python-dotenv not installed; using defaults.")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID_STR = os.getenv("OWNER_ID", "").strip()
ADMIN_ID_STR = os.getenv("ADMIN_ID", OWNER_ID_STR).strip()
OWNER_CONTACT_URL = os.getenv("OWNER_CONTACT_URL", "https://t.me/your_admin_username").strip()
UPDATE_CHANNEL_URL = os.getenv("UPDATE_CHANNEL_URL", "https://t.me/your_channel").strip()

if not BOT_TOKEN:
    logger.error("BOT_TOKEN is missing!")
    raise ValueError("BOT_TOKEN is required.")

try:
    if not OWNER_ID_STR or not ADMIN_ID_STR:
        raise ValueError("Set OWNER_ID and ADMIN_ID in Railway Variables.")
    OWNER_ID = int(OWNER_ID_STR)
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    logger.error("OWNER_ID or ADMIN_ID must be integers!")
    raise

# ---------- Constants & Paths ----------
BASE_DIR = Path(__file__).parent.absolute()
DATA_DIR = Path(os.getenv("DATA_DIR", "/data" if Path("/data").exists() else str(BASE_DIR / "data"))).expanduser().resolve()
PROJECTS_DIR = Path(os.getenv("PROJECTS_DIR", str(DATA_DIR / "projects"))).expanduser().resolve()
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", str(DATA_DIR / "projects.db"))).expanduser().resolve()

FREE_TIER_SLOTS = 1
PREMIUM_TIER_SLOTS = 10
SLOT_PRICE_STARS = 5

PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------- Standard Libraries Detection ----------
try:
    STANDARD_LIBS = set(sys.stdlib_module_names)
    logger.info(f"Loaded {len(STANDARD_LIBS)} stdlib modules.")
except AttributeError:
    logger.warning("Old Python detected. Using built-in stdlib list.")
    STANDARD_LIBS = {
        "os", "sys", "json", "asyncio", "sqlite3", "logging", "datetime",
        "pathlib", "hashlib", "zipfile", "re", "math", "collections", "time",
        "subprocess", "shutil", "ast", "random", "string", "io", "abc",
        "functools", "itertools", "operator", "copy", "pickle", "base64",
        "binascii", "struct", "socket", "ssl", "threading", "queue",
        "multiprocessing", "concurrent", "urllib", "http", "email",
        "smtplib", "csv", "xml", "html", "uuid", "secrets", "typing",
        "dataclasses", "enum", "contextlib", "warnings", "traceback",
    }

# ---------- Bot & Dispatcher ----------
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ---------- Global State ----------
bot_scripts = {}
admin_ids = {OWNER_ID, ADMIN_ID}
banned_users = set()
bot_locked = False
bot_stats = {}

# ---------- FSM States ----------
class ProjectCreation(StatesGroup):
    awaiting_name = State()
    awaiting_file = State()

class ProjectManagement(StatesGroup):
    awaiting_run_command = State()
    awaiting_deps_install = State()

class AdminFSM(StatesGroup):
    awaiting_broadcast = State()
    awaiting_add_admin_id = State()
    awaiting_remove_admin_id = State()
    awaiting_ban_id = State()
    awaiting_ban_reason = State()
    awaiting_unban_id = State()
    awaiting_premium_user_id = State()
    awaiting_premium_days = State()
    awaiting_premium_slots = State()
    awaiting_remove_premium_id = State()

# ---------- Database ----------
@asynccontextmanager
async def get_db_conn():
    """Open one SQLite connection for a single async with block."""
    conn = await aiosqlite.connect(DATABASE_PATH)
    try:
        conn.row_factory = aiosqlite.Row
        yield conn
    finally:
        await conn.close()

async def init_db():
    logger.info(f"Initializing DB: {DATABASE_PATH}")
    try:
        async with get_db_conn() as conn:
            await conn.execute('''CREATE TABLE IF NOT EXISTS projects (
                            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER NOT NULL,
                            project_name TEXT NOT NULL,
                            status TEXT DEFAULT 'stopped',
                            ram_tier TEXT DEFAULT 'free',
                            run_command TEXT,
                            main_file TEXT,
                            approval_status TEXT NOT NULL DEFAULT 'approved',
                            created_at TEXT,
                            UNIQUE(user_id, project_name)
                        )''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS users (
                            user_id INTEGER PRIMARY KEY,
                            join_date TEXT,
                            last_active TEXT,
                            is_banned BOOLEAN DEFAULT 0,
                            is_admin BOOLEAN DEFAULT 0,
                            purchased_slots INTEGER DEFAULT 0
                        )''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS bot_stats (
                            stat_name TEXT PRIMARY KEY,
                            stat_value INTEGER
                        )''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS premium_subscriptions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER NOT NULL,
                            slots INTEGER NOT NULL,
                            expiry_date TEXT NOT NULL,
                            created_at TEXT NOT NULL
                        )''')

            await conn.execute('INSERT OR IGNORE INTO users (user_id, is_admin) VALUES (?, ?)', (OWNER_ID, 1))
            if ADMIN_ID != OWNER_ID:
                await conn.execute('INSERT OR IGNORE INTO users (user_id, is_admin) VALUES (?, ?)', (ADMIN_ID, 1))

            for stat in ['total_uploads', 'total_downloads', 'total_runs']:
                await conn.execute('INSERT OR IGNORE INTO bot_stats (stat_name, stat_value) VALUES (?, 0)', (stat,))

            await conn.commit()
        logger.info("DB initialized.")
    except Exception as e:
        logger.error(f"DB init error: {e}", exc_info=True)

async def migrate_db():
    try:
        async with get_db_conn() as conn:
            async with conn.execute("PRAGMA table_info(users)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
            if 'purchased_slots' not in columns:
                await conn.execute("ALTER TABLE users ADD COLUMN purchased_slots INTEGER DEFAULT 0")
            async with conn.execute("PRAGMA table_info(projects)") as cursor:
                project_columns = [row[1] for row in await cursor.fetchall()]
            if 'approval_status' not in project_columns:
                # Existing projects remain usable; new uploads require approval.
                await conn.execute("ALTER TABLE projects ADD COLUMN approval_status TEXT NOT NULL DEFAULT 'approved'")
            await conn.commit()
        logger.info("DB migrations complete.")
    except Exception as e:
        logger.error(f"Migration error: {e}", exc_info=True)

async def load_global_data():
    try:
        async with get_db_conn() as conn:
            admin_ids.clear()
            async with conn.execute("SELECT user_id FROM users WHERE is_admin = 1") as cursor:
                for (user_id,) in await cursor.fetchall():
                    admin_ids.add(user_id)

            banned_users.clear()
            async with conn.execute("SELECT user_id FROM users WHERE is_banned = 1") as cursor:
                for (user_id,) in await cursor.fetchall():
                    banned_users.add(user_id)

            bot_stats.clear()
            async with conn.execute("SELECT stat_name, stat_value FROM bot_stats") as cursor:
                for stat_name, stat_value in await cursor.fetchall():
                    bot_stats[stat_name] = stat_value

        logger.info(f"Loaded {len(admin_ids)} admins, {len(banned_users)} banned, stats={bot_stats}")
    except Exception as e:
        logger.error(f"Load global data error: {e}", exc_info=True)

async def increment_stat(stat_name: str, delta: int = 1):
    """Atomic stat update."""
    try:
        async with get_db_conn() as conn:
            await conn.execute(
                "UPDATE bot_stats SET stat_value = stat_value + ? WHERE stat_name = ?",
                (delta, stat_name)
            )
            await conn.commit()
        bot_stats[stat_name] = bot_stats.get(stat_name, 0) + delta
    except Exception as e:
        logger.error(f"Stat update error ({stat_name}): {e}")

async def get_total_purchased_slots() -> int:
    async with get_db_conn() as conn:
        async with conn.execute("SELECT SUM(purchased_slots) FROM users") as cursor:
            result = await cursor.fetchone()
            return result[0] if result and result[0] is not None else 0

async def get_user_total_premium_slots(user_id: int) -> int:
    async with get_db_conn() as conn:
        async with conn.execute("SELECT purchased_slots FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            permanent = row[0] if row and row[0] is not None else 0

        now = datetime.now().isoformat()
        async with conn.execute(
            "SELECT SUM(slots) FROM premium_subscriptions WHERE user_id = ? AND expiry_date > ?",
            (user_id, now)
        ) as cursor:
            row = await cursor.fetchone()
            active = row[0] if row and row[0] is not None else 0

        return permanent + active

async def get_user_slot_limit(user_id: int) -> Union[int, float]:
    if user_id in admin_ids:
        return float('inf')
    try:
        premium = await get_user_total_premium_slots(user_id)
        return FREE_TIER_SLOTS + (premium or 0)
    except Exception as e:
        logger.error(f"slot limit error {user_id}: {e}")
        return FREE_TIER_SLOTS

async def get_user_projects(user_id: int) -> List[aiosqlite.Row]:
    async with get_db_conn() as conn:
        async with conn.execute(
            "SELECT project_name, status, ram_tier FROM projects WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            return await cursor.fetchall()

async def get_project_details(user_id: int, project_name: str) -> Optional[aiosqlite.Row]:
    async with get_db_conn() as conn:
        async with conn.execute(
            "SELECT * FROM projects WHERE user_id = ? AND project_name = ?",
            (user_id, project_name)
        ) as cursor:
            return await cursor.fetchone()

async def update_project_status(user_id: int, project_name: str, status: str):
    async with get_db_conn() as conn:
        await conn.execute(
            "UPDATE projects SET status = ? WHERE user_id = ? AND project_name = ?",
            (status, user_id, project_name)
        )
        await conn.commit()

async def delete_project_from_db(user_id: int, project_name: str):
    async with get_db_conn() as conn:
        await conn.execute(
            "DELETE FROM projects WHERE user_id = ? AND project_name = ?",
            (user_id, project_name)
        )
        await conn.commit()

async def update_user_activity(user_id: int):
    async with get_db_conn() as conn:
        now = datetime.now().isoformat()
        await conn.execute(
            'INSERT OR IGNORE INTO users (user_id, join_date, last_active) VALUES (?, ?, ?)',
            (user_id, now, now)
        )
        await conn.execute('UPDATE users SET last_active = ? WHERE user_id = ?', (now, user_id))
        await conn.commit()

def is_user_banned(user_id: int) -> bool:
    return user_id in banned_users

# ---------- Dependency Helpers ----------
async def create_venv_if_not_exists(user_folder: Path) -> Tuple[Optional[Path], Optional[str]]:
    venv_path = user_folder / "venv"
    if not venv_path.exists():
        logger.info(f"Creating venv for {user_folder.name}")
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "venv", str(venv_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                return None, f"Venv creation failed: {stderr.decode()}"
            return venv_path, None
        except Exception as e:
            return None, f"Venv exception: {e}"
    return venv_path, None

def get_venv_python(user_folder: Path) -> str:
    venv_path = user_folder / "venv"
    if sys.platform == "win32":
        python_exe = venv_path / "Scripts" / "python.exe"
    else:
        python_exe = venv_path / "bin" / "python"
    if python_exe.exists():
        return str(python_exe)
    return sys.executable

def parse_imports(file_content: str) -> Set[str]:
    imports = set()
    try:
        tree = ast.parse(file_content)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except SyntaxError as e:
        logger.warning(f"AST parse error: {e}")
    return imports

async def install_dependencies(
    user_folder: Path,
    status_msg: types.Message,
    dependencies: Optional[Set[str]] = None,
    requirements_file: Optional[Path] = None
) -> Optional[str]:
    venv_path, err = await create_venv_if_not_exists(user_folder)
    if err:
        return f"Failed to create venv: {err}"

    if sys.platform == "win32":
        pip_exe = venv_path / "Scripts" / "pip.exe"
    else:
        pip_exe = venv_path / "bin" / "pip"

    command = [str(pip_exe), "install", "--upgrade", "pip"]

    if requirements_file:
        command += ["-r", str(requirements_file)]
        install_msg = "⚙️ <b>Installing from requirements.txt...</b>"
    elif dependencies:
        command += list(dependencies)
        install_msg = f"⚙️ <b>Installing:</b> <code>{' '.join(dependencies)}</code>"
    else:
        install_msg = "⚙️ <b>Verifying environment...</b>"

    try:
        await status_msg.edit_text(f"{status_msg.text}\n\n{install_msg}")
    except Exception:
        pass

    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        return f"Dependency install failed:\n<pre>{stderr.decode()[:1000]}</pre>"

    logger.info(f"Pip install OK for {user_folder.name}")
    return None

# ---------- Process Management ----------
def _preexec_limits():
    """Apply resource limits (POSIX only)."""
    if sys.platform == "win32":
        return
    try:
        import resource
        # RAM limit: 512MB
        resource.setrlimit(resource.RLIMIT_AS, (512 * 1024 * 1024, 512 * 1024 * 1024))
        # CPU time: 600 seconds
        resource.setrlimit(resource.RLIMIT_CPU, (600, 600))
        # New session so we can kill whole group
        os.setsid()
    except Exception as e:
        logger.warning(f"preexec limits failed: {e}")

async def cleanup_script(user_id: int, project_name: str, new_status: str = "stopped"):
    script_key = f"{user_id}_{project_name}"
    process = bot_scripts.pop(script_key, None)
    if process:
        try:
            if process.poll() is None:
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    try:
                        child.kill()
                    except Exception:
                        pass
                parent.kill()
        except Exception as e:
            logger.error(f"cleanup kill error: {e}")
    await update_project_status(user_id, project_name, new_status)

async def monitor_script(user_id: int, project_name: str):
    script_key = f"{user_id}_{project_name}"
    process = bot_scripts.get(script_key)
    if not process:
        return
    try:
        return_code = await to_thread(process.wait)
        logger.info(f"{script_key} finished code={return_code}")
        if script_key in bot_scripts:
            if return_code == 0:
                await bot.send_message(user_id, f"✅ <b>Script Finished!</b>\nProject: <code>{project_name}</code>")
                await cleanup_script(user_id, project_name, new_status="stopped")
            else:
                await bot.send_message(user_id, f"❌ <b>Script Crashed!</b>\nProject: <code>{project_name}</code>\nCode: <code>{return_code}</code>")
                await cleanup_script(user_id, project_name, new_status="crashed")
    except Exception as e:
        logger.error(f"monitor error {script_key}: {e}")
        await cleanup_script(user_id, project_name, new_status="crashed")

async def start_project(user_id: int, project_name: str, message: types.Message):
    script_key = f"{user_id}_{project_name}"
    if script_key in bot_scripts:
        await message.answer("Project is already running.")
        return

    project_details = await get_project_details(user_id, project_name)
    if not project_details:
        await message.answer("Error: Project not found.")
        return

    if project_details['approval_status'] != 'approved' and user_id not in admin_ids:
        await message.answer("This project is awaiting admin approval.")
        return
    run_command = project_details['run_command']
    main_file = project_details['main_file']

    if not run_command:
        if main_file:
            run_command = f"python {main_file}"
        else:
            await message.answer("❌ No run command set.")
            return

    user_folder = PROJECTS_DIR / str(user_id)
    project_folder = user_folder / project_name
    python_exe = get_venv_python(user_folder)
    log_file_path = project_folder / "deploy.log"

    command_parts = shlex.split(run_command)
    if command_parts[0].lower() in ["python", "python3"]:
        command = [python_exe] + command_parts[1:]
    else:
        await message.answer("❌ Run command must start with `python` or `python3`.")
        return

    log_file = None
    try:
        log_file = open(log_file_path, 'w')
        log_file.write(f"\n--- Starting at {datetime.now()} ---\n")
        log_file.write(f"Executing: {' '.join(command)}\n")
        log_file.flush()

        # FIX: close_fds only POSIX, use preexec_fn for limits on POSIX
        popen_kwargs = dict(
            cwd=str(project_folder),
            stdout=log_file,
            stderr=log_file,
        )
        if sys.platform != "win32":
            popen_kwargs["preexec_fn"] = _preexec_limits
            popen_kwargs["close_fds"] = True

        process = subprocess.Popen(command, **popen_kwargs)
        log_file.close()  # FIX: close in parent; child keeps own fd
        log_file = None

        bot_scripts[script_key] = process
        await update_project_status(user_id, project_name, 'running')
        await increment_stat('total_runs', 1)

        await message.answer(f"✅ <b>{project_name}</b> started! (PID: {process.pid})")
        asyncio.create_task(monitor_script(user_id, project_name))

    except Exception as e:
        logger.error(f"Failed to start {script_key}: {e}", exc_info=True)
        await message.answer(f"❌ Failed to start: {e}")
    finally:
        if log_file:
            try:
                log_file.close()
            except Exception:
                pass

async def stop_project(user_id: int, project_name: str, message: types.Message):
    script_key = f"{user_id}_{project_name}"
    process = bot_scripts.get(script_key)
    if not process:
        await message.answer("Project is not running.")
        return

    try:
        if process.poll() is None:
            parent = psutil.Process(process.pid)
            for child in parent.children(recursive=True):
                try:
                    child.terminate()
                except Exception:
                    pass
            parent.terminate()
            try:
                await to_thread(process.wait, 5)
            except Exception:
                try:
                    parent.kill()
                except Exception:
                    pass
    except psutil.NoSuchProcess:
        pass
    except Exception as e:
        logger.warning(f"stop error {script_key}: {e}")

    await cleanup_script(user_id, project_name, new_status="stopped")
    await message.answer(f"🛑 <b>{project_name}</b> stopped.")

# ---------- FSM Cancel ----------
@dp.message(Command("cancel"))
@dp.message(Command("start"))
@dp.callback_query(F.data == "fsm_cancel")
async def fsm_cancel(event: Union[types.Message, types.CallbackQuery], state: FSMContext):
    await state.clear()
    if isinstance(event, types.Message):
        if event.text and event.text.startswith("/start"):
            await cmd_start(event, state)
        else:
            await event.answer("Action cancelled.")
            await cmd_start(event, state)
    else:
        try:
            await event.answer("Action cancelled.")
        except Exception:
            pass
        try:
            await event.message.delete()
        except Exception:
            pass
        await cmd_start(event.message, state)

# ---------- Keyboards ----------
def get_main_keyboard(user_id: int):
    buttons = [
        [InlineKeyboardButton(text="🤖 Create Bot", callback_data="start_new_project"),
         InlineKeyboardButton(text="📤 Upload Bot", callback_data="start_new_project")],
        [InlineKeyboardButton(text="📢 Update Channel", url=UPDATE_CHANNEL_URL),
         InlineKeyboardButton(text="🤖 My Bot", callback_data="my_projects")],
        [InlineKeyboardButton(text="💎 Premium", callback_data="premium_contact"),
         InlineKeyboardButton(text="👤 Account", callback_data="view_quota")],
    ]
    if user_id in admin_ids:
        buttons.append([InlineKeyboardButton(text="🛡️ Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ---------- /start ----------
@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()

    if bot_locked and user_id not in admin_ids:
        await message.answer("🔒 Bot is locked for maintenance!")
        return
    if is_user_banned(user_id):
        await message.answer("🚫 You are banned from this bot.")
        return

    await update_user_activity(user_id)

    slot_limit = await get_user_slot_limit(user_id)
    premium_slots = await get_user_total_premium_slots(user_id)
    account_type = "Premium" if premium_slots > 0 else ("Admin" if user_id in admin_ids else "Free")
    welcome_text = f"""
👋 <b>Welcome to Python Bot Hosting</b>

Create, upload, and manage your Python projects.

<b>Account Type:</b> {account_type}
<b>User ID:</b> <code>{user_id}</code>
<b>Bot Slots:</b> {len(await get_user_projects(user_id))} / {"Unlimited" if user_id in admin_ids else slot_limit}

Choose an option below to continue.
"""
    await message.answer(welcome_text, reply_markup=get_main_keyboard(user_id), disable_web_page_preview=True)

# ---------- New Project ----------
@dp.message(Command("newproject"))
async def cmd_new_project(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        await message.answer("🔒 Bot locked.")
        return

    slot_limit = await get_user_slot_limit(user_id)
    projects = await get_user_projects(user_id)

    if len(projects) >= slot_limit:
        await message.answer(
            f"❌ <b>Limit Reached!</b>\nYour limit: <b>{slot_limit}</b>\n"
            f"Delete a project or buy a slot."
        )
        return

    await state.set_state(ProjectCreation.awaiting_name)
    await message.answer(
        "✍️ Enter project name (letters, numbers, hyphens):\n\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(ProjectCreation.awaiting_name, F.text, ~F.text.startswith('/'))
async def fsm_project_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    project_name = message.text.strip().lower().replace(" ", "-")

    if not all(c.isalnum() or c == '-' for c in project_name) or len(project_name) > 30:
        await message.answer("❌ Invalid name. Use letters/numbers/hyphens (max 30).")
        return

    slot_limit = await get_user_slot_limit(user_id)
    projects = await get_user_projects(user_id)

    if len(projects) >= slot_limit:
        await message.answer(f"❌ Limit reached ({len(projects)}/{slot_limit}).")
        await state.clear()
        return

    try:
        async with get_db_conn() as conn:
            await conn.execute(
                "INSERT INTO projects (user_id, project_name, approval_status, created_at) VALUES (?, ?, ?, ?)",
                (user_id, project_name, "pending", datetime.now().isoformat())
            )
            await conn.commit()

        (PROJECTS_DIR / str(user_id) / project_name).mkdir(parents=True, exist_ok=True)

        await state.update_data(project_name=project_name)
        await state.set_state(ProjectCreation.awaiting_file)
        await message.answer(
            f"✅ Project <b>{project_name}</b> created!\n\n"
            "Upload <code>.py</code> or <code>.zip</code> (max 50MB).\nSend /cancel to abort."
        )
    except aiosqlite.IntegrityError:
        await message.answer(f"❌ You already have <code>{project_name}</code>.")
    except Exception as e:
        logger.error(f"create project error: {e}", exc_info=True)
        await message.answer(f"❌ Error: {e}")
        await state.clear()

@dp.message(ProjectCreation.awaiting_file, F.document)
async def fsm_project_file(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data.get('project_name')

    if not project_name:
        await message.answer("❌ Session expired. Use /newproject.")
        await state.clear()
        return

    document = message.document

    # 50MB limit
    if document.file_size and document.file_size > 50 * 1024 * 1024:
        await message.answer("❌ File too large (max 50MB).")
        return

    file_ext = os.path.splitext(document.file_name)[1].lower()
    if file_ext not in ['.py', '.zip']:
        await message.answer("❌ Only .py or .zip allowed.")
        return

    user_folder = PROJECTS_DIR / str(user_id)
    project_folder = user_folder / project_name
    file_path = project_folder / document.file_name
    status_msg = await message.answer(f"📥 <b>Downloading {document.file_name}...</b>")

    try:
        await bot.download(document, destination=file_path)
        main_file = None
        run_command = None

        if file_ext == '.zip':
            await status_msg.edit_text(f"📦 <b>Extracting {document.file_name}...</b>")

            # FIX: Safe extraction (ZIP slip protection)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                members = zip_ref.infolist()
                if len(members) > 1000:
                    raise ValueError("ZIP contains too many entries (maximum 1000).")
                expanded_size = sum(max(0, item.file_size) for item in members)
                if expanded_size > 200 * 1024 * 1024:
                    raise ValueError("ZIP expands beyond the 200 MB safety limit.")
                root = project_folder.resolve()
                for member in members:
                    member_path = (root / member.filename).resolve()
                    if member_path != root and root not in member_path.parents:
                        raise ValueError(f"Unsafe path in ZIP: {member.filename}")
                    if member.is_dir():
                        continue
                    if member.filename.startswith("/") or "\\" in member.filename:
                        raise ValueError("ZIP contains an invalid path.")
                zip_ref.extractall(root)
                extracted_files = [i.filename for i in members if not i.is_dir()]

            if not extracted_files:
                raise ValueError("ZIP is empty.")

            # Handle single root folder
            top_parts = [Path(f).parts for f in extracted_files if Path(f).parts]
            common_prefix = top_parts[0][0] + "/" if top_parts and all(len(parts) > 1 and parts[0] == top_parts[0][0] for parts in top_parts) else ""
            if common_prefix:
                src_dir = project_folder / common_prefix.rstrip("/")
                if src_dir.exists() and src_dir.is_dir():
                    for item in list(src_dir.iterdir()):
                        target = project_folder / item.name
                        if target.exists():
                            if target.is_dir():
                                shutil.rmtree(target, ignore_errors=True)
                            else:
                                target.unlink()
                        shutil.move(str(item), str(project_folder))
                    try:
                        src_dir.rmdir()
                    except Exception:
                        pass

            for main in ['main.py', 'bot.py', 'app.py', 'index.py', 'demo.py']:
                if (project_folder / main).exists():
                    main_file = main
                    break

            req_path = project_folder / "requirements.txt"
            if req_path.exists():
                err = await install_dependencies(user_folder, status_msg, requirements_file=req_path)
                if err:
                    raise Exception(err)
            else:
                await create_venv_if_not_exists(user_folder)

            try:
                file_path.unlink()
            except Exception:
                pass

        elif file_ext == '.py':
            await status_msg.edit_text(f"🐍 <b>Scanning {document.file_name}...</b>")
            main_file = document.file_name
            file_content = file_path.read_text(encoding='utf-8', errors='ignore')
            dependencies = parse_imports(file_content) - STANDARD_LIBS
            err = await install_dependencies(user_folder, status_msg, dependencies=dependencies)
            if err:
                raise Exception(err)

        run_command = f'python "{main_file}"' if main_file else "python main.py"

        async with get_db_conn() as conn:
            await conn.execute(
                "UPDATE projects SET main_file = ?, run_command = ?, approval_status = ? WHERE user_id = ? AND project_name = ?",
                (main_file, run_command, "pending", user_id, project_name)
            )
            await conn.commit()

        await increment_stat('total_uploads', 1)

        await status_msg.edit_text(f"⏳ <b>Upload Received</b>\nProject: <code>{project_name}</code>\nStatus: Pending admin approval.")
        await state.clear()
        approval_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_project:{user_id}:{project_name}"),
             InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_project:{user_id}:{project_name}")]
        ])
        notice = f"📥 <b>Project Approval Required</b>\nUser ID: <code>{user_id}</code>\nProject: <code>{project_name}</code>\nFile: <code>{document.file_name}</code>"
        for admin_id in list(admin_ids):
            try:
                await bot.send_message(admin_id, notice, reply_markup=approval_kb)
            except Exception as notify_error:
                logger.warning("Could not notify admin %s: %s", admin_id, notify_error)
        await message.answer("Your upload is awaiting admin approval. You will be notified after review.", reply_markup=get_main_keyboard(user_id))

    except Exception as e:
        logger.error(f"file handling error {project_name}: {e}", exc_info=True)
        await status_msg.edit_text("❌ <b>Upload failed.</b>\nThe file could not be processed. Check the file format and try again. The technical error was logged for admin review.")
        await delete_project_from_db(user_id, project_name)
        shutil.rmtree(project_folder, ignore_errors=True)
        await state.clear()


# ---------- Premium Contact and Admin Approval ----------
@dp.callback_query(F.data == "premium_contact")
async def callback_premium_contact(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    premium_slots = await get_user_total_premium_slots(user_id)
    account_type = "Premium" if premium_slots > 0 else "Free"
    contact = OWNER_CONTACT_URL if OWNER_CONTACT_URL.startswith(("https://t.me/", "http://t.me/")) else "https://t.me/your_admin_username"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Contact Admin", url=contact)],
        [InlineKeyboardButton(text="Account Details", callback_data="view_quota")],
        [InlineKeyboardButton(text="Back", callback_data="main_menu_back")]
    ])
    await callback.message.answer(f"💎 <b>Premium Plans</b>\n\nUser ID: <code>{user_id}</code>\nAccount Type: <b>{account_type}</b>\nCurrent Premium Slots: <b>{premium_slots}</b>\n\nContact the admin to request Premium activation.", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("approve_project:"))
async def callback_approve_project(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("Admin only.", show_alert=True)
    try:
        _, uid_text, project_name = callback.data.split(":", 2)
        uid = int(uid_text)
        project = await get_project_details(uid, project_name)
        if not project:
            return await callback.answer("Project not found.", show_alert=True)
        async with get_db_conn() as conn:
            await conn.execute("UPDATE projects SET approval_status='approved', status='stopped' WHERE user_id=? AND project_name=?", (uid, project_name))
            await conn.commit()
        await callback.message.edit_text(f"✅ <b>Project Approved</b>\nUser ID: <code>{uid}</code>\nProject: <code>{project_name}</code>")
        await bot.send_message(uid, f"✅ <b>Project Approved</b>\nProject: <code>{project_name}</code> is now available in My Bot.", reply_markup=get_main_keyboard(uid))
        await callback.answer("Approved")
    except Exception as exc:
        logger.exception("Project approval failed")
        await callback.answer("Approval failed; check logs.", show_alert=True)

@dp.callback_query(F.data.startswith("reject_project:"))
async def callback_reject_project(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("Admin only.", show_alert=True)
    try:
        _, uid_text, project_name = callback.data.split(":", 2)
        uid = int(uid_text)
        project_folder = PROJECTS_DIR / str(uid) / project_name
        await delete_project_from_db(uid, project_name)
        shutil.rmtree(project_folder, ignore_errors=True)
        await callback.message.edit_text(f"❌ <b>Project Rejected</b>\nUser ID: <code>{uid}</code>\nProject: <code>{project_name}</code>")
        await bot.send_message(uid, f"❌ Your project <code>{project_name}</code> was rejected by the admin. You can contact support for details.")
        await callback.answer("Rejected")
    except Exception:
        logger.exception("Project rejection failed")
        await callback.answer("Rejection failed; check logs.", show_alert=True)

# ---------- My Projects & Quota ----------
@dp.callback_query(F.data == "my_projects")
async def callback_my_projects(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer()
    projects = await get_user_projects(user_id)

    if not projects:
        text = "You have no projects. Use /newproject."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Create", callback_data="start_new_project")],
            [InlineKeyboardButton(text="📊 Quota", callback_data="view_quota")],
            [InlineKeyboardButton(text="🏠 Main", callback_data="main_menu_back")]
        ])
    else:
        text = "📁 <b>Your Projects:</b>\n\n"
        buttons = []
        for p in projects:
            status = p['status']
            details = await get_project_details(user_id, p['project_name'])
            approval = details['approval_status'] if details else 'pending'
            shown_status = "pending approval" if approval != "approved" else status
            icon = "🟡" if approval != "approved" else ("🟢" if status == 'running' else ("🔴" if status == 'stopped' else "⚠️"))
            text += f"{icon} <code>{p['project_name']}</code> ({shown_status}, {p['ram_tier']})\n"
            buttons.append([InlineKeyboardButton(
                text=f"⚙️ Manage {p['project_name']}",
                callback_data=f"manage_project:{p['project_name']}"
            )])
        buttons.append([InlineKeyboardButton(text="🚀 New Project", callback_data="start_new_project")])
        buttons.append([InlineKeyboardButton(text="📊 Quota", callback_data="view_quota")])
        buttons.append([InlineKeyboardButton(text="🏠 Main", callback_data="main_menu_back")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        await callback.message.answer(text, reply_markup=keyboard)

@dp.callback_query(F.data == "main_menu_back")
async def callback_main_menu_back(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await cmd_start(callback.message, state)
    await callback.answer()

@dp.callback_query(F.data == "start_new_project")
async def callback_start_new_project(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await cmd_new_project(callback.message, state)
    await callback.answer()

@dp.callback_query(F.data == "view_quota")
async def callback_view_quota(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    projects = await get_user_projects(user_id)
    slot_limit = await get_user_slot_limit(user_id)
    premium_slots = await get_user_total_premium_slots(user_id)
    total_purchased = await get_total_purchased_slots()

    if user_id in admin_ids:
        tier_name = "Admin"
        limit_text = f"{len(projects)} / Unlimited"
    elif premium_slots > 0:
        tier_name = "Premium"
        limit_text = f"{len(projects)} / {slot_limit}"
    else:
        tier_name = "Free"
        limit_text = f"{len(projects)} / {slot_limit}"

    text = f"""
📊 <b>Your Quota</b>

<b>{tier_name}</b>
Slots: {limit_text}
Free: {FREE_TIER_SLOTS}
Premium (active): {premium_slots}

<b>Global Premium</b>
Sold: {total_purchased} / {PREMIUM_TIER_SLOTS}
Price: {SLOT_PRICE_STARS} ⭐ per slot
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Buy 1 Slot ({SLOT_PRICE_STARS} ⭐)", callback_data="buy_slot")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="my_projects")],
        [InlineKeyboardButton(text="🏠 Main", callback_data="main_menu_back")]
    ])
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        await callback.message.answer(text, reply_markup=keyboard)
    await callback.answer()

# ---------- Payment ----------
@dp.callback_query(F.data == "buy_slot")
async def callback_buy_slot(callback: types.CallbackQuery):
    total_purchased = await get_total_purchased_slots()
    if total_purchased >= PREMIUM_TIER_SLOTS:
        await callback.answer("❌ All premium slots sold out!", show_alert=True)
        return
    try:
        await callback.message.edit_text(
            f"✨ <b>Checkout</b>\n\nBuy <b>1 project slot</b> for <b>{SLOT_PRICE_STARS} ⭐</b>.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Pay {SLOT_PRICE_STARS} ⭐", callback_data="pay_for_slot")],
                [InlineKeyboardButton(text="🚫 Cancel", callback_data="my_projects")]
            ])
        )
    except Exception:
        pass
    await callback.answer()

@dp.callback_query(F.data == "pay_for_slot")
async def callback_send_star_invoice(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    total_purchased = await get_total_purchased_slots()
    if total_purchased >= PREMIUM_TIER_SLOTS:
        await callback.answer("❌ Sold out!", show_alert=True)
        return

    payload = f"buy_slot_{user_id}_{int(datetime.now().timestamp())}"
    prices = [LabeledPrice(label="1 Project Slot", amount=SLOT_PRICE_STARS)]

    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="Additional Project Slot",
            description="Grants 1 permanent extra project slot.",
            payload=payload,
            currency="XTR",
            prices=prices
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Invoice error: {e}")
        await callback.answer(f"❌ Error: {e}", show_alert=True)

@dp.message(F.successful_payment)
async def handler_successful_payment(message: types.Message):
    user_id = message.from_user.id
    payload = message.successful_payment.invoice_payload

    if payload.startswith("buy_slot_"):
        total_purchased = await get_total_purchased_slots()
        if total_purchased >= PREMIUM_TIER_SLOTS:
            await message.answer("❌ Sold out during payment. Contact admin for refund.")
            await bot.send_message(OWNER_ID, f"🚨 Paid but no slots! User {user_id}")
            return

        try:
            async with get_db_conn() as conn:
                await conn.execute(
                    "UPDATE users SET purchased_slots = purchased_slots + 1 WHERE user_id = ?",
                    (user_id,)
                )
                await conn.commit()
                async with conn.execute("SELECT purchased_slots FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    new_purchased = row[0] if row else 0

            new_total = FREE_TIER_SLOTS + new_purchased
            await message.answer(
                f"✅ <b>Payment Successful!</b>\n\n"
                f"+1 slot added. New limit: <b>{new_total}</b>"
            )
            await bot.send_message(
                OWNER_ID,
                f"💰 <b>Sale!</b>\nUser: <code>{user_id}</code>\n"
                f"Amount: {message.successful_payment.total_amount} ⭐"
            )
        except Exception as e:
            logger.error(f"Payment DB error: {e}")
            await message.answer("✅ Paid, but DB error. Contact owner!")
            await bot.send_message(OWNER_ID, f"🚨 PAYMENT ERROR! User {user_id}")

# ---------- Deployment Menu ----------
async def send_deployment_menu(message: types.Message, user_id: int, project_name: str, edit: bool = False):
    project = await get_project_details(user_id, project_name)
    if not project:
        await message.answer("Error: project not found.")
        return

    status = project['status']
    ram = project['ram_tier']
    run_cmd = project['run_command'] or "N/A"
    icon = "🟢" if status == 'running' else ("🔴" if status == 'stopped' else "⚠️")

    text = f"""
⚙️ <b>{project_name}</b>
Status: {icon} <b>{status.capitalize()}</b>

RAM: {'512MB (Free)' if ram == 'free' else '1024MB (Premium)'}
Command: <code>{run_cmd}</code>
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ Start", callback_data=f"project_start:{project_name}"),
            InlineKeyboardButton(text="🛑 Stop", callback_data=f"project_stop:{project_name}"),
            InlineKeyboardButton(text="🔄 Restart", callback_data=f"project_restart:{project_name}")
        ],
        [
            InlineKeyboardButton(text="📝 Logs", callback_data=f"project_logs:{project_name}"),
            InlineKeyboardButton(text="📊 Usage", callback_data=f"project_usage:{project_name}"),
            InlineKeyboardButton(text="📈 Status", callback_data=f"project_status:{project_name}")
        ],
        [
            InlineKeyboardButton(text="🛠️ Deps", callback_data=f"project_deps:{project_name}"),
            InlineKeyboardButton(text="✏️ Edit Cmd", callback_data=f"project_run_cmd:{project_name}")
        ],
        [InlineKeyboardButton(text="🗑️ Delete", callback_data=f"project_delete:{project_name}")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="my_projects")],
        [InlineKeyboardButton(text="🏠 Main", callback_data="main_menu_back")]
    ])

    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await message.answer(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)

@dp.callback_query(F.data.startswith("manage_project:"))
async def callback_manage_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_deployment_menu(callback.message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_start:"))
async def callback_start_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer(f"Starting {project_name}...")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await start_project(user_id, project_name, callback.message)
    await send_deployment_menu(callback.message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_stop:"))
async def callback_stop_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer(f"Stopping {project_name}...")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await stop_project(user_id, project_name, callback.message)
    await send_deployment_menu(callback.message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_restart:"))
async def callback_restart_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer(f"Restarting {project_name}...")
    try:
        await callback.message.delete()
    except Exception:
        pass

    script_key = f"{user_id}_{project_name}"
    if script_key in bot_scripts:
        await stop_project(user_id, project_name, callback.message)
        await asyncio.sleep(2)
    await start_project(user_id, project_name, callback.message)
    await send_deployment_menu(callback.message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_logs:"))
async def callback_view_logs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    log_path = PROJECTS_DIR / str(user_id) / project_name / "deploy.log"

    if not log_path.exists():
        await callback.answer("❌ No log file. Start the project first.", show_alert=True)
        return
    try:
        await callback.answer("Sending...")
        await callback.message.answer_document(
            FSInputFile(log_path),
            caption=f"Logs: <b>{project_name}</b>"
        )
    except Exception as e:
        await callback.message.answer(f"❌ Error: {e}")

@dp.callback_query(F.data.startswith("project_usage:"))
async def callback_view_usage(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    script_key = f"{user_id}_{project_name}"

    process = bot_scripts.get(script_key)
    if not process:
        await callback.answer("Project not running.", show_alert=True)
        return
    try:
        p = psutil.Process(process.pid)
        cpu = p.cpu_percent(interval=0.1)
        mem_mb = p.memory_info().rss / (1024 * 1024)
        text = f"📊 <b>{project_name}</b>\n\nCPU: {cpu:.2f}%\nRAM: {mem_mb:.2f} MB"
        await callback.answer(text, show_alert=True)
    except psutil.NoSuchProcess:
        await callback.answer("Process gone.", show_alert=True)
        await cleanup_script(user_id, project_name, new_status="crashed")
    except Exception as e:
        await callback.answer(f"Error: {e}", show_alert=True)

@dp.callback_query(F.data.startswith("project_status:"))
async def callback_project_status(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    script_key = f"{user_id}_{project_name}"
    project = await get_project_details(user_id, project_name)
    db_status = project['status'] if project else 'unknown'
    in_mem = script_key in bot_scripts
    alive = False

    if in_mem:
        try:
            process = bot_scripts[script_key]
            if psutil.Process(process.pid).is_running():
                alive = True
        except psutil.NoSuchProcess:
            alive = False

    if alive and db_status == 'running':
        await callback.answer("✅ Running (synced)", show_alert=True)
    elif not alive and db_status == 'running':
        await callback.answer("⚠️ Crashed! Syncing...", show_alert=True)
        await cleanup_script(user_id, project_name, new_status="crashed")
        await send_deployment_menu(callback.message, user_id, project_name, edit=True)
    else:
        await callback.answer(f"Status: {db_status}", show_alert=True)

@dp.callback_query(F.data.startswith("project_run_cmd:"))
async def callback_edit_run_command(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    await state.set_state(ProjectManagement.awaiting_run_command)
    await state.update_data(project_name=project_name)
    try:
        await callback.message.edit_text(
            f"Enter new run command for <b>{project_name}</b>:\n"
            "e.g., <code>python main.py</code>\n\n/cancel to abort.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
            ])
        )
    except Exception:
        pass

@dp.message(ProjectManagement.awaiting_run_command, F.text, ~F.text.startswith('/'))
async def fsm_set_run_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data.get('project_name')
    run_command = message.text.strip()

    if not (run_command.lower().startswith("python ") or run_command.lower().startswith("python3 ")):
        await message.answer("❌ Must start with `python ` or `python3 `.")
        return

    try:
        async with get_db_conn() as conn:
            await conn.execute(
                "UPDATE projects SET run_command = ? WHERE user_id = ? AND project_name = ?",
                (run_command, user_id, project_name)
            )
            await conn.commit()

        await message.answer(f"✅ Updated! Restart to apply.")
        await state.clear()
        await send_deployment_menu(message, user_id, project_name)
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
        await state.clear()

@dp.callback_query(F.data.startswith("project_deps:"))
async def callback_install_deps(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    await state.set_state(ProjectManagement.awaiting_deps_install)
    await state.update_data(project_name=project_name, prompt_message_id=callback.message.message_id)
    try:
        await callback.message.edit_text(
            f"Enter packages for <b>{project_name}</b>:\n"
            "e.g., <code>requests pandas</code> or <code>-r requirements.txt</code>\n\n/cancel to abort.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
            ])
        )
    except Exception:
        pass

@dp.message(ProjectManagement.awaiting_deps_install, F.text, ~F.text.startswith('/'))
async def fsm_install_deps(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data.get('project_name')
    prompt_message_id = data.get('prompt_message_id')
    user_folder = PROJECTS_DIR / str(user_id)
    project_folder = user_folder / project_name

    status_msg = None
    try:
        status_msg = await bot.edit_message_text(
            f"⚙️ <b>Installing for {project_name}...</b>",
            chat_id=message.chat.id,
            message_id=prompt_message_id
        )
        try:
            await message.delete()
        except Exception:
            pass

        args = message.text.split()
        if "-r" in args:
            req_file_name = args[args.index("-r") + 1]
            req_path = project_folder / req_file_name
            if not req_path.exists():
                raise Exception(f"<code>{req_file_name}</code> not found.")
            err = await install_dependencies(user_folder, status_msg, requirements_file=req_path)
        else:
            err = await install_dependencies(user_folder, status_msg, dependencies=set(args))

        if err:
            raise Exception(err)

        await status_msg.edit_text(f"✅ <b>Installed!</b> Restart to apply.")
        await state.clear()
        await send_deployment_menu(message, user_id, project_name)
    except Exception as e:
        if status_msg:
            try:
                await status_msg.edit_text(f"❌ <b>Failed!</b>\n\n{e}")
            except Exception:
                pass
        else:
            await message.answer(f"❌ <b>Failed!</b>\n\n{e}")
        await state.clear()

# ---------- Delete ----------
@dp.callback_query(F.data.startswith("project_delete:"))
async def callback_delete_project(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes", callback_data=f"confirm_delete:{project_name}")],
        [InlineKeyboardButton(text="❌ No", callback_data=f"cancel_delete:{project_name}")]
    ])
    try:
        await callback.message.edit_text(
            f"⚠️ Delete <b>{project_name}</b>? Cannot be undone.",
            reply_markup=keyboard
        )
    except Exception:
        pass
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete:"))
async def callback_confirm_delete(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    await callback.answer()
    try:
        await callback.message.edit_text(f"Deleting <b>{project_name}</b>...")
    except Exception:
        pass
    try:
        await stop_project(user_id, project_name, callback.message)
    except Exception:
        pass
    try:
        await delete_project_from_db(user_id, project_name)
        project_folder = PROJECTS_DIR / str(user_id) / project_name
        if project_folder.exists():
            shutil.rmtree(project_folder, ignore_errors=True)
        await callback.message.answer(f"✅ Deleted <b>{project_name}</b>.")
    except Exception as e:
        await callback.message.answer(f"❌ Error: {e}")
    finally:
        await state.clear()

@dp.callback_query(F.data.startswith("cancel_delete:"))
async def callback_cancel_delete(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_deployment_menu(callback.message, user_id, project_name)
    await callback.answer()

# ---------- Admin Panel ----------
def get_admin_panel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 User Stats", callback_data="admin_user_stats"),
            InlineKeyboardButton(text="📁 Project Stats", callback_data="admin_project_stats")
        ],
        [
            InlineKeyboardButton(text="🚀 Running", callback_data="admin_running_scripts"),
            InlineKeyboardButton(text="📊 Analytics", callback_data="admin_analytics")
        ],
        [
            InlineKeyboardButton(text="➕ Add Admin", callback_data="admin_add_admin"),
            InlineKeyboardButton(text="➖ Remove Admin", callback_data="admin_remove_admin")
        ],
        [
            InlineKeyboardButton(text="🚫 Ban", callback_data="admin_ban_user"),
            InlineKeyboardButton(text="✅ Unban", callback_data="admin_unban_user")
        ],
        [
            InlineKeyboardButton(text="➕ Add Premium", callback_data="admin_add_premium"),
            InlineKeyboardButton(text="➖ Remove Premium", callback_data="admin_remove_premium")
        ],
        [
            InlineKeyboardButton(text="📋 Premium List", callback_data="admin_premium_list"),
            InlineKeyboardButton(text="⚙️ System", callback_data="admin_system_status")
        ],
        [
            InlineKeyboardButton(text=f"🔒 Lock ({'On' if bot_locked else 'Off'})", callback_data="admin_lock_bot"),
            InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast")
        ],
        [
            InlineKeyboardButton(text="💾 Backup DB", callback_data="admin_backup_db"),
            InlineKeyboardButton(text="🔄 Restart", callback_data="admin_restart_bot")
        ],
        [InlineKeyboardButton(text="🏠 Main", callback_data="main_menu_back")]
    ])

@dp.callback_query(F.data == "admin_panel")
async def callback_admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
    try:
        await callback.message.edit_text(
            "👑 <b>Admin Panel</b>\n\nSelect an option:",
            reply_markup=get_admin_panel_keyboard()
        )
    except Exception:
        await callback.message.answer(
            "👑 <b>Admin Panel</b>",
            reply_markup=get_admin_panel_keyboard()
        )
    await callback.answer()

@dp.callback_query(F.data == "admin_user_stats")
async def callback_admin_user_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    async with get_db_conn() as conn:
        async with conn.execute("SELECT COUNT(*) FROM users") as cursor:
            total = (await cursor.fetchone())[0]
        async with conn.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1") as cursor:
            banned = (await cursor.fetchone())[0]
        async with conn.execute("SELECT SUM(purchased_slots) FROM users") as cursor:
            purchased = (await cursor.fetchone())[0] or 0

    text = f"""
👥 <b>User Stats</b>
Total: {total}
Banned: {banned}
Active: {total - banned}
Premium slots: {purchased}/{PREMIUM_TIER_SLOTS}
"""
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_project_stats")
async def callback_admin_project_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    async with get_db_conn() as conn:
        async with conn.execute("SELECT COUNT(*) FROM projects") as cursor:
            total = (await cursor.fetchone())[0]
        async with conn.execute("SELECT COUNT(*) FROM projects WHERE status = 'running'") as cursor:
            running = (await cursor.fetchone())[0]
        async with conn.execute(
            "SELECT user_id, COUNT(*) as c FROM projects GROUP BY user_id ORDER BY c DESC LIMIT 5"
        ) as cursor:
            top_users = await cursor.fetchall()

    text = f"📁 <b>Project Stats</b>\n\nTotal: {total}\nRunning: {running}\n\n<b>Top Users:</b>\n"
    for uid, c in top_users:
        text += f"• <code>{uid}</code>: {c}\n"
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_running_scripts")
async def callback_admin_running_scripts(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")

    if not bot_scripts:
        text = "🚀 <b>Running</b>\n\nNone."
    else:
        text = f"🚀 <b>Running ({len(bot_scripts)})</b>\n\n"
        for key, proc in bot_scripts.items():
            try:
                uid, pname = key.split("_", 1)
                p = psutil.Process(proc.pid)
                mem = p.memory_info().rss / (1024 * 1024)
                text += f"🔸 <code>{pname}</code> | User: <code>{uid}</code> | PID: {proc.pid} | RAM: {mem:.1f}MB\n"
            except Exception as e:
                text += f"🔸 <code>{key}</code> (Err: {e})\n"

    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_analytics")
async def callback_admin_analytics(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    async with get_db_conn() as conn:
        async with conn.execute("SELECT COUNT(*) FROM users") as cursor:
            total_users = (await cursor.fetchone())[0]
        async with conn.execute("SELECT COUNT(*) FROM projects") as cursor:
            total_projects = (await cursor.fetchone())[0]

    text = f"""
📊 <b>Analytics</b>

Uploads: {bot_stats.get('total_uploads', 0)}
Runs: {bot_stats.get('total_runs', 0)}
Users: {total_users}
Projects: {total_projects}
Running: {len(bot_scripts)}

Banned: {len(banned_users)}
Admins: {len(admin_ids)}
Status: {'🔒 Locked' if bot_locked else '✅ Active'}
"""
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_system_status")
async def callback_admin_system_status(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await callback.answer("Fetching...")
    cpu = await to_thread(psutil.cpu_percent, 0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage('/')

    text = f"""
⚙️ <b>System</b>

CPU: {cpu}%
RAM: {mem.percent}% ({mem.available/(1024**3):.1f}GB free)
Disk: {disk.percent}% ({disk.free/(1024**3):.1f}GB free)
"""
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())

@dp.callback_query(F.data == "admin_lock_bot")
async def callback_admin_lock_bot(callback: types.CallbackQuery):
    global bot_locked
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    bot_locked = not bot_locked
    await callback.answer(f"{'🔒 Locked' if bot_locked else '🔓 Unlocked'}!", show_alert=True)
    try:
        await callback.message.edit_reply_markup(reply_markup=get_admin_panel_keyboard())
    except Exception:
        pass

@dp.callback_query(F.data == "admin_backup_db")
async def callback_admin_backup_db(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    try:
        backup_path = DATA_DIR / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        async with get_db_conn() as conn:
            async with aiosqlite.connect(backup_path) as backup_conn:
                await conn.backup(backup_conn)

        await callback.answer("✅ Backed up!", show_alert=True)
        await callback.message.answer_document(
            FSInputFile(backup_path),
            caption=f"💾 Backup: {backup_path.name}"
        )
        backup_path.unlink()
    except Exception as e:
        await callback.answer(f"❌ {e}", show_alert=True)

@dp.callback_query(F.data == "admin_restart_bot")
async def callback_admin_restart_bot(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("❌ Owner only!", show_alert=True)
    await callback.message.answer("🔄 Restarting... (all scripts stop)")
    for key in list(bot_scripts.keys()):
        try:
            uid, pname = key.split("_", 1)
            await cleanup_script(int(uid), pname, new_status="stopped")
        except Exception:
            pass
    await state.clear()
    try:
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await callback.message.answer(f"❌ Restart failed: {e}")

# ---------- Premium ----------
@dp.callback_query(F.data == "admin_premium_list")
async def callback_admin_premium_list(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    async with get_db_conn() as conn:
        now = datetime.now().isoformat()
        async with conn.execute(
            "SELECT user_id, slots, expiry_date FROM premium_subscriptions WHERE expiry_date > ? ORDER BY expiry_date",
            (now,)
        ) as cursor:
            subs = await cursor.fetchall()

    if not subs:
        text = "📋 No active subscriptions."
    else:
        text = "📋 <b>Active Premium</b>\n\n"
        for s in subs:
            text += f"👤 <code>{s['user_id']}</code> | {s['slots']} slots | {s['expiry_date']}\n"

    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_add_premium")
async def callback_admin_add_premium_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_premium_user_id)
    await callback.message.edit_text(
        "Enter User ID:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_premium_user_id, F.text, ~F.text.startswith('/'))
async def fsm_premium_user_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        await state.update_data(premium_user_id=user_id)
        await state.set_state(AdminFSM.awaiting_premium_days)
        await message.answer("Days until expiry (e.g., 30):")
    except ValueError:
        await message.answer("❌ Invalid ID.")

@dp.message(AdminFSM.awaiting_premium_days, F.text, ~F.text.startswith('/'))
async def fsm_premium_days(message: types.Message, state: FSMContext):
    try:
        days = int(message.text)
        if days <= 0:
            raise ValueError
        await state.update_data(premium_days=days)
        await state.set_state(AdminFSM.awaiting_premium_slots)
        await message.answer("Number of slots:")
    except ValueError:
        await message.answer("❌ Invalid number.")

@dp.message(AdminFSM.awaiting_premium_slots, F.text, ~F.text.startswith('/'))
async def fsm_premium_slots(message: types.Message, state: FSMContext):
    try:
        slots = int(message.text)
        if slots < 10:
            await message.answer("Minimum Premium allocation is 10 bot slots. Enter 10 or more.")
            return
        data = await state.get_data()
        user_id = data['premium_user_id']
        days = data['premium_days']
        expiry = (datetime.now() + timedelta(days=days)).isoformat()
        now = datetime.now().isoformat()

        async with get_db_conn() as conn:
            await conn.execute(
                'INSERT INTO premium_subscriptions (user_id, slots, expiry_date, created_at) VALUES (?, ?, ?, ?)',
                (user_id, slots, expiry, now)
            )
            await conn.commit()

        await message.answer(f"✅ Added {slots} Premium bot slots to <code>{user_id}</code> until {expiry}.")
        try:
            await bot.send_message(user_id, f"💎 <b>Premium Activated</b>\nAccount Type: Premium\nUser ID: <code>{user_id}</code>\nBot Slots: {slots}\nExpires: {expiry}", reply_markup=get_main_keyboard(user_id))
        except Exception as notify_error:
            logger.warning("Premium notification failed for %s: %s", user_id, notify_error)
        await state.clear()
    except ValueError:
        await message.answer("❌ Invalid slots.")

@dp.callback_query(F.data == "admin_remove_premium")
async def callback_admin_remove_premium_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_remove_premium_id)
    await callback.message.edit_text(
        "Enter User ID to remove premium:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_remove_premium_id, F.text, ~F.text.startswith('/'))
async def fsm_remove_premium(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        async with get_db_conn() as conn:
            now = datetime.now().isoformat()
            async with conn.execute(
                "SELECT id FROM premium_subscriptions WHERE user_id = ? AND expiry_date > ?",
                (user_id, now)
            ) as cursor:
                subs = await cursor.fetchall()
            if not subs:
                await message.answer("❌ No active subs.")
                await state.clear()
                return
            await conn.execute("DELETE FROM premium_subscriptions WHERE id = ?", (subs[0][0],))
            await conn.commit()
        await message.answer(f"✅ Removed from {user_id}.")
        await state.clear()
    except ValueError:
        await message.answer("❌ Invalid ID.")

# ---------- Broadcast ----------
@dp.callback_query(F.data == "admin_broadcast")
async def callback_admin_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_broadcast)
    await callback.message.edit_text(
        "Enter broadcast message:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_broadcast, F.text, ~F.text.startswith('/'))
async def fsm_admin_broadcast(message: types.Message, state: FSMContext):
    await state.clear()
    async with get_db_conn() as conn:
        async with conn.execute("SELECT user_id FROM users WHERE is_banned = 0") as cursor:
            all_users = await cursor.fetchall()

    status_msg = await message.answer(f"📢 Broadcasting to {len(all_users)} users...")
    sent = 0
    failed = 0
    for (uid,) in all_users:
        try:
            await bot.send_message(uid, f"📢 <b>Announcement:</b>\n\n{message.text}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await status_msg.edit_text(f"✅ Done!\nSent: {sent}\nFailed: {failed}")

# ---------- Admin Management ----------
@dp.callback_query(F.data == "admin_add_admin")
async def callback_admin_add_admin(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_add_admin_id)
    await callback.message.edit_text(
        "Enter User ID to add as admin:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_add_admin_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_add_admin(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        async with get_db_conn() as conn:
            await conn.execute(
                'INSERT OR IGNORE INTO users (user_id, join_date, is_admin) VALUES (?, ?, ?)',
                (user_id, datetime.now().isoformat(), 1)
            )
            await conn.execute('UPDATE users SET is_admin = 1 WHERE user_id = ?', (user_id,))
            await conn.commit()
        admin_ids.add(user_id)
        await message.answer(f"✅ <code>{user_id}</code> is now admin.")
    except ValueError:
        await message.answer("❌ Invalid ID.")
    finally:
        await state.clear()

@dp.callback_query(F.data == "admin_remove_admin")
async def callback_admin_remove_admin(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("❌ Owner only!")
    await state.set_state(AdminFSM.awaiting_remove_admin_id)
    await callback.message.edit_text(
        "Enter User ID to remove from admin:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_remove_admin_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_remove_admin(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id == OWNER_ID:
            await message.answer("❌ Cannot remove owner.")
            return
        async with get_db_conn() as conn:
            await conn.execute('UPDATE users SET is_admin = 0 WHERE user_id = ?', (user_id,))
            await conn.commit()
        admin_ids.discard(user_id)
        await message.answer(f"✅ <code>{user_id}</code> removed from admin.")
    except ValueError:
        await message.answer("❌ Invalid ID.")
    finally:
        await state.clear()

# ---------- Ban / Unban ----------
@dp.callback_query(F.data == "admin_ban_user")
async def callback_admin_ban_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_ban_id)
    await callback.message.edit_text(
        "Enter User ID to ban:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_ban_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_ban_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id in admin_ids:
            await message.answer("❌ Cannot ban admin.")
            return
        await state.update_data(ban_user_id=user_id)
        await state.set_state(AdminFSM.awaiting_ban_reason)
        await message.answer("Enter ban reason:")
    except ValueError:
        await message.answer("❌ Invalid ID.")

@dp.message(AdminFSM.awaiting_ban_reason, F.text, ~F.text.startswith('/'))
async def fsm_admin_ban_reason(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        user_id = data['ban_user_id']
        reason = message.text
        async with get_db_conn() as conn:
            await conn.execute(
                'INSERT OR IGNORE INTO users (user_id, join_date, is_banned) VALUES (?, ?, ?)',
                (user_id, datetime.now().isoformat(), 1)
            )
            await conn.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
            await conn.commit()
        banned_users.add(user_id)
        await message.answer(f"🚫 <code>{user_id}</code> banned. Reason: {reason}")
    except Exception as e:
        await message.answer(f"❌ Error: {e}")
    finally:
        await state.clear()

@dp.callback_query(F.data == "admin_unban_user")
async def callback_admin_unban_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_unban_id)
    await callback.message.edit_text(
        "Enter User ID to unban:\n\n/cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(AdminFSM.awaiting_unban_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_unban_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        async with get_db_conn() as conn:
            await conn.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
            await conn.commit()
        banned_users.discard(user_id)
        await message.answer(f"✅ <code>{user_id}</code> unbanned.")
    except ValueError:
        await message.answer("❌ Invalid ID.")
    finally:
        await state.clear()

# ---------- Web Server (for Render/Replit uptime) ----------
async def web_server():
    app = web.Application()

    async def handle(request):
        return web.Response(text="🚀 Python Project Hoster Bot is running!")

    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"🌐 Web server on port {port}")

# ---------- Main ----------
async def main():
    logger.info("🚀 Starting Bot...")
    await init_db()
    await migrate_db()
    await load_global_data()

    logger.info("Resetting project statuses...")
    async with get_db_conn() as conn:
        await conn.execute("UPDATE projects SET status = 'stopped' WHERE status = 'running'")
        await conn.commit()

    asyncio.create_task(web_server())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")