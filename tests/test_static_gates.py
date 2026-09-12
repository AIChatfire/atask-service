"""静态门禁：把「靠自觉」的纪律做成机械断言。

为什么要有这一组用例（而不是只写在 AGENTS.md 里）：本项目的红线大多无法靠
运行时测试覆盖——「tasks 表 SQL 只在 taskstore」「配置只有一个入口」「凭据不进
仓库」这类约定，违反之后**功能照常工作**，只有下一个人读代码时才发现。所以把
它们变成 `pytest` 用例，让 `make check` 与 CI 自动拦截。

比照 stask-service 的同类做法（其 `tests/test_misc.py` 的静态门禁段）。覆盖类别
（**刻意不写具体条数**：条数随迭代变化，写死就会出现「文档说八条、实际十四条」
这种自我漂移）：ruff 洁净、源码与文档无 emoji、原始 SQL 只在数据访问单点、
`os.environ` 只在一处例外、全仓无真实凭据、services 无孤儿公开函数、无孤儿异常
类、配置项与 `.env.example` 对齐、配置键名不带前缀（`env_prefix` 保持为空），
以及门禁自身的扫描范围非空。

**每条「扫描类」门禁都配一条范围断言**（`test_gates_scan_non_empty`、
`test_secret_scan_covers_whole_repo`、`test_exception_scan_is_non_vacuous`）。
这不是冗余：「扫到 0 个文件 → 0 个违规 → 绿灯」是这类断言最典型的失效形态，
而且它**看起来就是绿的**。本项目真实发生过一次——凭据门禁最初只扫
`.env.example` 一个文件，于是 `AI_TODO.md` 里的明文密钥安然存活一个月
（详见第 6 节注释）。

新增模块若合理地需要突破某条门禁，**必须往对应白名单里加条目并写明理由**——
白名单条目本身就是这条纪律的例外登记册。
"""

from __future__ import annotations

import ast
import pathlib
import re
import subprocess
import sys
from collections.abc import Iterator

ROOT = pathlib.Path(__file__).resolve().parent.parent
LINT_PATHS = ["app", "tests", "scripts", "gunicorn.conf.py"]

# ---------------------------------------------------------------------------
# 1. ruff 洁净（CI 与 Makefile 同样跑，此处让 `pytest` 单跑也会拦）
# ---------------------------------------------------------------------------


def test_ruff_clean():
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", *LINT_PATHS],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, f"ruff 发现问题：\n{result.stdout}{result.stderr}"


# ---------------------------------------------------------------------------
# 2/3. 无 emoji
# ---------------------------------------------------------------------------
# 规则目的：源码与文档要能被 grep / 终端 / 日志管线无损处理。
#
# 范围：源码（app / tests / scripts / gunicorn.conf.py）+ 仓库根级文档 + docs 全部 markdown。
# 例外登记（**每条必须写理由**）：
#   docs/SPEC.md —— 其文件头第 3 行自述为「旧 adapter 代架构」的历史文档，
#   与现实现严重漂移（描述 22 个已不存在的模块），正在按实现重写。
#   重写完成前不纳入门禁：让一条「即将消失」的旧文档常年把门禁染红，
#   只会训练人忽略它。重写后从例外表移除。

#: 文档 emoji 门禁的例外登记（**条目必须写明理由**）。
#: 当前为空：`docs/SPEC.md` 曾是唯一例外，理由是「自述历史文档，与现实现漂移，
#: 待重写后移出例外」。2026-09-12 该文档已按 stask-service 的契约体例重写，
#: 文中引用的模块/路径全部指向真实存在的文件，故**移出例外**。
#: 保留这个机制（而不是删掉）是为了下一次真的不得不豁免时仍有地方登记理由——
#: 否则人会直接去放宽扫描范围，而范围一旦放宽就再也收不回来。
_EMOJI_DOC_EXEMPT: dict[str, str] = {}

_EMOJI = re.compile(
    "["
    "\U0001F000-\U0001FAFF"   # 各类符号与图形
    "\U00002600-\U000027BF"   # 杂项符号 / 装饰符号
    "\U0001F1E6-\U0001F1FF"   # 区域指示符（国旗）
    "\u2b00-\u2bff"           # 杂项符号与箭头
    "\u2705\u274c\u26a0\u2757\u2753"  # 对勾 / 叉 / 警告 / 叹号 / 问号
    "]"
)


def _scan_emoji(paths: list[pathlib.Path]) -> list[str]:
    hits: list[str] = []
    for path in paths:
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _EMOJI.search(line):
                hits.append(f"{path.relative_to(ROOT)}:{lineno}: {line.strip()[:80]}")
    return hits


def test_no_emoji_in_source():
    """源码（app / tests / scripts / gunicorn.conf.py）不得含 emoji。"""
    paths: list[pathlib.Path] = []
    for entry in LINT_PATHS:
        target = ROOT / entry
        paths.extend(target.rglob("*.py") if target.is_dir() else [target])
    hits = _scan_emoji(paths)
    assert not hits, "源码含 emoji：\n" + "\n".join(hits)


def test_no_emoji_in_docs():
    """文档（仓库根级 *.md + docs 下全部 *.md）不得含 emoji，例外须登记理由。"""
    paths = [
        *(ROOT.glob("*.md")),
        *(ROOT / "docs").rglob("*.md"),
    ]
    paths = [
        p for p in paths
        if p.relative_to(ROOT).as_posix() not in _EMOJI_DOC_EXEMPT
    ]
    hits = _scan_emoji(paths)
    assert not hits, "文档含 emoji：\n" + "\n".join(hits)


# ---------------------------------------------------------------------------
# 4. 原始 SQL 只在数据访问单点
# ---------------------------------------------------------------------------
# 纪律（AGENTS.md）：tasks 表原生 SQL 的唯一归属是 app/services/taskstore.py。
# 散落到别处会绕过时间列归一（as_unix_seconds / _secs）与 platform 过滤，
# 而这两条正是共享表上最贵的两个坑（毫秒混写、误动别人的行）。

_SQL_ONLY_IN = {"app/services/taskstore.py"}
#: 只认「带表名的语句」——这既是本纪律真正的风险面，也让非 SQL 误报自然出局：
#: 例如就绪探针的 ``SELECT 1``（无 FROM）与普通字面量里的 ``select`` 单词。
_SQL_STMT = re.compile(
    r"^\s*(?:"
    r"SELECT\s[\s\S]*?\sFROM\s"
    r"|INSERT\s+INTO\s"
    r"|UPDATE\s+\w+\s"
    r"|DELETE\s+FROM\s"
    r")",
    re.I,
)


def _docstring_nodes(tree: ast.Module) -> set[int]:
    """模块 / 类 / 函数的 docstring 节点 id 集合（这些字符串不算 SQL 字面量）。

    docstring 里写「本模块不做 SELECT ... 」是注释，不是 SQL。
    """
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = getattr(node, "body", None)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                found.add(id(body[0].value))
    return found


def _string_literals(tree: ast.Module) -> Iterator[tuple[ast.AST, str]]:
    """产出代码里的字符串字面量（排除 docstring，含 f-string 的静态片段）。

    f-string 在 AST 里是 JoinedStr 而非 Constant——SQL 常用 f-string 拼列名
    （如 ``taskstore.patch_data``），漏掉它等于给门禁开一个后门。
    """
    docstrings = _docstring_nodes(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) not in docstrings:
                yield node, node.value
        elif isinstance(node, ast.JoinedStr):
            parts = [
                part.value
                for part in node.values
                if isinstance(part, ast.Constant) and isinstance(part.value, str)
            ]
            if parts:
                yield node, "".join(parts)


def test_raw_sql_only_in_taskstore():
    offenders: list[str] = []
    for path in sorted((ROOT / "app").rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in _SQL_ONLY_IN:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node, text in _string_literals(tree):
            if _SQL_STMT.search(text):
                offenders.append(f"{rel}:{node.lineno}: {text.strip()[:70]!r}")
    assert not offenders, (
        "tasks 表 SQL 出现在数据访问单点之外（请下沉到 app/services/taskstore.py）：\n"
        + "\n".join(offenders)
    )


# ---------------------------------------------------------------------------
# 5. os.environ 只在一处例外
# ---------------------------------------------------------------------------
# 纪律（app/config.py 文件头）：配置只有一个入口 `from app.config import settings`。
# 例外登记（**每条必须写理由**——与 _ENV_EXAMPLE_ALLOWED_MISSING 的「值=理由」
# 风格一致）：
#   gunicorn.conf.py —— 在 pydantic 单例之前由 master 进程加载，此时
#     app.config 尚不存在，只能直读 os.environ。
# 这条白名单**只有一项**：任何新增条目都意味着「又多了一个绕过配置单例的地方」，
# 加之前先确认真的没有别的办法（历史教训：曾为「扫描遗留 GW_ 前缀变量」开过第二项，
# 那是纯兼容性代码，已随「无需兼容旧版本」的决策删除）。
_ENVIRON_ONLY_IN: dict[str, str] = {
    "gunicorn.conf.py": "master 进程在 pydantic 单例之前加载，必须直读 os.environ",
}


def test_os_environ_only_in_gunicorn():
    offenders: list[str] = []
    candidates = [
        *sorted((ROOT / "app").rglob("*.py")),
        *(ROOT / entry for entry in LINT_PATHS if not (ROOT / entry).is_dir()),
    ]
    for path in candidates:
        rel = path.relative_to(ROOT).as_posix()
        if rel in _ENVIRON_ONLY_IN:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "os"
                and node.attr in {"environ", "getenv"}
            ):
                offenders.append(f"{rel}:{node.lineno}")
    assert not offenders, (
        "散读 os.environ（请走 app.config.settings）：\n" + "\n".join(offenders)
    )


# ---------------------------------------------------------------------------
# 6. 仓库不含真实凭据（**全仓**扫描，不止 .env.example）
# ---------------------------------------------------------------------------
# 血案一：.env.example 曾把生产 MySQL 公网口令、回调验签密钥、logfire token
#   一起提交进仓库并推上远端——凭据一旦入库，改文件不等于止损，必须轮换。
# 血案二（2026-09-12 发现）：上一版门禁**只扫 .env.example 这一个文件**，于是
#   AI_TODO.md 里三条 curl 示例中的上游明文密钥（mk- 开头）安然存活了一个月，
#   同样被推上远端。教训：密钥门禁的作用域必须是「仓库」而不是「某个文件」——
#   作用域过窄的门禁给的是**虚假的安全感**，比没有门禁更危险，因为它让人以为
#   已经查过了（同名门禁在 .env.example 上一直是绿的）。
#
# 判定策略：不看「像密钥的词」，而看**「不像散文的令牌」**。测试夹具是英文单词
#   拼接（形如 sk- + 可读单词），真实密钥是随机串（形如 mk- + 32 位大小写数字
#   混排）——后者稳定地含较多数字与大写字母。用这个特征区分，就不必把测试夹具
#   逐个塞进白名单（白名单越长，门禁越容易被绕过，也越容易被人当成噪音而忽视）。
#
# 注意：本文件**不得复述任何真实凭据**（哪怕是当反面教材引用）。第一版写完就
#   被这条门禁自己抓出来——注释里为了举例把泄露的密钥原样抄了一遍，等于新造
#   了一份副本。举反例时只描述形态，不写值。

_PLACEHOLDER_OK = {
    "", "user", "password", "root", "replace_me", "change-me", "change_me",
    "replace_me_openssl_rand_hex_16", "replace_me_pylf_v1_...",
}

#: 扫描时跳过的目录（构建产物、缓存、虚拟环境、本机记忆）
_SCAN_SKIP_DIRS = {
    ".git", ".venv", "__pycache__", ".workbuddy", ".ruff_cache",
    ".mypy_cache", ".pytest_cache", ".idea", "node_modules", "dist", "build",
}

#: 跳过的非文本后缀（二进制/字体/压缩包/锁文件）
_SCAN_SKIP_SUFFIXES = {
    ".pyc", ".pyo", ".so", ".dylib", ".png", ".jpg", ".jpeg", ".gif", ".ico",
    ".webp", ".woff", ".woff2", ".ttf", ".eot", ".zip", ".gz", ".tgz", ".whl",
    ".mp4", ".pdf", ".lock",
}

#: 真实凭据的唯一正确归宿是 `.env`（gitignore）或进程环境变量。
#: 因此除 `.env.example` 外的 `.env*` 一律不扫——否则开发者本机放一个真
#: `.env` 就会让测试变红，门禁会立刻被人绕过或删除。
_SCAN_SKIP_ENV_FILES = True

_DSN_PASSWORD = re.compile(r"://[^:@/\s]+:([^@/\s]+)@")
_TOKEN_PREFIX = re.compile(r"\b(?:sk|mk|pk|rk)-([A-Za-z0-9_-]{12,})\b")
_HEX_BLOB = re.compile(r"\b[0-9a-fA-F]{32,}\b")
_LOGFIRE_TOKEN = re.compile(r"\bpylf_[A-Za-z0-9_]{8,}")


def _looks_like_secret(payload: str) -> bool:
    """令牌是否「不像散文」：数字与大写字母足够多。

    真实随机密钥普遍满足；英文单词拼接的测试夹具（real-upstream-key）不满足。
    """
    digits = sum(ch.isdigit() for ch in payload)
    uppers = sum(ch.isupper() for ch in payload)
    return (digits >= 6 and uppers >= 2) or digits >= 12


def _scannable_files() -> list[pathlib.Path]:
    """仓库内全部文本文件（跳过 .git / 缓存 / 虚拟环境 / 本机记忆 / 真 .env）。"""
    files: list[pathlib.Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        if any(part in _SCAN_SKIP_DIRS for part in rel.parts):
            continue
        if path.suffix.lower() in _SCAN_SKIP_SUFFIXES:
            continue
        if _SCAN_SKIP_ENV_FILES and path.name.startswith(".env") and path.name != ".env.example":
            continue
        files.append(path)
    return files


def _mask(text: str) -> str:
    """只露首 4 位。失败信息本身会被抓进 CI 日志，原样回显等于二次泄露。"""
    return f"{text[:4]}…(len={len(text)})" if len(text) > 4 else "…"


def _secret_offenders() -> list[str]:
    offenders: list[str] = []
    for path in _scannable_files():
        rel = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            for label, candidate in _line_candidates(line):
                lowered = candidate.lower()
                if lowered in _PLACEHOLDER_OK:
                    continue
                if "replace" in lowered or "change" in lowered:
                    continue
                offenders.append(f"{rel}:{lineno}: {label} -> {_mask(candidate)}")
    return offenders


def _line_candidates(line: str) -> list[tuple[str, str]]:
    """单行里的疑似凭据：(标签, 用于判定的候选串)。"""
    found: list[tuple[str, str]] = []
    for match in _DSN_PASSWORD.finditer(line):
        found.append(("DSN 中的明文口令", match.group(1).strip()))
    for match in _TOKEN_PREFIX.finditer(line):
        payload = match.group(1)
        if _looks_like_secret(payload):
            found.append(("疑似上游/用户密钥", payload))
    for match in _HEX_BLOB.finditer(line):
        blob = match.group(0)
        if _looks_like_secret(blob):
            found.append(("32 位以上十六进制串（疑似密钥）", blob))
    for match in _LOGFIRE_TOKEN.finditer(line):
        token = match.group(0)
        if _looks_like_secret(token):
            found.append(("logfire token", token))
    return found


def test_repo_has_no_real_secrets():
    """全仓不得含真实凭据（血案一 + 血案二，见上方注释）。"""
    offenders = _secret_offenders()
    assert not offenders, (
        "仓库疑似含真实凭据（改占位符并**轮换已泄露的值**；"
        "密钥只应存在 .env 或进程环境变量里）：\n" + "\n".join(offenders)
    )


def test_secret_scan_covers_whole_repo():
    """扫描范围必须覆盖全仓——「扫 1 个文件 → 0 违规 → 绿灯」是这条门禁
    最致命的失效形态（正是血案二的成因），所以单独钉一条范围断言。"""
    scanned = {p.relative_to(ROOT).as_posix() for p in _scannable_files()}
    assert len(scanned) > 50, f"只扫到 {len(scanned)} 个文件，扫描范围可疑"
    # 锚点只钉**必须存在**的文件。历史锚点 AI_TODO.md（血案二的载体）已随项目精简
    # 从仓库移除；若继续钉它，这条断言会因一次合理的文件退役而永久变红，最终被人
    # 删掉——那才是真正的退化。锚在不会消失的配置面文件上，才守得住「范围没有变窄」。
    for anchor in (".env.example", "docker-compose.yml", "gunicorn.conf.py", "README.md"):
        assert anchor in scanned, f"扫描范围未覆盖 {anchor}，密码门禁已退化"



# ---------------------------------------------------------------------------
# 7. services 无孤儿公开函数
# ---------------------------------------------------------------------------
# 重构（拆模块 / 换调用方）最容易留下的残渣是「没人调的函数」——它不报错、
# 不降覆盖率，只是持续误导读者。此门禁用 AST 找出 app/services 下所有
# 无调用方的公开函数；确需保留的往白名单加条目并写明理由。

_ORPHAN_ALLOWLIST: dict[str, str] = {
    # 形如 "函数名": "为什么没人直接调它"。目前为空——services 下公开函数全有调用方。
}


def _defs_in_services() -> dict[str, list[str]]:
    defs: dict[str, list[str]] = {}
    for path in sorted((ROOT / "app" / "services").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                if node.name.startswith("_"):
                    continue
                defs.setdefault(node.name, []).append(
                    f"{path.relative_to(ROOT).as_posix()}:{node.lineno}"
                )
    return defs


def _corpus() -> list[str]:
    return [
        path.read_text(encoding="utf-8")
        for root in ("app", "tests")
        for path in (ROOT / root).rglob("*.py")
    ]


def test_no_orphan_service_functions():
    texts = _corpus()
    orphans: list[str] = []
    for name, where in sorted(_defs_in_services().items()):
        if name in _ORPHAN_ALLOWLIST:
            continue
        name_re = re.compile(rf"\b{re.escape(name)}\b")
        def_re = re.compile(rf"\b(?:async\s+)?def\s+{re.escape(name)}\b")
        occurrences = sum(len(name_re.findall(text)) for text in texts)
        definitions = sum(len(def_re.findall(text)) for text in texts)
        # 只有「出现次数不超过定义次数」才判定为无调用方（同名复用会拉高计数，
        # 宁可漏报也不误报——门禁的假阳性会让人学会忽略它）
        if occurrences <= definitions:
            orphans.append(f"{name}  (定义于 {', '.join(where)})")
    assert not orphans, (
        "app/services 下存在无调用方的公开函数（请删除，或加入白名单并写明理由）：\n"
        + "\n".join(orphans)
    )


# ---------------------------------------------------------------------------
# 7b. 自定义异常类不得是孤儿
# ---------------------------------------------------------------------------
# 与上一条同一思路：定义了却没人抛的异常，通常是重构后的残留（原本该抛的分支
# 被删了、异常类忘了删）。危害是误导——后来者会以为这条失败路径有专门的错误
# 类型，照着不存在的契约写处理逻辑。
#
# 例外：作为**基类**被继承的分类异常不算孤儿（分类基类本身可能不直接抛）。
#
# 顺便记录一条相关约定（详见本仓库 ADR-009）：本项目**不引入统一异常基类**。
# 因为异常的处置是逐点决策的——同一类失败在不同链路上要做的事不同（重试 /
# 判死 / 释槽 / 清会话），继承层级帮不上忙。就近定义（谁抛谁定义）让归属一眼可见。
# ADR-010 后新链路的失败分流已收敛为三档（见 tests/test_batch_failure_tiers.py），
# 但「谁抛谁定义、不为分类而建基类」这条约定不变。


def _exception_classes() -> dict[str, tuple[str, list[str]]]:
    """app/ 下定义的自定义异常类：名字 → (定义位置, 基类名列表)。"""
    defined: dict[str, tuple[str, list[str]]] = {}
    for path in sorted((ROOT / "app").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        where = path.relative_to(ROOT).as_posix()
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            bases = [ast.unparse(base) for base in node.bases]
            if any(base == "Exception" or base.endswith("Error") for base in bases):
                defined[node.name] = (f"{where}:{node.lineno}", bases)
    return defined


def _raised_names() -> set[str]:
    """app/ 下所有 raise 的目标名（``raise X(...)`` 与 ``raise X from e`` 都算）。"""
    raised: set[str] = set()
    for path in sorted((ROOT / "app").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Raise) or node.exc is None:
                continue
            exc = node.exc
            if isinstance(exc, ast.Call) and isinstance(exc.func, ast.Name):
                raised.add(exc.func.id)
            elif isinstance(exc, ast.Name):
                raised.add(exc.id)
    return raised


def test_no_orphan_exception_classes():
    """定义的异常类必须被 raise，或作为基类被继承（防重构残留）。"""
    defined = _exception_classes()
    raised = _raised_names()
    subclassed = {
        base.split(".")[-1] for _, (_, bases) in defined.items() for base in bases
    }
    orphans = [
        f"{name}  (定义于 {where})"
        for name, (where, _) in sorted(defined.items())
        if name not in raised and name not in subclassed
    ]
    assert not orphans, (
        "app/ 下存在从未被 raise、也未被继承的异常类"
        "（请删除，或加入白名单并写明理由）：\n" + "\n".join(orphans)
    )


def test_exception_scan_is_non_vacuous():
    """范围非空——「扫 0 个类 → 0 个孤儿 → 绿灯」是这类断言最典型的假绿。"""
    assert len(_exception_classes()) >= 3, "没扫到自定义异常类，扫描逻辑可疑"
    assert len(_raised_names()) >= 5, "没扫到任何 raise，扫描逻辑可疑"


# ---------------------------------------------------------------------------
# 7c. 本地私有目录不得纳入版本控制
# ---------------------------------------------------------------------------
# 反例后果（真实的死角，不是假想）：第 6 节的凭据门禁**按设计跳过 `.workbuddy/`**——
# 因为本机 AI 记忆里记录着**已泄露凭据的值**（当初为轮换可追溯而留），若不跳过，
# 门禁会天天报自己的笔记。于是出现一个反向风险：**「门禁不扫它」不等于「它安全」**，
# 只等于**没有任何东西会阻止它被提交**。一次 `git add -A` 就能把这些文件推上远端，
# 而凭据门禁全程保持全绿（它按设计不看这个目录）。
#
# 所以这里单独钉一条：这些目录下**不得有被跟踪的文件**，且**必须被 gitignore 覆盖**。
# 两道都要有——光靠忽略规则，一次 `git add -f` 就绕过；光靠「当前没被跟踪」，
# 新克隆的仓库里也没人拦。
_LOCAL_PRIVATE_DIRS = (".workbuddy", ".idea")


def test_local_private_dirs_are_not_tracked():
    """`.workbuddy/` 与 `.idea/` 不得被 git 跟踪、且必须被忽略（见上方注释）。"""
    tracked = subprocess.run(
        ["git", "ls-files", "--", *_LOCAL_PRIVATE_DIRS],
        cwd=ROOT, capture_output=True, text=True,
    ).stdout.split()
    assert not tracked, (
        "以下本地/私有文件被纳入了版本控制——**凭据门禁按设计不扫这些目录**，"
        "所以它们一旦入库不会被任何门禁拦住：\n  " + "\n  ".join(tracked)
    )
    for entry in _LOCAL_PRIVATE_DIRS:
        ignored = subprocess.run(
            ["git", "check-ignore", "-q", entry], cwd=ROOT, capture_output=True
        )
        assert ignored.returncode == 0, (
            f"{entry} 未被 .gitignore 覆盖——下一次 `git add -A` 会把它整个提交上去"
        )


# ---------------------------------------------------------------------------
# 8. 配置字段与 .env.example 对齐
# ---------------------------------------------------------------------------

#: 有意不在样例里出现的字段（条目必须写理由）
_ENV_EXAMPLE_ALLOWED_MISSING: dict[str, str] = {
    "bind": "由进程管理器决定（compose 内固定 0.0.0.0:8000，gunicorn 读 BIND）",
}


def test_env_example_covers_settings():
    """`.env.example` 必须是配置项的可信清单：允许缺样例的字段需显式登记。

    反例后果：新增 ``XXX`` 却忘了写进样例 → 运维照样例部署，功能静默不生效
    （本项目已经有「配了却不会生效」的历史包袱，见 OPEN-DECISIONS）。
    """
    from app.config import Settings

    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    missing = [
        field
        for field in Settings.model_fields
        if f"{field.upper()}=" not in text and field not in _ENV_EXAMPLE_ALLOWED_MISSING
    ]
    assert not missing, (
        "以下配置项在 .env.example 中没有样例（新增配置必须同步样例）：\n  "
        + "\n  ".join(sorted(missing))
    )


def test_env_example_has_no_prefixed_keys():
    """``.env.example`` 全文不得再含已废弃的 ``GW_`` 前缀。

    反例后果：改了一半——代码去掉了前缀、样例仍留 ``GW_`` 键，运维照样例部署
    得到一套**静默失效**的配置（网关用默认值启动，错误现场离「变量名写错了」
    很远）。这条门禁保证样例与 ``env_prefix=""`` 的事实一致。
    """
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    hits = [
        f".env.example:{lineno}: {line.strip()[:80]}"
        for lineno, line in enumerate(text.splitlines(), 1)
        if "GW_" in line
    ]
    assert not hits, "`.env.example` 仍含已废弃的 GW_ 前缀键：\n" + "\n".join(hits)


def test_settings_env_prefix_removed():
    """``Settings`` 不得再设置 ``env_prefix``。

    反例后果：有人「顺手」把 ``env_prefix="GW_"`` 加回来，全部无前缀变量瞬间
    失效、配置静默走默认值——这正是本次去前缀要根治的问题。
    """
    from app.config import Settings

    assert Settings.model_config.get("env_prefix", "") in ("", None)


# ---------------------------------------------------------------------------
# 9. 路由注册顺序：通配 /batch/{path:path} 必须最后
# ---------------------------------------------------------------------------
# 规则目的：Starlette 的路由匹配是「注册顺序 = 首匹配优先级」，而不是「最具体
# 优先」。ADR-010 后唯一带变量的路径路由是 ``/batch/{path:path}``（通配），它必须
# 排在字面前缀路由（``/healthz/*``、``/ops/*``、``/admin/*``）之后，否则会吞掉
# 它们——请求静默落到错误的处理器上，且不会有任何报错提示。
#
# 判据：用 AST 取 `app/main.py` 里 `include_router(<name>)` 的**名字序列**再比较
# index——不写死 `grep -n` 行号，因为行号会随任何一次无关编辑漂移：门禁要么假绿
# （行号移位后仍指向旧位置），要么假红（注释插一行就炸），两种结局都是被人删掉。

_ROUTER_MUST_BE_LAST = "batch_task_router"          # /batch/{path:path} 通配


def _include_router_names() -> list[str]:
    """按**源码顺序**取 `app/main.py` 里 `include_router(<name>)` 的路由名。

    只认第一个位置参数是裸名字（`ast.Name`）的调用；显式按 ``lineno`` 排序，
    不依赖 `ast.walk` 的遍历序（对同级语句恰好是源码序，但那属于实现细节，
    不该拿来当判据）。
    """
    tree = ast.parse((ROOT / "app" / "main.py").read_text(encoding="utf-8"))
    calls: list[tuple[int, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "include_router"):
            continue
        if node.args and isinstance(node.args[0], ast.Name):
            calls.append((node.lineno, node.col_offset, node.args[0].id))
    calls.sort()
    return [name for _, _, name in calls]


def test_router_mount_order():
    """通配 `batch_task_router`（``/batch/{path:path}``）必须最后注册。

    反例后果：通配若排在字面前缀路由之前，``/ops/*`` 与 ``/admin/*`` 会被当成
    其后的可变路径吞掉，管理面静默失效——请求照常返回，只是走到了错误的处理器。
    """
    names = _include_router_names()
    # 范围断言（本项目对扫描类门禁的固定纪律）：取到 0 个名字 → 0 个违规 →
    # 绿灯，是这类断言最典型的假绿；必须单独钉住扫描范围非空。
    assert len(names) >= 4, f"只取到 {len(names)} 个 include_router，扫描逻辑可疑：{names}"

    assert _ROUTER_MUST_BE_LAST in names, f"{_ROUTER_MUST_BE_LAST} 未注册"
    assert names.index(_ROUTER_MUST_BE_LAST) == len(names) - 1, (
        f"{_ROUTER_MUST_BE_LAST}（通配 /batch/{{path:path}}）必须最后注册，"
        f"否则会吞掉其后的字面前缀路由。实际注册序：{names}"
    )


# ---------------------------------------------------------------------------
# 9b. Redis 打桩清单必须覆盖全部消费方
# ---------------------------------------------------------------------------
# 反例后果：`tests/conftest.py` 的 `patch_redis` 清单是**手工维护**的。新增模块
# import 了 `app.redis` 却忘了登记，没挂该夹具的用例就会**静默打到真 Redis**：
# 本地恰好有 Redis 时绿、CI 没有时莫名红，或者更糟——用例跑在真实状态上却报绿
# （即「测试虚假绿灯」）。历史上这份清单已经漂移过三次：`app.main`、
# `services.dynconf`、`services.relayflow` 都被漏掉过，其中 relayflow 是写用例
# 的人自己另加了一个局部夹具才绕过去——**靠人绕过去而不是靠门禁挡住，就是缺口**。

_REDIS_PATCH_FUNCTION = "patch_redis"


def _redis_importers() -> set[str]:
    """`app/` 下**在模块顶层**导入 `r` 的模块的点分名。

    只看模块顶层语句，这一条判据的松紧是**由 patch 手段的能力边界决定**的：
    `monkeypatch.setattr(module, "r", ...)` 要求 `module.r` 已经存在，而函数内
    嵌套导入（本例：`app.main` 在 `lifespan()` 里 `from app.redis import r`）
    不会在模块上留下该属性 —— 按名字强塞会直接 `AttributeError` **炸掉整套测试**
    （实测：把 app.main 塞进清单后 187 条用例报错）。门禁必须与手段能力一致，
    否则它会逼着人写出一个坏配置。
    """
    importers: set[str] = set()
    for path in sorted((ROOT / "app").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:  # 仅模块顶层，不含函数/类内部的嵌套导入
            if isinstance(node, ast.ImportFrom) and node.module == "app.redis":
                if any(alias.name == "r" for alias in node.names):
                    importers.add(
                        ".".join(path.relative_to(ROOT).with_suffix("").parts)
                    )
    return importers


def _patched_redis_modules() -> set[str]:
    """conftest 里 `patch_redis` 夹具实际打桩到的模块名集合。"""
    tree = ast.parse((ROOT / "tests" / "conftest.py").read_text(encoding="utf-8"))
    patched: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == _REDIS_PATCH_FUNCTION:
            for inner in ast.walk(node):
                if isinstance(inner, ast.For) and isinstance(inner.iter, ast.Tuple):
                    patched.update(ast.unparse(elt) for elt in inner.iter.elts)
    return patched


def test_redis_patch_list_covers_importers():
    """每个 import `app.redis` 的模块都必须出现在 conftest 的 patch_redis 清单里。"""
    importers = _redis_importers()
    patched = _patched_redis_modules()
    # 两侧都要有范围断言：「0 个导入者 → 0 个遗漏 → 绿灯」是这条门禁的假绿形态
    assert len(importers) >= 5, f"只扫到 {len(importers)} 个 redis 消费方，扫描逻辑可疑"
    assert len(patched) >= 5, f"只从 conftest 取到 {len(patched)} 个打桩模块，解析可疑"
    missing = sorted(importers - patched)
    assert not missing, (
        "以下模块 import 了 app.redis，但 conftest 的 patch_redis 清单没覆盖——"
        "未挂该夹具的用例会静默打到真 Redis：\n  " + "\n  ".join(missing)
    )


# ---------------------------------------------------------------------------
# 10. 门禁自身的存在性检查
# ---------------------------------------------------------------------------


def test_gates_scan_non_empty():
    """扫描范围非空——「扫 0 个文件 → 0 个违规 → 绿灯」是这类断言最典型的假绿。"""
    assert list((ROOT / "app").rglob("*.py")), "app/ 下没扫到任何 py 文件"
    assert list((ROOT / "app" / "services").rglob("*.py")), "services 下没扫到任何 py 文件"
    assert list((ROOT / "docs" / "decisions").glob("*.md")), "docs/decisions 下没有决策文档"
    assert (ROOT / ".env.example").exists()
    assert (ROOT / "gunicorn.conf.py").exists()
