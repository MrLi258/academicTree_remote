from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urljoin
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# DrissionPage（需要：pip install DrissionPage）
from DrissionPage import Chromium, ChromiumOptions


BASE_URL = "https://academictree.org/"
INDEX_SELECTOR = 'table[align="center"] td[valign="top"] a'
COUNT_SELECTOR = 'div[class="boxfloat_clear"]:nth-child(2) div:nth-child(2) b:nth-child(1)'


@dataclass(frozen=True)
class SubfieldResult:
    name: str
    url: str
    author_count: Optional[int]
    error: Optional[str] = None


def _extract_first_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d[\d,]*)", text)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def _new_browser(headless: bool = False) -> Chromium:
    co = ChromiumOptions()
    # 你可按需打开无头：python script.py --headless
    if headless:
        co.headless(True)
    # 适度减少被拦/提升稳定性（可按需要删减）
    co.set_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    return Chromium(co)


def get_subfield_links(page) -> list[tuple[str, str]]:
    page.get(BASE_URL)
    # 等页面基础 DOM 就绪（DrissionPage 一般 get 后可直接取元素；这里保守一点）
    page.wait.load_start()

    anchors = page.eles("css:" + INDEX_SELECTOR) or []
    pairs: list[tuple[str, str]] = []
    print(f"anchors count: {len(anchors)}")
    for a in anchors:
        href = (a.attr("href") or "").strip()
        if not href:
            continue
        url = urljoin(BASE_URL, href)
        name = (a.text or "").strip() or url
        pairs.append((name, url))

    # 按 URL 去重（同一子领域可能在多个位置出现）
    seen_url = set()
    deduped: list[tuple[str, str]] = []
    for name, url in pairs:
        if url in seen_url:
            continue
        seen_url.add(url)
        deduped.append((name, url))
    return deduped


def fetch_author_count_for_subfield(browser, lock_new_tab: threading.Lock, name: str, url: str) -> SubfieldResult:
    tab = None
    try:
        # 多线程环境下新建 tab 用锁保护（参考你另一个文件的写法）
        with lock_new_tab:
            tab = browser.new_tab("about:blank")

        tab.get(url)
        tab.wait.load_start()

        b = tab.ele("css:" + COUNT_SELECTOR, timeout=10)
        if not b:
            return SubfieldResult(name=name, url=url, author_count=None, error="count node not found (selector changed?)")

        raw = (b.text or "").strip()
        n = _extract_first_int(raw)
        if n is None:
            return SubfieldResult(name=name, url=url, author_count=None, error=f"no int in text: {raw!r}")

        return SubfieldResult(name=name, url=url, author_count=n)
    except Exception as e:
        return SubfieldResult(name=name, url=url, author_count=None, error=str(e))
    finally:
        try:
            if tab:
                tab.close()
        except Exception:
            pass


def sum_all_subfield_authors(headless: bool = False, max_workers: int = 8) -> tuple[int, list[SubfieldResult]]:
    browser = _new_browser(headless=headless)
    try:
        page = browser.latest_tab
        subfields = get_subfield_links(page)

        results: list[SubfieldResult] = []
        total = 0

        lock_new_tab = threading.Lock()

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [
                ex.submit(fetch_author_count_for_subfield, browser, lock_new_tab, name, url)
                for name, url in subfields
            ]
            for fut in as_completed(futs):
                res = fut.result()
                results.append(res)
                if res.author_count is not None:
                    total += res.author_count

        results.sort(key=lambda r: (r.name or "", r.url))
        return total, results
    finally:
        try:
            browser.quit()
        except Exception:
            pass


def main(argv: Optional[Iterable[str]] = None) -> int:
    argv = list(argv or sys.argv[1:])

    headless = False
    if "--headless" in argv:
        headless = True
        argv = [x for x in argv if x != "--headless"]

    # 新增：--workers N
    max_workers = 8
    if "--workers" in argv:
        try:
            i = argv.index("--workers")
            max_workers = int(argv[i + 1])
            argv.pop(i + 1)
            argv.pop(i)
        except Exception:
            pass

    total, results = sum_all_subfield_authors(headless=headless, max_workers=max_workers)

    ok = [r for r in results if r.author_count is not None]
    bad = [r for r in results if r.author_count is None]

    print(f"子领域数量: {len(results)}")
    print(f"成功: {len(ok)} 失败: {len(bad)}")
    print(f"作者数量总和(按子领域页统计): {total}")

    if bad:
        print("\n失败明细（需要检查 selector/页面结构/加载时序）：")
        for r in bad:
            print(f"- {r.name} | {r.url} | error={r.error}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
