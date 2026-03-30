from __future__ import annotations

import sys
import asyncio
import re
from typing import Optional
from urllib.parse import (
	urljoin,
	urlparse,
	parse_qsl,
	urlencode,
	urlunparse,
)

import certifi
import httpx
from bs4 import BeautifulSoup, Tag
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# Windows 정책 충돌 방지: import 전에 미리 체크 (필요시)
if sys.platform.startswith("win"):
    try:
        asyncio.get_event_loop_policy()
    except:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="Minutes Crawl API", version="0.6.0")


USER_AGENT = (
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
	"AppleWebKit/537.36 (KHTML, like Gecko) "
	"Chrome/122.0.0.0 Safari/537.36"
)

FILE_EXTENSIONS = ("pdf", "hwp", "hwpx", "doc", "docx", "xls", "xlsx", "zip")
MEETING_NAME_KEYWORDS = (
	"본회의",
	"위원회",
	"특별위원회",
	"운영위원회",
	"행정재경위원회",
	"복지건설위원회",
	"예산결산특별위원회",
	"행정사무감사",
	"행정사무조사",
)
GENERIC_AGENDA_LINE_HINTS = (
	"조례안",
	"개정조례안",
	"제정조례안",
	"동의안",
	"의견제시의건",
	"의견제시의 건",
	"승인안",
	"예산안",
	"결산안",
	"계획안",
	"보고의건",
	"보고의 건",
	"보고서",
	"선임안",
	"청원",
	"건의안",
	"결의안",
)


# =========================
# Request / Response Model
# =========================

class GeneralCrawlRequest(BaseModel):
	list_url: HttpUrl = Field(..., description="회의록 목록 페이지 URL")
	list_root_selector: str = Field(..., description="목록 최상위 루트 CSS selector")
	item_selector: str = Field(..., description="목록 item CSS selector")
	target_selector: str = Field(..., description="각 item 내부 상세 진입 대상 CSS selector")
	ssl_mode: str = Field(..., description="SSL 검증 정책: Y | N")


class RegexCrawlRequest(GeneralCrawlRequest):
	mtgnm_regex: Optional[str] = Field(None, description="회의명 추출 정규식")
	inquiry_nm_regex: Optional[str] = Field(None, description="의회명 추출 정규식")
	mtr_sj_regex: Optional[str] = Field(None, description="안건 추출 정규식")
	rasmbly_sesn_regex: Optional[str] = Field(None, description="회기 추출 정규식")
	odr_nm_regex: Optional[str] = Field(None, description="차수 추출 정규식")
	mtg_de_regex: Optional[str] = Field(None, description="회의일자 추출 정규식")
	mints_html_regex: Optional[str] = Field(None, description="회의록 HTML 추출 정규식")
	max_pages: int = Field(1000, description="전체 색인 시 최대 탐색 페이지 수")


class OriginalFile(BaseModel):
	file_name: Optional[str] = None
	file_url: Optional[str] = None


class MinutesItem(BaseModel):
	rank: int
	list_title: str

	detail_url: Optional[str] = None
	access_method: str
	open_type: Optional[str] = None
	detail_access_success: bool

	MTGNM: Optional[str] = None
	INQUIRY_NM: Optional[str] = None
	MTR_SJ: Optional[str] = None
	RASMBLY_NUMPR: Optional[str] = None
	RASMBLY_SESN: Optional[str] = None
	ODR_NM: Optional[str] = None
	MTG_DE: Optional[str] = None
	MINTS_HTML: Optional[str] = None
	ORGINL_FILES: list[OriginalFile] = Field(default_factory=list)

	uid: Optional[str] = None

	raw_href: Optional[str] = None
	raw_onclick: Optional[str] = None
	note: Optional[str] = None


class CrawlResponse(BaseModel):
	list_url: str
	item_count: int
	items: list[MinutesItem]


# =========================
# Utility
# =========================

def normalize_text(text: Optional[str]) -> str:
	return re.sub(r"\s+", " ", text or "").strip()


def normalize_line_text(text: Optional[str]) -> str:
	return normalize_text((text or "").replace("\xa0", " "))


def safe_select_one(element, selector: str):
	try:
		return element.select_one(selector)
	except Exception:
		return None


def safe_select(element, selector: str):
	try:
		return element.select(selector)
	except Exception:
		return []


def unique_keep_order(values: list[str]) -> list[str]:
	seen = set()
	result: list[str] = []

	for value in values:
		normalized = normalize_text(value)
		if not normalized:
			continue
		if normalized in seen:
			continue
		seen.add(normalized)
		result.append(normalized)

	return result


def get_verify_options(ssl_mode: str):
	if ssl_mode == "Y":
		return certifi.where()
	if ssl_mode == "N":
		return False
	raise ValueError(f"Invalid SSL mode: {ssl_mode}")


async def fetch_html(url: str, ssl_mode: str) -> str:
	timeout = httpx.Timeout(20.0, connect=10.0)
	headers = {"User-Agent": USER_AGENT}
	verify_option = get_verify_options(ssl_mode)

	async with httpx.AsyncClient(
		headers=headers,
		timeout=timeout,
		follow_redirects=True,
		verify=verify_option,
	) as client:
		response = await client.get(url)
		response.raise_for_status()
		return response.text


async def fetch_html_by_method(
	url: str,
	ssl_mode: str,
	method: str = "GET",
	form_data: Optional[dict[str, str]] = None,
) -> str:
	timeout = httpx.Timeout(20.0, connect=10.0)
	headers = {"User-Agent": USER_AGENT}
	verify_option = get_verify_options(ssl_mode)

	async with httpx.AsyncClient(
		headers=headers,
		timeout=timeout,
		follow_redirects=True,
		verify=verify_option,
	) as client:
		if method.upper() == "POST":
			response = await client.post(url, data=form_data or {})
		else:
			response = await client.get(url)

		response.raise_for_status()
		return response.text


def is_javascript_href(href: Optional[str]) -> bool:
	if not href:
		return False
	return href.strip().lower().startswith("javascript:")


def is_http_like_href(href: Optional[str]) -> bool:
	if not href:
		return False

	lowered = href.strip().lower()
	return (
		lowered.startswith("http://")
		or lowered.startswith("https://")
		or lowered.startswith("/")
		or lowered.startswith("../")
		or lowered.startswith("./")
	)


def is_meaningful_detail_url(detail_url: Optional[str], list_url: str) -> bool:
	if not detail_url:
		return False

	normalized_detail = detail_url.strip()
	normalized_list = list_url.strip()

	if not normalized_detail:
		return False

	if normalized_detail.lower().startswith("javascript:"):
		return False

	if normalized_detail == normalized_list:
		return False

	return True


def extract_filename_from_url(url: str) -> Optional[str]:
	try:
		path = urlparse(url).path
		if not path:
			return None
		name = path.split("/")[-1]
		return normalize_text(name) or None
	except Exception:
		return None


def clean_title_candidate(text: str) -> str:
	value = normalize_text(text)
	value = re.sub(r"\b(회의록|회\s*의\s*록|회의록보기|원문보기)\b", "", value)
	value = normalize_text(value)
	return value


def find_first_regex(text: str, patterns: list[str]) -> Optional[str]:
	for pattern in patterns:
		match = re.search(pattern, text, re.IGNORECASE)
		if match:
			if match.groups():
				return normalize_text(match.group(1))
			return normalize_text(match.group(0))
	return None


def apply_regex(source: str, pattern: Optional[str]) -> Optional[str]:
	if not pattern:
		return None

	try:
		match = re.search(pattern, source, re.IGNORECASE | re.DOTALL)
	except re.error as exc:
		raise ValueError(f"잘못된 정규식입니다: {pattern} / {str(exc)}") from exc

	if not match:
		return None

	if match.groups():
		return normalize_text(match.group(1))

	return normalize_text(match.group(0))


def apply_regex_raw(source: str, pattern: Optional[str]) -> Optional[str]:
	if not pattern:
		return None

	try:
		match = re.search(pattern, source, re.IGNORECASE | re.DOTALL)
	except re.error as exc:
		raise ValueError(f"잘못된 정규식입니다: {pattern} / {str(exc)}") from exc

	if not match:
		return None

	if match.groups():
		return match.group(1)

	return match.group(0)


def strip_html_tags(value: Optional[str]) -> Optional[str]:
	if not value:
		return None

	soup = BeautifulSoup(value, "lxml")
	text = soup.get_text("\n", strip=True)

	lines = [normalize_text(line) for line in text.splitlines()]
	lines = [line for line in lines if line]

	return "".join(lines) if lines else None


def extract_uid(detail_url: Optional[str]) -> Optional[str]:
	if not detail_url:
		return None

	try:
		parsed = urlparse(detail_url)

		query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
		preferred_keys = ["uid", "key", "MINTS_SN", "minutesSn", "minutes_sn", "id", "no", "seq"]

		for preferred_key in preferred_keys:
			for key, value in query_pairs:
				if key == preferred_key and normalize_text(value):
					return normalize_text(value)

		for _, value in query_pairs:
			if normalize_text(value):
				return normalize_text(value)

		path = parsed.path or ""
		match = re.search(r"/(\d+)\.do(?:$|\?)", path)
		if match:
			return match.group(1)

		segments = [seg for seg in path.split("/") if seg]
		if segments:
			last_segment = segments[-1]
			match = re.fullmatch(r"(\d+)", last_segment)
			if match:
				return match.group(1)

		return None

	except Exception:
		return None


def extract_rasmbly_numpr_from_list_row(row_text: str) -> Optional[str]:
	return find_first_regex(
		row_text,
		[
			r"(제\s*\d+\s*대)",
			r"(\d+\s*대)",
		],
	)


def extract_rasmbly_numpr_from_detail_html(detail_html: str) -> Optional[str]:
	return find_first_regex(
		detail_html,
		[
			r"(제\s*\d+\s*대)",
			r"(\d+\s*대)",
		],
	)


def replace_query_param(url: str, param_name: str, param_value: str) -> str:
	parsed = urlparse(url)
	query_pairs = parse_qsl(parsed.query, keep_blank_values=True)

	new_pairs = []
	replaced = False

	for key, value in query_pairs:
		if key == param_name:
			new_pairs.append((key, param_value))
			replaced = True
		else:
			new_pairs.append((key, value))

	if not replaced:
		new_pairs.append((param_name, param_value))

	new_query = urlencode(new_pairs)
	return urlunparse((
		parsed.scheme,
		parsed.netloc,
		parsed.path,
		parsed.params,
		new_query,
		parsed.fragment,
	))


# =========================
# List parsing
# =========================

def extract_list_candidates(
	html: str,
	list_root_selector: str,
	item_selector: str,
	target_selector: str,
	limit: Optional[int] = 5,
) -> list[dict]:
	soup = BeautifulSoup(html, "lxml")

	root = safe_select_one(soup, list_root_selector)
	if not root:
		return []

	items = safe_select(root, item_selector)
	if not items:
		return []

	results: list[dict] = []

	for item in items:
		target = safe_select_one(item, target_selector)
		if not target:
			continue

		title = normalize_text(target.get_text(" ", strip=True))
		href = normalize_text(target.get("href"))
		onclick = normalize_text(target.get("onclick"))
		row_text = normalize_text(item.get_text(" ", strip=True))

		if not title:
			title = row_text

		if not title:
			continue

		results.append({
			"title": title,
			"href": href or None,
			"onclick": onclick or None,
			"row_text": row_text,
			"rasmbly_numpr": extract_rasmbly_numpr_from_list_row(row_text),
		})

	if limit is None:
		return results

	return results[:limit]


# =========================
# Generic detail parsing
# =========================

def extract_text_lines(soup: BeautifulSoup) -> list[str]:
	for bad in soup(["script", "style", "noscript"]):
		bad.decompose()

	raw_lines = soup.get_text("\n").splitlines()
	lines = [normalize_line_text(line) for line in raw_lines]
	lines = [line for line in lines if line]
	return lines


def extract_title_candidates(soup: BeautifulSoup, lines: list[str]) -> list[str]:
	candidates: list[str] = []

	if soup.title:
		candidates.append(normalize_text(soup.title.get_text(" ", strip=True)))

	for h in soup.find_all(["h1", "h2", "h3", "h4", "strong"], limit=20):
		text = normalize_text(h.get_text(" ", strip=True))
		if 2 <= len(text) <= 120:
			candidates.append(text)

	candidates.extend(lines[:20])

	return unique_keep_order(candidates)


def extract_key_value_candidates(soup: BeautifulSoup) -> dict[str, str]:
	extracted: dict[str, str] = {}

	for tr in soup.find_all("tr"):
		th = tr.find("th")
		td = tr.find("td")
		if th and td:
			key = normalize_text(th.get_text(" ", strip=True))
			value = normalize_text(td.get_text(" ", strip=True))
			if key and value and key not in extracted:
				extracted[key] = value

	for dt in soup.find_all("dt"):
		dd = dt.find_next_sibling("dd")
		if dd:
			key = normalize_text(dt.get_text(" ", strip=True))
			value = normalize_text(dd.get_text(" ", strip=True))
			if key and value and key not in extracted:
				extracted[key] = value

	for p in soup.find_all(["p", "li", "div"], limit=300):
		text = normalize_text(p.get_text(" ", strip=True))
		if ":" in text:
			parts = text.split(":", 1)
			key = normalize_text(parts[0])
			value = normalize_text(parts[1])
			if 1 <= len(key) <= 30 and value and key not in extracted:
				extracted[key] = value
		elif "：" in text:
			parts = text.split("：", 1)
			key = normalize_text(parts[0])
			value = normalize_text(parts[1])
			if 1 <= len(key) <= 30 and value and key not in extracted:
				extracted[key] = value

	return extracted


def extract_link_candidates(soup: BeautifulSoup, base_url: str) -> list[dict]:
	links: list[dict] = []

	for a in soup.find_all("a"):
		text = normalize_text(a.get_text(" ", strip=True))
		href = normalize_text(a.get("href"))
		onclick = normalize_text(a.get("onclick"))
		absolute_url = urljoin(base_url, href) if href else None

		links.append({
			"text": text,
			"href": href or None,
			"absolute_url": absolute_url,
			"onclick": onclick or None,
			"attrs": dict(a.attrs),
		})

	return links


def score_main_content_candidate(tag: Tag) -> int:
	if tag.name in ("html", "body", "header", "nav", "aside", "footer", "form"):
		return -10**9

	text = normalize_text(tag.get_text(" ", strip=True))
	if not text:
		return -10**9

	text_len = len(text)
	if text_len < 200:
		return -100000

	p_count = len(tag.find_all("p"))
	br_count = len(tag.find_all("br"))
	a_count = len(tag.find_all("a"))
	table_count = len(tag.find_all("table"))

	score = 0
	score += text_len // 20
	score += p_count * 20
	score += br_count * 4
	score += table_count * 6
	score -= a_count * 3

	noisy_keywords = (
		"로그인", "회원가입", "사이트맵", "검색", "메뉴",
		"처음으로", "이전", "다음", "목록", "프린트", "인쇄",
	)
	for keyword in noisy_keywords:
		if keyword in text:
			score -= 10

	positive_keywords = (
		"개의", "산회", "회의록", "출석의원", "출석공무원",
		"의사일정", "부의된안건", "부의된 안건", "의결 결과",
	)
	for keyword in positive_keywords:
		if keyword in text:
			score += 10

	return score


def extract_main_content_html(soup: BeautifulSoup) -> Optional[str]:
	work_soup = BeautifulSoup(str(soup), "lxml")

	for bad in work_soup(["script", "style", "noscript", "header", "nav", "aside", "footer", "form"]):
		bad.decompose()

	body = work_soup.body
	if not body:
		return None

	best_tag: Optional[Tag] = None
	best_score = -10**9

	for tag in body.find_all(["article", "section", "div", "main", "td"]):
		if not isinstance(tag, Tag):
			continue
		score = score_main_content_candidate(tag)
		if score > best_score:
			best_score = score
			best_tag = tag

	if best_tag is not None and best_score > 0:
		return str(best_tag).strip()

	return None


def extract_inquiry_nm_generic(
	title_candidates: list[str],
	lines: list[str],
	kv_pairs: dict[str, str],
) -> Optional[str]:
	for key, value in kv_pairs.items():
		merged = f"{key} {value}"
		match = re.search(r"([가-힣A-Za-z0-9·\s]+(?:특별시|광역시|도|시|군|구)?의회)", merged)
		if match:
			return normalize_text(match.group(1))

	for text in title_candidates + lines[:100]:
		match = re.search(r"([가-힣A-Za-z0-9·\s]+(?:특별시|광역시|도|시|군|구)?의회)", text)
		if match:
			value = normalize_text(match.group(1))
			value = re.sub(r"(회의록|회\s*의\s*록)$", "", value).strip()
			if value:
				return value

	return None


def extract_rasmbly_numpr_generic(
	title_candidates: list[str],
	lines: list[str],
	kv_pairs: dict[str, str],
) -> Optional[str]:
	sources = list(kv_pairs.keys()) + list(kv_pairs.values()) + title_candidates + lines[:100]
	joined = " ".join(sources)

	return find_first_regex(
		joined,
		[
			r"(제\s*\d+\s*대)",
			r"(\d+\s*대)",
			r"(제\s*\d+\s*회)",
		],
	)


def extract_rasmbly_sesn_generic(
	title_candidates: list[str],
	lines: list[str],
	kv_pairs: dict[str, str],
) -> Optional[str]:
	sources = list(kv_pairs.keys()) + list(kv_pairs.values()) + title_candidates + lines[:100]
	joined = " ".join(sources)

	match = re.search(r"\[([^\]]+)\]", joined)
	if match:
		return normalize_text(match.group(1))

	return find_first_regex(
		joined,
		[
			r"((?:정례회|임시회))",
		],
	)


def extract_odr_nm_generic(
	title_candidates: list[str],
	lines: list[str],
	kv_pairs: dict[str, str],
) -> Optional[str]:
	sources = list(kv_pairs.keys()) + list(kv_pairs.values()) + title_candidates + lines[:100]
	joined = " ".join(sources)

	return find_first_regex(
		joined,
		[
			r"(제\s*\d+\s*차)",
			r"(제\s*\d+\s*호)",
		],
	)


def extract_mtg_de_generic(
	title_candidates: list[str],
	lines: list[str],
	kv_pairs: dict[str, str],
) -> Optional[str]:
	sources = list(kv_pairs.keys()) + list(kv_pairs.values()) + title_candidates + lines[:200]
	joined = " ".join(sources)

	m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", joined)
	if m:
		y, mo, d = m.group(1), m.group(2), m.group(3)
		return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

	m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", joined)
	if m:
		y, mo, d = m.group(1), m.group(2), m.group(3)
		return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

	return None


def extract_mtgnm_generic(
	list_title: str,
	title_candidates: list[str],
	lines: list[str],
) -> Optional[str]:
	sources = [list_title] + title_candidates + lines[:80]

	for text in sources:
		cleaned = clean_title_candidate(text)

		for keyword in MEETING_NAME_KEYWORDS:
			if keyword in cleaned:
				if keyword == "위원회":
					match = re.search(r"([가-힣A-Za-z0-9·\s]+위원회)", cleaned)
					if match:
						return normalize_text(match.group(1))
					continue

				if "본회의" in cleaned:
					return "본회의"

				match = re.search(r"([가-힣A-Za-z0-9·\s]+위원회)", cleaned)
				if match:
					return normalize_text(match.group(1))

				if "특별위원회" in cleaned:
					match = re.search(r"([가-힣A-Za-z0-9·\s]+특별위원회)", cleaned)
					if match:
						return normalize_text(match.group(1))
					return "특별위원회"

				return keyword

		if "회의" in cleaned and len(cleaned) <= 50:
			match = re.search(r"([가-힣A-Za-z0-9·\s]+회의)", cleaned)
			if match:
				return normalize_text(match.group(1))

	return None


def extract_mtr_sj_generic(
	soup: BeautifulSoup,
	lines: list[str],
	links: list[dict],
	kv_pairs: dict[str, str],
) -> Optional[str]:
	# 1순위: key-value 영역
	for key, value in kv_pairs.items():
		if "안건" in key or "의사일정" in key:
			return normalize_text(value)

	# 2순위: lines 기반
	candidate_lines: list[str] = []
	in_agenda_section = False

	for line in lines:
		text = normalize_text(line)
		if not text:
			continue

		if "부의된안건" in text.replace(" ", "") or "의사일정" in text.replace(" ", ""):
			in_agenda_section = True
			continue

		if in_agenda_section:
			if any(stop_word in text for stop_word in ("출석의원", "출석공무원", "개의", "산회", "의결 결과")):
				break

			if re.match(r"^\d+[.)]\s*", text) or any(hint in text for hint in GENERIC_AGENDA_LINE_HINTS):
				candidate_lines.append(text)
				continue

			if candidate_lines:
				break

	if candidate_lines:
		return "".join(candidate_lines)

	# 3순위: 링크 텍스트
	link_texts = []
	for link in links:
		text = normalize_text(link.get("text"))
		if re.match(r"^\d+[.)]\s*", text) or any(hint in text for hint in GENERIC_AGENDA_LINE_HINTS):
			link_texts.append(text)

	if link_texts:
		return "".join(unique_keep_order(link_texts))

	return None


def link_looks_like_file(link: dict) -> bool:
	text = normalize_text(link.get("text"))
	href = normalize_text(link.get("href"))
	absolute_url = normalize_text(link.get("absolute_url"))
	onclick = normalize_text(link.get("onclick"))

	joined = " ".join([text, href, absolute_url, onclick]).lower()

	if "profile_popup" in joined or "member" in joined:
		return False
	if any(word in text for word in ("의원", "프로필", "약력", "목록", "이전", "다음")):
		return False

	if any(ext in joined for ext in [f".{ext}" for ext in FILE_EXTENSIONS]):
		return True
	if any(keyword in joined for keyword in ("download", "down", "attach", "file", "pdf", "hwp", "hwpx")):
		return True
	if any(keyword in text for keyword in ("첨부", "다운로드", "원문", "원본", "회의록", "부록")):
		return True

	return False


def is_view_like_url(url: str) -> bool:
	lowered = url.lower()
	return any(keyword in lowered for keyword in ["view", "detail", "list", "board", "content"])


def normalize_file_name_for_dedupe(name: str) -> str:
	value = normalize_text(name).lower()
	value = value.replace("\xa0", " ")
	value = re.sub(r"\s+", " ", value)
	return value


def extract_original_file_info_generic(
	links: list[dict],
	detail_html: str,
	detail_url: str,
) -> list[dict]:
	file_candidates: list[tuple[str, str, int]] = []

	for link in links:
		text = normalize_text(link.get("text"))
		absolute_url = normalize_text(link.get("absolute_url"))

		if not absolute_url:
			continue
		if absolute_url == detail_url:
			continue
		if is_view_like_url(absolute_url):
			continue
		if not link_looks_like_file(link):
			continue

		score = 10
		if "원문" in text or "원본" in text:
			score += 10
		if "회의록" in text:
			score += 5

		file_name = text or extract_filename_from_url(absolute_url)
		file_candidates.append((file_name or "", absolute_url, score))

	seen_names = set()
	results = []
	for file_name, file_url, score in sorted(file_candidates, key=lambda x: x[2], reverse=True):
		normalized_name = normalize_file_name_for_dedupe(file_name)
		if normalized_name in seen_names:
			continue

		seen_names.add(normalized_name)
		results.append({
			"file_name": file_name or None,
			"file_url": file_url or None,
		})

	return results


def parse_minutes_detail_generic(
	detail_html: str,
	detail_url: str,
	list_title: str,
) -> dict:
	soup = BeautifulSoup(detail_html, "lxml")

	lines = extract_text_lines(soup)
	title_candidates = extract_title_candidates(soup, lines)
	kv_pairs = extract_key_value_candidates(soup)
	links = extract_link_candidates(soup, detail_url)
	main_content_html = extract_main_content_html(soup)

	agenda_soup = soup
	if main_content_html:
		agenda_soup = BeautifulSoup(main_content_html, "lxml")

	return {
		"MTGNM": extract_mtgnm_generic(list_title, title_candidates, lines),
		"INQUIRY_NM": extract_inquiry_nm_generic(title_candidates, lines, kv_pairs),
		"MTR_SJ": extract_mtr_sj_generic(agenda_soup, lines, links, kv_pairs),
		"RASMBLY_NUMPR": extract_rasmbly_numpr_generic(title_candidates, lines, kv_pairs),
		"RASMBLY_SESN": extract_rasmbly_sesn_generic(title_candidates, lines, kv_pairs),
		"ODR_NM": extract_odr_nm_generic(title_candidates, lines, kv_pairs),
		"MTG_DE": extract_mtg_de_generic(title_candidates, lines, kv_pairs),
		"MINTS_HTML": main_content_html,
		"ORGINL_FILES": extract_original_file_info_generic(
			links=links,
			detail_html=detail_html,
			detail_url=detail_url,
		),
	}


# =========================
# Regex detail parsing
# =========================

def parse_minutes_detail_by_regex(
	detail_html: str,
	request: RegexCrawlRequest,
) -> dict[str, Optional[str]]:
	mtr_sj_raw = apply_regex_raw(detail_html, request.mtr_sj_regex)

	return {
		"MTGNM": apply_regex(detail_html, request.mtgnm_regex),
		"INQUIRY_NM": apply_regex(detail_html, request.inquiry_nm_regex),
		"MTR_SJ": strip_html_tags(mtr_sj_raw),
		"RASMBLY_NUMPR": extract_rasmbly_numpr_from_detail_html(detail_html),
		"RASMBLY_SESN": apply_regex(detail_html, request.rasmbly_sesn_regex),
		"ODR_NM": apply_regex(detail_html, request.odr_nm_regex),
		"MTG_DE": apply_regex(detail_html, request.mtg_de_regex),
		"MINTS_HTML": apply_regex_raw(detail_html, request.mints_html_regex),
	}


# =========================
# Paging auto-detection
# =========================

def extract_link_paging_info(html: str, list_url: str) -> tuple[Optional[str], list[int]]:
	soup = BeautifulSoup(html, "lxml")
	page_numbers = {1}

	candidate_param_names = ["page", "pageNo", "pageNum", "pageIndex", "currentPage"]
	param_counter: dict[str, int] = {}

	for a in soup.find_all("a"):
		href = normalize_text(a.get("href"))
		if not href or href.lower().startswith("javascript:"):
			continue

		absolute_url = urljoin(list_url, href)
		parsed = urlparse(absolute_url)
		query_pairs = parse_qsl(parsed.query, keep_blank_values=True)

		for key, value in query_pairs:
			if key in candidate_param_names and value.isdigit():
				page_numbers.add(int(value))
				param_counter[key] = param_counter.get(key, 0) + 1

	if len(page_numbers) <= 1:
		return None, [1]

	best_param_name = None
	best_count = -1
	for key, count in param_counter.items():
		if count > best_count:
			best_param_name = key
			best_count = count

	return best_param_name, sorted(page_numbers)


def extract_form_request_info(html: str, list_url: str) -> tuple[Optional[str], dict[str, str], Optional[str], list[int]]:
	soup = BeautifulSoup(html, "lxml")
	page_numbers = {1}

	js_matches = re.findall(r"fnActRetrieve\((\d+)\)", html)
	for match in js_matches:
		if match.isdigit():
			page_numbers.add(int(match))

	form = safe_select_one(soup, "#frmDefault")
	if not form:
		for candidate_form in soup.find_all("form"):
			if candidate_form.find(attrs={"name": "pageCurNo"}):
				form = candidate_form
				break

	if not form:
		return None, {}, None, [1]

	action = normalize_text(form.get("action"))
	action_url = urljoin(list_url, action) if action else list_url

	form_data: dict[str, str] = {}
	for inp in form.find_all(["input", "select", "textarea"]):
		name = normalize_text(inp.get("name"))
		if not name:
			continue
		value = normalize_text(inp.get("value"))
		form_data[name] = value

	page_field_name = None
	if "pageCurNo" in form_data:
		page_field_name = "pageCurNo"
	else:
		for key in form_data.keys():
			if key.lower() in ("page", "pageno", "pageindex", "currentpage", "pagecurno"):
				page_field_name = key
				break

	return action_url, form_data, page_field_name, sorted(page_numbers)


async def build_list_pages(
	request: RegexCrawlRequest,
	crawl_all: bool,
) -> list[tuple[str, str]]:
	list_url = str(request.list_url)
	first_html = await fetch_html(list_url, request.ssl_mode)

	if not crawl_all:
		return [(list_url, first_html)]

	pages: list[tuple[str, str]] = []
	seen_page_signatures: set[str] = set()

	# 1) 우선 링크형 파라미터 자동 감지
	link_param_name, _ = extract_link_paging_info(first_html, list_url)

	# 2) 폼형 정보 자동 감지
	action_url, form_data, page_field_name, _ = extract_form_request_info(first_html, list_url)

	# 현재 페이지 HTML에서 row 존재 여부 확인용 헬퍼
	def has_list_items(html: str) -> bool:
		candidates = extract_list_candidates(
			html=html,
			list_root_selector=request.list_root_selector,
			item_selector=request.item_selector,
			target_selector=request.target_selector,
			limit=1,
		)
		return len(candidates) > 0

	# 페이지 중복 감지용 서명
	def make_page_signature(html: str) -> str:
		candidates = extract_list_candidates(
			html=html,
			list_root_selector=request.list_root_selector,
			item_selector=request.item_selector,
			target_selector=request.target_selector,
			limit=None,
		)

		signature_parts = []
		for candidate in candidates[:10]:
			signature_parts.append(
				f"{candidate.get('title','')}|{candidate.get('href','')}|{candidate.get('onclick','')}"
			)

		return "||".join(signature_parts)

	# 3) 첫 페이지부터 시작
	current_page_no = 1
	current_url = list_url
	current_html = first_html

	while current_page_no <= request.max_pages:
		# 목록이 없으면 종료
		if not has_list_items(current_html):
			break

		# 같은 페이지가 반복되면 종료
		signature = make_page_signature(current_html)
		if signature in seen_page_signatures:
			break
		seen_page_signatures.add(signature)

		pages.append((current_url, current_html))

		next_page_no = current_page_no + 1

		# 링크형 우선
		if link_param_name:
			next_url = replace_query_param(list_url, link_param_name, str(next_page_no))

			try:
				next_html = await fetch_html(next_url, request.ssl_mode)
			except Exception:
				break

			current_page_no = next_page_no
			current_url = next_url
			current_html = next_html
			continue

		# 폼형
		if action_url and page_field_name:
			next_form_data = dict(form_data)
			next_form_data[page_field_name] = str(next_page_no)

			try:
				next_html = await fetch_html_by_method(
					url=action_url,
					ssl_mode=request.ssl_mode,
					method="POST",
					form_data=next_form_data,
				)
			except Exception:
				break

			current_page_no = next_page_no
			current_url = action_url
			current_html = next_html
			continue

		# 둘 다 감지 안 되면 1페이지만 처리
		break

	return pages


# =========================
# Playwright detail access
# =========================

async def try_extract_url_from_raw(
	list_url: str,
	href: Optional[str],
	onclick: Optional[str],
) -> tuple[Optional[str], str]:
	raw_candidates = []
	if href:
		raw_candidates.append(href)
	if onclick:
		raw_candidates.append(onclick)

	for raw in raw_candidates:
		if not raw:
			continue

		match = re.search(r"""['"](https?://[^'"]+)['"]""", raw)
		if match:
			return match.group(1), "string-resolve"

		match = re.search(r"""['"]((?:/|\.\./|\./)[^'"]+)['"]""", raw)
		if match:
			return urljoin(list_url, match.group(1)), "string-resolve"

	return None, "string-resolve-failed"


async def resolve_detail_by_playwright(
	list_url: str,
	list_root_selector: str,
	item_selector: str,
	target_selector: str,
	rank_index: int,
	ssl_mode: str,
) -> tuple[Optional[str], str, Optional[str], Optional[str], Optional[str]]:
	try:
		async with async_playwright() as p:
			browser = await p.chromium.launch(headless=True)
			context = await browser.new_context(
				user_agent=USER_AGENT,
				ignore_https_errors=(ssl_mode == "N"),
			)
			page = await context.new_page()

			await page.goto(list_url, wait_until="domcontentloaded", timeout=30000)

			root = page.locator(list_root_selector).first
			if await root.count() == 0:
				await browser.close()
				return None, "playwright-no-root", None, None, "list_root_selector에 해당하는 영역을 찾지 못했습니다."

			items = root.locator(item_selector)
			item_count = await items.count()
			if item_count == 0:
				await browser.close()
				return None, "playwright-no-item", None, None, "item_selector에 해당하는 item을 찾지 못했습니다."

			if rank_index >= item_count:
				await browser.close()
				return None, "playwright-item-out-of-range", None, None, "item index 범위를 벗어났습니다."

			item = items.nth(rank_index)
			target = item.locator(target_selector).first

			if await target.count() == 0:
				await browser.close()
				return None, "playwright-no-target", None, None, "target_selector에 해당하는 target을 찾지 못했습니다."

			original_url = page.url

			try:
				async with page.expect_popup(timeout=5000) as popup_info:
					await target.click()

				popup = await popup_info.value

				try:
					await popup.wait_for_load_state("domcontentloaded", timeout=5000)
				except PlaywrightTimeoutError:
					pass

				detail_url = popup.url
				detail_html = await popup.content()

				await popup.close()
				await browser.close()
				return detail_url, "playwright-click", "popup", detail_html, None

			except PlaywrightTimeoutError:
				pass

			try:
				await page.wait_for_load_state("networkidle", timeout=5000)
			except PlaywrightTimeoutError:
				pass

			if page.url and page.url != original_url:
				detail_url = page.url
				detail_html = await page.content()
				await browser.close()
				return detail_url, "playwright-click", "same_page", detail_html, None

			frames = page.frames
			if len(frames) > 1:
				for frame in frames[1:]:
					try:
						frame_html = await frame.content()
					except Exception:
						continue

					if frame_html and len(frame_html) > 200:
						detail_url = frame.url or page.url
						await browser.close()
						return detail_url, "playwright-click", "iframe", frame_html, None

			await browser.close()
			return None, "playwright-click", "unknown", None, "클릭은 수행했지만 popup/same-page/iframe 변화를 확인하지 못했습니다."

	except Exception as exc:
		return None, f"playwright-error:{type(exc).__name__}", None, None, f"Playwright 예외 발생: {type(exc).__name__}"


async def open_detail_page(
	list_url: str,
	list_root_selector: str,
	item_selector: str,
	target_selector: str,
	rank_index: int,
	href: Optional[str],
	onclick: Optional[str],
	ssl_mode: str,
) -> tuple[Optional[str], str, Optional[str], Optional[str], Optional[str]]:
	if href and not is_javascript_href(href) and is_http_like_href(href):
		detail_url = urljoin(list_url, href)
		if is_meaningful_detail_url(detail_url, list_url):
			try:
				detail_html = await fetch_html(detail_url, ssl_mode)
				return detail_url, "http-href", "direct", detail_html, None
			except Exception as exc:
				fallback_note = f"직접 접근 실패 후 Playwright fallback: {type(exc).__name__}"
		else:
			fallback_note = "href가 목록 URL과 동일하거나 유효하지 않아 Playwright fallback"
	else:
		fallback_note = "javascript/onclick 기반 상세 진입 또는 href 없음"

	resolved_url, resolved_method = await try_extract_url_from_raw(list_url, href, onclick)
	if resolved_url and is_meaningful_detail_url(resolved_url, list_url):
		try:
			detail_html = await fetch_html(resolved_url, ssl_mode)
			return resolved_url, resolved_method, "direct", detail_html, fallback_note
		except Exception:
			pass

	detail_url, method, open_type, detail_html, note = await resolve_detail_by_playwright(
		list_url=list_url,
		list_root_selector=list_root_selector,
		item_selector=item_selector,
		target_selector=target_selector,
		rank_index=rank_index,
		ssl_mode=ssl_mode,
	)

	merged_note_parts = [part for part in [fallback_note, note] if part]
	merged_note = " / ".join(merged_note_parts) if merged_note_parts else None

	return detail_url, method, open_type, detail_html, merged_note


# =========================
# Shared builders
# =========================

async def build_minutes_item_by_regex(
	request: RegexCrawlRequest,
	list_page_url: str,
	candidate: dict,
	rank_index_in_page: int,
	final_rank: int,
) -> MinutesItem:
	title = candidate["title"]
	href = candidate["href"]
	onclick = candidate["onclick"]
	list_rasmbly_numpr = candidate.get("rasmbly_numpr")

	detail_url, access_method, open_type, detail_html, note = await open_detail_page(
		list_url=list_page_url,
		list_root_selector=request.list_root_selector,
		item_selector=request.item_selector,
		target_selector=request.target_selector,
		rank_index=rank_index_in_page,
		href=href,
		onclick=onclick,
		ssl_mode=request.ssl_mode,
	)

	uid = extract_uid(detail_url)

	if not detail_html:
		return MinutesItem(
			rank=final_rank,
			list_title=title,
			detail_url=detail_url,
			access_method=access_method,
			open_type=open_type,
			detail_access_success=False,
			MTGNM=None,
			INQUIRY_NM=None,
			MTR_SJ=None,
			RASMBLY_NUMPR=list_rasmbly_numpr or None,
			RASMBLY_SESN=None,
			ODR_NM=None,
			MTG_DE=None,
			MINTS_HTML=None,
			ORGINL_FILES=[],
			uid=uid,
			raw_href=href,
			raw_onclick=onclick,
			note=note or "상세 view 접근 실패",
		)

	parsed = parse_minutes_detail_by_regex(
		detail_html=detail_html,
		request=request,
	)

	return MinutesItem(
		rank=final_rank,
		list_title=title,
		detail_url=detail_url,
		access_method=access_method,
		open_type=open_type,
		detail_access_success=True,
		MTGNM=parsed["MTGNM"],
		INQUIRY_NM=parsed["INQUIRY_NM"],
		MTR_SJ=parsed["MTR_SJ"],
		RASMBLY_NUMPR=list_rasmbly_numpr or parsed["RASMBLY_NUMPR"] or None,
		RASMBLY_SESN=parsed["RASMBLY_SESN"],
		ODR_NM=parsed["ODR_NM"],
		MTG_DE=parsed["MTG_DE"],
		MINTS_HTML=parsed["MINTS_HTML"],
		ORGINL_FILES=[],
		uid=uid,
		raw_href=href,
		raw_onclick=onclick,
		note=note,
	)


# =========================
# Main crawl services
# =========================

async def crawl_minutes_gereral_check(request: GeneralCrawlRequest) -> CrawlResponse:
	list_url = str(request.list_url)

	try:
		html = await fetch_html(list_url, request.ssl_mode)
	except Exception as exc:
		raise HTTPException(
			status_code=400,
			detail=f"목록 페이지 요청 실패: {type(exc).__name__} / {str(exc)}",
		) from exc

	candidates = extract_list_candidates(
		html=html,
		list_root_selector=request.list_root_selector,
		item_selector=request.item_selector,
		target_selector=request.target_selector,
		limit=5,
	)

	if not candidates:
		raise HTTPException(
			status_code=422,
			detail="지정한 selector 기준으로 목록 item 또는 target을 찾지 못했습니다.",
		)

	items: list[MinutesItem] = []

	for idx, candidate in enumerate(candidates, start=1):
		title = candidate["title"]
		href = candidate["href"]
		onclick = candidate["onclick"]

		detail_url, access_method, open_type, detail_html, note = await open_detail_page(
			list_url=list_url,
			list_root_selector=request.list_root_selector,
			item_selector=request.item_selector,
			target_selector=request.target_selector,
			rank_index=idx - 1,
			href=href,
			onclick=onclick,
			ssl_mode=request.ssl_mode,
		)

		uid = extract_uid(detail_url)

		if not detail_html:
			items.append(
				MinutesItem(
					rank=idx,
					list_title=title,
					detail_url=detail_url,
					access_method=access_method,
					open_type=open_type,
					detail_access_success=False,
					MTGNM=None,
					INQUIRY_NM=None,
					MTR_SJ=None,
					RASMBLY_NUMPR=None,
					RASMBLY_SESN=None,
					ODR_NM=None,
					MTG_DE=None,
					MINTS_HTML=None,
					ORGINL_FILES=[],
					uid=uid,
					raw_href=href,
					raw_onclick=onclick,
					note=note or "상세 view 접근 실패",
				)
			)
			continue

		try:
			parsed = parse_minutes_detail_generic(
				detail_html=detail_html,
				detail_url=detail_url or list_url,
				list_title=title,
			)

			items.append(
				MinutesItem(
					rank=idx,
					list_title=title,
					detail_url=detail_url,
					access_method=access_method,
					open_type=open_type,
					detail_access_success=True,
					MTGNM=parsed["MTGNM"],
					INQUIRY_NM=parsed["INQUIRY_NM"],
					MTR_SJ=parsed["MTR_SJ"],
					RASMBLY_NUMPR=parsed["RASMBLY_NUMPR"],
					RASMBLY_SESN=parsed["RASMBLY_SESN"],
					ODR_NM=parsed["ODR_NM"],
					MTG_DE=parsed["MTG_DE"],
					MINTS_HTML=parsed["MINTS_HTML"],
					ORGINL_FILES=parsed["ORGINL_FILES"],
					uid=uid,
					raw_href=href,
					raw_onclick=onclick,
					note=note,
				)
			)
		except Exception as exc:
			items.append(
				MinutesItem(
					rank=idx,
					list_title=title,
					detail_url=detail_url,
					access_method=access_method,
					open_type=open_type,
					detail_access_success=True,
					MTGNM=None,
					INQUIRY_NM=None,
					MTR_SJ=None,
					RASMBLY_NUMPR=None,
					RASMBLY_SESN=None,
					ODR_NM=None,
					MTG_DE=None,
					MINTS_HTML=None,
					ORGINL_FILES=[],
					uid=uid,
					raw_href=href,
					raw_onclick=onclick,
					note=f"{note + ' / ' if note else ''}상세 파싱 실패: {type(exc).__name__}",
				)
			)

	return CrawlResponse(
		list_url=list_url,
		item_count=len(items),
		items=items,
	)


async def crawl_minutes_regex_check(
	request: RegexCrawlRequest,
	crawl_all: bool = False,
) -> CrawlResponse:
	try:
		list_pages = await build_list_pages(request, crawl_all=crawl_all)
	except Exception as exc:
		raise HTTPException(
			status_code=400,
			detail=f"목록 페이지 요청 실패: {type(exc).__name__} / {str(exc)}",
		) from exc

	all_items: list[MinutesItem] = []
	seen_keys: set[str] = set()

	for page_url, page_html in list_pages:
		candidates = extract_list_candidates(
			html=page_html,
			list_root_selector=request.list_root_selector,
			item_selector=request.item_selector,
			target_selector=request.target_selector,
			limit=None if crawl_all else 5,
		)

		if not candidates:
			continue

		for idx, candidate in enumerate(candidates, start=1):
			try:
				item = await build_minutes_item_by_regex(
					request=request,
					list_page_url=page_url,
					candidate=candidate,
					rank_index_in_page=idx - 1,
					final_rank=len(all_items) + 1,
				)
			except ValueError as exc:
				raise HTTPException(status_code=400, detail=str(exc)) from exc
			except Exception as exc:
				item = MinutesItem(
					rank=len(all_items) + 1,
					list_title=candidate["title"],
					detail_url=None,
					access_method="error",
					open_type=None,
					detail_access_success=False,
					MTGNM=None,
					INQUIRY_NM=None,
					MTR_SJ=None,
					RASMBLY_NUMPR=candidate.get("rasmbly_numpr"),
					RASMBLY_SESN=None,
					ODR_NM=None,
					MTG_DE=None,
					MINTS_HTML=None,
					ORGINL_FILES=[],
					uid=None,
					raw_href=candidate.get("href"),
					raw_onclick=candidate.get("onclick"),
					note=f"상세 처리 실패: {type(exc).__name__}",
				)

			dedupe_key = item.uid or item.detail_url or f"{item.list_title}|{item.raw_href}|{item.raw_onclick}"
			if crawl_all and dedupe_key in seen_keys:
				continue

			seen_keys.add(dedupe_key)
			item.rank = len(all_items) + 1
			all_items.append(item)

	if not all_items:
		raise HTTPException(
			status_code=422,
			detail="지정한 selector 기준으로 목록 item 또는 target을 찾지 못했습니다.",
		)

	return CrawlResponse(
		list_url=str(request.list_url),
		item_count=len(all_items),
		items=all_items,
	)


# =========================
# API
# =========================

@app.get("/health")
async def health():
	return {"status": "ok"}


@app.post("/crawl/minutes/general-check", response_model=CrawlResponse)
async def crawl_minutes_general_check_api(request: GeneralCrawlRequest):
	return await crawl_minutes_gereral_check(request)


@app.post("/crawl/minutes/test", response_model=CrawlResponse)
async def crawl_minutes_test_api(request: RegexCrawlRequest):
	return await crawl_minutes_regex_check(request, crawl_all=False)


@app.post("/crawl/minutes/all", response_model=CrawlResponse)
async def crawl_minutes_all_api(request: RegexCrawlRequest):
	return await crawl_minutes_regex_check(request, crawl_all=True)