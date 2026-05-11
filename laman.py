from __future__ import annotations

import sys
import asyncio
import re
import time
from typing import Optional
from urllib.parse import (
	urljoin,
	urlparse,
	parse_qsl,
	urlencode,
	urlunparse,
	unquote,
)
import os

import certifi
import httpx
from bs4 import BeautifulSoup, Tag
from fastapi import FastAPI, HTTPException, BackgroundTasks
from uuid import uuid4
from pydantic import BaseModel, Field, HttpUrl
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

import traceback
import json
from datetime import datetime


if sys.platform.startswith("win"):
	asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="5min Free Speech Crawl API", version="0.1.0")


USER_AGENT = (
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
	"AppleWebKit/537.36 (KHTML, like Gecko) "
	"Chrome/122.0.0.0 Safari/537.36"
)

# CALLBACK_INSERT_API_URL = "http://211.219.26.15:18123/insert_api.do"		# 실제 CMS 서버 (도커 외부에서 접근용)
CALLBACK_INSERT_API_URL = "http://172.17.0.1:18123/insert_api.do"			# 도커 내에서 cms 컨테이너 접근용
# CALLBACK_INSERT_API_URL = "http://localhost:8900/insert_api"				# python 내 json 저장
# CALLBACK_INSERT_API_URL = "http://localhost:9000/insert_api.do"			# 로컬 cms
# CALLBACK_INSERT_API_URL = "http://10.201.38.157:8080/insert_api.do"		# 운영 cms

IMAGE_EXTENSIONS = ("jpg", "jpeg", "png", "gif", "webp", "bmp", "svg")
RESERVED_COL_PHOTO = "PHOTO_FILE_URL"

FIELD_LOGS_DIR = "field_logs"

# =========================
# Request / Response Model
# =========================


class LamanParam(BaseModel):
	list_url: HttpUrl = Field(...)
	list_root_selector: str = Field(...)
	item_selector: str = Field(...)
	target_selector: str = Field(...)
	ssl_mode: str = Field("Y")
	max_pages: int = Field(500)
	skip_top_count: int = Field(0, description="상단 게시물 패스 수 (고정 공지글 방어용)")
	profile_selector: str = Field(..., description="2뎁스 프로필 상세 보기 버튼 명시용")


class RegexItem(BaseModel):
	col: str = Field(..., description="응답 key 이름")
	regex: list[str] = Field(..., description="상세 HTML에서 추출할 정규식")
	xpath: list[str] = Field(None, description="(미구현) XPath 추출용 필드")
	removeTags: str = Field("Y", description="HTML 태그 제거 여부: Y | N")
	value: Optional[str] = Field(None, description="고정값. regex 있으면 무시하고 그냥 value에 있는거 넣어줄거임")


class LamanCrawlRequest(BaseModel):
	req_id: str = Field(..., description="날짜 포맷: yyyyMMddHHmmssSSSSSS")
	crw_id: str = Field(..., description="기관 코드")
	bbs_id: str = Field(..., description="게시판 ID")
	type: str = Field(..., description="수집 유형: minutes, bill 등")
	file_dir: str = Field(...)
	param: LamanParam = Field(...)
	item: list[RegexItem] = Field(default_factory=list)


class LamanItem(BaseModel):
	rank: int
	list_title: str

	detail_url: Optional[str] = None
	access_method: str
	open_type: Optional[str] = None
	detail_access_success: bool

	fields: dict[str, Optional[str]] = Field(default_factory=dict)

	uid: Optional[str] = None
	asemby_cn: Optional[str] = None

	raw_href: Optional[str] = None
	raw_onclick: Optional[str] = None
	note: Optional[str] = None


class LamanCrawlResponse(BaseModel):
	list_url: str
	item_count: int
	items: list[LamanItem]


# =========================
# Utility
# =========================

def normalize_text(text: Optional[str]) -> str:
	if not text:
		return ""
	cleaned = (
		text.replace("&nbsp;", " ")
			.replace("&#160;", " ")
			.replace("\xa0", " ")
	)
	return re.sub(r"\s+", " ", cleaned).strip()


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


def apply_regex_raw(source: str, patterns: list[str]) -> Optional[str]:
	"""정규식 체이닝: 배열의 각 패턴을 순차 적용. 이전 결과 안에서 다음 패턴 매치."""
	if not patterns:
		return None
	current = source
	for pattern in patterns:
		if not pattern or current is None:
			return None
		try:
			match = re.search(pattern, current, re.IGNORECASE | re.DOTALL)
		except re.error as exc:
			raise ValueError(f"잘못된 정규식입니다: {pattern} / {str(exc)}") from exc
		if not match:
			return None
		current = match.group(1) if match.groups() else match.group(0)
	return current


def apply_regex_all(source: str, patterns: list[str]) -> list[str]:
	"""정규식 체이닝(multi): 마지막 패턴 전까지는 search로 구간 축소, 마지막은 findall."""
	if not patterns:
		return []
	current = source
	for i, pattern in enumerate(patterns):
		if not pattern or current is None:
			return []
		is_last = (i == len(patterns) - 1)
		try:
			if is_last:
				matches = re.finditer(pattern, current, re.IGNORECASE | re.DOTALL)
				results = []
				for m in matches:
					results.append(m.group(1) if m.groups() else m.group(0))
				return results
			else:
				match = re.search(pattern, current, re.IGNORECASE | re.DOTALL)
				if not match:
					continue
				current = match.group(1) if match.groups() else match.group(0)
		except re.error as exc:
			raise ValueError(f"잘못된 정규식입니다: {pattern} / {str(exc)}") from exc
	return []


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
		preferred_keys = ["uid", "key", "id", "no", "seq", "idx"]

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
		parsed.scheme, parsed.netloc, parsed.path,
		parsed.params, new_query, parsed.fragment,
	))


def audit_fields_laman(
	asemby_cn: str,
	url: Optional[str],
	item_cols: list[str],
	fields: dict[str, Optional[str]],
) -> dict:
	collected, missing = [], []
	for col in sorted(set(item_cols)):
		val = fields.get(col)
		if val is not None and str(val).strip():
			collected.append(col)
		else:
			missing.append(col)

	return {
		"asemby_cn":  asemby_cn,
		"URL":       url,
		"collected": collected,
		"empty":     [],
		"missing":   missing,
	}


def save_field_logs(field_logs: list, request) -> None:
	now = datetime.now()
	path = os.path.join(
		FIELD_LOGS_DIR,
		request.type,
		request.crw_id or "unknown",
		now.strftime("%Y"),
		now.strftime("%m"),
		f"{request.req_id}.json",
	)
	os.makedirs(os.path.dirname(path), exist_ok=True)
	with open(path, "w", encoding="utf-8") as f:
		json.dump({FIELD_LOGS_DIR: field_logs}, f, ensure_ascii=False, indent=4)
	print(f"[+] field_logs 저장: {path} ({len(field_logs)}건)", flush=True)


def _build_result(data_list: list, error_logs: list, error: str = "") -> dict:
	"""수집 결과 상태 자동 판별"""
	has_timeout = any("Timeout" in (e.get("note") or e.get("error") or "") for e in error_logs)
	has_error = len(error_logs) > 0
	data_count = len(data_list)

	if error:
		status, code, message = "FAILED", "500", f"수집 실패: {error}"
	elif data_count == 0 and has_timeout:
		status, code, message = "TIMEOUT", "408", "타임아웃으로 수집 불가"
	elif data_count == 0 and has_error:
		status, code, message = "FAILED", "500", "수집 실패: " + error_logs[-1].get("error", "알 수 없는 오류")
	elif data_count == 0:
		status, code, message = "EMPTY", "204", "수집 결과 없음"
	elif has_timeout or has_error:
		status, code, message = "PARTIAL", "206", "일부 수집 완료 (오류 포함)"
	else:
		status, code, message = "SUCCESS", "200", "수집 완료"

	return {
		"status": status,
		"code": code,
		"message": message,
		"dataCount": data_count,
		"interrupted": False,
	}


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
		if target_selector == "self":
			target = item
		else:
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
			"row_html": str(item),
		})

	if limit is None:
		return results
	return results[:limit]


# =========================
# Dynamic regex detail parsing
# =========================

def parse_laman_detail_by_dynamic_regex(
	detail_html: str,
	request: LamanCrawlRequest,
	list_title: Optional[str] = None,
) -> dict[str, Optional[str]]:
	result: dict[str, Optional[str]] = {}

	for item in request.item:
		key = normalize_text(item.col)
		if not key:
			continue

		# value가 지정되어 있으면 고정값으로 사용 (regex 무시)
		if item.value is not None and normalize_text(item.value):
			result[key] = normalize_text(item.value)
			continue

		# 정규식이 비어있으면 건너뜀 (CMS에서 전체 컬럼을 보내는 경우 대응)
		if not item.regex or all(not normalize_text(r) for r in item.regex):
			continue

		# regex 목록 중 "list_title" 예약어 체크
		if len(item.regex) == 1 and normalize_text(item.regex[0]).lower() == "list_title":
			value = normalize_text(list_title)
			result[key] = value or None
			continue

		# PHOTO_FILE_URL은 handle_photo_reserved_col에서 별도 처리
		if key == RESERVED_COL_PHOTO:
			continue

		raw_value = apply_regex_raw(detail_html, item.regex)

		if item.removeTags == "Y":
			result[key] = strip_html_tags(raw_value)
		else:
			result[key] = normalize_text(raw_value)

	return result


# =========================
# Paging auto-detection
# =========================

def is_paging_area(tag) -> bool:
	parent = tag
	while parent:
		classes = parent.get("class", [])
		class_text = " ".join(classes) if isinstance(classes, list) else str(classes)
		tag_id = parent.get("id", "")
		marker = f"{class_text} {tag_id}"

		if re.search(r"page|paging|pager|pagination|navi", marker, re.I):
			return True

		parent = parent.parent

	return False


def extract_link_paging_info(html: str, list_url: str) -> tuple[Optional[str], list[int]]:
	soup = BeautifulSoup(html, "lxml")
	page_numbers = {1}
	candidate_param_names = ["page", "pageNo", "pageNum", "pageIndex", "currentPage"]
	param_counter: dict[str, int] = {}

	for a in soup.find_all("a"):
		href = normalize_text(a.get("href"))
		text = normalize_text(a.get_text(" ", strip=True))
		
		if not href:
			continue

		# href 쿼리스트링 기반 페이징 처리
		if not href.lower().startswith("javascript:"):
			absolute_url = urljoin(list_url, href)
			parsed = urlparse(absolute_url)
			query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
			for key, value in query_pairs:
				if key in candidate_param_names and value.isdigit():
					page_numbers.add(int(value))
					param_counter[key] = param_counter.get(key, 0) + 1
		
		# javascript 기반 페이징 처리
		# - 부모 영역이 page/paging/pagination/navi 계열이어야 함
		# - a 태그 내의 텍스트가 숫자여야 함
		if is_paging_area(a) and text.isdigit():
			page_numbers.add(int(text))

	if len(page_numbers) <= 1:
		return None, [1]

	best_param_name = None
	best_count = -1
	for key, count in param_counter.items():
		if count > best_count:
			best_param_name = key
			best_count = count
	return best_param_name, sorted(page_numbers)


async def fetch_pages_by_playwright_click(
	url: str,
	request: LamanCrawlRequest,
) -> list[tuple[str, str]]:
	pages = []
	seen_signatures = set()

	async with async_playwright() as p:
		browser = await p.chromium.launch(headless=True)
		context = await browser.new_context(
			user_agent=USER_AGENT,
			ignore_https_errors=(request.param.ssl_mode == "N"),
		)
		page = await context.new_page()
		await page.goto(url, wait_until="networkidle", timeout=30000)
		await page.wait_for_timeout(1000)

		for page_no in range(1, request.param.max_pages + 1):
			html = await page.content()

			candidates = extract_list_candidates(
				html=html,
				list_root_selector=request.param.list_root_selector,
				item_selector=request.param.item_selector,
				target_selector=request.param.target_selector,
				limit=None,
			)

			if not candidates:
				break

			signature = "||".join(
				f"{c.get('title', '')}|{c.get('href', '')}|{c.get('onclick', '')}"
				for c in candidates[:10]
			)

			if signature in seen_signatures:
				break

			seen_signatures.add(signature)
			pages.append((page.url, html))

			next_page_no = page_no + 1
			if next_page_no > request.param.max_pages:
				break

			next_link = page.locator(
				f"a.num:text-is('{next_page_no}'), "
				f".pageForm a:text-is('{next_page_no}'), "
				f".pageNavi a:text-is('{next_page_no}'), "
				f"[class*='page'] a:text-is('{next_page_no}')"
			).first

			if await next_link.count() == 0:
				break

			await next_link.click()
			await page.wait_for_timeout(1000)

		await browser.close()

	return pages


# =========================
# Playwright list access
# =========================


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


async def fetch_list_html_by_playwright(url: str, ssl_mode: str) -> str:
	"""목록 페이지를 Playwright로 렌더링하여 HTML 반환."""
	async with async_playwright() as p:
		browser = await p.chromium.launch(headless=True)
		context = await browser.new_context(
			user_agent=USER_AGENT,
			ignore_https_errors=(ssl_mode == "N")
		)
		page = await context.new_page()
		try:
			await page.goto(url, wait_until="networkidle", timeout=30000)
			await page.wait_for_timeout(3000)
			html = await page.content()
		finally:
			await browser.close()
		return html


async def build_list_pages(
	request: LamanCrawlRequest,
	crawl_all: bool,
) -> list[tuple[str, str]]:
	list_url = str(request.param.list_url)
	first_html = await fetch_html(list_url, request.param.ssl_mode)

	if not crawl_all:
		return [(list_url, first_html)]

	pages: list[tuple[str, str]] = []
	seen_page_signatures: set[str] = set()

	link_param_name, _ = extract_link_paging_info(first_html, list_url)
	action_url, form_data, page_field_name, _ = extract_form_request_info(first_html, list_url)

	def has_list_items(html: str) -> bool:
		return len(extract_list_candidates(
			html=html,
			list_root_selector=request.param.list_root_selector,
			item_selector=request.param.item_selector,
			target_selector=request.param.target_selector,
			limit=1,
		)) > 0

	use_playwright = False
	if not has_list_items(first_html):
		print(f"[LAMAN] httpx 목록 조회 결과 항목 없음 → Playwright 폴백 시도")
		try:
			first_html = await fetch_list_html_by_playwright(
				list_url, 
				ssl_mode=request.param.ssl_mode
			)
			use_playwright = True
			if not has_list_items(first_html):
				print(f"[LAMAN] Playwright 목록 조회에서도 항목 없음")
		except Exception as e:
			print(f"[LAMAN] Playwright 목록 조회 실패: {e}")

	def make_page_signature(html: str) -> str:
		candidates = extract_list_candidates(
			html=html,
			list_root_selector=request.param.list_root_selector,
			item_selector=request.param.item_selector,
			target_selector=request.param.target_selector,
			limit=None,
		)
		return "||".join(
			f"{c.get('title', '')}|{c.get('href', '')}|{c.get('onclick', '')}"
			for c in candidates[:10]
		)

	async def fetch_next_page(url: str) -> str:
		if use_playwright:
			return await fetch_list_html_by_playwright(
				url, 
				ssl_mode=request.param.ssl_mode
			)
		return await fetch_html(url, request.param.ssl_mode)

	current_page_no = 1
	current_url = list_url
	current_html = first_html

	while current_page_no <= request.param.max_pages:
		if not has_list_items(current_html):
			break
		signature = make_page_signature(current_html)
		if signature in seen_page_signatures:
			break
		seen_page_signatures.add(signature)
		pages.append((current_url, current_html))

		next_page_no = current_page_no + 1

		if link_param_name:
			next_url = replace_query_param(list_url, link_param_name, str(next_page_no))

			try:
				next_html = await fetch_next_page(next_url)
			except Exception:
				break

			current_page_no = next_page_no
			current_url = next_url
			current_html = next_html
			continue

		if action_url and page_field_name:
			next_form_data = dict(form_data)
			next_form_data[page_field_name] = str(next_page_no)

			try:
				next_html = await fetch_html_by_method(
					url=action_url,
					ssl_mode=request.param.ssl_mode,
					method="POST",
					form_data=next_form_data,
				)
			except Exception:
				break

			current_page_no = next_page_no
			current_url = action_url
			current_html = next_html
			continue
			
		print("[LAMAN] 일반 페이징 실패 → Playwright fallback")

		pw_pages = await fetch_pages_by_playwright_click(
			list_url,
			request,
		)

		if len(pw_pages) > 1:
			return pw_pages

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
				return None, "playwright-no-root", None, None, "list_root_selector를 찾지 못했습니다."

			items = root.locator(item_selector)
			item_count = await items.count()
			if item_count == 0:
				await browser.close()
				return None, "playwright-no-item", None, None, "item_selector를 찾지 못했습니다."

			if rank_index >= item_count:
				await browser.close()
				return None, "playwright-item-out-of-range", None, None, "item index 범위 초과."

			item = items.nth(rank_index)
			if target_selector == "self":
				target = item
			else:
				target = item.locator(target_selector).first

			if await target.count() == 0:
				await browser.close()
				return None, "playwright-no-target", None, None, "target_selector를 찾지 못했습니다."

			original_url = page.url

			# popup 시도
			try:
				async with page.expect_popup(timeout=5000) as popup_info:
					await target.click()
				popup = await popup_info.value
				try:
					await popup.wait_for_load_state("networkidle", timeout=10000)
				except PlaywrightTimeoutError:
					pass
				detail_url = popup.url
				detail_html = await popup.content()
				await popup.close()
				await browser.close()
				return detail_url, "playwright-click", "popup", detail_html, None
			except PlaywrightTimeoutError:
				pass

			# same page 이동
			try:
				await page.wait_for_load_state("networkidle", timeout=5000)
			except PlaywrightTimeoutError:
				pass

			if page.url and page.url != original_url:
				detail_url = page.url
				detail_html = await page.content()
				await browser.close()
				return detail_url, "playwright-click", "same_page", detail_html, None

			# iframe
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
			return None, "playwright-click", "unknown", None, "클릭 후 변화를 감지하지 못했습니다."

	except Exception as exc:
		return None, f"playwright-error:{type(exc).__name__}", None, None, (
			f"Playwright 예외: {type(exc).__name__} / {str(exc)}\n{traceback.format_exc()}"
		)


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
			fallback_note = "href가 유효하지 않아 Playwright fallback"
	else:
		fallback_note = "javascript/onclick 기반 또는 href 없음"

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


async def open_profile_page_by_selector(
	homepage_url: str,
	homepage_html: str,
	profile_selector: str,
	ssl_mode: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
	"""의원 홈페이지 HTML에서 profile_selector로 프로필 링크를 찾아 접근.
	1순위: href가 유효한 URL이면 httpx로 직접 접근
	2순위: href가 없거나 javascript이면 Playwright 클릭
	Returns: (profile_url, profile_html, note)
	"""
	soup = BeautifulSoup(homepage_html, "lxml")

	# 셀렉터로 프로필 링크 요소 탐색
	target = safe_select_one(soup, profile_selector)
	if not target:
		return None, None, f"profile_selector 요소를 찾지 못했습니다: {profile_selector}"

	href = normalize_text(target.get("href"))

	# 1순위: httpx 직접 접근
	if href and not is_javascript_href(href) and is_http_like_href(href):
		profile_url = urljoin(homepage_url, href)
		try:
			profile_html = await fetch_html(profile_url, ssl_mode)
			return profile_url, profile_html, None
		except Exception as exc:
			# httpx 실패 시 Playwright 폴백
			pass

	# 2순위: Playwright 클릭
	try:
		async with async_playwright() as p:
			browser = await p.chromium.launch(headless=True)
			context = await browser.new_context(
				user_agent=USER_AGENT,
				ignore_https_errors=(ssl_mode == "N"),
			)
			page = await context.new_page()
			await page.goto(homepage_url, wait_until="domcontentloaded", timeout=30000)
			await page.wait_for_timeout(1000)

			btn = page.locator(profile_selector).first
			if await btn.count() == 0:
				await browser.close()
				return None, None, f"Playwright에서 profile_selector 요소를 찾지 못했습니다: {profile_selector}"

			original_url = page.url

			try:
				async with page.expect_popup(timeout=5000) as popup_info:
					await btn.click()
				popup = await popup_info.value
				try:
					await popup.wait_for_load_state("networkidle", timeout=10000)
				except PlaywrightTimeoutError:
					pass
				profile_url = popup.url
				profile_html = await popup.content()
				await popup.close()
				await browser.close()
				return profile_url, profile_html, None
			except PlaywrightTimeoutError:
				pass

			# same page 이동
			try:
				await page.wait_for_load_state("domcontentloaded", timeout=10000)
				await page.wait_for_load_state("networkidle", timeout=5000)
			except PlaywrightTimeoutError:
				pass

			if page.url and page.url != original_url:
				profile_url = page.url
				profile_html = await page.content()
				await browser.close()
				return profile_url, profile_html, None

			profile_html = await page.content()
			await browser.close()
			return page.url, profile_html, "URL 변경 없이 콘텐츠 변경 감지 시도"

	except Exception as exc:
		return None, None, f"프로필 페이지 접근 실패: {type(exc).__name__}: {str(exc)}"


# =========================
# Shared builders
# =========================

async def build_laman_item_by_dynamic_regex(
	request: LamanCrawlRequest,
	list_page_url: str,
	candidate: dict,
	rank_index_in_page: int,
	final_rank: int,
) -> LamanItem:
	title = candidate["title"]
	href = candidate["href"]
	onclick = candidate["onclick"]

	detail_url, access_method, open_type, detail_html, note = await open_detail_page(
		list_url=list_page_url,
		list_root_selector=request.param.list_root_selector,
		item_selector=request.param.item_selector,
		target_selector=request.param.target_selector,
		rank_index=rank_index_in_page,
		href=href,
		onclick=onclick,
		ssl_mode=request.param.ssl_mode,
	)

	uid = extract_uid(detail_url)

	if not detail_html:
		return LamanItem(
			rank=final_rank,
			list_title=title,
			detail_url=detail_url,
			access_method=access_method,
			open_type=open_type,
			detail_access_success=False,
			fields={},
			uid=uid,
			raw_href=href,
			raw_onclick=onclick,
			note=note or "상세 view 접근 실패",
		)

	parsed = parse_laman_detail_by_dynamic_regex(
		detail_html=detail_html,
		request=request,
		list_title=title,
	)

	return LamanItem(
		rank=final_rank,
		list_title=title,
		detail_url=detail_url,
		access_method=access_method,
		open_type=open_type,
		detail_access_success=True,
		fields=parsed,
		uid=uid,
		raw_href=href,
		raw_onclick=onclick,
		note=note,
	)


# =========================
# Image Download
# =========================

def build_image_save_path(
	file_dir: str,
	crw_id: str,
	asemby_cn: str,
	original_filename: str,
) -> str:
	"""프로필 이미지 저장 경로: /{file_dir}/{crw_id}/{currentYear}/{ASEMBY_CN}.{확장자}"""
	ext = ""
	if original_filename and "." in original_filename:
		ext = original_filename.rsplit(".", 1)[-1].lower().split("?")[0]

	if ext not in IMAGE_EXTENSIONS:
		ext = "jpg"

	year = datetime.now().strftime("%Y")
	filename = f"{asemby_cn}.{ext}"

	return os.path.join(
		file_dir,
		crw_id,
		year,
		filename,
	)


async def download_profile_image(
	img_url: str,
	file_dir: str,
	crw_id: str,
	asemby_cn: str,
	ssl_mode: str,
) -> tuple[str, str]:
	"""프로필 이미지 다운로드. (save_path, file_name) 반환"""
	print(f"[PHOTO] 다운로드 시작: {img_url}")

	timeout = httpx.Timeout(30.0, connect=10.0)
	headers = {"User-Agent": USER_AGENT}
	verify_option = get_verify_options(ssl_mode)

	async with httpx.AsyncClient(
		headers=headers,
		timeout=timeout,
		follow_redirects=True,
		verify=verify_option,
	) as client:
		response = await client.get(img_url)
		response.raise_for_status()

		# URL에서 파일명 추출
		url_path = urlparse(img_url).path
		if url_path:
			original_name = os.path.basename(url_path)
		else:
			raise ValueError(f"이미지 URL에서 파일명을 추출할 수 없습니다. url: {img_url}")

		save_path = build_image_save_path(
			file_dir=file_dir,
			crw_id=crw_id,
			asemby_cn=asemby_cn,
			original_filename=original_name,
		)

		os.makedirs(os.path.dirname(save_path), exist_ok=True)

		if os.path.exists(save_path):
			print(f"[PHOTO] 이미 존재하는 파일 스킵: {save_path}")
			return save_path, original_name

		with open(save_path, "wb") as f:
			f.write(response.content)

		print(f"[PHOTO] 다운로드 성공: {original_name} -> {save_path} ({len(response.content)} bytes)")

	return save_path, original_name


def extract_photo_url_from_html(
	html: str,
	regex_patterns: list[str],
	base_url: str,
) -> Optional[str]:
	"""HTML에서 프로필 이미지 URL 추출. 정규식 매칭 후 절대 URL로 변환."""
	raw_value = apply_regex_raw(html, regex_patterns)
	if not raw_value:
		return None

	raw_value = normalize_text(raw_value)
	if not raw_value:
		return None

	return urljoin(base_url, raw_value)


async def handle_photo_reserved_col(
	request: LamanCrawlRequest,
	parsed: dict,
	list_item_html: str,
	detail_html: Optional[str],
	list_url: str,
	detail_url: Optional[str],
	asemby_cn: str,
	error_logs: list[dict],
	title: str,
) -> None:
	"""PHOTO_FILE_URL 예약어 처리: 목록 우선 → 상세 폴백으로 이미지 추출 및 다운로드"""
	photo_item = None
	for item in request.item:
		if normalize_text(item.col) == RESERVED_COL_PHOTO:
			photo_item = item
			break

	if not photo_item:
		return

	if not photo_item.regex or all(not normalize_text(r) for r in photo_item.regex):
		return

	# 1순위: 목록 HTML에서 시도
	photo_url = extract_photo_url_from_html(
		html=list_item_html,
		regex_patterns=photo_item.regex,
		base_url=list_url,
	)

	# 2순위: 상세 HTML에서 시도
	if not photo_url and detail_html:
		photo_url = extract_photo_url_from_html(
			html=detail_html,
			regex_patterns=photo_item.regex,
			base_url=detail_url or list_url,
		)

	if not photo_url:
		parsed[RESERVED_COL_PHOTO] = None
		parsed["PHOTO_FILE_NM"] = None
		parsed["PHOTO_FILE_PATH"] = None
		return

	try:
		save_path, file_name = await download_profile_image(
			img_url=photo_url,
			file_dir=request.file_dir if hasattr(request, 'file_dir') else "",
			crw_id=request.crw_id,
			asemby_cn=asemby_cn,
			ssl_mode=request.param.ssl_mode,
		)
		parsed[RESERVED_COL_PHOTO] = photo_url
		parsed["PHOTO_FILE_NM"] = file_name
		parsed["PHOTO_FILE_PATH"] = save_path
	except Exception as exc:
		parsed[RESERVED_COL_PHOTO] = photo_url
		parsed["PHOTO_FILE_NM"] = None
		parsed["PHOTO_FILE_PATH"] = None
		error_logs.append({
			"step": "프로필 이미지 다운로드",
			"title": title,
			"photo_url": photo_url,
			"error": f"{type(exc).__name__}: {str(exc)}",
		})


# =========================
# Callback
# =========================

def build_laman_callback_payload(
	request: LamanCrawlRequest,
	crawl_response: LamanCrawlResponse,
	error_logs: list | None = None,
	error: str = "",
) -> dict:
	data = []
	_error_logs = error_logs or []
	
	for item in crawl_response.items:
		if item.fields:
			row = dict(item.fields)
			row["HMPG"] = item.detail_url
			row["ASEMBY_CN"] = item.asemby_cn
			data.append(row)

	result_block = _build_result(data, _error_logs, error=error)

	return {
		"req_id": request.req_id,
		"type": request.type,
		"crw_id": request.crw_id,
		"result": result_block,
		"data": data,
		"log": _error_logs
	}


async def post_laman_callback(payload: dict) -> None:
	timeout = httpx.Timeout(60.0, connect=10.0)
	async with httpx.AsyncClient(timeout=timeout) as client:
		response = await client.post(
			CALLBACK_INSERT_API_URL,
			json=payload,
			headers={"Content-Type": "application/json"},
		)
		response.raise_for_status()


async def run_laman_all_and_callback(request: LamanCrawlRequest) -> None:
	error_logs: list[dict] = []
	crawl_response: Optional[LamanCrawlRequest] = None

	try:
		crawl_response = await crawl_laman_regex_check(request, crawl_all=True, error_logs=error_logs)
		payload = build_laman_callback_payload(request, crawl_response, error_logs=error_logs)
	except Exception as exc:
		traceback.print_exc()
	
		if crawl_response is None:
			crawl_response = LamanCrawlRequest(
				list_url=str(request.param.list_url),
				item_count=0,
				items=[],
			)
		error_logs.append({
			"step": "run_laman_all_and_callback",
			"error": f"{type(exc).__name__}: {str(exc)}",
		})
		payload = build_laman_callback_payload(
			request, crawl_response, error_logs=error_logs, error=str(exc)
		)
	
	try:
		await post_laman_callback(payload)
	except Exception as cb_exc:
		print(f"[CALLBACK] 콜백 전송 실패: {cb_exc}")
		traceback.print_exc()


# =========================
# Main crawl service
# =========================

async def crawl_laman_regex_check(
	request: LamanCrawlRequest,
	crawl_all: bool = False,
	error_logs: list[dict] | None = None,
) -> LamanCrawlResponse:
	if error_logs is None:
		error_logs = []

	if not request.item:
		error_logs.append({
			"step": "파라미터 검증",
			"error": "item은 최소 1개 이상이어야 합니다.",
		})
		return LamanCrawlResponse(
			list_url=str(request.param.list_url),
			item_count=0,
			items=[]
		)

	try:
		list_pages = await build_list_pages(request, crawl_all=crawl_all)
	except Exception as exc:
		error_logs.append({
			"step": "목록 페이지 요청",
			"url": str(request.param.list_url),
			"error": f"{type(exc).__name__}: {str(exc)}",
		})
		return LamanCrawlResponse(
			list_url=str(request.param.list_url),
			item_count=0,
			items=[],
		)

	field_logs: list[dict] = []
	item_cols: list[str] = [
		normalize_text(ri.col)
		for ri in request.item
		if normalize_text(ri.col)
		and ri.regex and any(normalize_text(r) for r in ri.regex)
	]
	all_items: list[LamanItem] = []
	seen_keys: set[str] = set()

	for page_idx, (page_url, page_html) in enumerate(list_pages, start=1):
		print(f"[LAMAN] ===== {page_idx} 페이지 처리 중 ===== URL: {page_url}")

		candidates = extract_list_candidates(
			html=page_html,
			list_root_selector=request.param.list_root_selector,
			item_selector=request.param.item_selector,
			target_selector=request.param.target_selector,
			limit=None if crawl_all else max(1, request.param.skip_top_count + 1),
		)

		if not candidates:
			continue

		if page_idx == 1 and request.param.skip_top_count > 0:
			candidates = candidates[request.param.skip_top_count:]

		if not candidates:
			continue

		for idx, candidate in enumerate(candidates, start=1):
			try:
				current_rank = idx
				print(f"[LAMAN] 현재 문서 색인 중: {current_rank}번째 | 제목: {candidate.get('title')}")

				# --- 상세 페이지 접근 ---
				title = candidate["title"]
				href = candidate["href"]
				onclick = candidate["onclick"]

				detail_url, access_method, open_type, detail_html, note = await open_detail_page(
					list_url=page_url,
					list_root_selector=request.param.list_root_selector,
					item_selector=request.param.item_selector,
					target_selector=request.param.target_selector,
					rank_index=idx - 1,
					href=href,
					onclick=onclick,
					ssl_mode=request.param.ssl_mode,
				)

				uid = extract_uid(detail_url)

				if not detail_html:
					items = [LamanItem(
						rank=current_rank,
						list_title=title,
						detail_url=detail_url,
						access_method=access_method,
						open_type=open_type,
						detail_access_success=False,
						fields={},
						uid=uid,
						raw_href=href,
						raw_onclick=onclick,
						note=note or "상세 view 접근 실패",
					)]
				else:
					asemby_cn = "CLIKM" + str(time.time_ns())
					asemby_cn = asemby_cn[:21]

					# ── 2뎁스: profile_selector가 있으면 프로필 상세 페이지 추가 접근 ──
					final_html = detail_html
					final_url = detail_url
					depth2_note = None

					if request.param.profile_selector:
						profile_url, profile_html, profile_note = await open_profile_page_by_selector(
							homepage_url=detail_url or page_url,
							homepage_html=detail_html,
							profile_selector=request.param.profile_selector,
							ssl_mode=request.param.ssl_mode,
						)
						if profile_html:
							final_html = profile_html
							final_url = profile_url or detail_url
							print(f"[LAMAN] 2뎁스 프로필 페이지 접근 성공: {final_url}")
						else:
							depth2_note = profile_note or "프로필 상세 페이지 접근 실패"
							error_logs.append({
								"step": f"프로필_2뎁스_{page_idx}p_{idx}",
								"title": title,
								"homepage_url": detail_url,
								"error": depth2_note,
							})

					parsed = parse_laman_detail_by_dynamic_regex(
						detail_html=final_html,
						request=request,
						list_title=title,
					)
					await handle_photo_reserved_col(
						request=request,
						parsed=parsed,
						list_item_html=str(candidate.get("row_html", "")),
						detail_html=final_html,
						list_url=page_url,
						detail_url=final_url,
						asemby_cn=asemby_cn,
						error_logs=error_logs,
						title=title,
					)

					merged_note = " / ".join(filter(None, [note, depth2_note])) or None

					items = [LamanItem(
						rank=current_rank,
						list_title=title,
						detail_url=final_url,
						access_method=access_method,
						open_type=open_type,
						detail_access_success=True,
						fields=parsed,
						uid=uid,
						asemby_cn=asemby_cn,
						raw_href=href,
						raw_onclick=onclick,
						note=merged_note,
					)]

			except ValueError as exc:
				error_logs.append({
					"step": f"상세수집_{page_idx}p_{idx}",
					"title": candidate.get("title", ""),
					"error": str(exc),
				})
				items = [LamanItem(
					rank=len(all_items) + 1,
					list_title=candidate["title"],
					detail_url=None,
					access_method="error",
					open_type=None,
					detail_access_success=False,
					fields={},
					uid=None,
					raw_href=candidate.get("href"),
					raw_onclick=candidate.get("onclick"),
					note=f"상세 처리 실패: {type(exc).__name__}",
				)]
			except Exception as exc:
				error_logs.append({
					"step": f"상세수집_{page_idx}p_{idx}",
					"title": candidate.get("title", ""),
					"error": str(exc),
				})
				items = [LamanItem(
					rank=len(all_items) + 1,
					list_title=candidate["title"],
					detail_url=None,
					access_method="error",
					open_type=None,
					detail_access_success=False,
					fields={},
					uid=None,
					raw_href=candidate.get("href"),
					raw_onclick=candidate.get("onclick"),
					note=f"상세 처리 실패: {type(exc).__name__}",
				)]

			for item in items:
				dedupe_key = item.uid or item.detail_url or f"{item.list_title}|{item.raw_href}|{item.raw_onclick}"
				if crawl_all and dedupe_key in seen_keys:
					continue
				seen_keys.add(dedupe_key)
				
				# ── 필드 감사 로그 ──
				if item.detail_access_success and item.fields:
					field_logs.append(audit_fields_laman(
						asemby_cn=item.uid or "",
						url=item.detail_url,
						item_cols=item_cols,
						fields=item.fields,
					))
		
				item.rank = len(all_items) + 1
				all_items.append(item)

	if not all_items:
		msg = "지정한 selector 기준으로 목록 item 또는 target을 찾지 못했습니다."
		error_logs.append({
			"step": "목록 수집 결과",
			"url": str(request.param.list_url),
			"error": msg
		})
		raise ValueError(msg)
	
	if field_logs:
		save_field_logs(field_logs, request)

	return LamanCrawlResponse(
		list_url=str(request.param.list_url),
		item_count=len(all_items),
		items=all_items,
	)