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
# CALLBACK_INSERT_API_URL = "http://10.201.38.157:8080/insert_api.do"			# 운영 cms

FIELD_LOGS_DIR = "field_logs"

# =========================
# Request / Response Model
# =========================


class SpchParam(BaseModel):
	list_url: HttpUrl = Field(...)
	list_root_selector: str = Field(...)
	item_selector: str = Field(...)
	target_selector: str = Field(...)
	ssl_mode: str = Field("Y")
	max_pages: int = Field(500)
	skip_top_count: int = Field(0, description="상단 게시물 패스 수 (고정 공지글 방어용)")
	is_multi_spch: str = Field("N", description="한 목록 당다건 추출이면 Y, 단건 추출이면 N")


class RegexItem(BaseModel):
	col: str = Field(..., description="응답 key 이름")
	regex: list[str] = Field(..., description="상세 HTML에서 추출할 정규식")
	xpath: list[str] = Field(None, description="(미구현) XPath 추출용 필드")
	removeTags: str = Field("Y", description="HTML 태그 제거 여부: Y | N")


class SpchCrawlRequest(BaseModel):
	req_id: str = Field(...)
	crw_id: Optional[str] = Field(None)
	type: str = Field(...)
	param: SpchParam = Field(...)
	item: list[RegexItem] = Field(default_factory=list)


class SpchItem(BaseModel):
	rank: int
	list_title: str

	detail_url: Optional[str] = None
	access_method: str
	open_type: Optional[str] = None
	detail_access_success: bool

	fields: dict[str, Optional[str]] = Field(default_factory=dict)

	uid: Optional[str] = None

	raw_href: Optional[str] = None
	raw_onclick: Optional[str] = None
	note: Optional[str] = None


class SpchCrawlResponse(BaseModel):
	list_url: str
	item_count: int
	items: list[SpchItem]


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


def audit_fields_minutes(
	mints_cn: str,
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
		"mints_cn":  mints_cn,
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
		})

	if limit is None:
		return results
	return results[:limit]


# =========================
# Dynamic regex detail parsing
# =========================

def parse_spch_detail_by_dynamic_regex(
	detail_html: str,
	request: SpchCrawlRequest,
	list_title: Optional[str] = None,
) -> dict[str, Optional[str]]:
	result: dict[str, Optional[str]] = {}

	for item in request.item:
		key = normalize_text(item.col)
		if not key:
			continue

		if len(item.regex) == 1 and normalize_text(item.regex[0]).lower() == "list_title":
			value = normalize_text(list_title)
			result[key] = value or None
			continue

		raw_value = apply_regex_raw(detail_html, item.regex)

		if item.removeTags == "Y":
			result[key] = strip_html_tags(raw_value)
		else:
			result[key] = normalize_text(raw_value)

	return result


def parse_spch_detail_multi(
	detail_html: str,
	request: SpchCrawlRequest,
	list_title: Optional[str] = None,
) -> list[dict[str, Optional[str]]]:
	"""multi_speech 모드: 각 RegexItem을 findall → 인덱스 매칭으로 N건 생성."""
	columns: dict[str, Optional[list[Optional[str]]]] = {}
	max_count = 0

	for item in request.item:
		key = normalize_text(item.col)
		if not key:
			continue

		if len(item.regex) == 1 and normalize_text(item.regex[0]).lower() == "list_title":
			columns[key] = None
			continue

		raw_values = apply_regex_all(detail_html, item.regex)

		if item.removeTags == "Y":
			columns[key] = [strip_html_tags(v) for v in raw_values]
		else:
			columns[key] = [normalize_text(v) for v in raw_values]

		max_count = max(max_count, len(columns[key]))

	if max_count == 0:
		return []

	for key, values in columns.items():
		if values is None:
			columns[key] = [normalize_text(list_title)] * max_count
		elif len(values) == 1 and max_count > 1:
			columns[key] = values * max_count
		elif len(values) < max_count:
			columns[key] = values + [None] * (max_count - len(values))

	return [
		{key: vals[i] for key, vals in columns.items()}
		for i in range(max_count)
	]


# 제목 + 날짜 + 이름 동일할 경우 spch_content 병합 (시간 초과로 마이크 꺼짐 방어용)
def merge_spch_rows(
	rows: list[dict[str, Optional[str]]],
	key_cols: tuple[str, ...] = ("SPCH_MEN", "SPCH_TITLE", "SPCH_DATE"),
	merge_col: str = "SPCH_CONTENT",
) -> list[dict[str, Optional[str]]]:
	"""동일 의원+제목+날짜인 행을 합쳐서 SPCH_CONTENT를 이어붙인다."""
	if not rows:
		return rows
	merged: list[dict[str, Optional[str]]] = []
	for row in rows:
		key = tuple(row.get(c) for c in key_cols)
		found = None
		for prev in merged:
			prev_key = tuple(prev.get(c) for c in key_cols)
			if prev_key == key:
				found = prev
				break
		if found is not None:
			prev_content = found.get(merge_col) or ""
			cur_content = row.get(merge_col) or ""
			found[merge_col] = prev_content + cur_content
		else:
			merged.append(dict(row))
	return merged


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
	request: SpchCrawlRequest,
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
	request: SpchCrawlRequest,
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
		print(f"[SPCH] httpx 목록 조회 결과 항목 없음 → Playwright 폴백 시도")
		try:
			first_html = await fetch_list_html_by_playwright(
				list_url, 
				ssl_mode=request.param.ssl_mode
			)
			use_playwright = True
			if not has_list_items(first_html):
				print(f"[SPCH] Playwright 목록 조회에서도 항목 없음")
		except Exception as e:
			print(f"[SPCH] Playwright 목록 조회 실패: {e}")

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
			
		print("[SPCH] 일반 페이징 실패 → Playwright fallback")

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


# =========================
# Shared builders
# =========================

async def build_spch_item_by_dynamic_regex(
	request: SpchCrawlRequest,
	list_page_url: str,
	candidate: dict,
	rank_index_in_page: int,
	final_rank: int,
) -> SpchItem:
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
		return SpchItem(
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

	parsed = parse_spch_detail_by_dynamic_regex(
		detail_html=detail_html,
		request=request,
		list_title=title,
	)

	return SpchItem(
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
# Callback
# =========================

def make_row_dedupe_key(row: dict) -> str:
	title = normalize_text(row.get("SPCH_TITLE"))
	date = normalize_text(row.get("SPCH_DATE"))
	men = normalize_text(row.get("SPCH_MEN"))
	content_head = normalize_text(row.get("SPCH_CONTENT"))[:50]

	return f"{title}|{date}|{men}|{content_head}"


def build_spch_callback_payload(
	request: SpchCrawlRequest,
	crawl_response: SpchCrawlResponse,
	error_logs: list | None = None,
	error: str = "",
) -> dict:
	data = []
	seen_row_keys = set()
	_error_logs = error_logs or []
	
	for item in crawl_response.items:
		if item.fields:
			row = dict(item.fields)
			row["url"] = item.detail_url

			dedupe_key = make_row_dedupe_key(row)
			if dedupe_key in seen_row_keys:
				print(f"[SPCH] 중복 발견 skip - dedupe_key: {dedupe_key}")
				continue

			seen_row_keys.add(dedupe_key)
			data.reverse()  # 뒤집어서 앞에 추가 (최신이 앞에 오도록)
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


async def post_spch_callback(payload: dict) -> None:
	timeout = httpx.Timeout(60.0, connect=10.0)
	async with httpx.AsyncClient(timeout=timeout) as client:
		response = await client.post(
			CALLBACK_INSERT_API_URL,
			json=payload,
			headers={"Content-Type": "application/json"},
		)
		response.raise_for_status()


async def run_spch_all_and_callback(request: SpchCrawlRequest) -> None:
	error_logs: list[dict] = []
	crawl_response: Optional[SpchCrawlRequest] = None

	try:
		crawl_response = await crawl_spch_regex_check(request, crawl_all=True, error_logs=error_logs)
		payload = build_spch_callback_payload(request, crawl_response, error_logs=error_logs)
	except Exception as exc:
		traceback.print_exc()
	
		if crawl_response is None:
			crawl_response = SpchCrawlRequest(
				list_url=str(request.param.list_url),
				item_count=0,
				items=[],
			)
		error_logs.append({
			"step": "run_spch_all_and_callback",
			"error": f"{type(exc).__name__}: {str(exc)}",
		})
		payload = build_spch_callback_payload(
			request, crawl_response, error_logs=error_logs, error=str(exc)
		)
	
	try:
		await post_spch_callback(payload)
	except Exception as cb_exc:
		print(f"[CALLBACK] 콜백 전송 실패: {cb_exc}")
		traceback.print_exc()


# =========================
# Main crawl service
# =========================

async def crawl_spch_regex_check(
	request: SpchCrawlRequest,
	crawl_all: bool = False,
	error_logs: list[dict] | None = None,
) -> SpchCrawlResponse:
	if error_logs is None:
		error_logs = []

	if not request.item:
		error_logs.append({
			"step": "파라미터 검증",
			"error": "item은 최소 1개 이상이어야 합니다.",
		})
		return SpchCrawlResponse(
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
		return SpchCrawlResponse(
			list_url=str(request.param.list_url),
			item_count=0,
			items=[],
		)

	is_multi = request.param.is_multi_spch == "Y"
	field_logs: list[dict] = []
	item_cols: list[str] = [
		normalize_text(ri.col)
		for ri in request.item
		if normalize_text(ri.col)
		and ri.regex and any(normalize_text(r) for r in ri.regex)
	]
	all_items: list[SpchItem] = []
	seen_keys: set[str] = set()
	visited_urls: set[str] = set()

	for page_idx, (page_url, page_html) in enumerate(list_pages, start=1):
		print(f"[SPCH] ===== {page_idx} 페이지 처리 중 ===== URL: {page_url}")

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
				print(f"[SPCH] 현재 문서 색인 중: {current_rank}번째 | 제목: {candidate.get('title')}")

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

				# -- 다건 item: 이미 방문한 url은 skip
				if is_multi and detail_url and detail_url in visited_urls:
					print(f"[SPCH] multi 모드 - 이미 방문한 URL skip: {detail_url}")
					continue
				if is_multi and detail_url:
					visited_urls.add(detail_url)

				uid = extract_uid(detail_url)

				if not detail_html:
					items = [SpchItem(
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
				elif is_multi:
					parsed_list = parse_spch_detail_multi(
						detail_html=detail_html,
						request=request,
						list_title=title,
					)
					parsed_list = merge_spch_rows(parsed_list)
					if not parsed_list:
						print(f"[SPCH] 문서 {idx}번째 | multi 모드이나 매치 결과 없음")
						items = [SpchItem(
							rank=current_rank,
							list_title=title,
							detail_url=detail_url,
							access_method=access_method,
							open_type=open_type,
							detail_access_success=True,
							fields={},
							uid=uid,
							raw_href=href,
							raw_onclick=onclick,
							note=(note or "") + " | multi 모드이나 매치 결과 없음",
						)]
					else:
						for speech_idx, parsed in enumerate(parsed_list, start=1):
							speaker_name = parsed.get("SPCH_MEN") or "의원명 없음"
							print(f"[SPCH] 문서 {idx}번째 | 발언 {speech_idx} 수집 성공 ({speaker_name} 의원)")

						items = [
							SpchItem(
								rank=current_rank + i,
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
							for i, parsed in enumerate(parsed_list)
						]
				else:
					parsed = parse_spch_detail_by_dynamic_regex(
						detail_html=detail_html,
						request=request,
						list_title=title,
					)
					items = [SpchItem(
						rank=current_rank,
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
					)]

			except ValueError as exc:
				error_logs.append({
					"step": f"상세수집_{page_idx}p_{idx}",
					"title": candidate.get("title", ""),
					"error": str(exc),
				})
				items = [SpchItem(
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
				items = [SpchItem(
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
				if is_multi:
					# multi: 같은 URL이라도 발언 내용이 다르면 별개 건
					field_sig = "|".join(str(v) for v in item.fields.values() if v)
					dedupe_key = f"{item.detail_url}|{field_sig}"
				else:
					dedupe_key = item.uid or item.detail_url or f"{item.list_title}|{item.raw_href}|{item.raw_onclick}"
				if crawl_all and dedupe_key in seen_keys:
					continue
				seen_keys.add(dedupe_key)
				
				# ── 필드 감사 로그 ──
				if item.detail_access_success and item.fields:
					field_logs.append(audit_fields_minutes(
						mints_cn=item.uid or "",
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

	return SpchCrawlResponse(
		list_url=str(request.param.list_url),
		item_count=len(all_items),
		items=all_items,
	)