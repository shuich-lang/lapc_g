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


if sys.platform.startswith("win"):
	asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
 
app = FastAPI(title="Minutes Crawl API", version="0.8.0")


USER_AGENT = (
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
	"AppleWebKit/537.36 (KHTML, like Gecko) "
	"Chrome/122.0.0.0 Safari/537.36"
)

# CALLBACK_INSERT_API_URL = "http://211.219.26.15:18123/insert_api.do"		# 실제 CMS 서버 (도커 외부에서 접근용)
# CALLBACK_INSERT_API_URL = "http://172.17.0.1:18123/insert_api.do"			# 도커 내에서 cms 컨테이너 접근용
# CALLBACK_INSERT_API_URL = "http://localhost:8900/insert_api"				# python 내 json 저장
# CALLBACK_INSERT_API_URL = "http://localhost:9000/insert_api.do"			# 로컬 cms
CALLBACK_INSERT_API_URL = "http://10.201.38.157:8080/insert_api.do"			# 운영 cms

FILE_EXTENSIONS = ("pdf", "hwp", "hwpx", "doc", "docx", "xls", "xlsx", "zip")


# =========================
# Request / Response Model
# =========================

class MinutesParam(BaseModel):
	list_url: HttpUrl = Field(...)
	list_root_selector: str = Field(...)
	item_selector: str = Field(...)
	target_selector: str = Field(...)
	ssl_mode: str = Field("Y")
	max_pages: int = Field(500)
	rasmbly_numpr: Optional[str] = Field(None, description="대수 정보. 목록/상세에서 추출 실패 시 fallback으로 사용")
	skip_top_count: int = Field(0, description="목록 상단에서 크롤링을 건너뛸 아이템 수. 기본값 0")


class RegexItem(BaseModel):
	col: str = Field(..., description="응답 key 이름")
	regex: list[str] = Field(..., description="상세 HTML에서 추출할 정규식")
	xpath: list[str] = Field(None, description="(미구현) XPath 추출용 필드 - 향후 지원 예정")
	removeTags: str = Field(..., description="HTML 태그 제거 여부: Y | N")


class CrawlRequest(BaseModel):
	req_id: str = Field(..., description="날짜 포맷: yyyyMMddHHmmssSSSSSS")
	crw_id: Optional[str] = Field(None, description="수집 설정 구분값")
	type: str = Field(..., description="수집 유형: minutes, bill 등")
	file_dir: str = Field("", description="파일 저장 절대 경로")
	param: dict = Field(..., description="type별 크롤링 파라미터")
	item: list[RegexItem] = Field(default_factory=list, description="동적으로 추출할 항목 목록")


class RegexCrawlRequest(BaseModel):
	req_id: str = Field(...)
	crw_id: Optional[str] = Field(None)
	type: str = Field(...)
	file_dir: str = Field("")
	param: MinutesParam = Field(...)
	item: list[RegexItem] = Field(default_factory=list)


class MinutesItem(BaseModel):
	rank: int
	list_title: str

	detail_url: Optional[str] = None
	access_method: str
	open_type: Optional[str] = None
	detail_access_success: bool

	fields: dict[str, Optional[str]] = Field(default_factory=dict)

	uid: Optional[str] = None
	mints_cn: Optional[str] = None

	raw_href: Optional[str] = None
	raw_onclick: Optional[str] = None
	note: Optional[str] = None


class CrawlResponse(BaseModel):
	list_url: str
	item_count: int
	items: list[MinutesItem]


class CrawlStartResponse(BaseModel):
	type: str
	req_id: str
	crw_id: str
	ok: str
	message: str


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


def normalize_date_to_yyyymmdd(value: Optional[str]) -> Optional[str]:
	"""다양한 한국어 날짜 형식을 yyyyMMdd로 변환"""
	if not value:
		return None

	text = normalize_text(value)
	if not text:
		return None

	# 이미 yyyyMMdd 형식이면 그대로 반환
	if re.fullmatch(r"\d{8}", text):
		return text

	patterns = [
		# 2026년 4월 13일, 2026년 04월 13일
		r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일?",
		# 2026-04-13, 2026-4-13
		r"(\d{4})-(\d{1,2})-(\d{1,2})",
		# 2026/04/13
		r"(\d{4})/(\d{1,2})/(\d{1,2})",
		# 2026.04.13, 2026. 04. 13.
		r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?",
	]

	for pattern in patterns:
		match = re.search(pattern, text)
		if match:
			y, m, d = match.group(1), match.group(2), match.group(3)
			return f"{y}{int(m):02d}{int(d):02d}"

	# 변환 실패 시 원본 반환
	return text


def extract_year_from_date(date_str: Optional[str]) -> str:
	"""yyyyMMdd 또는 다양한 날짜 형식에서 연도(yyyy)를 추출. 실패 시 '0000' 반환"""
	if not date_str:
		return "0000"

	normalized = normalize_date_to_yyyymmdd(date_str)
	if normalized and len(normalized) >= 4 and normalized[:4].isdigit():
		return normalized[:4]

	return "0000"


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


def extract_rasmbly_numpr(text: str) -> Optional[str]:
	"""텍스트에서 대수(숫자)를 추출. '제9대' -> '9', '12대' -> '12'"""
	patterns = [
		r"제\s*(\d+)\s*대",
		r"(\d+)\s*대",
	]
	for pattern in patterns:
		match = re.search(pattern, text)
		if match:
			return match.group(1)
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
		parsed.scheme,
		parsed.netloc,
		parsed.path,
		parsed.params,
		new_query,
		parsed.fragment,
	))


def to_model_dict(model) -> dict:
	if hasattr(model, "model_dump"):
		return model.model_dump()
	return model.dict()


def generate_crw_id() -> str:
	return f"CRW_{uuid4().hex}"


def build_file_save_path(
	file_dir: str,
	crawl_type: str,
	crw_id: str,
	rasmbly_numpr: Optional[str],
	year: str,
	mints_cn: str,
	seq: int,
	original_filename: str,
) -> str:
	"""파일 저장 경로 생성: /{file_dir}/{type}/{crw_id}/{rasmbly_numpr}/{year}/CLICK{mints_cn}_{seq}.{확장자}"""
	ext = ""
	if original_filename and "." in original_filename:
		ext = original_filename.rsplit(".", 1)[-1].lower()

	if ext not in FILE_EXTENSIONS:
		ext = "bin"

	safe_rasmbly = normalize_text(rasmbly_numpr) if rasmbly_numpr else "unknown"
	safe_rasmbly = re.sub(r'[\\/:*?"<>|\s]+', "_", safe_rasmbly)

	filename = f"CLICK{mints_cn}_{seq}.{ext}"

	path = os.path.join(
		file_dir,
		crawl_type,
		crw_id or "unknown",
		safe_rasmbly,
		year,
		filename,
	)

	return path


def build_minutes_callback_payload(
	request: RegexCrawlRequest,
	crawl_response: CrawlResponse,
) -> dict:
	data = []

	for item in crawl_response.items:
		if item.fields:
			row = dict(item.fields)
			row["url"] = item.detail_url
			row["mints_cn"] = item.mints_cn
			data.append(row)

	return {
		"req_id": request.req_id,
		"type": request.type,
		"crw_id": request.crw_id,
		"data": data,
	}


async def post_minutes_callback(payload: dict) -> None:
	timeout = httpx.Timeout(60.0, connect=10.0)

	async with httpx.AsyncClient(timeout=timeout) as client:
		response = await client.post(
			CALLBACK_INSERT_API_URL,
			json=payload,
			headers={"Content-Type": "application/json"},
		)
		response.raise_for_status()


def parse_crawl_request(raw: CrawlRequest):
	if raw.type == "minutes":
		return RegexCrawlRequest(
			req_id=raw.req_id,
			crw_id=raw.crw_id,
			type=raw.type,
			file_dir=raw.file_dir,
			param=MinutesParam(**raw.param),
			item=raw.item,
		)

	# 나중에 다른 type 추가 시 여기서 분기
	# if raw.type == "bill":
	#     return BillCrawlRequest(...)

	raise HTTPException(status_code=400, detail=f"지원하지 않는 type입니다: {raw.type}")


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
			"rasmbly_numpr": extract_rasmbly_numpr(row_text),
		})

	if limit is None:
		return results

	return results[:limit]


# =========================
# Dynamic regex detail parsing
# =========================

def parse_minutes_detail_by_dynamic_regex(
	detail_html: str,
	request: RegexCrawlRequest,
	list_title: Optional[str] = None,
) -> dict[str, Optional[str]]:
	result: dict[str, Optional[str]] = {}

	for item in request.item:
		key = normalize_text(item.col)
		if not key:
			continue

		# regex 목록 중 "list_title" 예약어 체크
		if len(item.regex) == 1 and normalize_text(item.regex[0]).lower() == "list_title":
			value = normalize_text(list_title)
			result[key] = value or None
			continue

		# 정규식 목록을 순서대로 시도, 첫 매칭 결과 사용
		raw_value = None
		for pattern in item.regex:
			raw_value = apply_regex_raw(detail_html, pattern)
			if raw_value is not None:
				break

		if item.removeTags == "Y":
			result[key] = strip_html_tags(raw_value)
		else:
			result[key] = normalize_text(raw_value)

	# MTG_DE 날짜 포맷 정규화
	if "MTG_DE" in result and result["MTG_DE"]:
		result["MTG_DE"] = normalize_date_to_yyyymmdd(result["MTG_DE"])

	return result


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


def extract_file_info_from_reserved_value(
	raw_file_value: str,
	base_url: str,
) -> tuple[str, Optional[str]]:
	raw_value = normalize_text(raw_file_value)

	if not raw_value:
		raise ValueError("ORIGINL_FILE_URL 값이 비어 있습니다.")

	# <a ...>...</a> 전체가 넘어온 경우
	if "<a" in raw_value.lower():
		soup = BeautifulSoup(raw_value, "lxml")
		a_tag = soup.find("a")

		if not a_tag:
			raise ValueError("ORIGINL_FILE_URL에서 a 태그를 찾지 못했습니다.")

		href = normalize_text(a_tag.get("href"))
		file_name = normalize_text(a_tag.get_text(" ", strip=True))

		if not href:
			raise ValueError("ORIGINL_FILE_URL a 태그에 href가 없습니다.")

		return urljoin(base_url, href), (file_name or None)

	# 그냥 URL만 넘어온 경우
	return urljoin(base_url, raw_value), None


async def build_list_pages(
	request: RegexCrawlRequest,
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
		candidates = extract_list_candidates(
			html=html,
			list_root_selector=request.param.list_root_selector,
			item_selector=request.param.item_selector,
			target_selector=request.param.target_selector,
			limit=1,
		)
		return len(candidates) > 0

	def make_page_signature(html: str) -> str:
		candidates = extract_list_candidates(
			html=html,
			list_root_selector=request.param.list_root_selector,
			item_selector=request.param.item_selector,
			target_selector=request.param.target_selector,
			limit=None,
		)

		signature_parts = []
		for candidate in candidates[:10]:
			signature_parts.append(
				f"{candidate.get('title', '')}|{candidate.get('href', '')}|{candidate.get('onclick', '')}"
			)

		return "||".join(signature_parts)

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
				next_html = await fetch_html(next_url, request.param.ssl_mode)
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

			if target_selector == "self":
				target = item
			else:
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
					await popup.wait_for_load_state("networkidle", timeout=10000)
				except PlaywrightTimeoutError:
					pass

				detail_html = None
				for _ in range(3):
					try:
						detail_html = await popup.content()
						break
					except Exception:
						await asyncio.sleep(0.5)

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
		return None, f"playwright-error:{type(exc).__name__}", None, None, (
			f"Playwright 예외 발생: {type(exc).__name__} / {str(exc)}\n{traceback.format_exc()}"
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

async def build_minutes_item_by_dynamic_regex(
	request: RegexCrawlRequest,
	list_page_url: str,
	candidate: dict,
	rank_index_in_page: int,
	final_rank: int,
) -> MinutesItem:
	title = candidate["title"]
	href = candidate["href"]
	onclick = candidate["onclick"]

	# 1순위: 목록 row에서 대수 추출
	rasmbly_numpr = extract_rasmbly_numpr(candidate.get("row_text", ""))

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
	mints_cn = str(time.time_ns())

	# 2순위: 상세 페이지에서 대수 추출
	if not rasmbly_numpr and detail_html:
		rasmbly_numpr = extract_rasmbly_numpr(detail_html)

	# 3순위: request param fallback
	if not rasmbly_numpr:
		rasmbly_numpr = request.param.rasmbly_numpr

	if not detail_html:
		return MinutesItem(
			rank=final_rank,
			list_title=title,
			detail_url=detail_url,
			access_method=access_method,
			open_type=open_type,
			detail_access_success=False,
			fields={},
			uid=uid,
			mints_cn=mints_cn,
			raw_href=href,
			raw_onclick=onclick,
			note=note or "상세 view 접근 실패",
		)

	parsed = parse_minutes_detail_by_dynamic_regex(
		detail_html=detail_html,
		request=request,
		list_title=title,
	)

	file_value = parsed.pop("ORIGINL_FILE_URL", None)

	if file_value:
		try:
			full_file_url, extracted_file_name = extract_file_info_from_reserved_value(
				raw_file_value=file_value,
				base_url=detail_url or list_page_url,
			)

			# MTG_DE에서 연도 추출
			year = extract_year_from_date(parsed.get("MTG_DE"))

			# 임시 경로에 다운로드, 원본 파일명 확정
			save_path, saved_name = await download_attachment_file(
				file_url=full_file_url,
				file_name=extracted_file_name,
				file_dir=request.file_dir,
				crawl_type=request.type,
				crw_id=request.crw_id or "unknown",
				rasmbly_numpr=rasmbly_numpr,
				year=year,
				mints_cn=mints_cn,
				seq=1,
				ssl_mode=request.param.ssl_mode,
			)

			parsed["ORGINL_FILE_URL"] = full_file_url
			parsed["MINTS_FILE_PATH"] = save_path
			parsed["ORGINL_FILE_NM"] = saved_name

		except Exception as exc:
			parsed["ORGINL_FILE_URL"] = None
			parsed["MINTS_FILE_PATH"] = None
			parsed["ORGINL_FILE_NM"] = None
			note = f"{note} / 첨부파일 다운로드 실패: {type(exc).__name__}" if note else f"첨부파일 다운로드 실패: {type(exc).__name__}"

	return MinutesItem(
		rank=final_rank,
		list_title=title,
		detail_url=detail_url,
		access_method=access_method,
		open_type=open_type,
		detail_access_success=True,
		fields=parsed,
		uid=uid,
		mints_cn=mints_cn,
		raw_href=href,
		raw_onclick=onclick,
		note=note,
	)


async def download_attachment_file(
	file_url: str,
	file_name: Optional[str],
	file_dir: str,
	crawl_type: str,
	crw_id: str,
	rasmbly_numpr: Optional[str],
	year: str,
	mints_cn: str,
	seq: int,
	ssl_mode: str,
) -> tuple[str, str]:
	"""파일을 최종 경로에 다운로드하고 (save_path, original_name)을 반환"""
	timeout = httpx.Timeout(60.0, connect=10.0)
	headers = {"User-Agent": USER_AGENT}
	verify_option = get_verify_options(ssl_mode)

	async with httpx.AsyncClient(
		headers=headers,
		timeout=timeout,
		follow_redirects=True,
		verify=verify_option,
	) as client:
		response = await client.get(file_url)
		response.raise_for_status()

		# Content-Disposition 헤더에서 원본 파일명 추출
		original_name = None
		content_disposition = response.headers.get("content-disposition", "")
		if content_disposition:
			cd_match = re.search(
				r'filename\*?=["\']?(?:UTF-8\'\')?([^"\';\r\n]+)',
				content_disposition,
				re.IGNORECASE,
			)
			if cd_match:
				original_name = unquote(cd_match.group(1).strip())

		resolved_name = normalize_text(original_name) or normalize_text(file_name) or "unknown.bin"

		# 확정된 원본 파일명으로 저장 경로 생성
		save_path = build_file_save_path(
			file_dir=file_dir,
			crawl_type=crawl_type,
			crw_id=crw_id,
			rasmbly_numpr=rasmbly_numpr,
			year=year,
			mints_cn=mints_cn,
			seq=seq,
			original_filename=resolved_name,
		)

		os.makedirs(os.path.dirname(save_path), exist_ok=True)

		if os.path.exists(save_path):
			return save_path, resolved_name

		with open(save_path, "wb") as f:
			f.write(response.content)

	return save_path, resolved_name


def _resolve_original_filename(file_url: str, content_disposition_name: Optional[str]) -> str:
	"""원본 파일명 결정: Content-Disposition > URL path > fallback"""
	if content_disposition_name:
		return normalize_text(content_disposition_name)

	path_name = urlparse(file_url).path.split("/")[-1]
	if path_name:
		return normalize_text(unquote(path_name))

	return "unknown"


async def run_minutes_all_and_callback(request: RegexCrawlRequest) -> None:
	try:
		crawl_response = await crawl_minutes_regex_check(request, crawl_all=True)
		payload = build_minutes_callback_payload(request, crawl_response)
		await post_minutes_callback(payload)
	except Exception as exc:
		traceback.print_exc()
		raise


# =========================
# Main crawl services
# =========================

async def crawl_minutes_regex_check(
	request: RegexCrawlRequest,
	crawl_all: bool = False,
) -> CrawlResponse:
	if not request.item:
		raise HTTPException(status_code=400, detail="item은 최소 1개 이상이어야 합니다.")

	try:
		list_pages = await build_list_pages(request, crawl_all=crawl_all)
	except Exception as exc:
		raise HTTPException(
			status_code=400,
			detail=f"목록 페이지 요청 실패: {type(exc).__name__} / {str(exc)}",
		) from exc

	all_items: list[MinutesItem] = []
	seen_keys: set[str] = set()

	for page_idx, (page_url, page_html) in enumerate(list_pages, start=1):
		print(f"[CRAWL] ===== {page_idx} 페이지 처리 중 ===== URL: {page_url}")

		candidates = extract_list_candidates(
			html=page_html,
			list_root_selector=request.param.list_root_selector,
			item_selector=request.param.item_selector,
			target_selector=request.param.target_selector,
			limit=None if crawl_all else max(1, request.param.skip_top_count + 1),
		)

		if not candidates:
			continue

		# 최상단 게시물 skip 처리: 첫 페이지에서만 적용
		if page_idx == 1 and request.param.skip_top_count > 0:
			candidates = candidates[request.param.skip_top_count:]

		if not candidates:
			continue

		for idx, candidate in enumerate(candidates, start=1):
			try:
				current_rank = len(all_items) + 1

				print(f"[CRAWL] 현재 문서 색인 중: {current_rank}번째 | 제목: {candidate.get('title')}")

				item = await build_minutes_item_by_dynamic_regex(
					request=request,
					list_page_url=page_url,
					candidate=candidate,
					rank_index_in_page=idx - 1,
					final_rank=current_rank,
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
					fields={},
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
		list_url=str(request.param.list_url),
		item_count=len(all_items),
		items=all_items,
	)


# =========================
# API
# =========================

@app.post("/crawl/all", response_model=CrawlStartResponse, status_code=202)
async def crawl_all_api(
	raw: CrawlRequest,
	background_tasks: BackgroundTasks,
):
	request = parse_crawl_request(raw)

	crw_id = request.crw_id or generate_crw_id()
	request_dict = to_model_dict(request)
	request_dict["crw_id"] = crw_id
	request_copy = type(request)(**request_dict)

	background_tasks.add_task(run_minutes_all_and_callback, request_copy)

	return CrawlStartResponse(
		req_id=request_copy.req_id,
		type=request_copy.type,
		crw_id=request_copy.crw_id,
		ok="true",
		message="전체 색인을 시작했습니다.",
	)