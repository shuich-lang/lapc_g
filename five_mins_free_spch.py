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


if sys.platform.startswith("win"):
	asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="5min Free Speech Crawl API", version="0.1.0")


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
	skip_top_count: int = Field(0)


class RegexItem(BaseModel):
	col: str = Field(..., description="응답 key 이름")
	regex: list[str] = Field(..., description="상세 HTML에서 추출할 정규식")
	xpath: list[str] = Field(None, description="(미구현) XPath 추출용 필드")
	removeTags: str = Field(..., description="HTML 태그 제거 여부: Y | N")


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
# Utility (minutes.py와 동일)
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


def to_model_dict(model) -> dict:
	if hasattr(model, "model_dump"):
		return model.model_dump()
	return model.dict()


def generate_crw_id() -> str:
	return f"CRW_{uuid4().hex}"


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

		raw_value = None
		for pattern in item.regex:
			raw_value = apply_regex_raw(detail_html, pattern)
			if raw_value is not None:
				break

		if item.removeTags == "Y":
			result[key] = strip_html_tags(raw_value)
		else:
			result[key] = normalize_text(raw_value)

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

def build_spch_callback_payload(
	request: SpchCrawlRequest,
	crawl_response: SpchCrawlResponse,
) -> dict:
	data = []
	
	for item in crawl_response.items:
		if item.fields:
			row = dict(item.fields)
			row["url"] = item.detail_url
			data.append(row)

	return {
		"req_id": request.req_id,
		"type": request.type,
		"crw_id": request.crw_id,
		"data": data,
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
	try:
		crawl_response = await crawl_spch_regex_check(request, crawl_all=True)
		payload = build_spch_callback_payload(request, crawl_response)
		await post_spch_callback(payload)
	except Exception as exc:
		traceback.print_exc()
		raise


# =========================
# Main crawl service
# =========================

async def crawl_spch_regex_check(
	request: SpchCrawlRequest,
	crawl_all: bool = False,
) -> SpchCrawlResponse:
	if not request.item:
		raise HTTPException(status_code=400, detail="item은 최소 1개 이상이어야 합니다.")

	try:
		list_pages = await build_list_pages(request, crawl_all=crawl_all)
	except Exception as exc:
		raise HTTPException(
			status_code=400,
			detail=f"목록 페이지 요청 실패: {type(exc).__name__} / {str(exc)}",
		) from exc

	all_items: list[SpchItem] = []
	seen_keys: set[str] = set()

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
				current_rank = len(all_items) + 1
				print(f"[SPCH] 현재 문서 색인 중: {current_rank}번째 | 제목: {candidate.get('title')}")

				item = await build_spch_item_by_dynamic_regex(
					request=request,
					list_page_url=page_url,
					candidate=candidate,
					rank_index_in_page=idx - 1,
					final_rank=current_rank,
				)
			except ValueError as exc:
				raise HTTPException(status_code=400, detail=str(exc)) from exc
			except Exception as exc:
				item = SpchItem(
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

	return SpchCrawlResponse(
		list_url=str(request.param.list_url),
		item_count=len(all_items),
		items=all_items,
	)