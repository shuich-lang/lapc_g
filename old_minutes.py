"""
old_minutes.py
──────────────
폐지된(통합된) 과거 지방의회 회의록 수집 모듈.

두 가지 트리 탐색 전략을 지원한다:
  - async_api       : AJAX API를 재귀 호출하여 트리를 탐색 (연기군 등)
  - page_navigation : 페이지 이동(href)으로 트리를 탐색 (북제주 등)

트리 탐색으로 회의록 URL 목록을 확보한 뒤,
각 페이지에서 정규식 기반으로 본문을 파싱하여 CMS에 콜백한다.
"""

from __future__ import annotations

import asyncio
import re
import time
import traceback
from dataclasses import dataclass, field as dc_field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, NavigableString
from pydantic import BaseModel, Field, HttpUrl

# ── minutes.py 공통 함수 재사용 ──────────────────────────────────
from minutes import (
	RegexItem,
	MinutesItem,
	USER_AGENT,
	CALLBACK_INSERT_API_URL,
	normalize_text,
	get_verify_options,
	parse_minutes_detail_by_dynamic_regex,
	extract_file_info_from_reserved_value,
	download_attachment_file,
	post_minutes_callback,
	_build_result,
)


# =================================================================
# Request / Response Model
# =================================================================

class OldMinutesParam(BaseModel):
	base_url: HttpUrl = Field(
		...,
		description="대상 사이트 베이스 URL (예: https://council.sejong.go.kr)",
	)
	tree_type: str = Field(
		"async_api",
		description="트리 탐색 방식. async_api | page_navigation",
	)
	# ── async_api 전용 ──
	tree_api_path: str = Field(
		"/cms/mntsGunSesnTreeChk.do",
		description="[async_api] 트리 AJAX API 경로",
	)
	viewer_path: str = Field(
		"/cms/mntsGunViewer.do",
		description="[async_api] 회의록 뷰어 경로. ?mntsId= 뒤에 ID가 붙는다.",
	)
	# ── page_navigation 전용 ──
	tree_entry_path: str = Field(
		"",
		description="[page_navigation] 트리 최상위 페이지 경로 (예: /source/past/njeju/minutes.html)",
	)
	ssl_mode: str = Field("Y")
	request_delay: float = Field(
		0.3,
		description="API 호출 간 딜레이(초). 서버 부하 방지용.",
	)
	encoding: str = Field(
		"",
		description="응답 인코딩. 빈 값이면 자동 감지. 예: euc-kr, cp949",
	)
	crawl_mode: str = Field(
		"html",
		description="수집 방식. html: 본문 정규식 파싱 / file: 파일 다운로드만",
	)
	viewer_id_param: str = Field(
		"mntsId",
		description="[async_api] 뷰어 URL의 ID 파라미터 키. 예: mntsId, schSn",
	)
	tree_var11_sesn: str = Field(
		"sesn",
		description="[async_api] 대수→회차 조회 시 var11 값. 예: sesn, SvcMntsTreeGnrtnSesn",
	)


class OldMinutesCrawlRequest(BaseModel):
	req_id: str = Field(..., description="요청 ID (yyyyMMddHHmmssSSSSSS)")
	crw_id: str = Field(..., description="기관 코드")
	bbs_id: str = Field(..., description="게시판 ID")
	type: str = Field(..., description="old_minutes")
	file_dir: str = Field("", description="파일 저장 절대 경로")
	param: OldMinutesParam
	item: list[RegexItem] = Field(
		default_factory=list,
		description="동적으로 추출할 항목 목록 (정규식 기반)",
	)
	generations: list[str] = Field(
		default_factory=list,
		description="수집 대상 대수 ID 목록. 비어있으면 전체 대수 수집.",
	)
	test: str = Field(
		"",
		description="Y면 최대 10건만 수집. 그 외(N, 빈값, 키 없음)는 전체 수집.",
	)


# =================================================================
# Tree Node 타입
# =================================================================

@dataclass
class TreeNode:
	"""트리 API 응답의 단일 노드."""
	text: str
	node_id: str
	vars: dict[str, str] = dc_field(default_factory=dict)
	has_children: bool = False


@dataclass
class MinutesLeaf:
	"""트리 리프 — 회의록 본문 접근에 필요한 정보.

	async_api 에서는 mnts_id 가 뷰어 파라미터 값,
	page_navigation 에서는 mnts_id 가 전체 URL 이 된다.
	"""
	mnts_id: str
	title: str
	generation: str = ""
	session_name: str = ""
	committee_name: str = ""
	meeting_order: str = ""


# =================================================================
#  전략 1 — async_api (AJAX 비동기 트리)
# =================================================================

VAR_KEYS = [f"var{str(i).zfill(2)}" for i in range(1, 13)]


def _empty_vars() -> dict[str, str]:
	return {k: "" for k in VAR_KEYS}


def _build_tree_form_data(vars_dict: dict[str, str]) -> dict[str, str]:
	form: dict[str, str] = {
		"additional": datetime.now().strftime(
			"yeah: %a %b %d %Y %H:%M:%S GMT+0900 (한국 표준시)"
		),
	}
	for k in VAR_KEYS:
		form[k] = vars_dict.get(k, "")
	return form


def _parse_tree_response(json_list: list[dict]) -> list[TreeNode]:
	nodes: list[TreeNode] = []
	for item in json_list:
		vars_dict = {k: item.get(k, "") for k in VAR_KEYS}
		nodes.append(TreeNode(
			text=item.get("text", ""),
			node_id=item.get("id", ""),
			vars=vars_dict,
			has_children=item.get("hasChildren", False),
		))
	return nodes


async def _fetch_tree_children(
	client: httpx.AsyncClient,
	api_url: str,
	vars_dict: dict[str, str],
) -> list[TreeNode]:
	form_data = _build_tree_form_data(vars_dict)
	resp = await client.post(api_url, data=form_data)
	resp.raise_for_status()
	return _parse_tree_response(resp.json())


async def _fetch_html_with_encoding(url: str, ssl_mode: str, encoding: str = "") -> str:
	"""인코딩을 지정하여 HTML을 가져온다. encoding이 비어있으면 기존 fetch_html과 동일."""
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

		if encoding:
			return response.content.decode(encoding, errors="replace")
		return response.text


async def _crawl_tree_recursive(
	client: httpx.AsyncClient,
	api_url: str,
	node: TreeNode,
	context: dict[str, str],
	results: list[MinutesLeaf],
	delay: float,
):
	ctx = {**context}
	level = node.vars.get("var11", "")

	level_ctx_map = {
		# 연기군 (짧은 코드)
		"sesn": "generation",
		"mn": "session_name",
		"cha": "committee_name",
		"agnd": "meeting_order",
		# 청주 등 (긴 이름)
		"SvcMntsTreeGnrtnSesn": "generation",
		"SvcMntsTreeGnrtnCmmtNm": "session_name",
		"SvcMntsTreeGnrtnChasu": "committee_name",
		"SvcMntsTreeAgnd": "meeting_order",
	}
	if level in level_ctx_map:
		ctx[level_ctx_map[level]] = node.text

	if node.has_children:
		await asyncio.sleep(delay)
		try:
			children = await _fetch_tree_children(client, api_url, node.vars)
		except Exception as exc:
			print(f"[OLD_MINUTES:ASYNC] 트리 자식 조회 실패: {node.text} / {exc}")
			return

		for child in children:
			await _crawl_tree_recursive(client, api_url, child, ctx, results, delay)
	else:
		mnts_id = node.vars.get("var12", "")
		if not mnts_id:
			mnts_id = node.node_id.replace("cha", "")
		if mnts_id:
			results.append(MinutesLeaf(
				mnts_id=mnts_id,
				title=node.text,
				generation=ctx.get("generation", ""),
				session_name=ctx.get("session_name", ""),
				committee_name=ctx.get("committee_name", ""),
				meeting_order=ctx.get("meeting_order", ""),
			))


async def _collect_leaves_by_async_api(
	request: OldMinutesCrawlRequest,
	error_logs: list[dict],
) -> list[MinutesLeaf]:
	"""비동기 트리 API 방식으로 리프 수집."""
	base_url = str(request.param.base_url).rstrip("/")
	api_url = base_url + request.param.tree_api_path
	ssl_mode = request.param.ssl_mode
	delay = request.param.request_delay

	async with httpx.AsyncClient(
		headers={"User-Agent": USER_AGENT},
		timeout=httpx.Timeout(20.0, connect=10.0),
		follow_redirects=True,
		verify=get_verify_options(ssl_mode),
	) as client:

		generation_ids = list(request.generations)

		if not generation_ids:
			try:
				root_vars = _empty_vars()
				root_vars["var09"] = "gnrtn"
				root_vars["var11"] = request.param.tree_var11_sesn
				top_nodes = await _fetch_tree_children(client, api_url, root_vars)
				generation_ids = [
					re.search(r"\d+", n.node_id).group()
					for n in top_nodes
					if re.search(r"\d+", n.node_id)
				]
			except Exception as exc:
				print(f"[OLD_MINUTES:ASYNC] 대수 목록 자동 조회 실패: {exc}")
				error_logs.append({
					"step": "대수 목록 조회",
					"error": f"{type(exc).__name__}: {str(exc)}",
				})

		if not generation_ids:
			error_logs.append({
				"step": "대수 목록 확인",
				"error": "수집 대상 대수를 찾지 못했습니다. generations 파라미터를 직접 지정해주세요.",
			})
			return []

		results: list[MinutesLeaf] = []

		for gen_id in generation_ids:
			print(f"[OLD_MINUTES:ASYNC] ── 제{gen_id}대 트리 탐색 시작 ──")

			root_vars = _empty_vars()
			root_vars["var01"] = gen_id
			root_vars["var09"] = "gnrtn"
			root_vars["var11"] = request.param.tree_var11_sesn

			root_node = TreeNode(
				text=f"제{gen_id}대",
				node_id=f"gnrtn{gen_id}",
				vars=root_vars,
				has_children=True,
			)

			try:
				await _crawl_tree_recursive(
					client, api_url, root_node,
					{"generation": f"제{gen_id}대"},
					results, delay,
				)
			except Exception as exc:
				error_logs.append({
					"step": f"제{gen_id}대 트리 탐색",
					"error": f"{type(exc).__name__}: {str(exc)}",
				})

			print(f"[OLD_MINUTES:ASYNC] 제{gen_id}대 완료 (누적: {len(results)}건)")

	print(f"[OLD_MINUTES:ASYNC] 트리 탐색 완료: 총 {len(results)}건")
	return results


# =================================================================
#  전략 2 — page_navigation (페이지 이동 방식)
# =================================================================

def _is_folder_link(href: str) -> bool:
	"""하위 페이지 이동 링크인지 판별 (회의록 본문이 아닌 트리 탐색 링크)."""
	indicators = ["minutes2.html", "minutes.html", "tag=t_tth", "tag=t_tname"]
	return any(ind in href for ind in indicators)


def _extract_direct_text(element) -> str:
	"""자식 태그(ul 등)를 제외한, 요소 직계 텍스트만 추출."""
	parts: list[str] = []
	for child in element.children:
		if isinstance(child, NavigableString):
			t = normalize_text(str(child))
			if t:
				parts.append(t)
	return " ".join(parts)


def _extract_url_from_a_tag(a_tag, page_url: str) -> str:
	"""a 태그에서 실제 URL을 추출.
	
	href가 유효하면 href 사용.
	href가 javascript://이면 onclick의 window.open URL 추출.
	"""
	href = normalize_text(a_tag.get("href", ""))

	if href and not href.startswith("javascript"):
		return urljoin(page_url, href)

	onclick = a_tag.get("onclick", "")
	if onclick:
		match = re.search(r"window\.open\(['\"]([^'\"]+)['\"]", onclick)
		if match:
			return urljoin(page_url, match.group(1))

	return ""


def _parse_leaf_links_from_tree(
	tree_element,
	page_url: str,
	generation: str,
	session_name: str,
	results: list[MinutesLeaf],
):
	"""HTML 트리 내의 최종 회의록 링크를 파싱하여 MinutesLeaf로 수집.

	li.minus 내부 또는 target="_blank" 를 가진 a 태그가 회의록 본문 링크이다.
	상위 li.folder 의 직계 텍스트에서 회의체명(본회의 등)을 추출한다.
	"""
	# li.minus 안의 a[href] 를 회의록 본문 링크로 수집. li.minus가 없으면 li.folder 하위의 모든 직계 li도 포함
	leaf_items = tree_element.select("li.minus")
	if not leaf_items:
		# class 없는 li 중 a 태그를 가진 것들 (onclick 포함)
		leaf_items = [
			li for li in tree_element.find_all("li")
			if not li.get("class")
			and (li.find("a", href=True) or li.find("a", onclick=True))
		]

	if leaf_items:
		for leaf_li in leaf_items:
			# 기존
			a_tags = leaf_li.find_all("a", href=True)
			if not a_tags:
				a_tags = leaf_li.find_all("a", onclick=True)

			# 변경: href와 onclick 둘 다 한 번에 탐색
			a_tags = leaf_li.find_all("a", href=True) + leaf_li.find_all("a", onclick=True)
			# 중복 제거
			seen = set()
			unique_tags = []
			for a in a_tags:
				if id(a) not in seen:
					seen.add(id(a))
					unique_tags.append(a)
			a_tags = unique_tags

			if not a_tags:
				continue
			a_tag = a_tags[-1]

			text = normalize_text(a_tag.get_text(" ", strip=True))
			full_url = _extract_url_from_a_tag(a_tag, page_url)

			if not full_url or not text or _is_folder_link(full_url):
				continue
			a_tag = a_tags[-1]

			text = normalize_text(a_tag.get_text(" ", strip=True))
			full_url = _extract_url_from_a_tag(a_tag, page_url)

			if not full_url or not text or _is_folder_link(full_url):
				continue

			# 회의체명: 가장 가까운 상위 li.folder 의 직계 텍스트
			committee_name = ""
			parent_folder = leaf_li.find_parent("li", class_="folder")
			if parent_folder:
				committee_name = _extract_direct_text(parent_folder)

			# 차수: 직계 부모 li.minus (자신이 아닌) 의 직계 텍스트
			meeting_order = ""
			parent_minus = leaf_li.find_parent("li", class_="minus")
			if parent_minus and parent_minus != leaf_li:
				meeting_order = _extract_direct_text(parent_minus)

			results.append(MinutesLeaf(
				mnts_id=full_url,
				title=text,
				generation=generation,
				session_name=session_name,
				committee_name=committee_name,
				meeting_order=meeting_order,
			))
		return

	# li.minus 가 없으면 target="_blank" a 태그를 수집
	for a_tag in tree_element.select("a[href][target='_blank'], a[onclick]"):
		text = normalize_text(a_tag.get_text(" ", strip=True))
		full_url = _extract_url_from_a_tag(a_tag, page_url)
		if not full_url or not text or _is_folder_link(full_url):
			continue
		results.append(MinutesLeaf(
			mnts_id=full_url,
			title=text,
			generation=generation,
			session_name=session_name,
		))


async def _collect_leaves_by_page_navigation(
	request: OldMinutesCrawlRequest,
	error_logs: list[dict],
) -> list[MinutesLeaf]:
	"""페이지 이동 방식 트리에서 모든 회의록 리프를 수집."""
	base_url = str(request.param.base_url).rstrip("/")
	entry_path = request.param.tree_entry_path
	ssl_mode = request.param.ssl_mode
	delay = request.param.request_delay
	target_generations = set(request.generations) if request.generations else None

	results: list[MinutesLeaf] = []

	# ── 1단계: 최상위 페이지 ──
	entry_url = base_url + entry_path
	try:
		entry_html = await _fetch_html_with_encoding(entry_url, ssl_mode, request.param.encoding)
	except Exception as exc:
		error_logs.append({
			"step": "최상위 페이지 조회",
			"url": entry_url,
			"error": f"{type(exc).__name__}: {str(exc)}",
		})
		return []

	soup = BeautifulSoup(entry_html, "lxml")
	tree_root = soup.select_one("#tree") or soup

	# ── 2단계: 대수 링크 수집 ──
	# 대수 링크(tag=t_tth)와 회차 링크(tag=t_tname)가 같은 셀렉터로 잡히므로
	# daesu 파라미터가 있으면서 회차 링크가 아닌 것만 대수 링크로 판별
	gen_links: list[tuple[str, str, str]] = []  # (url, text, daesu)
	for a_tag in tree_root.select("li.folder > a[href]"):
		href = normalize_text(a_tag.get("href", ""))
		text = normalize_text(a_tag.get_text(" ", strip=True))
		if not href:
			continue

		daesu_match = re.search(r"daesu=(\d+)", href)
		daesu = daesu_match.group(1) if daesu_match else ""

		if not daesu:
			continue
		if "tag=t_tname" in href:
			continue

		if target_generations and daesu not in target_generations:
			continue

		full_url = urljoin(entry_url, href)
		gen_links.append((full_url, text, daesu))

	if not gen_links:
		# 대수 링크가 없으면 현재 페이지 자체가 이미 전개된 트리일 수 있음
		_parse_leaf_links_from_tree(tree_root, entry_url, "", "", results)
		if results:
			print(f"[OLD_MINUTES:PAGE_NAV] 최상위 페이지에서 직접 {len(results)}건 수집")
			return results

		error_logs.append({
			"step": "대수 목록 파싱",
			"url": entry_url,
			"error": "대수 링크를 찾지 못했습니다.",
		})
		return []

	# ── 3단계: 대수별 → 회차별 → 리프 수집 ──
	for gen_url, gen_text, daesu in gen_links:
		print(f"[OLD_MINUTES:PAGE_NAV] ── 대수 조회: {gen_text} ──")
		await asyncio.sleep(delay)

		try:
			gen_html = await _fetch_html_with_encoding(gen_url, ssl_mode, request.param.encoding)
		except Exception as exc:
			error_logs.append({
				"step": f"대수 페이지 조회 (daesu={daesu})",
				"url": gen_url,
				"error": f"{type(exc).__name__}: {str(exc)}",
			})
			continue

		gen_soup = BeautifulSoup(gen_html, "lxml")
		gen_tree = gen_soup.select_one("#tree") or gen_soup

		# 회차 링크 수집 (tag=t_tname 또는 th= 파라미터)
		session_links: list[tuple[str, str]] = []
		for a_tag in gen_tree.select("li.folder > a[href]"):
			href = normalize_text(a_tag.get("href", ""))
			text = normalize_text(a_tag.get_text(" ", strip=True))
			if not href or not text:
				continue
			if "tag=t_tname" in href or ("th=" in href and "tag=t_tth" not in href):
				session_links.append((urljoin(gen_url, href), text))

		# 회차 링크가 없으면 현재 페이지에 이미 트리가 펼쳐져 있음
		if not session_links:
			_parse_leaf_links_from_tree(gen_tree, gen_url, gen_text, "", results)
			print(f"[OLD_MINUTES:PAGE_NAV] {gen_text} — 직접 파싱 (누적: {len(results)}건)")
			continue

		for sess_url, sess_text in session_links:
			print(f"[OLD_MINUTES:PAGE_NAV]   회차 조회: {sess_text}")
			await asyncio.sleep(delay)

			try:
				sess_html = await _fetch_html_with_encoding(sess_url, ssl_mode, request.param.encoding)
			except Exception as exc:
				error_logs.append({
					"step": "회차 페이지 조회",
					"url": sess_url,
					"title": sess_text,
					"error": f"{type(exc).__name__}: {str(exc)}",
				})
				continue

			sess_soup = BeautifulSoup(sess_html, "lxml")
			sess_tree = sess_soup.select_one("#tree") or sess_soup

			_parse_leaf_links_from_tree(sess_tree, sess_url, gen_text, sess_text, results)

		print(f"[OLD_MINUTES:PAGE_NAV] {gen_text} 완료 (누적: {len(results)}건)")

	print(f"[OLD_MINUTES:PAGE_NAV] 트리 탐색 완료: 총 {len(results)}건")
	return results


def _fix_filename_encoding(name: str, encoding: str) -> str:
	"""Content-Disposition에서 잘못 디코딩된 파일명을 보정."""
	if not name or not encoding:
		return name
	try:
		# UTF-8로 잘못 디코딩된 바이트를 원래 인코딩으로 재디코딩
		return name.encode("latin-1").decode(encoding)
	except (UnicodeDecodeError, UnicodeEncodeError):
		return name


# =================================================================
# 리프 수집 분기
# =================================================================

async def _collect_all_leaves(
	request: OldMinutesCrawlRequest,
	error_logs: list[dict],
) -> list[MinutesLeaf]:
	"""tree_type에 따라 적절한 전략으로 리프를 수집."""
	tree_type = request.param.tree_type

	if tree_type == "page_navigation":
		return await _collect_leaves_by_page_navigation(request, error_logs)

	return await _collect_leaves_by_async_api(request, error_logs)


# =================================================================
# 본문 수집 + 정규식 파싱
# =================================================================

async def crawl_old_minutes(
	request: OldMinutesCrawlRequest,
	error_logs: list[dict],
) -> list[MinutesItem]:
	"""트리 탐색 → 회의록 본문 수집 → 정규식 파싱 → MinutesItem 리스트 반환."""
	base_url = str(request.param.base_url).rstrip("/")
	viewer_path = request.param.viewer_path
	tree_type = request.param.tree_type
	max_items = 70 if request.test == "Y" else 0  # 0 = 무제한

	leaves = await _collect_all_leaves(request, error_logs)
	if not leaves:
		return []

	all_items: list[MinutesItem] = []
	seen_keys: set[str] = set()
	crawl_mode = request.param.crawl_mode

	for idx, leaf in enumerate(leaves, start=1):
		# async_api  → mntsId 기반 뷰어 URL 생성
		# page_navigation → leaf.mnts_id 가 이미 전체 URL

		if max_items and len(all_items) >= max_items:
			print(f"[OLD_MINUTES] 테스트 모드: {max_items}건 도달, 수집 중단")
			break

		if tree_type == "page_navigation":
			viewer_url = leaf.mnts_id
		else:
			id_param = request.param.viewer_id_param
			viewer_url = f"{base_url}{viewer_path}?{id_param}={leaf.mnts_id}"

		print(f"[OLD_MINUTES] [{idx}/{len(leaves)}] 본문 수집: {leaf.title} | {viewer_url}")

		# ── 파일 다운로드 모드 ──
		if crawl_mode == "file":
			mints_cn = ("CLIKR" + str(time.time_ns()))[:21]
			parsed: dict[str, Optional[str]] = {}

			# 트리 메타정보 주입
			gen_match = re.search(r"(\d+)", leaf.generation)
			if gen_match:
				parsed["RASMBLY_NUMPR"] = gen_match.group(1)

			# 파일명 추출
			from urllib.parse import urlparse, unquote
			url_path = urlparse(viewer_url).path
			decode_encoding = request.param.encoding or "utf-8"
			file_name = unquote(url_path.split("/")[-1], encoding=decode_encoding) or f"minutes_{idx}"

			try:
				year = datetime.now().strftime("%Y")
				save_path, saved_name, file_url = await download_attachment_file(
					file_url=viewer_url,
					file_name=file_name,
					file_dir=request.file_dir,
					crawl_type=request.type,
					crw_id=request.crw_id or "unknown",
					rasmbly_numpr=parsed.get("RASMBLY_NUMPR"),
					year=year,
					mints_cn=mints_cn,
					seq=1,
					ssl_mode=request.param.ssl_mode,
					detail_url=viewer_url,
				)
				parsed["ORGINL_FILE_URL"] = file_url
				parsed["MINTS_FILE_PATH"] = save_path
				parsed["ORGINL_FILE_NM"] = _fix_filename_encoding(
					saved_name, request.param.encoding
				)
				note = None
			except Exception as exc:
				parsed["ORGINL_FILE_URL"] = None
				parsed["MINTS_FILE_PATH"] = None
				parsed["ORGINL_FILE_NM"] = None
				fail_msg = f"파일 다운로드 실패: {type(exc).__name__}: {str(exc)}"
				note = fail_msg
				error_logs.append({
					"step": f"파일다운로드_{idx}",
					"title": leaf.title,
					"url": viewer_url,
					"error": fail_msg,
				})

			all_items.append(MinutesItem(
				rank=idx,
				list_title=leaf.title,
				detail_url=viewer_url,
				access_method="page-navigation",
				open_type="download",
				detail_access_success=note is None,
				fields=parsed,
				uid=leaf.mnts_id,
				mints_cn=mints_cn,
				note=note,
			))
			await asyncio.sleep(request.param.request_delay)
			continue

		# ── 뷰어 페이지 HTML 수집 ──
		try:
			detail_html = await _fetch_html_with_encoding(viewer_url, request.param.ssl_mode, request.param.encoding)
		except Exception as exc:
			error_logs.append({
				"step": f"본문수집_{idx}",
				"title": leaf.title,
				"url": viewer_url,
				"error": f"{type(exc).__name__}: {str(exc)}",
			})
			all_items.append(MinutesItem(
				rank=idx,
				list_title=leaf.title,
				detail_url=viewer_url,
				access_method="tree-api" if tree_type == "async_api" else "page-navigation",
				open_type="direct",
				detail_access_success=False,
				fields={},
				uid=leaf.mnts_id,
				note=f"본문 수집 실패: {type(exc).__name__}",
			))
			continue

		mints_cn = ("CLIKR" + str(time.time_ns()))[:21]

		# ── 정규식 파싱 (minutes.py 공통 함수 재사용) ──
		parsed = parse_minutes_detail_by_dynamic_regex(
			detail_html=detail_html,
			request=request,
			list_title=leaf.title,
		)

		# ── 트리 메타정보 주입 ──
		if not parsed.get("RASMBLY_NUMPR"):
			gen_match = re.search(r"(\d+)", leaf.generation)
			if gen_match:
				parsed["RASMBLY_NUMPR"] = gen_match.group(1)

		# ── 첨부파일 다운로드 (기존 로직 동일) ──
		file_value = parsed.pop("ORGINL_FILE_URL", None)
		note = None

		if file_value:
			try:
				full_file_url, extracted_file_name = extract_file_info_from_reserved_value(
					raw_file_value=file_value,
					base_url=viewer_url,
				)
				year = datetime.now().strftime("%Y")

				save_path, saved_name, file_url = await download_attachment_file(
					file_url=full_file_url,
					file_name=extracted_file_name,
					file_dir=request.file_dir,
					crawl_type=request.type,
					crw_id=request.crw_id or "unknown",
					rasmbly_numpr=parsed.get("RASMBLY_NUMPR"),
					year=year,
					mints_cn=mints_cn,
					seq=1,
					ssl_mode=request.param.ssl_mode,
					detail_url=viewer_url,
				)

				parsed["ORGINL_FILE_URL"] = file_url
				parsed["MINTS_FILE_PATH"] = save_path
				parsed["ORGINL_FILE_NM"] = saved_name

			except Exception as exc:
				parsed["ORGINL_FILE_URL"] = None
				parsed["MINTS_FILE_PATH"] = None
				parsed["ORGINL_FILE_NM"] = None
				fail_msg = f"첨부파일 다운로드 실패: {type(exc).__name__}: {str(exc)}"
				note = fail_msg
				error_logs.append({
					"step": "파일 다운로드",
					"title": leaf.title,
					"file_url": file_value,
					"error": fail_msg,
				})

		# ── 중복 제거 ──
		dedupe_key = make_row_dedupe_key(parsed)
		if dedupe_key in seen_keys:
			print(f"[OLD_MINUTES] 중복 건너뜀: {leaf.title}")
			continue
		seen_keys.add(dedupe_key)

		all_items.append(MinutesItem(
			rank=idx,
			list_title=leaf.title,
			detail_url=viewer_url,
			access_method="tree-api" if tree_type == "async_api" else "page-navigation",
			open_type="direct",
			detail_access_success=True,
			fields=parsed,
			uid=leaf.mnts_id,
			mints_cn=mints_cn,
			note=note,
		))

		await asyncio.sleep(request.param.request_delay)

	return all_items


# =================================================================
# 콜백 페이로드 빌드
# =================================================================

def make_row_dedupe_key(fields: dict[str, Optional[str]]) -> str:
	"""수집된 필드 기반 중복 제거 키 생성."""
	mtgnm = normalize_text(fields.get("MTGNM"))
	numpr = normalize_text(fields.get("RASMBLY_NUMPR"))
	sesn = normalize_text(fields.get("RASMBLY_SESN"))
	mtg_de = normalize_text(fields.get("MTG_DE"))
	html_head = normalize_text(fields.get("MINTS_HTML"))[:50]

	return f"{mtgnm}|{numpr}|{sesn}|{mtg_de}|{html_head}"

def build_old_minutes_callback_payload(
	request: OldMinutesCrawlRequest,
	items: list[MinutesItem],
	error_logs: list[dict],
	error: str = "",
) -> dict:
	"""CMS 콜백용 페이로드 생성. 기존 minutes와 동일한 포맷."""
	data: list[dict] = []
	for item in items:
		if item.fields:
			row = dict(item.fields)
			row["url"] = item.detail_url
			row["mints_cn"] = item.mints_cn
			data.append(row)

	result_block = _build_result(data, error_logs, error=error)

	return {
		"req_id": request.req_id,
		"type": request.type,
		"crw_id": request.crw_id,
		"bbs_id": request.bbs_id,
		"result": result_block,
		"data": data,
		"log": error_logs,
	}


# =================================================================
# 엔트리 함수 (router에서 호출)
# =================================================================

async def run_old_minutes_and_callback(request: OldMinutesCrawlRequest) -> None:
	"""전체 수집 실행 후 CMS 콜백 전송."""
	error_logs: list[dict] = []

	try:
		items = await crawl_old_minutes(request, error_logs)
		payload = build_old_minutes_callback_payload(request, items, error_logs)
	except Exception as exc:
		traceback.print_exc()
		error_logs.append({
			"step": "run_old_minutes_and_callback",
			"error": f"{type(exc).__name__}: {str(exc)}",
		})
		payload = build_old_minutes_callback_payload(
			request, [], error_logs, error=str(exc),
		)

	try:
		await post_minutes_callback(payload)
	except Exception as cb_exc:
		print(f"[CALLBACK] 콜백 전송 실패: {cb_exc}")
		traceback.print_exc()