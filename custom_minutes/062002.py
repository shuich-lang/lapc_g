"""
광산구의회 회의록 크롤러 (crw_id: 062002)
- 사이트: https://assembly.gjgc.or.kr
- 구조: 메인 페이지에서 대수별 회기 목록 파싱 → 회기별 AJAX JSON API로 회의 목록 조회 → viewer.do 상세 접근
- 일반적인 목록/페이징 구조가 아니라 JS 기반 동적 로딩이므로 커스텀 처리 필요
"""
from __future__ import annotations

import re
import time
from typing import Optional
from urllib.parse import urljoin

import httpx

from minutes import (
    RegexCrawlRequest,
    CrawlResponse,
    MinutesItem,
    USER_AGENT,
    normalize_text,
    get_verify_options,
    fetch_html,
    parse_minutes_detail_by_dynamic_regex,
    extract_file_info_from_reserved_value,
    download_attachment_file,
    matches_last_data,
    audit_fields_minutes,
    save_field_logs,
)
from datetime import datetime

BASE_URL = "https://assembly.gjgc.or.kr"
SESSION_LIST_API = f"{BASE_URL}/assem/search/simple/LoadingList.json"
VIEWER_URL_TPL = f"{BASE_URL}/assem/viewer.do?cdUid={{cd_uid}}"
MAIN_PAGE_URL = f"{BASE_URL}/assem/search/simple/session.do"


# ─── 유틸 ────────────────────────────────────────────────────────

async def _fetch_json(url: str, ssl_mode: str) -> list[dict]:
    """AJAX JSON 엔드포인트 호출"""
    timeout = httpx.Timeout(20.0, connect=10.0)
    headers = {"User-Agent": USER_AGENT}
    verify_option = get_verify_options(ssl_mode)

    async with httpx.AsyncClient(
        headers=headers, timeout=timeout,
        follow_redirects=True, verify=verify_option,
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


def _parse_daesoo_sessions(html: str) -> list[dict]:
    """
    메인 페이지 JS의 jArray에서 대수별 회기 범위를 파싱.
    반환: [{"daesoo": 9, "s_num": 272, "e_num": 303}, ...]  (최신순)
    """
    results = []
    pattern = re.compile(
        r'jobj\.csNum\s*=\s*"(\d+)".*?'
        r'jobj\.csSnum\s*=\s*"(\d+)".*?'
        r'jobj\.csEnum\s*=\s*"(\d+)"',
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        results.append({
            "daesoo": int(m.group(1)),
            "s_num": int(m.group(2)),
            "e_num": int(m.group(3)),
        })
    return results


def _build_session_numbers(daesoo_list: list[dict]) -> list[tuple[int, int]]:
    """
    대수별 회기 범위 → (대수, 회기번호) 리스트 (최신순).
    """
    pairs = []
    for d in daesoo_list:
        for s in range(d["e_num"], d["s_num"] - 1, -1):
            pairs.append((d["daesoo"], s))
    return pairs


# ─── 메인 크롤 함수 ──────────────────────────────────────────────

async def crawl_minutes(
    request: RegexCrawlRequest,
    crawl_all: bool = False,
    error_logs: list[dict] | None = None,
) -> CrawlResponse:
    if error_logs is None:
        error_logs = []

    ssl_mode = request.param.ssl_mode
    is_additional = bool(request.last_data)

    # 1) 메인 페이지에서 대수/회기 목록 파싱
    try:
        main_html = await fetch_html(MAIN_PAGE_URL, ssl_mode)
    except Exception as exc:
        error_logs.append({"step": "메인 페이지 요청", "error": f"{type(exc).__name__}: {exc}"})
        return CrawlResponse(list_url=MAIN_PAGE_URL, item_count=0, items=[])

    daesoo_list = _parse_daesoo_sessions(main_html)
    if not daesoo_list:
        error_logs.append({"step": "대수/회기 파싱", "error": "jArray 파싱 실패"})
        return CrawlResponse(list_url=MAIN_PAGE_URL, item_count=0, items=[])

    session_pairs = _build_session_numbers(daesoo_list)

    # 추가수집이면 최신 대수만 (last_data 매칭 시 중단)
    # 테스트(crawl_all=False)면 최신 대수만
    if is_additional or not crawl_all:
        latest = daesoo_list[0]
        session_pairs = [
            (latest["daesoo"], s)
            for s in range(latest["e_num"], latest["s_num"] - 1, -1)
        ]

    item_cols = [
        normalize_text(ri.col)
        for ri in request.item
        if normalize_text(ri.col)
        and (
            (ri.regex and any(normalize_text(r) for r in ri.regex))
            or (ri.value is not None and normalize_text(ri.value))
        )
    ]

    all_items: list[MinutesItem] = []
    field_logs: list[dict] = []
    seen_uids: set[str] = set()
    stop = False

    # 2) 회기별 순회 (최신순)
    for daesoo, session_no in session_pairs:
        if stop:
            break

        api_url = f"{SESSION_LIST_API}?searchCsSession={session_no}&searchCtGroup="
        print(f"[062002] 제{daesoo}대 {session_no}회 조회: {api_url}")

        try:
            meetings = await _fetch_json(api_url, ssl_mode)
        except Exception as exc:
            error_logs.append({
                "step": f"회기 {session_no} JSON 조회",
                "error": f"{type(exc).__name__}: {exc}",
            })
            continue

        if not meetings:
            continue

        # 3) 각 회의 순회
        for meeting in meetings:
            if stop:
                break

            cd_uid = meeting.get("cdUid")
            if not cd_uid:
                continue

            uid_str = str(cd_uid)
            if uid_str in seen_uids:
                continue
            seen_uids.add(uid_str)

            detail_url = VIEWER_URL_TPL.format(cd_uid=cd_uid)
            current_rank = len(all_items) + 1

            # 회의 제목 구성
            ct_nm = meeting.get("ctNm", "")
            cs_session = meeting.get("csSession", "")
            cd_chasoo = meeting.get("cdChasoo", "")
            cd_date = meeting.get("cdDate", "")
            cd_ritual_nm = meeting.get("cdRitualNm")
            title_parts = [f"제{cs_session}회"]
            if cd_ritual_nm:
                title_parts.append(cd_ritual_nm)
            else:
                title_parts.append(ct_nm)
                if cd_chasoo:
                    title_parts.append(f"제{cd_chasoo}차")
            if cd_date:
                title_parts.append(f"({cd_date})")
            list_title = " ".join(title_parts)

            print(f"[062002] {current_rank}번째 | {list_title} | cdUid={cd_uid}")

            mints_cn = ("CLIKR" + str(time.time_ns()))[:21]

            # 상세 페이지 접근
            try:
                detail_html = await fetch_html(detail_url, ssl_mode)
            except Exception as exc:
                error_logs.append({
                    "step": f"상세접근_{current_rank}",
                    "title": list_title,
                    "error": f"{type(exc).__name__}: {exc}",
                })
                all_items.append(MinutesItem(
                    rank=current_rank, list_title=list_title,
                    detail_url=detail_url, access_method="http-href",
                    open_type="direct", detail_access_success=False,
                    fields={}, uid=uid_str, mints_cn=mints_cn,
                    note=f"상세 접근 실패: {exc}",
                ))
                continue

            # 정규식 적용
            parsed = parse_minutes_detail_by_dynamic_regex(
                detail_html=detail_html,
                request=request,
                list_title=list_title,
            )

            rasmbly_numpr = str(daesoo)

            # 첨부파일 처리
            file_value = parsed.pop("ORGINL_FILE_URL", None)
            if file_value:
                try:
                    full_file_url, extracted_file_name = extract_file_info_from_reserved_value(
                        raw_file_value=file_value,
                        base_url=detail_url,
                    )
                    year = datetime.now().strftime("%Y")
                    save_path, saved_name, file_url = await download_attachment_file(
                        file_url=full_file_url, file_name=extracted_file_name,
                        file_dir=request.file_dir, crawl_type=request.type,
                        crw_id=request.crw_id or "unknown",
                        rasmbly_numpr=rasmbly_numpr, year=year,
                        mints_cn=mints_cn, seq=1, ssl_mode=ssl_mode,
                        detail_url=detail_url,
                    )
                    parsed["ORGINL_FILE_URL"] = file_url
                    parsed["MINTS_FILE_PATH"] = save_path
                    parsed["ORGINL_FILE_NM"] = saved_name
                except Exception as exc:
                    parsed["ORGINL_FILE_URL"] = None
                    parsed["MINTS_FILE_PATH"] = None
                    parsed["ORGINL_FILE_NM"] = None
                    error_logs.append({
                        "step": "파일 다운로드", "title": list_title,
                        "error": f"첨부파일 다운로드 실패: {type(exc).__name__}: {exc}",
                    })

            parsed["RASMBLY_NUMPR"] = rasmbly_numpr

            item = MinutesItem(
                rank=current_rank, list_title=list_title,
                detail_url=detail_url, access_method="http-href",
                open_type="direct", detail_access_success=True,
                fields=parsed, uid=uid_str, mints_cn=mints_cn,
            )

            # 추가수집: last_data 매칭 시 중단
            if is_additional and matches_last_data(
                item_fields=item.fields,
                item_detail_url=item.detail_url,
                item_mints_cn=item.mints_cn,
                last_data=request.last_data,
            ):
                print(f"[062002] last_data 일치 — 추가수집 종료 (rank: {current_rank})")
                stop = True
                break

            # 필드 감사 로그
            if item.fields:
                field_logs.append(audit_fields_minutes(
                    mints_cn=mints_cn, url=detail_url,
                    item_cols=item_cols, fields=item.fields,
                ))

            all_items.append(item)

    if field_logs:
        save_field_logs(field_logs, request)

    return CrawlResponse(
        list_url=MAIN_PAGE_URL,
        item_count=len(all_items),
        items=all_items,
    )