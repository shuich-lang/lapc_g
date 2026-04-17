# 일반 필드 매핑 (한글 라벨 → 표준 키)
FIELD_MAP = {
    "공포 번호": "PRMLGT_NO", "공포번호": "PRMLGT_NO",
    "공포일": "PRMLGT_DE", "공포 일자": "PRMLGT_DE", "공포일자": "PRMLGT_DE",
    "대수": "RASMBLY_NUMPR", "제안대수": "RASMBLY_NUMPR", "제 안대수": "RASMBLY_NUMPR",     
    "회기": "RASMBLY_SESN", "처리 회기": "RASMBLY_SESN", "처리회기": "RASMBLY_SESN",
    "제안 회기": "RASMBLY_SESN", "제안회기": "RASMBLY_SESN",
    "제안대수/회기": "RASMBLY_NUMPR_SESN", "제안 대수 / 대수": "RASMBLY_NUMPR_SESN", "대수/회기": "RASMBLY_NUMPR_SESN", 
    "대수 / 회기": "RASMBLY_NUMPR_SESN", "대수/회기(제안)": "RASMBLY_NUMPR_SESN", "제안(제출)회기": "RASMBLY_NUMPR_SESN",
    "소관위": "JRSD_CMIT_NM", "소관 위원회": "JRSD_CMIT_NM", "소관위원회": "JRSD_CMIT_NM",
    "소관특별위원회": "JRSD_CMIT_NM", "관련위원회": "JRSD_CMIT_NM", "소관부서": "JRSD_CMIT_NM",
    "대표 발의자": "PROPSR", "대표발의": "PROPSR", "대표 발의 의원": "PROPSR",
    "발의자": "PROPSR", "발의 의원": "PROPSR", "발의의원": "PROPSR", "제출(발의)자": "PROPSR",
    "제안(발의)자": "PROPSR", "공동발의자": "PROPSR", "제안자": "PROPSR", "제안(제출)자": "PROPSR",
    "공동 발의 의원": "PROPSR", "공동발의의원": "PROPSR", "발의(제출)자": "PROPSR", "발의(제안)자": "PROPSR",
    "발의일": "ITNC_DE", "발의 일자": "ITNC_DE", "제안일": "ITNC_DE",
    "제안 일자": "ITNC_DE", "제안일자": "ITNC_DE", "제안(제출)일": "ITNC_DE",
    "발의(제출)일자": "ITNC_DE", "발의(제출)일": "ITNC_DE", "제출(발의)일": "ITNC_DE",
    "의안 번호": "BI_NO", "의안번호": "BI_NO", "의안 관리번호": "BI_NO", "접수번호": "BI_NO",
    "의안명": "BI_SJ", "의안 명": "BI_SJ", "의안제목": "BI_SJ",
    "의안 제목": "BI_SJ", "제목": "BI_SJ",
    "의안 종류": "BI_KND_NM", "의안종류": "BI_KND_NM", "의안 구분": "BI_KND_NM",
    "의안구분": "BI_KND_NM", "의안 유형": "BI_KND_NM", "의안유형": "BI_KND_NM", "처리내용": "BI_KND_NM",
    "주요 내용": "BI_OUTLINE", "주요내용": "BI_OUTLINE", "의안 요지": "BI_OUTLINE", "제안 이유 및 주요내용": "BI_OUTLINE",
    "의안요지": "BI_OUTLINE", "처리 요지": "BI_OUTLINE", "처리요지": "BI_OUTLINE", "재의요지": "BI_OUTLINE",
    "결과 요지": "BI_OUTLINE", "결과요지": "BI_OUTLINE", "철회요지": "BI_OUTLINE", "본문 내용": "BI_OUTLINE",
    "이송일": "TRNSF_DE", "집행부 이송일": "TRNSF_DE", "재이송일": "TRNSF_DE",
    "자치단체 이송일": "TRNSF_DE", "자치단체이송일": "TRNSF_DE", "집행기관 이송일": "TRNSF_DE", "집행이송일": "TRNSF_DE",
    "철회일": "RETRAC_DE", "철회일자": "RETRAC_DE", "철회/폐기일": "RETRAC_DE",
    "비고": "REMARK", "메모": "REMARK", "철회/폐기요지": "REMARK", "기타": "REMARK",
    "첨부파일": "BI_FILE_NM", "첨부": "BI_FILE_NM", "의안파일": "BI_FILE_NM",
    "의안원문": "BI_FILE_NM", "원안": "BI_FILE_NM", "접수의안": "BI_FILE_NM", "원안파일": "BI_FILE_NM",
    "심의안건": "BI_FILE_NM", "발의(제출)안": "BI_FILE_NM", "본문내용_첨부파일": "BI_FILE_NM",
    "제안안(원안)": "BI_FILE_NM", "의안": "BI_FILE_NM", "관련자료": "BI_FILE_NM", "회의록보기": "BI_FILE_NM", "보고서": "BI_FILE_NM",
    "첨부파일링크": "BI_FILE_URL",
    "소관위 주요 내용": "CMIT_UPDT_OUTLINE",
    "접수일": "PLNMT_FRWRD_DE", "접수일자": "PLNMT_FRWRD_DE", "심사보고접수일": "PLNMT_FRWRD_DE", "심사보고회접수일": "PLNMT_FRWRD_DE",
    "회부일": "PLNMT_FRWRD_DE", "회부일자": "PLNMT_FRWRD_DE", "심사보고서접수일": "PLNMT_FRWRD_DE", "재의일": "PLNMT_FRWRD_DE",
    "보고일": "PLNMT_REPORT_DE", "보고일자": "PLNMT_REPORT_DE",
    "본회의 보고일": "PLNMT_REPORT_DE", "심사보고일": "PLNMT_REPORT_DE",
    "상정일": "PLNMT_SBMISN_DE", "상정일자": "PLNMT_SBMISN_DE", "본회의 상정일": "PLNMT_SBMISN_DE",
    "의결일": "PLNMT_PROCESS_DE", "의결일자": "PLNMT_PROCESS_DE",
    "본회의 의결일": "PLNMT_PROCESS_DE", "처리일": "PLNMT_PROCESS_DE", "처리일자": "PLNMT_PROCESS_DE",
    "처리 결과": "PLNMT_RESULT", "처리결과": "PLNMT_RESULT", "처리상태": "PLNMT_RESULT", "처리결과 내용": "PLNMT_RESULT",
    "심사 결과": "PLNMT_RESULT", "심사결과": "PLNMT_RESULT", "결과": "PLNMT_RESULT",
    "위원회": "JRSD_CMIT_NM",
}

# 위원회 섹션 공통 필드
_CMIT_FIELDS = {
    "소관위": "JRSD_CMIT_NM", "소관 위원회": "JRSD_CMIT_NM",
    "소관위원회": "JRSD_CMIT_NM", "소관위원회명": "JRSD_CMIT_NM",
    "회부일": "FRWRD_DE", "회부일자": "FRWRD_DE",
    "보고일": "CMIT_REPORT_DE", "보고일자": "CMIT_REPORT_DE", "심사보고일": "CMIT_REPORT_DE",
    "상정일": "CMIT_SBMISN_DE", "상정일자": "CMIT_SBMISN_DE",
    "의결일": "CMIT_PROCESS_DE", "의결일자": "CMIT_PROCESS_DE", "처리일": "CMIT_PROCESS_DE",
    "처리 결과": "CMIT_RESULT", "처리결과": "CMIT_RESULT", "본회의 처리사항": "CMIT_RESULT",
    "심사 결과": "CMIT_RESULT", "심사결과": "CMIT_RESULT", "결과": "CMIT_RESULT",
    "비고": "CMIT_UPDT_OUTLINE", "소관위 주요 내용": "CMIT_UPDT_OUTLINE",
    "관련 회의록": "CMIT_RELATED_MEETING",
    "소관위원회 관련 회의록": "CMIT_RELATED_MEETING",
}

# 본회의 섹션 공통 필드
_PLNMT_FIELDS = {
    "접수일": "PLNMT_FRWRD_DE", "접수일자": "PLNMT_FRWRD_DE",
    "회부일": "PLNMT_FRWRD_DE", "회부일자": "PLNMT_FRWRD_DE",
    "보고일": "PLNMT_REPORT_DE", "보고일자": "PLNMT_REPORT_DE",
    "본회의 보고일": "PLNMT_REPORT_DE", "심사보고일": "PLNMT_REPORT_DE",
    "상정일": "PLNMT_SBMISN_DE", "상정일자": "PLNMT_SBMISN_DE", "본회의 상정일": "PLNMT_SBMISN_DE",
    "의결일": "PLNMT_PROCESS_DE", "의결일자": "PLNMT_PROCESS_DE",
    "본회의 의결일": "PLNMT_PROCESS_DE", "처리일": "PLNMT_PROCESS_DE",
    "처리 결과": "PLNMT_RESULT", "처리결과": "PLNMT_RESULT",
    "심사 결과": "PLNMT_RESULT", "심사결과": "PLNMT_RESULT", "결과": "PLNMT_RESULT",
    "비고": "PLNMT_REMARK",
    "관련 회의록": "PLNMT_RELATED_MEETING",
    "본회의 관련 회의록": "PLNMT_RELATED_MEETING",
}

# 섹션 이름 변형 -> 공통 템플릿 매핑 (사이트별 섹션명 차이 흡수)
SECTION_FIELD_MAP = {k: _CMIT_FIELDS for k in [
    "위원회", "위원회 처리사항", "위원회처리사항", "위원회 처리", "위원회처리",
    "소관위원회 심사경과", "소관위원회 처리결과", "위원회<br>처리사항", "위 원 회<br>처리사항", "위 원 회 처리사항",
]}
SECTION_FIELD_MAP.update({k: _PLNMT_FIELDS for k in [
    "본회의", "본회의 처리사항", "본회의처리사항", "본회의 처리", "본회의 심사경과",
    "본회의<br>(최종상황)", "본회의 처리결과", "본회의<br>처리사항", "본 회 의<br>처리사항", "본 회 의 처리사항",
]})