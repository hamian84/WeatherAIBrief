# WeatherAIBrief

## 개요

이 저장소는 일기도, 위성, ASOS 기반의 일별 입력 자료를 수집하고, 도메인별 feature를 추출한 뒤, 그 결과와 `hands37_rule_pack.yaml`를 바탕으로 기상 실황 브리핑을 생성하기 위한 실험/운영 저장소다.

현재 기준의 핵심 흐름은 아래와 같다.

1. 수집
2. feature LLM 응답 생성
3. normalized 산출물 생성
4. compact 산출물 생성
5. 브리핑용 section source 생성
6. timeline event 추출
7. section 본문 생성
8. 검토용 초안 생성
9. 최종 Markdown/JSON 브리핑 조합

## 현재 디렉토리 구조

- `config/`
  - 브리핑 섹션 구성과 표시명 설정
- `daba/`
  - feature manifest, schema, prompt template, rule pack
- `dain/`
  - 수집 원천 자료
- `daio/`
  - feature 및 briefing 산출물
- `jobs/`
  - 일괄 실행기
- `scripts/`
  - 수집, feature, compact, briefing 생성 스크립트

## 현재 구현 상태

### 1. 수집 + feature

현재 유지되는 feature 파이프라인은 아래와 같다.

- 수집
  - `scripts/collect_asos.py`
  - `scripts/collect_charts.py`
  - `scripts/collect_satellite.py`
  - `scripts/run_collection_stage.py`
- feature
  - `scripts/run_feature_stage.py`
  - `scripts/run_feature_pipeline.py`
  - `scripts/feature_stage_runner_bundle.py`
  - `scripts/common/feature_llm_client.py`
- compact
  - `scripts/run_feature_compact.py`
  - `scripts/common/feature_compactor.py`

feature 산출물은 도메인별로 아래 경로에 저장된다.

- `daio/<date>/features/<domain>/stage1_raw.json`
- `daio/<date>/features/<domain>/stage1_normalized.json`
- `daio/<date>/features/<domain>/stage1_compact.json`
- `daio/<date>/features/<domain>/stage2_raw.json`
- `daio/<date>/features/<domain>/stage2_normalized.json`
- `daio/<date>/features/<domain>/stage2_compact.json`

현재 feature 단계는 bundle CSV를 사용한다.

- `daba/tables/*_feature_stage1_bundle_table.csv`
- `daba/tables/*_feature_stage2_bundle_header.csv`
- `daba/tables/*_feature_stage2_bundle_targets.csv`

manifest는 `daba/manifests/` 아래의 `synoptic_*.yaml`을 사용한다.

### 2. 브리핑 생성 파이프라인

현재 브리핑은 예전 `feature_bundle -> direct briefing` 경로가 아니라, 아래의 새 파이프라인으로 생성한다.

1. `stage1/2_compact.json`과 `hands37_rule_pack.yaml`을 읽어 section source 생성
2. section source를 LLM으로 해석해 timeline event 추출
3. timeline event를 LLM으로 해석해 section 본문 생성
4. 앞선 section들을 기반으로 `검토용 초안` 생성
5. 최종 Markdown/JSON 브리핑 조합

관련 스크립트:

- `scripts/build_briefing_section_sources.py`
- `scripts/extract_timeline_events.py`
- `scripts/write_briefing_from_events.py`
- `scripts/write_briefing_draft.py`
- `scripts/compose_weather_briefing.py`

관련 공통 모듈:

- `scripts/common/briefing_section_source_builder.py`
- `scripts/common/openai_structured_client.py`

관련 설정:

- `config/briefing/section_map.yaml`
- `config/briefing/display_labels.yaml`

관련 schema:

- `daba/schemas/briefing_section_source.schema.json`
- `daba/schemas/timeline_event.schema.json`
- `daba/schemas/timeline_event_list.schema.json`
- `daba/schemas/briefing_section_body.schema.json`
- `daba/schemas/briefing_section.schema.json`
- `daba/schemas/briefing_draft_body.schema.json`
- `daba/schemas/weather_briefing.schema.json`
- `daba/schemas/weather_briefing_validation.schema.json`

관련 template:

- `daba/templates/timeline_event_prompt.txt`
- `daba/templates/briefing_section_prompt.txt`
- `daba/templates/briefing_draft_prompt.txt`

### 3. 브리핑 문서 구조

현재 브리핑은 아래 고정 소제목 체계를 사용한다.

- `전체 개황`
- `종관 해석`
- `강수 구조 해석`
- `지상 및 해상 영향 해석`
- `검토용 초안`

각 섹션은 아래 블록을 포함한다.

- 본문
- `관측 근거`
- `적용 패턴`

`적용 패턴`은 아래 형식으로 출력하도록 설계되어 있다.

- `rule_id, 섹션 N, p.xx-yy`

예:

- `front_boundary_identification, 섹션 9, p.68-70`

### 4. rule 참조 방식

현재 브리핑 파이프라인은 `daba/rules/hands37_rule_pack.yaml`을 사용한다.

- `section_map.yaml`은 더 이상 섹션별 고정 rule 묶음을 강제하지 않는다.
- 현재는 `all_relevant` 방식으로, 해당 날짜의 활성 feature 신호와 관련된 rule 후보를 동적으로 section source에 포함한다.

참고:

- `hands_37.pdf`는 로컬 참고 자료로 둘 수 있다.
- 현재 자동 브리핑 런타임은 PDF 원문이 아니라 `hands37_rule_pack.yaml`을 기준으로 동작한다.
- 대용량 PDF는 기본적으로 git 추적 대상에 포함하지 않는다.

## 현재 산출물 구조

브리핑 중간 산출물은 아래에 저장된다.

- `daio/<date>/briefing/section_sources/<section_id>.json`
- `daio/<date>/briefing/events/<section_id>.prompt_input.json`
- `daio/<date>/briefing/events/<section_id>.raw.json`
- `daio/<date>/briefing/events/<section_id>.json`
- `daio/<date>/briefing/sections/<section_id>.prompt_input.json`
- `daio/<date>/briefing/sections/<section_id>.raw.json`
- `daio/<date>/briefing/sections/<section_id>.json`

최종 브리핑은 아래에 저장된다.

- `daio/<date>/briefing/weather_briefing.json`
- `daio/<date>/briefing/weather_briefing.md`

## 실행 방법

### 수집 + feature

```powershell
cd D:\99.project\03.WeatherAIBrief
python jobs\run_daily.py --date 2026-04-01
```

또는 단계별로 실행:

```powershell
python scripts\run_collection_stage.py --date 2026-04-01
python scripts\run_feature_pipeline.py --date 2026-04-01
python scripts\run_feature_compact.py --date 2026-04-01
```

### 브리핑 생성

현재 브리핑 파이프라인은 아직 `run_daily.py`에 통합되지 않았고, 아래 순서로 별도 실행한다.

```powershell
python scripts\build_briefing_section_sources.py --date 2026-04-01

python scripts\extract_timeline_events.py --date 2026-04-01 --section overall_summary
python scripts\extract_timeline_events.py --date 2026-04-01 --section synoptic_analysis
python scripts\extract_timeline_events.py --date 2026-04-01 --section precipitation_structure
python scripts\extract_timeline_events.py --date 2026-04-01 --section surface_marine_impacts

python scripts\write_briefing_from_events.py --date 2026-04-01 --section overall_summary
python scripts\write_briefing_from_events.py --date 2026-04-01 --section synoptic_analysis
python scripts\write_briefing_from_events.py --date 2026-04-01 --section precipitation_structure
python scripts\write_briefing_from_events.py --date 2026-04-01 --section surface_marine_impacts

python scripts\write_briefing_draft.py --date 2026-04-01
python scripts\compose_weather_briefing.py --date 2026-04-01
```

## 현재까지의 진행 결과

`2026-04-01` 기준으로 아래 단계는 실제 동작 확인을 마쳤다.

- section source 생성
- timeline event 추출
- section 본문 생성
- 검토용 초안 생성
- 최종 브리핑 Markdown/JSON 조합

실제 생성 예시는 아래 경로에 있다.

- `daio/2026-04-01/briefing/weather_briefing.md`
- `daio/2026-04-01/briefing/weather_briefing.json`

## 설계 원칙

- feature 원자료는 가능한 한 `stage1/2_compact.json`을 직접 사용한다.
- 브리핑 본문은 신호 나열이 아니라 시간 변화가 있는 사건 흐름으로 작성한다.
- 본문에는 내부 영문 키를 드러내지 않는다.
- `관측 근거`와 `적용 패턴`은 본문과 분리한다.
- LLM 응답은 strict하게 검증하고, 유효하지 않으면 결과를 보정하지 않고 재질의한다.

## 현재 한계

- 브리핑 파이프라인은 아직 `run_daily.py`에 통합되지 않았다.
- 최종 `validate_briefing.py`는 아직 구현되지 않았다.
- PowerShell 콘솔에서는 한글이 깨져 보일 수 있으나, 생성 파일은 UTF-8로 저장된다.
- 현재 rule 참조는 `hands37_rule_pack.yaml` 기준이며, `hands_37.pdf` 원문 직접 참조는 아직 운영 경로에 포함되지 않았다.
