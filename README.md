# WeatherAIBrief

`03.WeatherAIBrief`는 관측 자료를 수집하고, 구조화된 feature를 추출한 뒤, 근거 기반 브리핑을 생성하고 검증하는 신규 관측 기반 기상 브리핑 시스템이다.

현재 기본 파이프라인은 아래와 같다.

`수집 -> feature -> feature bundle -> direct grounded briefing -> validation`

이 저장소는 과거 `observed_v1` 체계를 그대로 재현하는 것이 아니라, `dain / daio / daou` 분리 원칙과 manifest 중심 실행 구조를 기준으로 운영한다.

## 1. 디렉터리 원칙

- 입력 자료: `dain/<date>/...`
- 중간 산출물: `daio/<date>/...`
- 최종 산출물: `daou/<date>/...`
- 로그: `logs/<date>/...`

의미:

- `dain`: 수집된 원자료
- `daio`: feature, bundle, 브리핑 입력/원응답, validation 같은 중간 산출물
- `daou`: 최종 브리핑 산출물

## 2. 최상위 구조

- `config`
  - API 키, 카드 정책, feature 정책
- `dain`
  - 날짜별 입력 자료
- `daio`
  - feature, feature bundle, 브리핑 중간 산출물, validation
- `daou`
  - 최종 브리핑 초안
- `jobs`
  - 일괄 실행 진입점
- `logs`
  - 날짜별 실행 로그
- `prompts`
  - manifests, schemas, tables, templates
- `scripts`
  - 단계별 메인 스크립트와 공통 유틸
- `yaml`
  - rule pack 등 YAML 자산

## 3. 입력 자료

수집 자료는 모두 `dain/<date>/...` 아래에 저장한다.

현재 운영 입력:

- `300hPa`
  - `up30_6h_{yyyymmddhh}.gif`
- `500hPa`
  - `up50_6h_{yyyymmddhh}.gif`
- `850hPa`
  - `up85_6h_{yyyymmddhh}.gif`
- `925hPa`
  - `up92_6h_{yyyymmddhh}.gif`
- `surface`
  - `surf_anl_6h_{yyyymmddhh}.gif`
- `sfc12h_synoptic`
  - `surf_12h_{yyyymmddhh}.png`
- `satellite_wv`
  - GK2A WV063 EA PNG
- `asos`
  - raw 및 curated 통계

기준 테스트일은 현재 `2026-04-01`이다.

## 4. 실행 단계

### 4.1 수집 단계

수집 단계 메인 스크립트:

- `scripts/run_collection_stage.py`
- `scripts/collect_asos.py`
- `scripts/collect_charts.py`
- `scripts/collect_satellite.py`

수집 결과는 `dain/<date>/...`에 저장된다.

### 4.2 feature 단계

feature 단계 메인 스크립트:

- `scripts/run_feature_stage.py`
- `scripts/run_feature_pipeline.py`
- `scripts/feature_stage_runner_bundle.py`

도메인별 stage:

- `stage1`
  - 존재 여부 또는 bundle 선택
- `stage2`
  - 위치, 강도, 범위, 방향 등 세부 속성

현재 운영 도메인:

- `300hPa`
- `500hPa`
- `850hPa`
- `925hPa`
- `surface`
- `satellite_wv`
- `sfc12h_synoptic`

### 4.3 feature bundle 단계

feature 결과를 묶는 스크립트:

- `scripts/run_feature_bundle.py`
- `scripts/feature_bundle_builder.py`

산출물:

- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`
- `daio/<date>/features/feature_bundle.json`

의미:

- `image_feature_cards.json`
  - 이미지 단위 feature 카드
- `domain_sequence_features.json`
  - 도메인 단위 시간 변화 정보
- `feature_bundle.json`
  - 브리핑 입력용 통합 feature 산출물

### 4.4 direct grounded briefing 단계

브리핑 단계 메인 스크립트:

- `scripts/run_grounded_briefing.py`
- `scripts/direct_grounded_briefing_runner.py`

현재 direct grounded briefing은 압축 요약본이 아니라 아래 원자료를 직접 활용한다.

- `daio/<date>/features/feature_bundle.json`
- `daio/<date>/features/image_feature_cards.json`
- `yaml/rules/hands37_rule_pack.yaml`

현재 입력 조립기:

- `scripts/common/direct_briefing_inputs.py`

현재 구조의 핵심:

- `feature_bundle`와 `image_feature_cards`를 직접 읽는다.
- `domain_sequence_features`도 함께 사용한다.
- `evidence_catalog`, `allowed_evidence_ids`, `allowed_evidence_refs`, `allowed_regions`를 writer 입력에 포함한다.
- writer는 근거와 규칙을 유지하면서 섹션별 브리핑 문안을 생성한다.

브리핑 산출물:

- `daio/<date>/briefing/direct_grounded_briefing_prompt_input.json`
- `daio/<date>/briefing/direct_grounded_briefing_raw.json`
- `daou/<date>/direct_grounded_briefing_draft.json`
- `daou/<date>/direct_grounded_briefing_draft.md`

### 4.5 validation 단계

검증 단계 메인 스크립트:

- `scripts/validate_grounded_briefing.py`

검증 대상:

- schema 유효성
- unsupported claim
- `evidence_ids` / `evidence_refs` / `focus_regions` / `rule_refs` 정합성

검증 리포트:

- `daio/<date>/validation/direct_grounded_briefing_validation.json`

## 5. feature bundle CSV 구조

현재 feature는 bundle CSV 기반 구조를 사용한다.

### 5.1 stage1

파일 패턴:

- `*_feature_stage1_bundle_table.csv`

의미:

- 각 row가 영역 단위 bundle 질문
- `allowed_answers`는 signal 선택 목록과 `none`을 포함

### 5.2 stage2

파일 패턴:

- `*_feature_stage2_bundle_header.csv`
- `*_feature_stage2_bundle_targets.csv`

의미:

- header는 bundle 질문 정의
- targets는 bundle 내부 세부 항목 정의
- `bundle_id`로 연결

### 5.3 manifest

대표 manifest:

- `prompts/manifests/synoptic_300hPa_stage1.yaml`
- `prompts/manifests/synoptic_300hPa_stage2.yaml`
- 동일 패턴이 다른 도메인에도 존재

대표 schema:

- `prompts/schemas/feature_stage1_response.schema.json`
- `prompts/schemas/feature_stage2_response.schema.json`

## 6. run_daily 기준 전체 단계

일괄 실행 진입점:

- `jobs/run_daily.py`

현재 지원 단계:

- `collect`
- `feature`
- `feature-bundle`
- `briefing`
- `validation`

기본 전체 실행:

```powershell
cd D:\99.project\03.WeatherAIBrief
python jobs\run_daily.py --date 2026-04-01
```

선택 실행:

```powershell
python jobs\run_daily.py --date 2026-04-01 --stage feature
python jobs\run_daily.py --date 2026-04-01 --stage feature-bundle --stage briefing --stage validation
```

## 7. 단계별 직접 실행 예시

수집:

```powershell
python scripts\run_collection_stage.py --date 2026-04-01
```

feature 전체:

```powershell
python scripts\run_feature_pipeline.py --date 2026-04-01
```

단일 feature manifest:

```powershell
python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_300hPa_stage1.yaml
```

feature bundle:

```powershell
python -m scripts.run_feature_bundle --date 2026-04-01
```

브리핑:

```powershell
python -m scripts.run_grounded_briefing --date 2026-04-01
python -m scripts.validate_grounded_briefing --date 2026-04-01
```

## 8. 현재 모델 설정

현재 코드 기준 기본 모델:

- feature 단계
  - `gemini-2.5-flash`
- direct grounded briefing writer
  - `gemini-2.5-pro`

기본 모델은 manifest와 loader fallback에서 관리한다.

관련 파일:

- `scripts/common/feature_llm_client.py`
- `scripts/common/findings_llm_client.py`
- `scripts/common/feature_manifest_loader.py`
- `scripts/common/card_manifest_loader.py`
- `prompts/manifests/direct_grounded_briefing_writer_manifest.yaml`

## 9. Gemini 전환 상태

현재 OpenAI 경로는 Gemini REST 호출로 전환되어 있다.

핵심 변경:

- feature LLM 호출: Gemini `generateContent`
- briefing writer 호출: Gemini `generateContent`
- 이미지 입력: Gemini `inline_data`
- structured output: `responseMimeType=application/json`, `responseJsonSchema` 사용

현재 확인 상태:

- dry-run
  - 정상
- `gemini-2.5-pro`
  - 현재 API 키 기준 free tier quota 문제로 실제 브리핑 생성 실패 가능
- `gemini-2.5-flash`
  - 현재 입력 크기에 따라 free tier input token quota 초과 가능
- `gemini-1.5-flash`
  - 2026-04-06 실제 테스트 결과, 현재 `v1beta` `generateContent` 기준 `404 NOT_FOUND`
  - 즉 현재 코드/키 환경에서는 사용 불가

## 10. 운영 메모

- `config/keys.env`는 Git에 포함하지 않는다.
- Gemini 실호출 테스트에는 `GEMINI_API_KEY`가 필요하다.
- 현재 브리핑 품질은 원자료 기반 경로로 개선되었지만, Gemini quota와 입력 크기 제한은 계속 확인이 필요하다.
- 브리핑 실패 시 먼저 아래를 확인한다.
  - `daio/<date>/features/feature_bundle.json`
  - `daio/<date>/features/image_feature_cards.json`
  - `yaml/rules/hands37_rule_pack.yaml`
  - `logs/<date>/run_grounded_briefing.log`

## 11. 주요 산출물 경로

feature:

- `daio/<date>/features/<domain>/stage1_raw.json`
- `daio/<date>/features/<domain>/stage1_normalized.json`
- `daio/<date>/features/<domain>/stage2_raw.json`
- `daio/<date>/features/<domain>/stage2_normalized.json`

feature bundle:

- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`
- `daio/<date>/features/feature_bundle.json`

briefing:

- `daio/<date>/briefing/direct_grounded_briefing_prompt_input.json`
- `daio/<date>/briefing/direct_grounded_briefing_raw.json`
- `daio/<date>/validation/direct_grounded_briefing_validation.json`
- `daou/<date>/direct_grounded_briefing_draft.json`
- `daou/<date>/direct_grounded_briefing_draft.md`

## 12. 현재 기준 테스트일

현재 문서와 운영 기준 테스트일은 `2026-04-01`이다.
