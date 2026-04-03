# WeatherAIBrief

관측 기반 기상 브리핑 생성을 위한 신규 파이프라인 저장소입니다.  
이 저장소는 기존 시스템을 그대로 복제하지 않고, `dain -> daio -> daou` 구조를 기준으로 수집, feature 추출, 해석 카드, 브리핑 초안, 검증 단계를 분리해 운영합니다.

## 디렉터리 원칙

- 입력 자료: `dain/<date>/...`
- 중간 산출물: `daio/<date>/...`
- 최종 산출물: `daou/<date>/...`
- 로그와 검증 로그: `logs/<date>/...`
- 날짜가 항상 최상위 경로입니다.

## 현재 디렉터리 구조

```text
03.WeatherAIBrief/
├─ config/
│  └─ features/card_policies/
├─ dain/
│  └─ <date>/...
├─ daio/
│  └─ <date>/...
├─ daou/
│  └─ <date>/...
├─ jobs/
├─ logs/
│  └─ <date>/...
├─ prompts/
│  ├─ manifests/
│  ├─ schemas/
│  └─ tables/
└─ scripts/
   └─ common/
```

## 현재 구현 범위

### 1. 수집 단계

수집 결과는 모두 `dain/<date>/...` 아래에 저장됩니다.

- ASOS 수집: [collect_asos.py](/d:/99.project/03.WeatherAIBrief/scripts/collect_asos.py)
- 일기도 수집: [collect_charts.py](/d:/99.project/03.WeatherAIBrief/scripts/collect_charts.py)
- 위성 수집: [collect_satellite.py](/d:/99.project/03.WeatherAIBrief/scripts/collect_satellite.py)
- 수집 통합 실행기: [run_collection_stage.py](/d:/99.project/03.WeatherAIBrief/scripts/run_collection_stage.py)
- ASOS 검증: [verify_asos_outputs.py](/d:/99.project/03.WeatherAIBrief/scripts/verify_asos_outputs.py)
- 위성 검증: [verify_satellite_outputs.py](/d:/99.project/03.WeatherAIBrief/scripts/verify_satellite_outputs.py)

대표 입력 자료:

- `dain/<date>/charts/nuri/up30_6h_*.gif`
- `dain/<date>/charts/nuri/up50_6h_*.gif`
- `dain/<date>/charts/nuri/up85_6h_*.gif`
- `dain/<date>/charts/nuri/up92_6h_*.gif`
- `dain/<date>/charts/nuri/surf_anl_6h_*.gif`
- `dain/<date>/charts/nuri/surf_12h_*.png`
- `dain/<date>/satellite/LE1B/WV063/EA/*.png`
- `dain/<date>/asos/asos_hourly.csv`
- `dain/<date>/curated/asos/asos_daily_summary.csv`

### 2. feature 단계

feature 단계는 manifest 기반 범용 실행기로 운영합니다.

- 메인 실행기: [run_feature_stage.py](/d:/99.project/03.WeatherAIBrief/scripts/run_feature_stage.py)
- 내부 러너: [feature_stage_runner.py](/d:/99.project/03.WeatherAIBrief/scripts/feature_stage_runner.py)
- 배치 실행기: [run_feature_pipeline.py](/d:/99.project/03.WeatherAIBrief/scripts/run_feature_pipeline.py)

공통 유틸:

- [feature_manifest_loader.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_manifest_loader.py)
- [feature_prompt_table_loader.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_prompt_table_loader.py)
- [feature_image_resolver.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_image_resolver.py)
- [feature_stage_gating.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_stage_gating.py)
- [feature_normalizer.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_normalizer.py)
- [feature_artifact_writer.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_artifact_writer.py)
- [feature_llm_client.py](/d:/99.project/03.WeatherAIBrief/scripts/common/feature_llm_client.py)

현재 운영 중인 도메인:

- `300hPa`
- `500hPa`
- `850hPa`
- `925hPa`
- `surface`
- `sfc12h_synoptic`
- `satellite_wv`

중요한 현재 상태:

- feature 단계는 현재 **기본 분리형 stage1/stage2 구조**입니다.
- `300hPa`, `500hPa`를 포함한 현재 운영 체계는 **CSV 질문표 기반 일반 stage1/stage2 흐름**으로 동작합니다.
- 실험했던 stage1 bundle, stage2 bundle, stage1+stage2 통합 호출 경로는 현재 운영 기준에서 제거되었습니다.
- 즉 현재는 `stage1 존재 여부 판정 -> stage2 세부 속성 판정`의 원래 흐름을 사용합니다.

feature 산출물:

- `daio/<date>/features/<domain>/stage1_raw.json`
- `daio/<date>/features/<domain>/stage1_normalized.json`
- `daio/<date>/features/<domain>/stage2_raw.json`
- `daio/<date>/features/<domain>/stage2_normalized.json`

### 3. feature 통합 단계

도메인별 stage 결과를 상위 입력 구조로 정리합니다.

- 실행기: [run_feature_bundle.py](/d:/99.project/03.WeatherAIBrief/scripts/run_feature_bundle.py)
- 빌더: [feature_bundle_builder.py](/d:/99.project/03.WeatherAIBrief/scripts/feature_bundle_builder.py)

산출물:

- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`
- `daio/<date>/features/feature_bundle.json`

### 4. findings 단계

feature bundle을 바탕으로 LLM findings를 생성합니다.

- 실행기: [run_findings_stage.py](/d:/99.project/03.WeatherAIBrief/scripts/run_findings_stage.py)
- 러너: [findings_stage_runner.py](/d:/99.project/03.WeatherAIBrief/scripts/findings_stage_runner.py)
- 클라이언트: [findings_llm_client.py](/d:/99.project/03.WeatherAIBrief/scripts/common/findings_llm_client.py)

산출물:

- `daio/<date>/findings/findings_prompt_input.json`
- `daio/<date>/findings/findings_llm_raw.json`
- `daio/<date>/findings/findings_bundle.json`

### 5. 의미 카드 계층

feature와 최종 브리핑 사이에 의미 카드 계층을 둡니다.

변경 전 개념:

- `feature -> 바로 findings 또는 briefing`

현재 개념:

- `feature -> domain semantic cards -> upper reasoning cards -> briefing draft -> validation`

#### 5-1. domain semantic cards

- 실행기: [build_domain_semantic_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/build_domain_semantic_cards.py)
- manifest: [domain_semantic_cards_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/domain_semantic_cards_manifest.yaml)
- 정책: [domain_card_policy.yaml](/d:/99.project/03.WeatherAIBrief/config/features/card_policies/domain_card_policy.yaml)
- schema:
  - [domain_semantic_card.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/domain_semantic_card.schema.json)
  - [domain_semantic_cards.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/domain_semantic_cards.schema.json)

산출물:

- `daio/<date>/cards/domain_semantic_cards_prompt_input.json`
- `daio/<date>/cards/domain_semantic_cards_raw.json`
- `daio/<date>/cards/domain_semantic_cards.json`

#### 5-2. upper reasoning cards

- 실행기: [compose_upper_reasoning_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/compose_upper_reasoning_cards.py)
- manifest: [upper_reasoning_cards_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/upper_reasoning_cards_manifest.yaml)
- 정책: [upper_card_policy.yaml](/d:/99.project/03.WeatherAIBrief/config/features/card_policies/upper_card_policy.yaml)
- schema:
  - [upper_reasoning_card.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/upper_reasoning_card.schema.json)
  - [upper_reasoning_cards.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/upper_reasoning_cards.schema.json)

대상 카드:

- `overall_summary`
- `synoptic_overview`
- `surface_overview`

산출물:

- `daio/<date>/cards/upper_reasoning_cards_prompt_input.json`
- `daio/<date>/cards/upper_reasoning_cards_raw.json`
- `daio/<date>/cards/upper_reasoning_cards.json`

#### 5-3. briefing draft

- 실행기: [write_briefing_from_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/write_briefing_from_cards.py)
- manifest: [briefing_writer_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/briefing_writer_manifest.yaml)
- schema: [briefing_draft.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/briefing_draft.schema.json)

산출물:

- `daio/<date>/cards/briefing_writer_prompt_input.json`
- `daio/<date>/cards/briefing_writer_raw.json`
- `daou/<date>/briefing_draft.json`

#### 5-4. validation

- 실행기: [validate_briefing_from_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/validate_briefing_from_cards.py)
- manifest: [briefing_validator_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/briefing_validator_manifest.yaml)
- 정책: [validator_policy.yaml](/d:/99.project/03.WeatherAIBrief/config/features/card_policies/validator_policy.yaml)
- schema: [briefing_validation.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/briefing_validation.schema.json)

검증 항목:

- `schema_invalid`
- `missing_source_card_ids`
- `missing_evidence_refs`
- `unsupported_claim`
- `contradiction`
- `duplicate_meaning`
- `allow_new_claims_violation`

산출물:

- `daio/<date>/validation/briefing_validation.json`

## fail-fast 원칙

현재 카드 계층은 fail-fast 구조입니다.

- `fail_fast=true`
- `auto_recovery=false`
- `allow_partial_output=false`

즉 다음과 같은 경우 자동 복원 없이 즉시 실패합니다.

- schema invalid
- source card 연결 누락
- evidence refs 누락
- unsupported claim
- contradiction
- allow_new_claims 위반

정상 입력에서는 그대로 통과하고, 입력 불일치가 있으면 원인 메시지를 남기고 non-zero 종료합니다.

## 실행 방법

### 전체 일일 실행

현재는 [run_daily.py](/d:/99.project/03.WeatherAIBrief/jobs/run_daily.py) 가 단계 선택 실행을 지원합니다.

```powershell
cd D:\99.project\03.WeatherAIBrief
python jobs\run_daily.py --date 2026-04-01
```

### 특정 단계만 실행

```powershell
python jobs\run_daily.py --date 2026-04-01 --stage collect
python jobs\run_daily.py --date 2026-04-01 --stage feature
python jobs\run_daily.py --date 2026-04-01 --stage feature-bundle --stage findings
python jobs\run_daily.py --date 2026-04-01 --stage domain-cards --stage upper-cards --stage briefing --stage validation
```

### 단계 메인 스크립트 단독 실행

```powershell
python scripts\run_collection_stage.py --date 2026-04-01
python scripts\run_feature_pipeline.py --date 2026-04-01
python -m scripts.run_feature_bundle --date 2026-04-01
python -m scripts.run_findings_stage --date 2026-04-01
python scripts\build_domain_semantic_cards.py --date 2026-04-01
python scripts\compose_upper_reasoning_cards.py --date 2026-04-01
python scripts\write_briefing_from_cards.py --date 2026-04-01
python scripts\validate_briefing_from_cards.py --date 2026-04-01
```

## 현재 기준 테스트일

현재 기본 검증 날짜는 `2026-04-01`입니다.

이번 점검 기준으로 확인한 내용:

- feature 핵심 스크립트 `py_compile` 통과
- `300hPa stage1` dry-run 정상
- `300hPa stage2` dry-run 정상
- `500hPa stage1` dry-run 정상
- `500hPa stage2` dry-run 정상

즉 현재 feature 기본 체계는 다시 원래 분리형 stage 구조로 수행 가능합니다.

## 운영 원칙 요약

- 입력은 `dain`
- 중간 산출물은 `daio`
- 최종 초안은 `daou`
- 날짜가 항상 최상위 경로
- schema-heavy, wording-light
- interpretation은 LLM 중심
- validation은 규칙 중심
- evidence_refs와 image_ref 추적 유지
- `allow_new_claims=false` 기본 유지
