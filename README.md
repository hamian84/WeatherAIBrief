# WeatherAIBrief

관측 기반 기상 브리핑 생성을 위한 신규 파이프라인입니다. 이 저장소는 기존 시스템을 그대로 복제하지 않고, `입력자료(dain) -> 중간 산출물(daio) -> 최종 산출물(daou)` 구조를 기준으로 다시 구축하고 있습니다.

## 핵심 원칙

- 날짜가 항상 최상위 기준입니다.
- 입력자료는 `dain/<date>/...`에 저장합니다.
- 중간 산출물은 `daio/<date>/...`에 저장합니다.
- 최종 산출물은 `daou/<date>/...`에 저장합니다.
- 로그와 검증 결과는 `logs/<date>/...`에 저장합니다.
- 해석은 LLM 중심으로 수행하고, 검증은 규칙 중심으로 수행합니다.
- wording rule은 최소화하고 schema, evidence, validation rule을 강하게 유지합니다.
- 기본 정책은 `allow_new_claims=false`입니다.

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

## 현재 구축 범위

### 1. 수집 단계

수집 결과는 모두 `dain/<date>/...` 기준으로 저장됩니다.

- ASOS 수집: [collect_asos.py](/d:/99.project/03.WeatherAIBrief/scripts/collect_asos.py)
- 일기도 수집: [collect_charts.py](/d:/99.project/03.WeatherAIBrief/scripts/collect_charts.py)
- 위성 수집: [collect_satellite.py](/d:/99.project/03.WeatherAIBrief/scripts/collect_satellite.py)
- ASOS 검증: [verify_asos_outputs.py](/d:/99.project/03.WeatherAIBrief/scripts/verify_asos_outputs.py)
- 위성 검증: [verify_satellite_outputs.py](/d:/99.project/03.WeatherAIBrief/scripts/verify_satellite_outputs.py)

입력자료 예시:
- `dain/<date>/charts/nuri/up30_6h_*.gif`
- `dain/<date>/charts/nuri/up50_6h_*.gif`
- `dain/<date>/charts/nuri/up85_6h_*.gif`
- `dain/<date>/charts/nuri/up92_6h_*.gif`
- `dain/<date>/charts/nuri/surf_anl_6h_*.gif`
- `dain/<date>/charts/nuri/surf_12h_*.png`
- `dain/<date>/satellite/LE1B/WV063/EA/*.png`
- `dain/<date>/asos/asos_hourly.csv`
- `dain/<date>/curated/asos/asos_daily_summary.csv`

### 2. feature stage

feature stage는 질문표 기반 2단계 구조를 사용합니다.

- stage1: signal 존재 여부 판정
- stage2: stage1 결과 중 `yes` 또는 `unknown`에 대해서만 세부 속성 판정

실행기:
- [run_feature_stage.py](/d:/99.project/03.WeatherAIBrief/scripts/run_feature_stage.py)
- [feature_stage_runner.py](/d:/99.project/03.WeatherAIBrief/scripts/feature_stage_runner.py)

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

manifest 예시:
- [synoptic_300hPa_stage1.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/synoptic_300hPa_stage1.yaml)
- [synoptic_300hPa_stage2.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/synoptic_300hPa_stage2.yaml)
- [synoptic_surface_stage1.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/synoptic_surface_stage1.yaml)
- [synoptic_satellite_wv_stage1.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/synoptic_satellite_wv_stage1.yaml)

질문표 위치:
- `prompts/tables/*.csv`

schema 위치:
- [feature_stage1_response.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/feature_stage1_response.schema.json)
- [feature_stage2_response.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/feature_stage2_response.schema.json)

feature 산출물:
- `daio/<date>/features/<domain>/stage1_raw.json`
- `daio/<date>/features/<domain>/stage1_normalized.json`
- `daio/<date>/features/<domain>/stage2_raw.json`
- `daio/<date>/features/<domain>/stage2_normalized.json`

### 3. feature 통합 단계

도메인별 stage 결과를 다시 묶어 상위 feature 입력으로 정리합니다.

실행기:
- [run_feature_bundle.py](/d:/99.project/03.WeatherAIBrief/scripts/run_feature_bundle.py)
- [feature_bundle_builder.py](/d:/99.project/03.WeatherAIBrief/scripts/feature_bundle_builder.py)

산출물:
- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`
- `daio/<date>/features/feature_bundle.json`

### 4. findings 단계

기존 관측 시스템의 findings 구조를 차용한 LLM findings 단계가 있습니다.

실행기:
- [run_findings_stage.py](/d:/99.project/03.WeatherAIBrief/scripts/run_findings_stage.py)
- [findings_stage_runner.py](/d:/99.project/03.WeatherAIBrief/scripts/findings_stage_runner.py)
- [findings_llm_client.py](/d:/99.project/03.WeatherAIBrief/scripts/common/findings_llm_client.py)

산출물:
- `daio/<date>/findings/findings_prompt_input.json`
- `daio/<date>/findings/findings_llm_raw.json`
- `daio/<date>/findings/findings_bundle.json`

참고:
- 현재는 findings 경로와 카드 계층 경로가 함께 존재합니다.
- 최신 설계 방향은 `feature -> semantic cards -> upper cards -> draft -> validation`입니다.

### 5. 의미 카드 계층

feature와 최종 브리핑 사이에 의미 카드 계층을 추가했습니다.

변경 전:
- `feature -> 바로 findings 또는 briefing`

변경 후:
- `feature -> domain semantic cards -> upper reasoning cards -> briefing draft -> validation`

#### 5-1. domain semantic cards

도메인별 feature를 해석 가능한 의미 카드로 압축합니다.

실행기:
- [build_domain_semantic_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/build_domain_semantic_cards.py)

manifest:
- [domain_semantic_cards_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/domain_semantic_cards_manifest.yaml)

정책:
- [domain_card_policy.yaml](/d:/99.project/03.WeatherAIBrief/config/features/card_policies/domain_card_policy.yaml)

schema:
- [domain_semantic_card.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/domain_semantic_card.schema.json)
- [domain_semantic_cards.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/domain_semantic_cards.schema.json)

산출물:
- `daio/<date>/cards/domain_semantic_cards_prompt_input.json`
- `daio/<date>/cards/domain_semantic_cards_raw.json`
- `daio/<date>/cards/domain_semantic_cards.json`

#### 5-2. upper reasoning cards

domain semantic cards를 바탕으로 브리핑에 가까운 상위 카드 3개를 만듭니다.

대상 카드:
- `overall_summary`
- `synoptic_overview`
- `surface_overview`

실행기:
- [compose_upper_reasoning_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/compose_upper_reasoning_cards.py)

manifest:
- [upper_reasoning_cards_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/upper_reasoning_cards_manifest.yaml)

정책:
- [upper_card_policy.yaml](/d:/99.project/03.WeatherAIBrief/config/features/card_policies/upper_card_policy.yaml)

schema:
- [upper_reasoning_card.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/upper_reasoning_card.schema.json)
- [upper_reasoning_cards.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/upper_reasoning_cards.schema.json)

산출물:
- `daio/<date>/cards/upper_reasoning_cards_prompt_input.json`
- `daio/<date>/cards/upper_reasoning_cards_raw.json`
- `daio/<date>/cards/upper_reasoning_cards.json`

#### 5-3. briefing draft

upper reasoning cards만을 입력으로 브리핑 초안을 생성합니다.

실행기:
- [write_briefing_from_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/write_briefing_from_cards.py)

manifest:
- [briefing_writer_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/briefing_writer_manifest.yaml)

schema:
- [briefing_draft.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/briefing_draft.schema.json)

산출물:
- `daio/<date>/cards/briefing_writer_prompt_input.json`
- `daio/<date>/cards/briefing_writer_raw.json`
- `daou/<date>/briefing_draft.json`

#### 5-4. validation

validator는 규칙 기반으로 작동합니다. wording rule은 최소화하고, evidence 및 구조 검증을 강하게 유지합니다.

실행기:
- [validate_briefing_from_cards.py](/d:/99.project/03.WeatherAIBrief/scripts/validate_briefing_from_cards.py)

manifest:
- [briefing_validator_manifest.yaml](/d:/99.project/03.WeatherAIBrief/prompts/manifests/briefing_validator_manifest.yaml)

정책:
- [validator_policy.yaml](/d:/99.project/03.WeatherAIBrief/config/features/card_policies/validator_policy.yaml)

schema:
- [briefing_validation.schema.json](/d:/99.project/03.WeatherAIBrief/prompts/schemas/briefing_validation.schema.json)

검증 항목:
- schema invalid
- missing evidence
- unsupported claim
- contradiction
- duplicate meaning
- `allow_new_claims=false` 위반 여부

산출물:
- `daio/<date>/validation/briefing_validation.json`

## 실행 예시

### 1. 수집

```powershell
cd D:\99.project\03.WeatherAIBrief
python -m scripts.collect_asos --date 2026-04-01
python -m scripts.collect_charts --date 2026-04-01
python -m scripts.collect_satellite --date 2026-04-01
```

### 2. feature stage 배치 실행

```powershell
python -m scripts.run_feature_stage --date 2026-04-01 --manifest-dir prompts/manifests
```

특정 manifest만 실행:

```powershell
python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_300hPa_stage1.yaml
```

dry-run:

```powershell
python -m scripts.run_feature_stage --date 2026-04-01 --manifest-dir prompts/manifests --dry-run --no-stop-on-error
```

### 3. feature 통합

```powershell
python -m scripts.run_feature_bundle --date 2026-04-01
```

### 4. findings 실행

```powershell
python -m scripts.run_findings_stage --date 2026-04-01
```

### 5. 카드 계층 실행

```powershell
python scripts/build_domain_semantic_cards.py --date 2026-04-01
python scripts/compose_upper_reasoning_cards.py --date 2026-04-01
python scripts/write_briefing_from_cards.py --date 2026-04-01
python scripts/validate_briefing_from_cards.py --date 2026-04-01
```

## 현재 기준 테스트일

현재 구현 검증은 주로 `2026-04-01`을 기준 테스트일로 사용합니다.

기준 확인 사항:
- 일기도 입력 존재
- 위성 WV 입력 존재
- ASOS raw/curated 존재
- feature stage 전 도메인 실행 완료
- 카드 계층 실행 완료
- validation `pass` 확인

## 운영 원칙 요약

- 입력은 항상 `dain`
- 중간 산출물은 항상 `daio`
- 최종 초안은 항상 `daou`
- 날짜가 항상 최상위 경로
- schema-heavy, wording-light
- interpretation은 LLM 중심
- validation은 규칙 중심
- evidence_refs와 image_ref 추적 유지
- `allow_new_claims=false` 기본 유지

## 현재 상태 요약

현재 저장소는 아래 단계까지 구축되어 있습니다.

- 수집
- feature 추출
- feature 통합
- findings
- domain semantic cards
- upper reasoning cards
- briefing draft
- validation

즉, raw feature를 바로 브리핑하지 않고, 카드 계층을 거쳐 근거 추적성과 구조 검증을 유지하는 방향으로 운영 중입니다.
