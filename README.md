# WeatherAIBrief

`03.WeatherAIBrief`는 관측 기반 기상 브리핑 자동 생성을 목표로 하는 독립 프로젝트입니다.

현재 기준 기본 파이프라인은 아래와 같습니다.

`수집 -> feature -> feature bundle -> direct grounded briefing -> validation`

이 저장소는 예전 `observed_v1` 계열 브리핑 체계를 그대로 따르지 않습니다. 현재 운영 경로의 중심은 `feature_bundle.json`과 `image_feature_cards.json`을 기반으로 직접 브리핑을 생성하는 `direct grounded briefing`입니다.

## 1. 핵심 원칙

- 입력 자료는 `dain/<date>/...` 아래에 둡니다.
- 중간 산출물은 `daio/<date>/...` 아래에 둡니다.
- 최종 산출물은 `daou/<date>/...` 아래에 둡니다.
- 날짜가 항상 최상위 경로입니다.
- feature 단계는 manifest 기반으로 동작합니다.
- feature 질문 구조는 bundle CSV 기반입니다.
- 브리핑 단계는 `allow_new_claims=false`를 기본값으로 사용합니다.
- 브리핑 문구 규칙은 최소화하고, 근거 연결과 validation을 강하게 유지합니다.

## 2. 주요 디렉터리

- `config`
  - 수집 및 카드/브리핑 정책 파일
- `dain`
  - 날짜별 입력 자료
- `daio`
  - feature, bundle, briefing 중간 산출물, validation 리포트
- `daou`
  - 최종 브리핑 산출물
- `jobs`
  - 일일 전체 실행기
- `logs`
  - 실행 로그
- `prompts`
  - manifest, schema, feature table, prompt template
- `scripts`
  - 단계별 메인 스크립트와 공통 유틸
- `yaml`
  - 룰 팩과 관련 YAML 자산

## 3. 현재 입력 자료

입력 자료는 날짜 기준으로 `dain/<date>/...` 아래에 저장됩니다.

### 3.1 수집 대상

- ASOS 시간자료
- 날씨누리 상층/지상 일기도
- GK2A 위성자료

### 3.2 현재 사용하는 주요 차트/영상

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

## 4. 현재 파이프라인

### 4.1 수집

수집 단계는 아래 스크립트가 담당합니다.

- `scripts/collect_asos.py`
- `scripts/collect_charts.py`
- `scripts/collect_satellite.py`
- `scripts/run_collection_stage.py`
- `scripts/verify_asos_outputs.py`
- `scripts/verify_satellite_outputs.py`

### 4.2 feature

feature 단계는 각 도메인에 대해 stage1, stage2를 수행합니다.

- stage1
  - 영역/신호 존재 여부 판정
- stage2
  - 위치, 강도, 방향, 범위 등 세부 속성 판정

현재 운영 도메인:

- `300hPa`
- `500hPa`
- `850hPa`
- `925hPa`
- `surface`
- `satellite_wv`
- `sfc12h_synoptic`

실행 스크립트:

- `scripts/run_feature_stage.py`
- `scripts/run_feature_pipeline.py`
- `scripts/feature_stage_runner_bundle.py`
- `scripts/run_feature_bundle.py`
- `scripts/feature_bundle_builder.py`

### 4.3 feature bundle

feature 개별 산출물을 묶어 아래 파일을 생성합니다.

- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`
- `daio/<date>/features/feature_bundle.json`

이 중 `feature_bundle.json`은 상위 요약과 도메인별 요약을, `image_feature_cards.json`은 이미지 단위 구조화 결과를, `domain_sequence_features.json`은 시간 흐름 축 정보를 담습니다.

### 4.4 direct grounded briefing

현재 브리핑 기본 경로는 `findings -> claims`가 아니라 `feature_bundle` 기반 `direct grounded briefing`입니다.

입력 자료:

- `daio/<date>/features/feature_bundle.json`
- `daio/<date>/features/image_feature_cards.json`
- `yaml/rules/hands37_rule_pack.yaml`

실행 스크립트:

- `scripts/run_grounded_briefing.py`
- `scripts/direct_grounded_briefing_runner.py`
- `scripts/validate_grounded_briefing.py`

현재 direct grounded briefing의 핵심 특징:

- `feature_bundle`와 `image_feature_cards`를 직접 읽습니다.
- 예전 `briefing_priority_summary`, `image_feature_signal_summary` 중심 압축 경로를 기본 경로로 사용하지 않습니다.
- section별로 나누어 순차 생성합니다.
- 각 section은 원자료를 바탕으로 해석한 뒤 문장을 작성합니다.
- `evidence_refs`, `focus_regions`, `rule_refs`를 구조적으로 남깁니다.
- `evidence_ids`는 최종 구조 필드로 유지하되, writer 내부에서 후처리 검증을 거칩니다.
- OpenAI TPM 제한을 만나면 `429` 메시지의 대기 시간을 읽어 재시도합니다.

현재 생성 section:

- `overall_summary`
- `synoptic_overview`
- `precipitation_structure`
- `surface_marine_impacts`
- `review_draft`

### 4.5 validation

validation 단계는 direct grounded briefing draft를 검사합니다.

검사 항목:

- schema 적합 여부
- unsupported claim 여부
- `evidence_ids` / `evidence_refs` 정합성
- `focus_regions` 정합성
- `rule_refs` 정합성

validation 결과는 아래에 저장됩니다.

- `daio/<date>/validation/direct_grounded_briefing_validation.json`

## 5. bundle CSV 기반 feature 구조

현재 feature 질문표는 bundle CSV 구조를 사용합니다.

### 5.1 stage1

stage1은 아래 파일을 사용합니다.

- `*_feature_stage1_bundle_table.csv`

특징:

- 한 row가 영역 bundle 단위입니다.
- `allowed_answers`는 해당 영역에서 선택 가능한 signal 목록과 `none`을 포함합니다.
- 모델은 `selected_answers` 형태로 응답합니다.

### 5.2 stage2

stage2는 아래 2개 파일을 사용합니다.

- `*_feature_stage2_bundle_header.csv`
- `*_feature_stage2_bundle_targets.csv`

특징:

- header는 bundle 단위 메타를 정의합니다.
- targets는 해당 bundle 안의 세부 판정 항목을 정의합니다.
- `bundle_id`로 header와 targets를 연결합니다.
- stage1 결과를 gating에 사용해 필요한 bundle만 stage2로 보냅니다.

### 5.3 manifest 기반 실행

feature manifest에는 아래 정보가 들어갑니다.

- `prompt_table_mode`
- `prompt_table_path`
- `stage2_bundle_header_path`
- `stage2_bundle_targets_path`
- `response_schema_path`
- `gating_source`
- `gating_match_keys`
- `gating_answer`
- `bundle_fail_fast`
- `max_bundles_per_request`

즉 현재 feature 단계는 `bundle CSV + manifest + 범용 runner` 구조입니다.

## 6. 현재 주요 manifest / schema

### 6.1 feature manifest

- `prompts/manifests/synoptic_300hPa_stage1.yaml`
- `prompts/manifests/synoptic_300hPa_stage2.yaml`
- `prompts/manifests/synoptic_500hPa_stage1.yaml`
- `prompts/manifests/synoptic_500hPa_stage2.yaml`
- `prompts/manifests/synoptic_850hPa_stage1.yaml`
- `prompts/manifests/synoptic_850hPa_stage2.yaml`
- `prompts/manifests/synoptic_925hPa_stage1.yaml`
- `prompts/manifests/synoptic_925hPa_stage2.yaml`
- `prompts/manifests/synoptic_surface_stage1.yaml`
- `prompts/manifests/synoptic_surface_stage2.yaml`
- `prompts/manifests/synoptic_satellite_wv_stage1.yaml`
- `prompts/manifests/synoptic_satellite_wv_stage2.yaml`
- `prompts/manifests/synoptic_sfc12h_stage1.yaml`
- `prompts/manifests/synoptic_sfc12h_stage2.yaml`

### 6.2 briefing manifest

- `prompts/manifests/direct_grounded_briefing_writer_manifest.yaml`
- `prompts/manifests/direct_grounded_briefing_validator_manifest.yaml`

### 6.3 schema

- `prompts/schemas/feature_stage1_response.schema.json`
- `prompts/schemas/feature_stage2_response.schema.json`
- `prompts/schemas/direct_grounded_briefing.schema.json`
- `prompts/schemas/direct_grounded_briefing_validation.schema.json`
- `prompts/schemas/rule_pack.schema.json`

## 7. 주요 산출물

### 7.1 feature 개별 산출물

- `daio/<date>/features/<domain>/stage1_raw.json`
- `daio/<date>/features/<domain>/stage1_normalized.json`
- `daio/<date>/features/<domain>/stage2_raw.json`
- `daio/<date>/features/<domain>/stage2_normalized.json`

### 7.2 feature 통합 산출물

- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`
- `daio/<date>/features/feature_bundle.json`

### 7.3 briefing 산출물

- `daio/<date>/briefing/briefing_priority_summary.json`
  - 현재는 과거 priority 요약이 아니라 source summary 호환 산출물 역할입니다.
- `daio/<date>/briefing/direct_grounded_briefing_prompt_input.json`
- `daio/<date>/briefing/direct_grounded_briefing_raw.json`
- `daio/<date>/validation/direct_grounded_briefing_validation.json`
- `daou/<date>/direct_grounded_briefing_draft.json`
- `daou/<date>/direct_grounded_briefing_draft.md`

## 8. 기본 실행 방법

### 8.1 전체 실행

```powershell
python jobs\run_daily.py --date 2026-04-01
```

현재 `run_daily.py`의 기본 stage 순서:

- `collect`
- `feature`
- `feature-bundle`
- `briefing`
- `validation`

### 8.2 특정 단계만 실행

```powershell
python jobs\run_daily.py --date 2026-04-01 --stage collect
python jobs\run_daily.py --date 2026-04-01 --stage feature
python jobs\run_daily.py --date 2026-04-01 --stage feature-bundle
python jobs\run_daily.py --date 2026-04-01 --stage briefing
python jobs\run_daily.py --date 2026-04-01 --stage validation
```

### 8.3 feature 전체 실행

```powershell
python scripts\run_feature_pipeline.py --date 2026-04-01
```

### 8.4 개별 feature manifest 실행

```powershell
python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_300hPa_stage1.yaml
```

### 8.5 feature bundle 실행

```powershell
python -m scripts.run_feature_bundle --date 2026-04-01
```

### 8.6 direct grounded briefing 실행

```powershell
python -m scripts.run_grounded_briefing --date 2026-04-01
python -m scripts.validate_grounded_briefing --date 2026-04-01
```

## 9. 현재 direct grounded briefing 구조

현재 direct grounded briefing은 아래 흐름으로 동작합니다.

`feature_bundle + image_feature_cards + hands37_rule_pack -> section별 해석 -> 최종 브리핑 문장`

현재 구조의 특징:

- 원자료를 직접 읽습니다.
- section별로 입력을 나누어 토큰 과부하를 줄입니다.
- `domain_sequence_features`와 `image_feature_cards`를 같이 사용합니다.
- 해석과 문장화를 분리하기 위해 `review_draft`는 앞서 생성된 section들을 바탕으로 작성합니다.
- validation은 최종 draft에 대해 별도로 수행합니다.

## 10. 현재 모델 사용

- feature 단계
  - 기본값: `gpt-4.1-mini`
- direct grounded briefing 단계
  - 기본값: `gpt-5.4-mini`

필요하면 manifest 또는 CLI에서 override할 수 있습니다.

## 11. 운영 시 주의 사항

- `config/keys.env`는 Git에 올리지 않습니다.
- feature 단계 실행 전 `dain/<date>/charts/nuri` 입력이 있어야 합니다.
- briefing 단계 실행 전 `daio/<date>/features/feature_bundle.json`과 `image_feature_cards.json`이 있어야 합니다.
- validation 단계 실행 전 `daou/<date>/direct_grounded_briefing_draft.json`이 있어야 합니다.
- rule pack은 `yaml/rules/hands37_rule_pack.yaml`을 사용합니다.

## 12. 기준 테스트 날짜

현재 이 저장소에서 가장 많이 검증한 기준 날짜는 `2026-04-01`입니다.
