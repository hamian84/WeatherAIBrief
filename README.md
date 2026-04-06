# WeatherAIBrief

현재 운영 경로는 `수집 -> feature -> feature bundle -> direct grounded briefing -> validation`입니다.

## 1. 결론

- 브리핑 생성은 direct grounded briefing 경로만 사용합니다.
- 입력 근거는 `feature_bundle.json`, `image_feature_cards.json`, `hands37_rule_pack.yaml`, `briefing_priority_summary.json`입니다.
- 기존 카드 기반 브리핑 경로와 `reasoned_findings`, `section_plans` 경로는 현재 운영 기준에서 사용하지 않습니다.
- 모델은 단계별로 분리합니다.
  - feature 추출: `gpt-4.1-mini` 계열
  - direct grounded briefing: `gpt-5.4-mini`

## 2. 현재 디렉터리 구조

- 입력 자료: `dain/<date>/...`
- 중간 산출물: `daio/<date>/...`
- 최종 브리핑: `daou/<date>/...`
- 로그: `logs/<date>/...`

## 3. 현재 운영 스크립트

### 수집

- [collect_asos.py](D:\11.Project\07.WeatherAIBrief\scripts\collect_asos.py)
- [collect_charts.py](D:\11.Project\07.WeatherAIBrief\scripts\collect_charts.py)
- [collect_satellite.py](D:\11.Project\07.WeatherAIBrief\scripts\collect_satellite.py)
- [run_collection_stage.py](D:\11.Project\07.WeatherAIBrief\scripts\run_collection_stage.py)
- [verify_asos_outputs.py](D:\11.Project\07.WeatherAIBrief\scripts\verify_asos_outputs.py)
- [verify_satellite_outputs.py](D:\11.Project\07.WeatherAIBrief\scripts\verify_satellite_outputs.py)

### feature

- [run_feature_stage.py](D:\11.Project\07.WeatherAIBrief\scripts\run_feature_stage.py)
- [run_feature_pipeline.py](D:\11.Project\07.WeatherAIBrief\scripts\run_feature_pipeline.py)
- [run_feature_bundle.py](D:\11.Project\07.WeatherAIBrief\scripts\run_feature_bundle.py)
- [feature_stage_runner_bundle.py](D:\11.Project\07.WeatherAIBrief\scripts\feature_stage_runner_bundle.py)
- [feature_bundle_builder.py](D:\11.Project\07.WeatherAIBrief\scripts\feature_bundle_builder.py)

### direct briefing

- [run_grounded_briefing.py](D:\11.Project\07.WeatherAIBrief\scripts\run_grounded_briefing.py)
- [validate_grounded_briefing.py](D:\11.Project\07.WeatherAIBrief\scripts\validate_grounded_briefing.py)
- [direct_grounded_briefing_runner.py](D:\11.Project\07.WeatherAIBrief\scripts\direct_grounded_briefing_runner.py)
- [direct_grounded_briefing_prompt.txt](D:\11.Project\07.WeatherAIBrief\prompts\templates\direct_grounded_briefing_prompt.txt)
- [direct_grounded_briefing_writer_manifest.yaml](D:\11.Project\07.WeatherAIBrief\prompts\manifests\direct_grounded_briefing_writer_manifest.yaml)
- [direct_grounded_briefing_validator_manifest.yaml](D:\11.Project\07.WeatherAIBrief\prompts\manifests\direct_grounded_briefing_validator_manifest.yaml)
- [direct_grounded_briefing.schema.json](D:\11.Project\07.WeatherAIBrief\prompts\schemas\direct_grounded_briefing.schema.json)
- [direct_grounded_briefing_validation.schema.json](D:\11.Project\07.WeatherAIBrief\prompts\schemas\direct_grounded_briefing_validation.schema.json)
- [hands37_rule_pack.yaml](D:\11.Project\07.WeatherAIBrief\yaml\rules\hands37_rule_pack.yaml)

## 4. 현재 핵심 산출물

### feature

- `daio/<date>/features/feature_bundle.json`
- `daio/<date>/features/image_feature_cards.json`
- `daio/<date>/features/domain_sequence_features.json`

### briefing

- `daio/<date>/briefing/briefing_priority_summary.json`
- `daio/<date>/briefing/direct_grounded_briefing_prompt_input.json`
- `daio/<date>/briefing/direct_grounded_briefing_raw.json`
- `daio/<date>/validation/direct_grounded_briefing_validation.json`
- `daou/<date>/direct_grounded_briefing_draft.json`
- `daou/<date>/direct_grounded_briefing_draft.md`

## 5. 실행 방법

### 일일 실행

```powershell
python jobs\run_daily.py --date 2026-04-01
```

### 단계별 실행

```powershell
python jobs\run_daily.py --date 2026-04-01 --stage collect
python jobs\run_daily.py --date 2026-04-01 --stage feature
python jobs\run_daily.py --date 2026-04-01 --stage feature-bundle
python jobs\run_daily.py --date 2026-04-01 --stage briefing --stage validation
```

### direct 브리핑만 실행

```powershell
python -m scripts.run_grounded_briefing --date 2026-04-01
python -m scripts.validate_grounded_briefing --date 2026-04-01
```

## 6. 현재 운영 기준

- 현재 자동화 운영 후보 모델은 `gpt-5.4-mini`입니다.
- Codex 수동 브리핑은 품질 기준선으로만 유지하며, 자동 실행 경로에는 포함되지 않습니다.
- validator 기준 `issue_count = 0`인 direct briefing만 운영 산출물로 봅니다.
- 현재 운영 브리핑 단계에는 별도의 `findings`, `reasoned_findings`, `section_plans`, 카드 계층이 없습니다.
