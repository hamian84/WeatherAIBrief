# WeatherAIBrief

## 개요

WeatherAIBrief는 일별 `00 UTC`, `12 UTC` 자료를 수집하고, 이미지 기반 feature를 추출한 뒤, 근거가 남는 기상 브리핑을 생성하는 파이프라인이다.

현재 기본 실행 흐름은 아래와 같다.

1. 입력자료 수집
2. feature stage1/stage2 추출
3. section source 생성
4. timeline event 생성
5. section 본문 생성
6. draft 생성
7. 최종 Markdown/JSON 브리핑 생성

## 현재 표준 실행 방식

상위 실행 기준 날짜 형식은 `YYYYMMDD`이다.

```powershell
cd D:\99.project\03.WeatherAIBrief
python jobs\run_daily.py --date 20260408
```

위 명령은 아래를 한 번에 수행한다.

1. `2026040800`, `2026040812` 자료 수집
2. `dain/20260408` 디렉토리 생성 및 저장
3. `00 UTC`, `12 UTC` 이미지 기준 feature 추출
4. `daio/20260408/features` 생성
5. `daio/20260408/briefing` 생성

`run_daily.py` 기본 stage는 `collect -> feature -> briefing`이다.

선택 실행도 가능하다.

```powershell
python jobs\run_daily.py --date 20260408 --stage collect
python jobs\run_daily.py --date 20260408 --stage feature --stage briefing
```

## 날짜 입력 규칙

### 1. run_date

- 상위 실행 날짜는 `YYYYMMDD`
- 예: `20260408`
- 디렉토리, 로그, 산출물 경로도 모두 `YYYYMMDD` 형식을 사용

### 2. collection target

수집 스크립트는 `YYYYMMDD` 또는 `YYYYMMDDHH`를 받는다.

- `YYYYMMDD`
  - 해당 날짜의 `00 UTC`, `12 UTC` 두 cycle을 모두 처리
- `YYYYMMDDHH`
  - 명시한 UTC cycle 하나만 처리
  - `HH`는 `00` 또는 `12`만 허용

예시:

```powershell
python scripts\run_collection_stage.py --date 20260408
python -m scripts.collect_radar --date 2026040812
python -m scripts.collect_satellite --date 2026040800
```

## 디렉토리 구조

- `config/`
  - 수집 설정, section map 등 운영 설정
- `daba/`
  - feature manifest, schema, prompt table, template, rules
- `dain/`
  - 수집 원천 자료
  - `dain/YYYYMMDD/...`
- `daio/`
  - feature 및 briefing 산출물
  - `daio/YYYYMMDD/...`
- `jobs/`
  - 상위 오케스트레이션
- `scripts/`
  - 수집, feature, briefing 실행 스크립트
- `logs/`
  - 실행 로그
  - `logs/YYYYMMDD/...`

## 수집 단계

### 1. ASOS

- 스크립트: `scripts/collect_asos.py`
- 검증: `scripts/verify_asos_outputs.py`
- 저장:
  - `dain/YYYYMMDD/asos/asos_hourly.csv`
  - `dain/YYYYMMDD/curated/asos/asos_daily_summary.csv`

명시 cycle로 여러 번 수집해도 같은 날짜 아래에 병합 저장된다.

### 2. 날씨누리 차트

- 스크립트: `scripts/collect_charts.py`
- 설정: `config/nuri_charts.txt`
- 저장:
  - `dain/YYYYMMDD/charts/nuri/...`

`config/nuri_charts.txt`는 현재 5열 형식을 지원한다.

```text
name | interval_hours | ext | url_template | required(optional)
```

- `required`
  - 누락되면 수집 실패로 처리
- `optional`
  - 누락돼도 경고만 남기고 파이프라인은 계속 진행

### 3. GK2A 위성

- 스크립트: `scripts/collect_satellite.py`
- 검증: `scripts/verify_satellite_outputs.py`
- 설정:
  - `config/satellite_le1b_products.txt`
  - `config/satellite_areas.txt`
- 저장:
  - `dain/YYYYMMDD/satellite/LE1B/<PRODUCT>/<AREA>/...`
  - `dain/YYYYMMDD/satellite/_meta/...`

현재는 `WV063` 외에도 `IR112`, `IR105`, `IR123`, `IR087`를 함께 수집할 수 있다.

검증은 날짜 전체가 아니라 현재 실행 cycle 기준으로 수행하며, 활성 manifest가 실제로 요구하는 위성 product/area 조합을 우선 필수 입력으로 본다.

### 4. 레이더 강수 합성영상

- 스크립트: `scripts/collect_radar.py`
- API:
  - `https://apihub.kma.go.kr/api/typ04/url/rdr_cmp_file.php`
- 현재 파라미터:
  - `data=img`
  - `cmp=cmi`
- 저장:
  - `dain/YYYYMMDD/radar/cmi/rdr_cmp_cmi_<YYYYMMDDHHMM>.*`

예시:

```powershell
python -m scripts.collect_radar --date 2026040812
```

### 5. 수집 오케스트레이션

- 스크립트: `scripts/run_collection_stage.py`

`--date 20260408`로 실행하면 내부적으로 아래 순서로 확장된다.

1. `2026040800`
2. `2026040812`

기본 수집 대상은 아래 4개이다.

- ASOS
- 날씨누리 차트
- GK2A 위성
- 레이더

필요하면 개별 비활성화가 가능하다.

```powershell
python scripts\run_collection_stage.py --date 20260408 --no-radar
python scripts\run_collection_stage.py --date 20260408 --no-satellite
```

## feature 단계

### 1. 실행 스크립트

- `scripts/run_feature_pipeline.py`
- `scripts/run_feature_stage.py`
- `scripts/feature_stage_runner_bundle.py`

### 2. 현재 분석 도메인

- `300hPa`
- `500hPa`
- `850hPa`
- `925hPa`
- `satellite_wv`
- `sfc12h_synoptic`
- `surface`
- `radar_precipitation`

### 3. 시간 기준

종관 manifest는 모두 `target_hours: ["00", "12"]`를 사용한다.

즉, 같은 날짜 안에서도 `00 UTC`, `12 UTC` 자료만 분석에 사용한다.

### 4. 산출물

각 도메인별로 아래 3종 산출물이 stage1/stage2마다 생성된다.

- `stage*_raw.json`
- `stage*_normalized.json`
- `stage*_compact.json`

경로 예시:

- `daio/YYYYMMDD/features/<domain>/stage1_raw.json`
- `daio/YYYYMMDD/features/<domain>/stage1_normalized.json`
- `daio/YYYYMMDD/features/<domain>/stage1_compact.json`
- `daio/YYYYMMDD/features/<domain>/stage2_raw.json`
- `daio/YYYYMMDD/features/<domain>/stage2_normalized.json`
- `daio/YYYYMMDD/features/<domain>/stage2_compact.json`

compact 산출물은 별도 후처리 없이 stage artifact 저장 시점에 함께 생성된다.

### 5. manifest / table

- manifest: `daba/manifests/synoptic_*.yaml`
- stage1 table: `daba/tables/*_feature_stage1_bundle_table.csv`
- stage2 table:
  - `daba/tables/*_feature_stage2_bundle_header.csv`
  - `daba/tables/*_feature_stage2_bundle_targets.csv`

레이더 feature는 아래 manifest/table을 사용한다.

- `daba/manifests/synoptic_radar_precipitation_stage1.yaml`
- `daba/manifests/synoptic_radar_precipitation_stage2.yaml`
- `daba/tables/radar_precipitation_feature_stage1_bundle_table.csv`
- `daba/tables/radar_precipitation_feature_stage2_bundle_header.csv`
- `daba/tables/radar_precipitation_feature_stage2_bundle_targets.csv`

## briefing 단계

### 1. 실행 스크립트

- `scripts/run_briefing_pipeline.py`
- `scripts/build_briefing_section_sources.py`
- `scripts/extract_timeline_events.py`
- `scripts/write_briefing_from_events.py`
- `scripts/write_briefing_draft.py`
- `scripts/compose_weather_briefing.py`

### 2. 현재 동작

브리핑 파이프라인은 이제 `run_daily.py`에 통합되어 있다.

즉, 아래 명령만으로 briefing까지 생성된다.

```powershell
python jobs\run_daily.py --date 20260408
```

### 3. section source 설정

- `config/briefing/section_map.yaml`
- `config/briefing/display_labels.yaml`

현재 `radar_precipitation`은 아래 섹션에 연결되어 있다.

- `overall_summary`
- `precipitation_structure`

### 4. 브리핑 산출물

- `daio/YYYYMMDD/briefing/section_sources/<section_id>.json`
- `daio/YYYYMMDD/briefing/events/<section_id>.prompt_input.json`
- `daio/YYYYMMDD/briefing/events/<section_id>.raw.json`
- `daio/YYYYMMDD/briefing/events/<section_id>.json`
- `daio/YYYYMMDD/briefing/sections/<section_id>.prompt_input.json`
- `daio/YYYYMMDD/briefing/sections/<section_id>.raw.json`
- `daio/YYYYMMDD/briefing/sections/<section_id>.json`
- `daio/YYYYMMDD/briefing/weather_briefing.json`
- `daio/YYYYMMDD/briefing/weather_briefing.md`

## 실행 예시

### 전체 실행

```powershell
python jobs\run_daily.py --date 20260408
```

### 수집만 실행

```powershell
python scripts\run_collection_stage.py --date 20260408
```

### feature만 실행

```powershell
python scripts\run_feature_pipeline.py --date 20260408
```

### briefing만 실행

```powershell
python scripts\run_briefing_pipeline.py --date 20260408
```

### 개별 collector 직접 실행

```powershell
python -m scripts.collect_asos --date 2026040812
python -m scripts.collect_charts --date 2026040800
python -m scripts.collect_satellite --date 2026040812
python -m scripts.collect_radar --date 2026040812
```

## 현재 결과 경로 예시

`20260408` 기준 실제 생성 경로 예시는 아래와 같다.

- 수집:
  - `dain/20260408/asos/...`
  - `dain/20260408/charts/nuri/...`
  - `dain/20260408/satellite/...`
  - `dain/20260408/radar/cmi/...`
- feature:
  - `daio/20260408/features/...`
- briefing:
  - `daio/20260408/briefing/weather_briefing.md`
  - `daio/20260408/briefing/weather_briefing.json`

## 알려진 한계

- 최종 `validate_briefing.py`는 아직 없다.
- feature 추출은 이미지 기반 LLM 판독이므로 rerun 시 완전한 결정론을 보장하지 않는다.
- 외부 API 상태에 따라 일부 보조 입력은 누락될 수 있다.
- PowerShell 터미널 미리보기에서는 한글이 깨져 보일 수 있으나, 저장 파일은 UTF-8 기준으로 사용한다.
