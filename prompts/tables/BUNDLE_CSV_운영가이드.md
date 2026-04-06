# Bundle CSV 운영 가이드

이 가이드는 `*_feature_stage1_bundle_table.csv`, `*_feature_stage2_bundle_header.csv`, `*_feature_stage2_bundle_targets.csv`를 수정할 때의 최소 규칙을 정리한 문서다.

## 1. 파일 역할

- `stage1_bundle_table`
  - 한 row가 한 region bundle이다.
  - `allowed_answers`에는 이 영역에서 고를 수 있는 signal 목록과 `none`을 pipe(`|`)로 넣는다.
  - stage1 LLM은 `selected_answers` 배열로 복수 선택한다.

- `stage2_bundle_header`
  - 한 row가 한 region bundle header다.
  - `bundle_id`는 targets 파일과 연결되는 기준 키다.
  - `gate_signals`는 이 bundle에서 참조하는 대표 signal 목록이다.

- `stage2_bundle_targets`
  - 한 row가 한 target 질문이다.
  - `bundle_id`로 header와 연결된다.
  - 시스템은 header + targets를 합쳐 bundle 단위 질의를 만든다.

## 2. 주요 컬럼 의미

- `question_id`
  - stage1 bundle 질문의 고정 식별자다.

- `bundle_id`
  - stage2 header/targets 연결 키다. 중복되면 안 된다.

- `signal_key`
  - 실제 종관 signal 키다.

- `attribute_key`
  - signal의 2차 속성 키다.

- `target_label`
  - stage2 target 식별자다. bundle 안에서 중복되면 안 된다.

- `allowed_answers`
  - LLM이 그대로 복사해야 하는 허용 답 목록이다.
  - 공백 없이 `a|b|c` 형식으로 유지하는 편이 가장 안전하다.

- `gate_on_stage1`
  - stage2 target을 열기 위한 stage1 signal 키다.
  - 복수 조건이 필요하면 `confluent_flow|diffluent_flow`처럼 pipe로 쓴다.

- `gate_rule`
  - 현재 허용값:
    - `full_if_yes_core_if_unknown_skip_if_no`
    - `full_if_yes_extended_if_unknown_skip_if_no`

- `tier`
  - 현재 허용값:
    - `core`
    - `extended`

## 3. allowed_answers 작성 규칙

- stage1 bundle:
  - 실제 signal 목록 + `none`
  - `none`은 반드시 단독 선택용 값이다.

- stage2 targets:
  - 모델이 그대로 복사할 짧고 명확한 값만 사용한다.
  - JSON 문자열이나 설명 문장을 넣지 않는다.

## 4. gate_rule / core / extended 규칙

- `yes`
  - core/extended 모두 허용한다.

- `unknown`
  - core만 허용한다.
  - extended는 기본 skip된다.

- `no`
  - 해당 target은 skip된다.

## 5. 자주 나는 실수

- `bundle_id`를 header와 targets에서 다르게 쓰는 경우
- `target_label` 중복
- `allowed_answers`에 오타를 넣는 경우
- `gate_rule` 오타
- `tier`를 `main`, `optional`처럼 임의로 바꾸는 경우
- stage1에서 `none`을 빼먹거나, stage2에서 JSON 문자열을 셀에 넣는 경우
- `gate_on_stage1`에 여러 signal을 쓰면서 pipe 구분을 빠뜨리는 경우

## 6. 수정 후 검증 방법

- stage1 dry-run:
  - `python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_850hPa_stage1.yaml --dry-run`

- stage2 dry-run:
  - `python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_850hPa_stage2.yaml --dry-run`

- 실제 bundle 실행:
  - `python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_850hPa_stage1.yaml`
  - `python -m scripts.run_feature_stage --date 2026-04-01 --manifest prompts/manifests/synoptic_850hPa_stage2.yaml`

- downstream 확인:
  - `python -m scripts.run_feature_bundle --date 2026-04-01`

검증에서 아래가 나오면 즉시 수정해야 한다.

- 필수 컬럼 누락
- bundle/header-target 연결 불일치
- 지원하지 않는 `gate_rule`
- 지원하지 않는 `tier`
- 허용되지 않은 answer 값
