"""
서울 열린데이터 광장에서 '실시간 돌발 상황 정보' 와 '서울시 실시간 도로 소통 정보' 를 수집한다.

- 1시간마다 실행되고, 실행 한 번에 5분 간격으로 최대 12번 수집한다. (55분 동안 루프 실행)
- 수집 시각은 '5분 경계(00, 05, 10 ...)'에 맞춰 대기하므로 간격이 밀리지 않는다. (08분에 시작 시 10분까지 대기 후 루프 시작. github actions 가 워크플로우 자체를 늦게 시작한 경우에는 수집 주기가 밀릴 수 있다. )
- 6회(약 30분)마다 중간 커밋, 종료 시 최종 커밋.
- 돌발 정보(AccInfo)는 전체를 받고, 소통 정보(TrafficInfo)는 LINK_ID가 필수라서 links.txt 에 적은 링크만 하나씩 호출한다.
- 저장: data/{accident|traffic}/{이름}_YYYYMMDD.csv.gz (날짜별 파일 하나에 이어 붙인다. 날짜별로 테스트 데이터 분리해서 저장한다.)
- 수집 성공 여부: data/collect_log.csv (돌발 실패는 빈 칸, 소통은 실패한 링크 수를 별도 컬럼에 기록)
"""
import os
import sys
import csv
import gzip
import time
import subprocess
import urllib.error
import xml.etree.ElementTree as ET
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

KEY = os.environ["SEOUL_API_KEY"]
BASE = "http://openapi.seoul.go.kr:8088"
KST = timezone(timedelta(hours=9))

INTERVAL = 300          # 수집 간격(초) = 5분
RUN_MINUTES = 56        # 현재 시(hour) 시작 후 이 시각(분)까지만 수집 -> 마지막 수집은 :55
COMMIT_EVERY = 6        # 6회 수집(약 30분)마다 중간 커밋
MAX_ROUNDS = int(os.environ.get("MAX_ROUNDS", "0"))  # 0이면 제한 없음, 수동 테스트 때는 1
WORKERS = 5             # 소통 정보를 동시에 호출할 개수

# 서비스명은 각 데이터 페이지의 '샘플 테스트' URL에서 확인할 수 있다.
ACC_SERVICE = "AccInfo"          # 실시간 돌발 정보
TRAFFIC_SERVICE = "TrafficInfo"  # 실시간 도로 소통 정보 (LINK_ID 필수)
LINKS_FILE = "links.txt"         # 소통 정보를 수집할 LINK_ID 목록 (한 줄에 하나)


def load_links():
    if not os.path.exists(LINKS_FILE):
        sys.exit(f"{LINKS_FILE} 파일이 없습니다. 수집할 LINK_ID를 한 줄에 하나씩 적어 주세요.")
    with open(LINKS_FILE, encoding="utf-8-sig") as f:  # BOM이 붙은 파일도 읽을 수 있게
        links = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    links = list(dict.fromkeys(links))  # 중복 제거 (순서 유지)
    if not links:
        sys.exit(f"{LINKS_FILE} 에 LINK_ID가 하나도 없습니다.")
    return links


def parse_xml(service, raw):
    """서울 API의 XML 응답을 {서비스명: {"list_total_count": N, "row": [...]}} 형태로 바꾼다.
    오류 응답처럼 서비스 블록이 없으면 {"RESULT": {...}} 를 돌려준다.
    행의 컬럼명은 문서(출력명)와 맞추기 위해 대문자로 통일한다."""
    root = ET.fromstring(raw)

    def result_of(node):
        res = node if node.tag == "RESULT" else node.find("RESULT")
        if res is None:
            return {}
        return {c.tag: (c.text or "").strip() for c in res}

    if root.tag != service:
        return {"RESULT": result_of(root)}

    rows = [
        {c.tag.upper(): (c.text or "").strip() for c in row}
        for row in root.findall("row")
    ]
    result = result_of(root)
    code = result.get("CODE", "")
    if not rows and code and code != "INFO-000":
        print(f"[{service}] 응답: {result}", flush=True)
    total = int(root.findtext("list_total_count") or 0)
    return {service: {"list_total_count": total, "row": rows, "RESULT": result}}


def fetch(service, start, end, extra=""):
    """extra: URL 맨 끝에 붙는 추가 인자 (소통 정보의 LINK_ID)"""
    url = f"{BASE}/{KEY}/xml/{service}/{start}/{end}/{extra}"
    for attempt in range(3):
        status, raw = None, b""
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                status = r.status
                raw = r.read()
            return parse_xml(service, raw)
        except Exception as e:
            if isinstance(e, urllib.error.HTTPError):
                status = e.code
                try:
                    raw = e.read()
                except Exception:
                    pass
            msg = str(e).replace(KEY, "***")
            preview = raw[:200].decode("utf-8", errors="replace").replace(KEY, "***")
            preview = preview.replace("\n", " ")
            print(
                f"[{service} {extra}] 시도 {attempt + 1} 실패: {msg} "
                f"| HTTP {status} | 응답 {len(raw)}바이트 | 미리보기: {preview!r}",
                flush=True,
            )
            time.sleep(3)
    return None


def fetch_all(service):
    """돌발 정보용. 1000건씩 페이지를 넘기며 전부 받는다. 실패 -> None, 데이터 없음 -> []"""
    rows, start, step = [], 1, 1000
    while True:
        data = fetch(service, start, start + step - 1)
        if data is None:
            return None
        if service not in data:  # 예: INFO-200 (해당하는 데이터 없음)
            print(f"[{service}] 응답: {data.get('RESULT')}", flush=True)
            return rows
        block = data[service]
        rows += block.get("row", [])
        if start + step - 1 >= int(block["list_total_count"]):
            return rows
        start += step


def fetch_traffic(links):
    """소통 정보용. 링크마다 한 번씩 호출한다. 반환: (행 목록, 실패한 링크 수)"""
    def one(link_id):
        data = fetch(TRAFFIC_SERVICE, 1, 5, link_id)
        if data is None:
            return None
        if TRAFFIC_SERVICE not in data:
            print(f"[{TRAFFIC_SERVICE} {link_id}] 응답: {data.get('RESULT')}", flush=True)
            return []
        return data[TRAFFIC_SERVICE].get("row", [])

    rows, failed = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for result in ex.map(one, links):
            if result is None:
                failed += 1
            else:
                rows += result
    return rows, failed


def save(name, rows, day, collected_at):
    """날짜별 파일 하나에 이어 붙인다. (gzip은 이어 붙여도 하나의 파일처럼 읽힌다)"""
    if not rows:
        return
    folder = f"data/{name}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{name}_{day}.csv.gz"
    is_new = not os.path.exists(path)
    fields = ["collected_at"] + list(rows[0].keys())
    with gzip.open(path, "at", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if is_new:
            w.writeheader()  # 헤더는 파일을 처음 만들 때 한 번만
        for r in rows:
            w.writerow({"collected_at": collected_at, **r})


def collect_once(links):
    now = datetime.now(KST)
    collected_at = now.strftime("%Y-%m-%d %H:%M:%S")
    day = now.strftime("%Y%m%d")

    acc_rows = fetch_all(ACC_SERVICE)
    acc_count = "" if acc_rows is None else len(acc_rows)  # 실패는 빈 칸
    if acc_rows:
        save("accident", acc_rows, day, collected_at)

    traffic_rows, failed = fetch_traffic(links)
    if traffic_rows:
        save("traffic", traffic_rows, day, collected_at)

    os.makedirs("data", exist_ok=True)
    log = "data/collect_log.csv"
    is_new = not os.path.exists(log)
    with open(log, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow(["collected_at", "acc_count", "traffic_count", "traffic_failed_links"])
        w.writerow([collected_at, acc_count, len(traffic_rows), failed])

    print(
        f"[{collected_at}] 돌발 {acc_count}건, 소통 {len(traffic_rows)}건 "
        f"(링크 {len(links)}개 중 실패 {failed}개)",
        flush=True,
    )


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True)


def commit_push():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    git("add", "data")
    if git("diff", "--cached", "--quiet").returncode == 0:
        return  # 바뀐 게 없음
    msg = "data: " + datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    git("commit", "-m", msg)
    for _ in range(3):
        git("pull", "--rebase", "--autostash")
        if git("push").returncode == 0:
            print("커밋/푸시 완료", flush=True)
            return
        time.sleep(5)
    print("푸시 실패 (마지막 단계에서 다시 시도함)", flush=True)


def next_boundary(ts):
    """ts 이후의 가장 가까운 5분 경계(epoch). KST는 정시 오프셋이라 그대로 맞는다."""
    return (int(ts // INTERVAL) + 1) * INTERVAL


def sleep_until(target):
    time.sleep(max(0, target - time.time()))


def main():
    links = load_links()
    print(f"수집 대상 링크 {len(links)}개", flush=True)

    start = time.time()
    hour_start = int(start // 3600) * 3600
    deadline = hour_start + RUN_MINUTES * 60

    # 시작이 지연돼 경계에서 30초 넘게 지났으면 다음 5분 경계까지 기다려 정렬한다.
    if MAX_ROUNDS == 0 and start % INTERVAL > 30:
        nxt = next_boundary(start)
        if nxt <= deadline:
            print(f"경계 정렬을 위해 {int(nxt - start)}초 대기", flush=True)
            sleep_until(nxt)

    rounds = 0
    while True:
        collect_once(links)
        rounds += 1

        if MAX_ROUNDS and rounds >= MAX_ROUNDS:
            break
        if rounds % COMMIT_EVERY == 0:
            commit_push()

        nxt = next_boundary(time.time())
        if nxt > deadline:
            break
        sleep_until(nxt)

    commit_push()
    print(f"종료: 총 {rounds}회 수집", flush=True)


if __name__ == "__main__":
    main()
