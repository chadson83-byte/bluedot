import pandas as pd
import glob

print("🩺 [BLUEDOT X-RAY] 공공데이터 컬럼명 스캔을 시작합니다...")

def scan_columns(filepath, name):
    encodings = ['utf-8', 'cp949', 'utf-8-sig', 'utf-16', 'euc-kr']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc, errors='ignore') as f:
                sep = '\t' if '\t' in f.readline() else ','
            df = pd.read_csv(filepath, encoding=enc, sep=sep)
            print(f"\n✅ [{name}] 데이터 스캔 완료!")
            # 어떤 이름표(컬럼명)를 쓰는지 상위 15개만 출력
            print(f"👉 실제 컬럼명: {df.columns.tolist()[:15]}") 
            return
        except:
            pass
    print(f"\n❌ [{name}] 파일 읽기 실패")

try:
    bus_file = (glob.glob('*bus*.csv') + glob.glob('data/1_raw/*bus*.csv'))[0]
    sub_file = (glob.glob('*도시철도*.csv') + glob.glob('data/1_raw/*도시철도*.csv'))[0]

    scan_columns(bus_file, "🚌 버스")
    scan_columns(sub_file, "🚇 지하철")
except IndexError:
    print("❌ 파일을 찾을 수 없습니다. 경로를 확인해주세요.")