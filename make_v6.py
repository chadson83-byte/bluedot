import pandas as pd
import glob
import os
import re

# =====================================================================
# 🚀 BLUEDOT 마스터 데이터 V6 생성기 (상권 인프라 융합 - 최종 안정화 버전)
# =====================================================================

BASE_DIR = r"C:\Users\chads\Desktop\병원입지"
COMMERCIAL_DIR = os.path.join(BASE_DIR, "data", "2_상가정보")
V5_FILE = os.path.join(BASE_DIR, "bluedot_master_v5.csv")
V6_FILE = os.path.join(BASE_DIR, "bluedot_master_v6.csv")

# 💡 분석할 핵심 키워드 세팅
ANCHOR_KEYWORDS = '스타벅스|올리브영|다이소|파리바게뜨|메가엠지씨커피'
ACADEMY_KEYWORDS = '학원|교습|독서실|스터디'
FITNESS_KEYWORDS = '필라테스|요가|헬스|피트니스|골프'

print("1. 🏪 2_상가정보 폴더 내의 모든 CSV 파일 스캔 시작...")
# 와일드카드(*)를 써서 '소상공인..._광주_202512.csv' 등 모든 형태의 csv를 다 잡아냅니다.
csv_files = glob.glob(os.path.join(COMMERCIAL_DIR, "*.csv"))

if not csv_files:
    print(f"🚨 {COMMERCIAL_DIR} 폴더에 CSV 파일이 없습니다! 압축을 풀고 파일을 넣어주세요.")
    exit()

aggregated_results = []
# 소상공인 데이터 표준 컬럼명 (광주, 서울 등 모든 파일 동일)
required_cols = ['상호명', '상권업종소분류명', '시도명', '시군구명', '행정동명']

for file in csv_files:
    file_name = os.path.basename(file)
    print(f" ⏳ 스캔 중: {file_name} ...", end=" ")
    
    try:
        # 데이터가 커서 메모리 초과(OOM) 방지를 위해 필요한 컬럼만 문자열로 읽기
        df = pd.read_csv(file, usecols=required_cols, dtype=str, encoding='utf-8')
    except ValueError:
        print(" [스킵] 필요한 컬럼이 없는 파일입니다.")
        continue
    except UnicodeDecodeError:
        try:
            # 공공데이터는 간혹 cp949(euc-kr)로 인코딩된 경우가 있어 방어 로직 추가
            df = pd.read_csv(file, usecols=required_cols, dtype=str, encoding='cp949')
        except:
            print(" [스킵] 인코딩 에러 발생.")
            continue
    except Exception as e:
        print(f" [스킵] 알 수 없는 에러: {e}")
        continue

    # 결측치(빈칸) 안전 처리
    df.fillna("", inplace=True)
    
    # 💡 [핵심] 상호명과 업종명에서 돈이 되는 타겟 키워드만 필터링!
    df['is_anchor'] = df['상호명'].str.contains(ANCHOR_KEYWORDS, case=False, na=False)
    df['is_pharmacy'] = df['상권업종소분류명'].str.contains('약국', case=False, na=False) | df['상호명'].str.contains('약국', case=False, na=False)
    df['is_academy'] = df['상권업종소분류명'].str.contains(ACADEMY_KEYWORDS, case=False, na=False) | df['상호명'].str.contains(ACADEMY_KEYWORDS, case=False, na=False)
    df['is_fitness'] = df['상호명'].str.contains(FITNESS_KEYWORDS, case=False, na=False)

    # 행정동별로 True(1) 값을 더하여 개수를 셈
    agg = df.groupby(['시도명', '시군구명', '행정동명'])[['is_anchor', 'is_pharmacy', 'is_academy', 'is_fitness']].sum().reset_index()
    aggregated_results.append(agg)
    print("완료!")

print("\n2. 🧩 지역별 상가 데이터를 하나로 합치는 중...")
full_poi_df = pd.concat(aggregated_results, ignore_index=True)

# 병합을 위한 스마트 매칭키 생성 (예: "부산광역시 해운대구 우제1동" -> "부산 우1동")
full_poi_df['sido'] = full_poi_df['시도명'].str[:2]
full_poi_df['dong'] = full_poi_df['행정동명'].str.replace(r'제?(\d+)동$', r'동', regex=True)
full_poi_df['match_key'] = full_poi_df['sido'] + " " + full_poi_df['dong']

# 지역 이름별로 최종 합산
final_poi = full_poi_df.groupby('match_key').agg({
    'is_anchor': 'sum',
    'is_pharmacy': 'sum',
    'is_academy': 'sum',
    'is_fitness': 'sum'
}).reset_index()

print("3. 🚀 기존 V5 마스터 데이터 불러와서 결합 중...")
master_df = pd.read_csv(V5_FILE)

# 마스터 데이터에도 동일한 규격의 매칭키 생성
master_df['temp_sido'] = master_df['행정구역(동읍면)별'].apply(lambda x: str(x).split()[0][:2])
master_df['temp_dong'] = master_df['행정구역(동읍면)별'].apply(lambda x: str(x).split()[-1])
master_df['temp_dong'] = master_df['temp_dong'].str.replace(r'제?(\d+)동$', r'동', regex=True)
master_df['match_key'] = master_df['temp_sido'] + " " + master_df['temp_dong']

# V5와 전국 상가 데이터 병합 (Left Join)
v6_master = pd.merge(master_df, final_poi, on='match_key', how='left')

# 상가가 없어서 매칭 안 된 동네(시골 등)는 0으로 채움
v6_master['is_anchor'] = v6_master['is_anchor'].fillna(0).astype(int)
v6_master['is_pharmacy'] = v6_master['is_pharmacy'].fillna(0).astype(int)
v6_master['is_academy'] = v6_master['is_academy'].fillna(0).astype(int)
v6_master['is_fitness'] = v6_master['is_fitness'].fillna(0).astype(int)

# 불필요한 임시 매칭키 삭제 및 이름 정리
v6_master.drop(columns=['temp_sido', 'temp_dong', 'match_key'], inplace=True)
v6_master.rename(columns={
    'is_anchor': 'anchor_cnt',
    'is_pharmacy': 'pharmacy_cnt',
    'is_academy': 'academy_cnt',
    'is_fitness': 'fitness_cnt'
}, inplace=True)

print("4. ✅ 저장 준비 완료!")
v6_master.to_csv(V6_FILE, index=False, encoding='utf-8-sig')
print(f"🎉 대성공! 세상에 없던 초정밀 [의료+대중교통+인구+상가인프라] 마스터 데이터가 생성되었습니다!")
print(f"➡️ 파일 위치: {V6_FILE}")