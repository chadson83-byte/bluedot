import psycopg2
import folium
import json

# ==========================================
# 1. DB 접속 정보 설정
# ==========================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "gis_db"
DB_USER = "postgres"
DB_PASS = "bluedot1234" # 원장님께서 설정하신 비밀번호 확인

# ==========================================
# 2. 분석할 타겟 병원(해운대 중동 인근) 좌표 및 조건
# ==========================================
TARGET_LAT = 35.1631  # 예시 위도 (해운대 중동역 인근)
TARGET_LON = 129.1666 # 예시 경도
MAX_WALKING_MINUTES = 10 # 도보 10분 거리
WALKING_SPEED_KMH = 4.0 # 평균 도보 속도 (km/h)

# 도보 10분을 물리적 거리(km)로 환산
# (4.0km/h 속도로 10분 걸으면 나오는 거리, 약 0.66km)
MAX_COST = (WALKING_SPEED_KMH / 60) * MAX_WALKING_MINUTES 

def get_walking_polygon():
    try:
        # DB 연결
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()

        # 1단계: 입력한 위경도와 가장 가까운 도로망의 '노드(교차로)' ID 찾기
        cur.execute(f"""
            SELECT source 
            FROM korea_2po_4pgr 
            ORDER BY geom_way <-> ST_SetSRID(ST_MakePoint({TARGET_LON}, {TARGET_LAT}), 4326) 
            LIMIT 1;
        """)
        start_node = cur.fetchone()[0]
        print(f"✅ 출발 노드 ID 확보: {start_node}")

        # 2단계: pgRouting (pgr_drivingDistance) + ST_ConvexHull을 이용해 폴리곤(다각형) 형태의 GeoJSON 추출
        # ⚠️ 수정된 핵심 포인트: 'cost' 대신 물리적 거리인 'km'를 기준으로 길을 찾도록 강제함
        polygon_query = f"""
            SELECT ST_AsGeoJSON(ST_ConvexHull(ST_Collect(geom_way)))
            FROM korea_2po_4pgr
            JOIN pgr_drivingDistance(
                'SELECT id, source, target, km AS cost, km AS reverse_cost FROM korea_2po_4pgr',
                {start_node}, {MAX_COST}, false
            ) AS d ON korea_2po_4pgr.id = d.edge;
        """
        
        print("⏳ 도보 폴리곤 계산 중... (DB 엔진 가동)")
        cur.execute(polygon_query)
        result = cur.fetchone()
        
        if result and result[0]:
            geojson_str = result[0]
            cur.close()
            conn.close()
            return geojson_str
        else:
            print("❌ 쿼리 결과가 없습니다. 좌표 주변에 인식된 도로망이 없을 수 있습니다.")
            cur.close()
            conn.close()
            return None

    except Exception as e:
        print(f"❌ DB 연결 또는 쿼리 에러: {e}")
        return None

# ==========================================
# 3. 지도에 그리고 HTML 파일로 저장
# ==========================================
print("🚀 블루닷 도보 상권 분석 시작 (물리적 거리 기준)...")
polygon_geojson = get_walking_polygon()

if polygon_geojson:
    # 기본 지도 생성 (해운대 중심)
    m = folium.Map(location=[TARGET_LAT, TARGET_LON], zoom_start=15)
    
    # 타겟 병원 마커 찍기
    folium.Marker(
        [TARGET_LAT, TARGET_LON], 
        popup='Target Clinic', 
        icon=folium.Icon(color='red', icon='info-sign')
    ).add_to(m)

    # 추출한 폴리곤을 지도에 반투명한 파란색으로 덮어씌우기
    folium.GeoJson(
        json.loads(polygon_geojson),
        style_function=lambda x: {
            'fillColor': '#3186cc',
            'color': '#3186cc',
            'weight': 2,
            'fillOpacity': 0.3
        }
    ).add_to(m)

    # 결과물 저장
    output_file = "bluedot_walking_polygon.html"
    m.save(output_file)
    print(f"🎉 성공! '{output_file}' 파일이 생성되었습니다. 웹 브라우저로 열어서 확인해 보세요!")
else:
    print("폴리곤 생성에 실패했습니다. 앞선 4단계(SQL 데이터 임포트)가 잘 완료되었는지 확인해 주세요.")