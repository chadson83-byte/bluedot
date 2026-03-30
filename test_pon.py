import requests
import urllib3

# 공공기관 API 호출 시 자주 뜨는 성가신 SSL 인증서 경고 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. 테스트할 API 키 및 타겟 주소 (해운대 중동 1378-9)
# ==========================================
SERVICE_KEY = "8ee102c5d025b9a9709736175aa0168bac653098ef0f762e797f727d77dc7da9"

SIGUNGU_CD = "26350"  # 해운대구
BJDONG_CD = "10500"   # 중동
BUN = "1378"          # 본번
JI = "0009"           # 부번

def test_api_key_status():
    # 파이썬이 키를 마음대로 변환하지 못하도록 URL 전체를 직접 텍스트로 조립합니다.
    url = (
        f"https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
        f"?serviceKey={SERVICE_KEY}"
        f"&sigunguCd={SIGUNGU_CD}"
        f"&bjdongCd={BJDONG_CD}"
        f"&bun={BUN}"
        f"&ji={JI}"
        f"&numOfRows=10"
        f"&pageNo=1"
        f"&_type=json"
    )

    print("📡 공공데이터포털 API 키 생존 테스트를 시작합니다...")
    print(f"🔑 확인 중인 키: {SERVICE_KEY[:10]}... (보안상 앞부분만)")
    
    try:
        response = requests.get(url, verify=False)
        
        print(f"\n[응답 코드]: {response.status_code}")
        
        # 1. 진짜 성공 (데이터 수신)
        if response.status_code == 200:
            # 공공 API는 에러가 나도 200을 띄우며 XML 에러 메시지를 줄 때가 있습니다.
            if 'SERVICE KEY IS NOT REGISTERED' in response.text or '등록되지 않은 서비스키' in response.text:
                print("❌ 판독 결과: 아직 서버에 키가 동기화되지 않았습니다. (가짜 200 응답)")
                print("💡 조치: 마음 편히 30분~1시간 정도 더 기다리신 후 다시 화살표 위(↑) 키를 눌러 실행해 보세요.")
                
            elif 'NODATA_ERROR' in response.text or '데이터가 없습니다' in response.text:
                print("✅ 판독 결과: API 키는 완벽하게 살아서 작동 중입니다! (동기화 완료)")
                print("ℹ️ 다만, 해운대 중동 1378-9 지번에 해당하는 건축물대장 데이터가 없을 뿐입니다.")
                
            else:
                print("🎉 판독 결과: API 키 완벽 작동! 데이터도 정상적으로 수신되었습니다!")
                print("-" * 40)
                # 데이터 앞부분 300자만 잘라서 보여주기
                print(response.text[:300] + "\n... (이하 생략)")
                print("-" * 40)
                
        # 2. 확실한 403 에러 (동기화 전이거나 키가 틀림)
        elif response.status_code == 403:
            print("❌ 판독 결과: 403 Forbidden 에러. 아직 서버 동기화가 진행 중입니다.")
            print("💡 조치: 조금 더 기다리셨다가 다시 실행해 주세요.")
            
        else:
            print(f"⚠️ 기타 에러 발생 (코드: {response.status_code})")
            
    except Exception as e:
        print(f"❌ 요청 중 에러가 발생했습니다: {e}")

if __name__ == "__main__":
    test_api_key_status()