# OAuth 로그인 설정 가이드

## 카카오 로그인
1. [카카오 개발자 콘솔](https://developers.kakao.com) 접속
2. 앱 생성 후 **앱 키** 메뉴에서 **JavaScript 키** 확인
3. `index.html` 내 `window.KAKAO_JS_KEY = "여기에_키_입력"`
4. **동의 항목**에서 이메일, 프로필 정보 수집 동의 설정
5. 백엔드 검증용 **REST API 키** → 환경변수 `KAKAO_REST_KEY` 또는 `auth_config.py` 수정

## 구글 로그인
1. [Google Cloud Console](https://console.cloud.google.com) 접속
2. **API 및 서비스** → **사용자 인증 정보** → **OAuth 2.0 클라이언트 ID** 생성
3. 애플리케이션 유형: **웹 애플리케이션**
4. 승인된 JavaScript 원본: `http://127.0.0.1:5500`, `http://localhost:5500` 등 추가
5. **클라이언트 ID**를 `index.html` 내 `window.GOOGLE_CLIENT_ID = "여기에_입력"`
6. 환경변수 `GOOGLE_CLIENT_ID` 또는 `auth_config.py` 수정

## 테스트 로그인
OAuth 키 설정 없이 **테스트 로그인**으로 개발·테스트 가능합니다.
- 사이드바 또는 로그인 모달에서 "테스트 로그인" 클릭
