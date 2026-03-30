# -*- coding: utf-8 -*-
"""OAuth 클라이언트 설정. 카카오/구글 개발자 콘솔에서 발급받은 키를 입력하세요."""
import os

# 카카오 JavaScript 키 (카카오 개발자 콘솔 > 앱 설정 > 앱 키)
KAKAO_REST_KEY = os.environ.get("KAKAO_REST_KEY", "")

# 구글 OAuth 클라이언트 ID (Google Cloud Console > API 및 서비스 > 사용자 인증 정보)
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")

# JWT 시크릿 (임의의 긴 문자열로 변경 권장)
JWT_SECRET = os.environ.get("BLUEDOT_JWT_SECRET", "bluedot-dev-secret-change-in-production")

# 테스트 모드: 1일 때 imp_uid 없이 결제 API 허용 (로컬 개발용, 프로덕션에서는 0 필수)
BLUEDOT_TEST_MODE = os.environ.get("BLUEDOT_TEST_MODE", "0") == "1"

# 포트원 REST API (프로덕션에서는 환경변수로 설정)
PORTONE_API_KEY = os.environ.get("PORTONE_API_KEY", "YOUR_REST_API_KEY")
PORTONE_API_SECRET = os.environ.get("PORTONE_API_SECRET", "YOUR_REST_API_SECRET")
