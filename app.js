// =========================================================
// BLUEDOT - B2B 스마트 개원 입지 분석 솔루션 (v5.2 프리미엄 뼈대)
// =========================================================

// [0] fetch with timeout + JSON 안전 파싱 (CB-3 대응)
const FETCH_TIMEOUT_MS = 90000; // 일반 API
/** 심평원·마스터 병합 등 무거운 분석 — Vercel 프록시/클라이언트 모두 여유 있게 */
const BLUEDOT_ANALYZE_TIMEOUT_MS = 180000; // 3분
async function fetchWithTimeout(url, opts = {}) {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort(), opts.timeout ?? FETCH_TIMEOUT_MS);
    try {
        const res = await fetch(url, { ...opts, signal: ctrl.signal });
        clearTimeout(id);
        return res;
    } catch (e) {
        clearTimeout(id);
        if (e.name === 'AbortError') throw new Error('서버 응답 시간이 초과되었습니다. 잠시 후 다시 시도해 주세요.');
        throw e;
    }
}
async function parseJsonSafe(res) {
    const text = await res.text();
    try {
        return text ? JSON.parse(text) : {};
    } catch {
        throw new Error('서버 응답 형식이 올바르지 않습니다.');
    }
}

// Vercel 배포 시 vercel.json 리라이트로 /api 를 넘기면 장시간 분석이 프록시 타임아웃(약 60초)에 걸림 → Fly 직접 호출
const BLUEDOT_VERCEL_FLY_ORIGIN = 'https://bluedot-backend-autumn-grass-4638.fly.dev';

const BLUEDOT_API_BASE = (() => {
    const w = (typeof window !== 'undefined') ? window : null;
    const explicit = w && typeof w.BLUEDOT_API_BASE === 'string' ? w.BLUEDOT_API_BASE.trim() : '';
    if (explicit) return explicit;
    // file:// 로 열면 hostname 이 비어 '' 가 되어 API·지도가 깨짐 → 백엔드로 고정
    if (w && w.location && w.location.protocol === 'file:') {
        return 'http://127.0.0.1:8000';
    }
    if (w && w.location && (w.location.hostname === '127.0.0.1' || w.location.hostname === 'localhost')) {
        return 'http://127.0.0.1:8000';
    }
    if (w && w.location && /\.vercel\.app$/i.test(w.location.hostname)) {
        return BLUEDOT_VERCEL_FLY_ORIGIN;
    }
    // 그 외 커스텀 도메인 등: 같은 오리진 /api (리버스 프록시가 긴 타임아웃 허용할 때)
    return '';
})();

/** 카카오맵 sdk.js — kakao.min.js 이후에 주입해 kakao.maps 가 사라지지 않게 함 */
function loadKakaoMapsScript() {
    return new Promise((resolve, reject) => {
        if (typeof kakao !== 'undefined' && kakao.maps) {
            resolve();
            return;
        }
        const key = (typeof window !== 'undefined' && window.KAKAO_JS_KEY) ? String(window.KAKAO_JS_KEY).trim() : '';
        if (!key) {
            reject(new Error('KAKAO_JS_KEY가 비어 있습니다.'));
            return;
        }
        const url = 'https://dapi.kakao.com/v2/maps/sdk.js?appkey=' + encodeURIComponent(key) + '&libraries=services&autoload=false';
        const el = document.createElement('script');
        el.src = url;
        el.async = true;
        el.onload = () => resolve();
        el.onerror = () => reject(new Error('카카오맵 SDK 스크립트를 불러오지 못했습니다.'));
        document.head.appendChild(el);
    });
}

/** API HTTP 오류 시 사용자용 메시지 (404 Not Found 등) */
function bluedotApiErrorMessage(response, data) {
    const status = response.status;
    const detail = data && (data.detail != null ? String(data.detail) : '');
    const msg = data && data.message ? String(data.message) : '';
    if (status === 404 || detail === 'Not Found') {
        return '백엔드 API를 찾을 수 없습니다(404).\n\n'
            + '① 프로젝트 폴더에서 터미널을 열고 아래를 실행했는지 확인하세요:\n'
            + '   uvicorn main:app --reload --host 127.0.0.1 --port 8000\n\n'
            + '② 브라우저에서 ' + BLUEDOT_API_BASE + '/api/health 가 열리면 서버가 정상입니다.\n'
            + '③ 주소가 ' + BLUEDOT_API_BASE + ' 인지(index.html의 BLUEDOT_API_BASE) 확인하세요.';
    }
    if (status === 503) return msg || detail || '서버가 일시적으로 사용할 수 없습니다.';
    return detail || msg || ('HTTP 오류 ' + status);
}

async function apiJson(url, opts) {
    const r = await fetchWithTimeout(url, opts);
    const data = await parseJsonSafe(r);
    if (!r.ok) {
        let msg = '요청 실패';
        if (data && data.detail) {
            const d = data.detail;
            msg = typeof d === 'string' ? d : (Array.isArray(d) && d[0] && d[0].msg) ? d[0].msg : JSON.stringify(d);
        }
        throw new Error(msg);
    }
    return data;
}

function _fmtKrw(n) {
    if (n == null || n === '') return '-';
    return Number(n).toLocaleString('ko-KR');
}

/** 서버/캐시 payload로 CFO Phase1 DOM 채우기 */
function applyCfoPhase1Payload(payload) {
    const bepData = payload.bepData || {};
    const surv = payload.surv || {};
    const rent = payload.rent || {};
    const persona = payload.persona || {};
    const walk = payload.walk || {};

    const bep = bepData.bep || {};
    const s = surv.survival || surv;
    const rr = rent.rent_risk || {};
    const p = persona.persona || persona;
    const wprop = (walk && walk.properties) || {};

    const staffBepEl = document.getElementById('cfo-staff-bep-box');
    if (staffBepEl) {
        staffBepEl.innerHTML = `
                <div style="font-size:12px;font-weight:800;color:#0f172a;margin-bottom:8px;">직원 수 기반 BEP <span style="color:#64748b;font-weight:600;">(${bepData.region_name || '상권'})</span></div>
                <p style="font-size:13px;line-height:1.65;color:#334155;margin-bottom:10px;font-weight:600;">${bep.headline || ''}</p>
                <table style="width:100%;font-size:11px;border-collapse:collapse;">
                    <tr><td style="color:#64748b;padding:3px 0;">월 고정비</td><td style="text-align:right;font-weight:800;">${_fmtKrw(bep.monthly_fixed_total_krw)}원</td></tr>
                    <tr><td style="color:#64748b;padding:3px 0;">객단가(추정)</td><td style="text-align:right;font-weight:800;">${_fmtKrw(bep.estimated_ticket_krw)}원</td></tr>
                    <tr><td style="color:#0f172a;padding:6px 0 0;font-weight:800;">BEP 월간</td><td style="text-align:right;font-weight:900;color:#4f46e5;">${_fmtKrw(bep.breakeven_monthly_patients)}명</td></tr>
                    <tr><td style="color:#0f172a;font-weight:800;">BEP 일평균</td><td style="text-align:right;font-weight:900;color:#4f46e5;">${bep.breakeven_daily_patients != null ? bep.breakeven_daily_patients : '-'}명</td></tr>
                </table>
                <p style="font-size:10px;color:#94a3b8;margin-top:8px;">엔진 ${bep.engine_version || ''}</p>`;
    }

    const survEl = document.getElementById('cfo-survival-box');
    if (survEl) {
        survEl.innerHTML = `
                <div style="font-size:12px;font-weight:800;color:#166534;margin-bottom:8px;">상권 생존 · 폐업률 추정</div>
                <p style="font-size:13px;line-height:1.65;color:#14532d;font-weight:600;">${s.comment || ''}</p>
                <div style="margin-top:8px;font-size:11px;color:#15803d;">등급 <strong>${s.safety_grade || '-'}</strong> · 연간 폐업률 추정 ${s.closure_rate_annual_pct ?? '-'}% · 평균 생존 ${s.avg_survival_years_est ?? '-'}년</div>`;
    }

    const rentEl = document.getElementById('cfo-rent-risk-box');
    if (rentEl) {
        rentEl.innerHTML = `
                <div style="font-size:12px;font-weight:800;color:#92400e;margin-bottom:8px;">임대 · 젠트리피케이션 리스크</div>
                <p style="font-size:13px;line-height:1.65;color:#78350f;font-weight:600;">${rr.cfo_hint || ''}</p>
                <div style="margin-top:8px;font-size:11px;">추정 연간 임대 상승률 <strong>${rr.estimated_rent_yoy_pct ?? '-'}%</strong> · 등급 <strong>${rr.risk_level || '-'}</strong> (${rr.risk_label_ko || ''})</div>
                <div style="font-size:10px;color:#94a3b8;margin-top:6px;">평당 임대 추정 ${_fmtKrw(rent.estimated_rent_per_pyeong)}원</div>`;
    }

    const personaEl = document.getElementById('cfo-persona-box');
    if (personaEl) {
        const sc = p.scores || {};
        personaEl.innerHTML = `
                <div style="font-size:12px;font-weight:800;color:#6b21a8;margin-bottom:8px;">과목별 페르소나 적합도</div>
                <p style="font-size:13px;line-height:1.65;color:#4c1d95;font-weight:600;margin-bottom:8px;">${p.narrative || ''}</p>
                <div style="font-size:11px;color:#7e22ce;">직장인 ${sc.office_worker_affinity ?? '-'} · 가족·영유아 ${sc.family_children_affinity ?? '-'} · 고령·거주 ${sc.elderly_residential_affinity ?? '-'}</div>`;
    }

    const walkEl = document.getElementById('cfo-walkable-box');
    if (walkEl) {
        const rm = wprop.radius_meters_approx != null ? Math.round(wprop.radius_meters_approx) : '-';
        walkEl.innerHTML = `
                <div style="font-size:12px;font-weight:800;color:#1e40af;margin-bottom:6px;">도보 유효 범위 (V1 근사)</div>
                <p style="font-size:13px;line-height:1.6;color:#1e3a8a;">도보 약 <strong>${wprop.walk_minutes != null ? wprop.walk_minutes : 10}분</strong> 기준 추정 반경 약 <strong>${rm}m</strong>. 실제 도보 isochrone은 카카오/TMAP 연동 시 정밀화됩니다.</p>`;
    }
}

/** Phase 1 AI CFO 확장 패널 — 리포트 모달에서 병렬 호출 */
async function renderCfoPhase1Extended(rec) {
    const loading = document.getElementById('cfo-phase1-loading');
    const content = document.getElementById('cfo-phase1-content');
    const errEl = document.getElementById('cfo-phase1-error');
    if (!loading || !content) return;
    if (errEl) { errEl.style.display = 'none'; errEl.textContent = ''; }

    const lat = rec.lat;
    const lng = rec.lng;
    const dept = (typeof selectedDeptName === 'string' && selectedDeptName) ? selectedDeptName : (rec.dept_name || '한의원');

    if (lat == null || lng == null) {
        loading.style.display = 'none';
        content.style.display = 'none';
        if (errEl) {
            errEl.textContent = '좌표 정보가 없어 AI CFO 확장 분석을 표시할 수 없습니다.';
            errEl.style.display = 'block';
        }
        return;
    }

    if (rec.cfo_phase1 && rec.cfo_phase1.bepData) {
        loading.style.display = 'none';
        content.style.display = 'grid';
        applyCfoPhase1Payload({
            bepData: rec.cfo_phase1.bepData,
            surv: rec.cfo_phase1.surv,
            rent: rec.cfo_phase1.rent,
            persona: rec.cfo_phase1.persona,
            walk: rec.cfo_phase1.walk
        });
        return;
    }

    loading.style.display = 'block';
    content.style.display = 'none';

    const base = BLUEDOT_API_BASE;
    try {
        const [bepData, surv, rent, persona, walk] = await Promise.all([
            apiJson(base + '/api/cfo/bep-simulate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    lat, lng, dept,
                    doctors: 1,
                    staff: 4,
                    clinic_pyeong: 35,
                    radius_km: 3,
                    variable_cost_ratio: 0.12
                })
            }),
            apiJson(base + '/api/cfo/survival?lat=' + encodeURIComponent(lat) + '&lng=' + encodeURIComponent(lng) + '&dept=' + encodeURIComponent(dept)),
            apiJson(base + '/api/cfo/rent-risk?lat=' + encodeURIComponent(lat) + '&lng=' + encodeURIComponent(lng) + '&radius_km=3'),
            apiJson(base + '/api/targeting/persona-score', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lat, lng, dept, radius_km: 3 })
            }),
            apiJson(base + '/api/geo/walkable-polygon?lat=' + encodeURIComponent(lat) + '&lng=' + encodeURIComponent(lng) + '&minutes=10')
        ]);

        applyCfoPhase1Payload({ bepData, surv, rent, persona, walk });
        loading.style.display = 'none';
        content.style.display = 'grid';

        const pack = { bepData, surv, rent, persona, walk, fetched_at: new Date().toISOString() };
        if (typeof lastOpenedReportData === 'object' && lastOpenedReportData) {
            lastOpenedReportData.cfo_phase1 = pack;
        }
        if (typeof rec === 'object' && rec) rec.cfo_phase1 = pack;
    } catch (e) {
        loading.style.display = 'none';
        if (errEl) {
            errEl.textContent = 'AI CFO 확장: ' + (e.message || '백엔드 연결 실패');
            errEl.style.display = 'block';
        }
    }
}

// [1] 글로벌 변수 세팅
let map; 
let selectedDeptName = null; 
let selectedDeptId = null;   

let mapObjects = [];            
let currentAnalysisData = [];   
let currentHospitals = []; // 주변 병원 데이터를 차트 그리기 위해 저장
let infoWindows = [];
let hoverMarkers = []; // 호버 시 로드한 경쟁기관 마커 (별도 관리)
let hoverMapListener = null; // 지도 mousemove 리스너 제거용
let hoverFetchTimer = null;
let hoverFetchInProgress = false;
let lastOpenedReportData = null; 

// 차트 객체를 저장할 변수 (새로 열 때마다 기존 차트를 지우기 위함)
let demoChart = null;
let revChart = null;
let radarChart = null;
let timeMatrixChart = null;

const DEPT_ICONS = {
    1:'🩺', 2:'✨', 3:'🦷', 4:'👁️', 5:'🦴', 6:'🌿', 
    7:'🧸', 8:'👂', 9:'🤰', 10:'🧠', 11:'💊', 12:'🐶'
};

// [2] 카카오 지도 초기화 
function initMap() {
    if (typeof kakao === 'undefined' || !kakao.maps) {
        console.error('카카오맵 API를 불러오지 못했습니다. (JavaScript 키·플랫폼 Web 도메인 등록 확인, REST 키는 지도용 아님)');
        return;
    }
    kakao.maps.load(function() {
        const mapContainer = document.getElementById('map');
        const defaultLatLng = new kakao.maps.LatLng(35.1631, 129.1636); 
        map = new kakao.maps.Map(mapContainer, { center: defaultLatLng, level: 6 });

        kakao.maps.event.addListener(map, 'idle', function() {
            const centerCoords = map.getCenter();
            searchAddrFromCoords(centerCoords, displayCenterInfo);
        });

        kakao.maps.event.addListener(map, 'click', function() {
            infoWindows.forEach(iw => iw.setMap(null));
        });
    });
}

function searchAddrFromCoords(coords, callback) {
    const geocoder = new kakao.maps.services.Geocoder();
    geocoder.coord2RegionCode(coords.getLng(), coords.getLat(), callback);
}

function displayCenterInfo(result, status) {
    if (status === kakao.maps.services.Status.OK) {
        for(let i = 0; i < result.length; i++) {
            if (result[i].region_type === 'H') {
                document.getElementById('current-region-text').innerText = result[i].address_name;
                break;
            }
        }
    }
}

window.addEventListener('load', () => {
    loadKakaoMapsScript()
        .then(() => initMap())
        .catch((e) => {
            console.error(e && e.message ? e.message : e);
            console.error('카카오 개발자 콘솔: 앱 키는 "JavaScript 키"를 쓰고, [플랫폼] Web에 http://127.0.0.1:8000 (또는 사용 중인 URL)을 등록하세요.');
        });
    setTimeout(() => {
        const splash = document.getElementById('splash-screen');
        if (splash) {
            splash.style.opacity = '0';
            setTimeout(() => splash.style.visibility = 'hidden', 600);
        }
    }, 2000);
});

function zoomIn() { if (map) map.setLevel(map.getLevel() - 1); }
function zoomOut() { if (map) map.setLevel(map.getLevel() + 1); }
function moveToMyLocation() {
    if (!navigator.geolocation) { alert("위치 정보를 지원하지 않습니다."); return; }
    if (map) {
        navigator.geolocation.getCurrentPosition(
            function(pos) { map.panTo(new kakao.maps.LatLng(pos.coords.latitude, pos.coords.longitude)); },
            function(err) { alert("현재 위치를 가져올 수 없습니다. GPS 권한을 확인해주세요."); }
        );
    }
}

function openRegionModal() {
    const modal = document.getElementById('region-modal');
    if (!modal) return;
    modal.style.display = 'flex';
    const inp = document.getElementById('region-search-input');
    if (inp) setTimeout(() => inp.focus(), 100);
}

function closeRegionModal() {
    const modal = document.getElementById('region-modal');
    if (modal) modal.style.display = 'none';
}

function executeRegionSearch() {
    if (typeof kakao === 'undefined' || !kakao.maps || !kakao.maps.services) {
        alert('카카오 지도 API가 준비되지 않았습니다. 페이지를 새로고침하거나 JavaScript 키·Web 도메인 등록을 확인하세요.');
        return;
    }
    const inputEl = document.getElementById('region-search-input');
    const container = document.getElementById('region-search-results');
    if (!inputEl || !container) return;
    const keyword = inputEl.value;
    if (!keyword.trim()) return;

    const geocoder = new kakao.maps.services.Geocoder();

    geocoder.addressSearch(keyword, function(data, status) {
        if (status === kakao.maps.services.Status.OK) {
            let html = '';
            data.forEach(place => {
                html += `<div class="search-result-item" onclick="changeRegion('${place.address_name}', ${place.y}, ${place.x})">
                            <div><div class="search-result-title">${place.address_name}</div><div class="search-result-addr">행정구역 탐색</div></div>
                         </div>`;
            });
            container.innerHTML = html;
        } else {
            const ps = new kakao.maps.services.Places();
            ps.keywordSearch(keyword, function(pData, pStatus) {
                if (pStatus === kakao.maps.services.Status.OK) {
                    let html = '';
                    pData.forEach(place => {
                        html += `<div class="search-result-item" onclick="changeRegion('${place.place_name}', ${place.y}, ${place.x})">
                                    <div><div class="search-result-title">${place.place_name}</div><div class="search-result-addr">${place.address_name}</div></div>
                                 </div>`;
                    });
                    container.innerHTML = html;
                } else {
                    container.innerHTML = `<div style="text-align:center; padding:40px 20px; color:var(--text-sub);">검색 결과가 없습니다.</div>`;
                }
            });
        }
    });
}

function changeRegion(regionName, lat, lng) {
    document.getElementById('current-region-text').innerText = regionName;
    if (map) { map.panTo(new kakao.maps.LatLng(lat, lng)); map.setLevel(6); }
    closeRegionModal();
}

function selectDepartment(deptName, deptId) {
    document.querySelectorAll('.dept-item').forEach(item => item.classList.remove('selected'));
    const selectedItem = document.querySelector(`.dept-${deptId}`);
    if (selectedItem) selectedItem.classList.add('selected');

    selectedDeptName = deptName;
    selectedDeptId = deptId;
    
    document.getElementById('analysis-title-text').innerText = `${deptName} 거시 상권 분석 준비완료`;
    document.getElementById('analyze-submit-btn').style.display = 'block';
    updateAnalyzeButtonCredits();
}

const CREDITS_KEY = "bluedot_analysis_credits";

function getCredits() {
    const v = localStorage.getItem(CREDITS_KEY);
    return v ? parseInt(v, 10) : 0;
}

function setCredits(n) {
    localStorage.setItem(CREDITS_KEY, String(Math.max(0, n)));
    updateAnalyzeButtonCredits();
}

function useCredit() {
    const c = getCredits();
    if (c > 0) setCredits(c - 1);
    return c > 0;
}

function addCredits(n) {
    setCredits(getCredits() + n);
}

function updateAnalyzeButtonCredits() {
    const btn = document.getElementById('analyze-submit-btn');
    if (!btn) return;
    const c = getCredits();
    btn.innerText = c > 0 ? `거시 상권 정밀 분석 (남은 ${c}회)` : "거시 상권 정밀 분석 (결제)";
}

function syncCreditsFromServer(serverCredits) {
    if (typeof serverCredits === 'number') setCredits(serverCredits);
}

async function triggerPaymentFlow() {
    if (!selectedDeptName) { alert("분석할 대상을 먼저 선택해주세요."); return; }
    if (typeof window !== 'undefined' && window.BLUEDOT_SKIP_CREDIT_CHECK) {
        startAnalysis();
        return;
    }
    const token = typeof getToken === 'function' ? getToken() : null;
    let credits = getCredits();
    if (token) {
        try {
            const res = await fetchCredits();
            credits = typeof res === 'number' ? res : (res || 0);
        } catch (e) { credits = getCredits(); }
    }
    if (credits > 0) {
        if (confirm(`1회 분석을 사용합니다. (남은 ${credits}회)\n진행할까요?`)) {
            if (token) {
                try {
                    await useCreditViaApi();
                    if (typeof fetchMe === 'function') fetchMe().then(me => me.logged_in && me.user && typeof onAuthStateChange === 'function' && onAuthStateChange(me.user));
                } catch (e) {
                    alert("크레딧 처리 실패. 로컬 분석으로 진행합니다.");
                    useCredit();
                }
            } else {
                useCredit();
            }
            startAnalysis();
        }
        return;
    }
    document.getElementById('payment-selected-plan').value = '';
    document.querySelectorAll('.payment-plan-card').forEach(el => {
        el.style.borderColor = '#e2e8f0';
        el.style.background = '';
    });
    document.getElementById('payment-submit-btn').disabled = true;
    document.getElementById('payment-modal').style.display = 'flex';
}

function closePaymentModal() { 
    document.getElementById('payment-modal').style.display = 'none'; 
}

function selectPaymentPlan(plan) {
    document.getElementById('payment-selected-plan').value = plan;
    document.getElementById('payment-submit-btn').disabled = false;
    document.querySelectorAll('.payment-plan-card').forEach(el => {
        if (el.dataset.plan === plan) {
            el.style.borderColor = '#10B981';
            el.style.background = '#f0fdf4';
        } else {
            el.style.borderColor = '#e2e8f0';
            el.style.background = '';
        }
    });
}

async function processPayment() { 
    const plan = document.getElementById('payment-selected-plan').value;
    if (!plan) { alert("결제 옵션을 선택해주세요."); return; }

    const amount = plan === '5' ? 30000 : 7000;
    const adds = plan === '5' ? 5 : 1;
    const token = typeof getToken === 'function' ? getToken() : null;

    if (token) {
        try {
            await addCreditsViaApi(plan, amount, adds);
            if (typeof fetchMe === 'function') fetchMe().then(me => { if (me.logged_in && me.user && typeof onAuthStateChange === 'function') onAuthStateChange(me.user); });
        } catch (e) {
            alert("결제 처리 실패: " + (e.message || (e.detail || "다시 시도해 주세요.")));
            return;
        }
        try {
            await useCreditViaApi();
        } catch (e) { /* ignore */ }
    } else {
        addCredits(adds);
        useCredit();
    }
    closePaymentModal();
    alert(`테스트 모드: 결제가 완료되었습니다. (${amount.toLocaleString()}원)\n분석 ${adds}회가 반영되었습니다.`);
    startAnalysis();

    /* =========================================================================
    🚨 [실전용] 포트원 연동 시 amount를 plan에 따라 7000 또는 30000으로 설정하세요.
    =========================================================================
    var IMP = window.IMP; 
    IMP.init("본인의_가맹점_식별코드_입력");
    IMP.request_pay({
        pg: "html5_inicis",
        pay_method: "card",
        merchant_uid: "bluedot_order_" + new Date().getTime(),
        name: plan === '5' ? `BLUEDOT 상권 분석 5회권` : `BLUEDOT 상권 분석 1회`,
        amount: amount,
        buyer_email: "doctor@example.com",
        buyer_name: "원장님",
        buyer_tel: "010-1234-5678"
    }, function (rsp) { 
        if (rsp.success) {
            addCredits(adds);
            useCredit();
            startAnalysis();
        } else {
            alert("결제에 실패하였습니다. 사유: " + rsp.error_msg);
        }
    });
    */
}
function renderMapAndResults(data, searchRadius) {
    teardownHoverHospitalFetch();
    const recommendations = Array.isArray(data && data.recommendations) ? data.recommendations : [];
    const hospitals = Array.isArray(data && data.hospitals) ? data.hospitals : [];
    currentAnalysisData = recommendations;
    currentHospitals = hospitals;

    if (recommendations.length === 0) {
        const msg = (data && data.message) ? data.message : '분석 결과(추천 노드)가 없습니다. 반경을 넓히거나 다른 지역을 선택해 주세요.';
        alert(msg);
        document.getElementById('analysis-panel').classList.remove('hidden-mode');
        const container = document.getElementById('results-cards-container');
        if (container) container.innerHTML = '';
        return;
    }

    // BLUEDOT 로고 스타일: 어두운 파란 안쪽 원 + 반투명 밝은 파란 바깥 원
    const BLUEDOT_HTML = '<div class="bluedot-dot" style="width:26px;height:26px;border-radius:50%;background:rgba(59,130,246,0.4);display:flex;align-items:center;justify-content:center;box-shadow:0 2px 6px rgba(0,0,0,0.2);border:2px solid white;"><div style="width:12px;height:12px;border-radius:50%;background:#0f172a;"></div></div>';

    if (hospitals.length > 0) {
        hospitals.forEach(hospital => {
            const hPos = new kakao.maps.LatLng(hospital.lat, hospital.lng);
            const markerContent = `<div style="cursor:pointer;display:flex;align-items:center;justify-content:center;">${BLUEDOT_HTML}</div>`;
            const customOverlay = new kakao.maps.CustomOverlay({
                position: hPos, content: markerContent, yAnchor: 1 
            });
            customOverlay.setMap(map); mapObjects.push(customOverlay);

            const displayName = hospital.display_name || hospital.name;
            const factTags = hospital.fact_tags || [];
            const docs = hospital.doctors || 1;
            const staff = hospital.staff_count;
            const hours = hospital.hours || "🕒 일반 진료시간";
            const rev = hospital.estimated_revenue || "";

            const tagsHtml = factTags.length > 0
                ? `<div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px;">${factTags.map(t => `<span style="background:#e0e7ff; color:#4338ca; font-size:11px; font-weight:700; padding:4px 8px; border-radius:6px;">${t}</span>`).join('')}</div>`
                : '';

            const infoContent = `
                <div style="padding:16px; border-radius:12px; background:white; border:1px solid #e2e8f0; box-shadow:0 10px 25px rgba(0,0,0,0.2); min-width:260px; font-family:'Pretendard', sans-serif; cursor:default;">
                    <div style="font-weight:900; font-size:16px; color:#0f172a; margin-bottom:8px;">${displayName}</div>
                    ${tagsHtml}
                    <div style="font-size:12px; color:#64748b; margin-bottom:6px;">${hours.replace('🕒 ', '')}${staff != null ? ` · 직원 ${staff}명` : ''}</div>
                    ${rev ? `<div style="font-size:13px; color:#10B981; font-weight:800;">${rev}</div>` : ''}
                    <div style="margin-top:10px; font-size:10px; color:#94a3b8;">※ 심평원·공공데이터 기반</div>
                </div>
            `;

            const infoOverlay = new kakao.maps.CustomOverlay({
                content: infoContent, position: hPos, yAnchor: 1.3, zIndex: 300
            });

            let isOpened = false;
            let checkExist = setInterval(function() {
                const markerElement = customOverlay.a;
                if (markerElement) {
                    clearInterval(checkExist);
                    markerElement.addEventListener('click', function() {
                        infoWindows.forEach(iw => iw.setMap(null));
                        if (!isOpened) {
                            infoOverlay.setMap(map); isOpened = true; infoWindows.push(infoOverlay);
                            map.panTo(hPos); 
                        } else {
                            infoOverlay.setMap(null); isOpened = false;
                        }
                    });
                }
            }, 100);
        });
    }

    const container = document.getElementById('results-cards-container');
    let cardsHtml = '';
    
    currentAnalysisData.forEach((rec, index) => {
        const circle = new kakao.maps.Circle({
            center: new kakao.maps.LatLng(rec.lat, rec.lng), radius: searchRadius * 1000, 
            strokeWeight: 2, strokeColor: rec.color, strokeOpacity: 0.8, fillColor: rec.color, fillOpacity: 0.05 
        });
        circle.setMap(map); mapObjects.push(circle);

        const badgeOverlay = new kakao.maps.CustomOverlay({
            position: new kakao.maps.LatLng(rec.lat, rec.lng),
            content: `<div class="ranking-badge" style="background:${rec.color};" onclick="openReportModal(${index})"><span class="rank-num">${rec.rank}</span><span class="rank-label">Rank</span></div>`,
            yAnchor: 0.5, zIndex: 100
        });
        badgeOverlay.setMap(map); mapObjects.push(badgeOverlay);

        // 🚀 [추가] 부동산 임대료 및 상권 소비력 뼈대 로직
        let baseRent = Math.floor((rec.score_val || 6.0) * 1.5) * 10000; 
        if (baseRent < 50000) baseRent = 50000;
        let rentText = `평당 약 ${(baseRent / 10000).toFixed(1)}만원`;

        let spending = Math.floor(Math.random() * 3 + 4) * 10000; 
        let spendingText = `건당 약 ${(spending / 10000).toFixed(1)}만원`;

        cardsHtml += `
        <div class="result-card" style="border-top: 4px solid ${rec.color};" onclick="panMapToNode(${rec.lat}, ${rec.lng})">
            <div class="rc-top">
                <div class="rc-rank" style="background:${rec.color};">${rec.rank}</div>
                <div class="rc-title" style="font-size:16px;">${rec.name}</div>
            </div>
            <div class="rc-info-row">
                <span class="rc-label">AI 추천 점수</span>
                <span class="rc-value" style="color:${rec.color}; font-size:18px;">${rec.score}</span>
            </div>
            <div class="rc-info-row">
                <span class="rc-label">경쟁 강도</span>
                <span class="rc-value" style="color:var(--text-main);">${rec.comp_text}</span>
            </div>
            <div class="rc-info-row">
                <span class="rc-label">배후 인구</span>
                <span class="rc-value">${rec.pop_text}</span>
            </div>

            <div class="rc-premium-box">
                <div class="premium-item">
                    <span class="premium-label">🏢 예상 상가 임대료 (1층)</span>
                    <span class="premium-value">${rentText}</span>
                </div>
                <div class="premium-item">
                    <span class="premium-label">💳 타겟 월평균 의료소비액</span>
                    <span class="premium-value">${spendingText}</span>
                </div>
            </div>

            <button class="rc-btn" onclick="openReportModal(${index}); event.stopPropagation();">정밀 컨설팅 리포트 (수식공개)</button>
        </div>`;
    });
    
    container.innerHTML = cardsHtml;

    if (currentAnalysisData.length > 0) {
        panMapToNode(currentAnalysisData[0].lat, currentAnalysisData[0].lng);
    }

    document.getElementById('analysis-panel').classList.add('hidden-mode');
    document.getElementById('results-panel').style.display = 'block';
    setupHoverHospitalFetch();
}

window.panMapToNode = function(lat, lng) {
    if(!map) return;
    let offset = 0.015; 
    if(map.getLevel() <= 4) offset = 0.005;
    else if(map.getLevel() >= 7) offset = 0.03;
    const moveLatLon = new kakao.maps.LatLng(lat - offset, lng);
    map.panTo(moveLatLon);
};

async function startAnalysis() {
    if (!map) return;
    const center = map.getCenter();
    const radius = document.getElementById('analysis-radius').value;

    mapObjects.forEach(obj => obj.setMap(null)); mapObjects = [];
    infoWindows.forEach(iw => iw.setMap(null)); infoWindows = [];
    
    const submitBtn = document.getElementById('analyze-submit-btn');
    submitBtn.innerText = "데이터 수집 및 분석 중..."; submitBtn.style.pointerEvents = "none";
    document.getElementById('loading-overlay').style.display = 'flex';

    const deptQ = encodeURIComponent(selectedDeptName || '한의원');
    const walkMinutes = 10;
    const url = `${BLUEDOT_API_BASE}/api/hospitals?lat=${center.getLat()}&lng=${center.getLng()}&dept=${deptQ}&radius=${radius}&walk_minutes=${walkMinutes}`;
    try {
        const response = await fetchWithTimeout(url, { timeout: BLUEDOT_ANALYZE_TIMEOUT_MS });
        const data = await parseJsonSafe(response);
        if (!response.ok) {
            alert(bluedotApiErrorMessage(response, data));
            document.getElementById('analysis-panel').classList.remove('hidden-mode');
            return;
        }
        if (data.status === 'error') {
            alert(data.message);
            document.getElementById('analysis-panel').classList.remove('hidden-mode');
            return;
        }
        renderMapAndResults(data, radius);
    } catch (error) {
        alert(bluedotNetworkErrorMessage(error));
    } finally {
        submitBtn.innerText = "거시 상권 정밀 분석 (결제)"; submitBtn.style.pointerEvents = "auto";
        document.getElementById('loading-overlay').style.display = 'none';
    }
}

function bluedotNetworkErrorMessage(error) {
    const m = (error && error.message) ? String(error.message) : '';
    if (m.includes('Failed to fetch') || m.includes('NetworkError') || m.includes('Network request failed')) {
        return '백엔드에 연결할 수 없습니다.\n\n터미널에서 프로젝트 폴더로 이동한 뒤:\n'
            + 'uvicorn main:app --reload --host 127.0.0.1 --port 8000\n\n'
            + '실행 후 브라우저에서 ' + BLUEDOT_API_BASE + '/api/health 를 열어 {"ok":true} 가 나오는지 확인하세요.';
    }
    return m || "🚨 파이썬 백엔드 서버에 연결할 수 없습니다.";
}

async function submitAISearch() {
    const promptInput = document.getElementById('ai-search-input').value;
    if (!promptInput.trim()) { alert("원하시는 입지 조건을 입력해주세요."); return; }
    if (!map) return;

    mapObjects.forEach(obj => obj.setMap(null)); mapObjects = [];
    infoWindows.forEach(iw => iw.setMap(null)); infoWindows = [];
    
    const submitBtn = document.querySelector('.ai-submit-btn');
    submitBtn.style.opacity = "0.5"; submitBtn.style.pointerEvents = "none";
    document.getElementById('analysis-panel').classList.add('hidden-mode');

    const center = map.getCenter();
    let radius = document.getElementById('analysis-radius') ? document.getElementById('analysis-radius').value : 3;
    
    selectedDeptName = "한의원";
    selectedDeptId = 6; 
    document.getElementById('loading-overlay').style.display = 'flex';

    const walkMinutes = 10;
    const url = `${BLUEDOT_API_BASE}/api/ai-search?lat=${center.getLat()}&lng=${center.getLng()}&prompt=${encodeURIComponent(promptInput)}&radius=${radius}&walk_minutes=${walkMinutes}`;
    try {
        const response = await fetchWithTimeout(url, { timeout: BLUEDOT_ANALYZE_TIMEOUT_MS });
        const data = await parseJsonSafe(response);
        if (!response.ok) {
            alert(bluedotApiErrorMessage(response, data));
            document.getElementById('analysis-panel').classList.remove('hidden-mode');
            return;
        }
        if (data.status === 'error') {
            alert(data.message);
            document.getElementById('analysis-panel').classList.remove('hidden-mode');
            return;
        }
        if (data.map_center && data.region_filtered && map) {
            map.panTo(new kakao.maps.LatLng(data.map_center.lat, data.map_center.lng));
            map.setLevel(8);
            if (data.region_name) {
                document.getElementById('current-region-text').innerText = data.region_name + ' 검색 결과';
            }
        }
        renderMapAndResults(data, radius);
    } catch (error) {
        alert(bluedotNetworkErrorMessage(error));
        document.getElementById('analysis-panel').classList.remove('hidden-mode');
    } finally {
        submitBtn.style.opacity = "1"; submitBtn.style.pointerEvents = "auto";
        document.getElementById('loading-overlay').style.display = 'none';
    }
}

function clearHoverMarkers() {
    hoverMarkers.forEach(obj => obj.setMap && obj.setMap(null));
    hoverMarkers = [];
}

function renderHoverHospitals(hospitals) {
    if (!map || !hospitals || hospitals.length === 0) return;
    const BLUEDOT_HTML = '<div style="width:24px;height:24px;border-radius:50%;background:rgba(59,130,246,0.4);display:flex;align-items:center;justify-content:center;box-shadow:0 2px 6px rgba(0,0,0,0.2);border:2px solid white;"><div style="width:10px;height:10px;border-radius:50%;background:#0f172a;"></div></div>';
    hospitals.forEach(h => {
        const hPos = new kakao.maps.LatLng(h.lat, h.lng);
        const markerContent = `<div style="cursor:pointer;display:flex;align-items:center;justify-content:center;">${BLUEDOT_HTML}</div>`;
        const overlay = new kakao.maps.CustomOverlay({ position: hPos, content: markerContent, yAnchor: 1 });
        overlay.setMap(map);
        hoverMarkers.push(overlay);
        const displayName = h.display_name || h.name || '의료기관';
        const tags = (h.fact_tags || []).map(t => `<span style="background:#e0e7ff; color:#4338ca; font-size:10px; padding:2px 6px; border-radius:4px; margin-right:4px;">${t}</span>`).join('');
        const infoContent = `<div style="padding:12px; border-radius:10px; background:white; border:1px solid #e2e8f0; box-shadow:0 8px 20px rgba(0,0,0,0.15); min-width:220px; font-size:13px;">
            <div style="font-weight:800; color:#0f172a; margin-bottom:6px;">${displayName}</div>
            ${tags ? `<div style="margin-bottom:6px;">${tags}</div>` : ''}
            <div style="font-size:11px; color:#64748b;">${h.hours || ''} ${h.staff_count != null ? '· 직원 ' + h.staff_count + '명' : ''}</div>
            ${h.estimated_revenue ? `<div style="color:#10B981; font-weight:700; margin-top:4px;">${h.estimated_revenue}</div>` : ''}
        </div>`;
        const infoOverlay = new kakao.maps.CustomOverlay({ content: infoContent, position: hPos, yAnchor: 1.3, zIndex: 250 });
        let isOpened = false;
        setTimeout(() => {
            const el = overlay.a;
            if (el) el.addEventListener('click', () => {
                infoWindows.forEach(iw => iw.setMap(null));
                if (!isOpened) { infoOverlay.setMap(map); isOpened = true; infoWindows.push(infoOverlay); map.panTo(hPos); }
                else { infoOverlay.setMap(null); isOpened = false; }
            });
        }, 100);
    });
}

async function fetchHoverHospitals(lat, lng) {
    if (hoverFetchInProgress || !selectedDeptName) return;
    hoverFetchInProgress = true;
    const toast = document.getElementById('hover-loading-toast');
    if (toast) { toast.textContent = '경쟁기관 조회 중...'; toast.style.display = 'block'; }
    clearHoverMarkers();
    try {
        const res = await fetch(`${BLUEDOT_API_BASE}/api/hospitals-nearby?lat=${lat}&lng=${lng}&dept=${encodeURIComponent(selectedDeptName)}&radius=1`);
        const data = await res.json();
        const hospitals = data.hospitals || [];
        renderHoverHospitals(hospitals);
        if (toast) {
            toast.textContent = hospitals.length > 0 ? `이 지역 경쟁기관 ${hospitals.length}개소` : '해당 반경 내 경쟁기관 없음';
            setTimeout(() => { toast.style.display = 'none'; }, 2000);
        }
    } catch (e) {
        if (toast) { toast.textContent = '조회 실패'; toast.style.display = 'none'; }
    } finally {
        hoverFetchInProgress = false;
    }
}

function setupHoverHospitalFetch() {
    if (!map || hoverMapListener) return;
    let pendingLat = null, pendingLng = null;
    let lastFetchedLat = null, lastFetchedLng = null;
    const DEBOUNCE_MS = 450;
    const MIN_MOVE = 0.003; // 약 300m 이상 이동 시에만 새로 조회
    hoverMapListener = function(mouseEvent) {
        const lat = mouseEvent.latLng.getLat();
        const lng = mouseEvent.latLng.getLng();
        pendingLat = lat; pendingLng = lng;
        if (hoverFetchTimer) clearTimeout(hoverFetchTimer);
        hoverFetchTimer = setTimeout(() => {
            const needFetch = lastFetchedLat == null || lastFetchedLng == null ||
                Math.abs(pendingLat - lastFetchedLat) >= MIN_MOVE || Math.abs(pendingLng - lastFetchedLng) >= MIN_MOVE;
            if (needFetch) {
                lastFetchedLat = pendingLat;
                lastFetchedLng = pendingLng;
                fetchHoverHospitals(pendingLat, pendingLng);
            }
        }, DEBOUNCE_MS);
    };
    kakao.maps.event.addListener(map, 'mousemove', hoverMapListener);
}

function teardownHoverHospitalFetch() {
    if (hoverMapListener && map) {
        kakao.maps.event.removeListener(map, 'mousemove', hoverMapListener);
        hoverMapListener = null;
    }
    if (hoverFetchTimer) { clearTimeout(hoverFetchTimer); hoverFetchTimer = null; }
    clearHoverMarkers();
    const toast = document.getElementById('hover-loading-toast');
    if (toast) toast.style.display = 'none';
}

function closeResults() {
    document.getElementById('results-panel').style.display = 'none';
    document.getElementById('analysis-panel').classList.remove('hidden-mode');
    mapObjects.forEach(obj => obj.setMap(null)); mapObjects = [];
    infoWindows.forEach(iw => iw.setMap(null)); infoWindows = [];
    teardownHoverHospitalFetch();
}

// =========================================================
// [11] 🚨 화이트박스 리포트 & Chart.js 렌더링 로직 연동
// =========================================================
/** `**text**` → <strong>, 백틱은 코드 스타일 */
function formatInsightNarrative(text) {
    if (!text) return '';
    let s = String(text).replace(/`([^`]+)`/g, '<code style="font-size:12px;background:#e0f2fe;padding:2px 6px;border-radius:4px;">$1</code>');
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    // 문장 끝의 출처 표기 제거: "(출처: ...)" / "출처: ..."
    s = s.replace(/\s*\(출처:[^)]+\)\s*$/g, '');
    s = s.replace(/\s*출처:\s*[^<\n]+$/g, '');
    return s;
}

function renderCarInsuranceBanner(rec) {
    const wrap = document.getElementById('report-car-insurance-banner');
    const el = document.getElementById('report-car-insurance-text');
    if (!wrap || !el) return;
    const ci = rec.car_insurance_insight;
    if (!ci || !ci.narrative) {
        wrap.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    wrap.style.display = 'block';
    el.innerHTML = formatInsightNarrative(ci.narrative);
    wrap.style.borderColor = ci.ok ? '#0ea5e9' : '#fbbf24';
    wrap.style.background = ci.ok
        ? 'linear-gradient(135deg, #ecfeff 0%, #f0f9ff 100%)'
        : 'linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%)';
}

function renderPhase2Status(rec) {
    const wrap = document.getElementById('report-phase2-badge');
    const chip = document.getElementById('report-phase2-chip');
    const text = document.getElementById('report-phase2-text');
    const persona = document.getElementById('report-phase2-persona');
    const warn = document.getElementById('report-phase2-warning');
    if (!wrap || !chip || !text || !persona || !warn) return;

    const p2 = rec && rec.phase2 ? rec.phase2 : (rec && rec.phase2_meta ? rec.phase2_meta : null);
    const top = rec && rec._top_phase2 ? rec._top_phase2 : null;
    const meta = p2 || top || null;

    if (!meta) {
        wrap.style.display = 'none';
        text.innerHTML = '';
        persona.innerHTML = '';
        warn.style.display = 'none';
        warn.innerHTML = '';
        return;
    }

    const usedFallback = !!meta.used_fallback;
    const wm = meta.walk_minutes != null ? meta.walk_minutes : (meta.walk_minutes || 10);
    const filterApplied = meta.walk_filter_applied !== false;
    wrap.style.display = 'block';

    if (meta.postgis_skipped) {
        chip.style.background = '#eff6ff';
        chip.style.color = '#1d4ed8';
        chip.style.borderColor = '#bfdbfe';
        text.innerHTML = `도보 네트워크(DB) 미연결 — 반경 근사 분석 (${wm}분 기준)`;
    } else if (usedFallback) {
        chip.style.background = '#fffbeb';
        chip.style.color = '#b45309';
        chip.style.borderColor = '#fcd34d';
        text.innerHTML = `도보권 생성 실패 → 500m 반경 폴백 (${wm}분 요청)`;
    } else if (filterApplied) {
        chip.style.background = '#ecfdf5';
        chip.style.color = '#047857';
        chip.style.borderColor = '#a7f3d0';
        text.innerHTML = `실제 도보 폴리곤 기반 필터 적용 (${wm}분)`;
    } else {
        chip.style.background = '#eff6ff';
        chip.style.color = '#1d4ed8';
        chip.style.borderColor = '#bfdbfe';
        text.innerHTML = `도보권 메타 없음 (기본 반경 분석)`;
    }

    const pr = meta.persona || null;
    if (pr && pr.score != null) {
        persona.innerHTML = `페르소나 점수: <span style="color:#0f172a;">${pr.score}</span>/100`;
    } else {
        persona.innerHTML = '';
    }

    if (meta.warning) {
        warn.style.display = 'block';
        warn.innerHTML = formatInsightNarrative(meta.warning);
    } else {
        warn.style.display = 'none';
        warn.innerHTML = '';
    }
}

function renderBuildingAgingReport(rec) {
    const box = document.getElementById('building-aging-box');
    if (!box) return;
    const rep = rec && rec.building_aging_report ? rec.building_aging_report : null;
    if (!rep) {
        box.innerHTML = '<p style="margin:0;color:#94a3b8;font-size:13px;">건물 노후화 데이터를 불러오는 중입니다…</p>';
        return;
    }
    const sum = rep.summary || {};
    const lists = rep.lists || {};
    const avg = sum.avg_building_age_years;
    const ratio = sum.old_building_ratio_pct;
    const cnt = sum.competitor_count != null ? sum.competitor_count : '-';
    const apiFail = sum.api_unavailable_count != null ? sum.api_unavailable_count : 0;

    const pill = (label, val, bg, fg) =>
        `<span style="display:inline-block;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:900;background:${bg};color:${fg};border:1px solid rgba(0,0,0,0.06);">${label} ${val}</span>`;

    const listLine = (title, arr) => {
        const a = Array.isArray(arr) ? arr : [];
        const body = a.length ? a.slice(0, 8).join(', ') + (a.length > 8 ? ` 외 ${a.length - 8}곳` : '') : '없음';
        return `<div style="display:flex;justify-content:space-between;gap:12px;"><div style="font-weight:800;color:#0f172a;">${title}</div><div style="font-weight:700;color:#334155;text-align:right;flex:1;">${body}</div></div>`;
    };

    const top = `
        <div style="border:1px solid #e2e8f0;border-radius:12px;padding:14px;background:#fff;">
            <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;">
                ${pill('분석대상', `${cnt}곳`, '#eff6ff', '#1d4ed8')}
                ${pill('평균 연차', (avg != null ? `${avg}년` : '산출불가'), '#ecfdf5', '#047857')}
                ${pill('20년+ 비율', (ratio != null ? `${ratio}%` : '산출불가'), '#fff7ed', '#9a3412')}
                ${apiFail ? pill('API 실패', `${apiFail}건`, '#fef2f2', '#b91c1c') : ''}
            </div>
            <div style="font-size:13px;line-height:1.65;color:#334155;font-weight:800;">${formatInsightNarrative(rep.insight || '')}</div>
        </div>
    `;

    const details = `
        <div style="border:1px solid #e2e8f0;border-radius:12px;padding:14px;background:#fff;display:grid;gap:10px;">
            ${listLine('엘리베이터 없음', lists.no_elevator)}
            ${listLine('주차 취약(<5)', lists.low_parking_under_5)}
            ${listLine('데이터 없음', lists.data_missing)}
            <div style="margin-top:6px;font-size:12px;font-weight:700;color:#64748b;">
                ※ 데이터 없음이 많다면, 주소→법정동코드(bjdongCd) 매핑 데이터(주소 API/법정동 코드 테이블) 연동이 필요합니다.
            </div>
        </div>
    `;

    box.innerHTML = top + details;
}

async function fetchBuildingAgingReportForRec(rec) {
    if (!rec || rec._buildingAgingLoading) return;
    if (!rec.lat || !rec.lng) return;
    rec._buildingAgingLoading = true;
    try {
        const deptQ = encodeURIComponent(selectedDeptName || rec.dept_name || '한의원');
        const url = `${BLUEDOT_API_BASE}/api/building-aging?lat=${rec.lat}&lng=${rec.lng}&dept=${deptQ}&radius_km=1&limit=8`;
        const res = await fetchWithTimeout(url, { timeout: 12000 });
        const data = await parseJsonSafe(res);
        if (res.ok && data && data.status === 'success' && data.report) {
            rec.building_aging_report = data.report;
        } else {
            rec.building_aging_report = {
                summary: { competitor_count: (data && data.competitor_count) || 0 },
                lists: { no_elevator: [], low_parking_under_5: [], data_missing: [] },
                insight: '현재 공공데이터 서버 점검 중으로 확인 불가',
                details: [],
            };
        }
    } catch (e) {
        rec.building_aging_report = {
            summary: { competitor_count: 0 },
            lists: { no_elevator: [], low_parking_under_5: [], data_missing: [] },
            insight: '현재 공공데이터 서버 점검 중으로 확인 불가',
            details: [],
        };
    } finally {
        rec._buildingAgingLoading = false;
    }
}

function openReportModal(index) {
    const rec = currentAnalysisData[index]; 
    if (!rec) return;

    const modal = document.getElementById('report-modal');
    
    document.getElementById('report-dept-badge').innerText = `Top ${rec.rank} | ${selectedDeptName} 거시 타당성 분석`;
    document.getElementById('report-region-title').innerText = rec.name;
    document.getElementById('report-insight-text').innerHTML = `"${rec.insight}"`;

    const f = rec.formula || {};
    const parseScore = (str) => {
        if (!str) return { val: '+0.0', desc: '' };
        const parts = str.split(' ');
        return { val: parts[0], desc: parts[1] || '' };
    };

    const age = parseScore(f.age_score);
    const rev = parseScore(f.revenue_score);
    const anc = parseScore(f.anchor_score);

    const formulaHtml = `
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>기본 상권 베이스 점수 (하한선 보장)</span><span style="color:#10B981; font-weight:800;">+ 20.0 ~ 30.0점</span></div>
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>타겟 연령 최적화 가점 ${age.desc}</span><span style="color:#10B981; font-weight:800;">${age.val}점</span></div>
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>결제 소비력 및 배후 인구 가점 ${rev.desc}</span><span style="color:#10B981; font-weight:800;">${rev.val}점</span></div>
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>교통/유동인구 앵커 가점 ${anc.desc}</span><span style="color:#10B981; font-weight:800;">${anc.val}점</span></div>
        <div style="display:flex; justify-content:space-between; border-top:1px dashed #E5E7EB; padding-top:10px; margin-bottom:8px;"><span>상권 공실/폐업 기본 리스크 감점</span><span style="color:#EF4444; font-weight:800;">- ${f.risk_penalty || '20.0'}점</span></div>
        <div style="display:flex; justify-content:space-between;"><span>동일 과목 레드오션 밀집도 감점</span><span style="color:#EF4444; font-weight:800;">${f.comp_penalty}점</span></div>
    `;
    
    document.getElementById('report-formula-breakdown').innerHTML = formulaHtml;
    
    const finalScoreEl = document.getElementById('report-final-score');
    const scoreVal = rec.score_val ?? (rec.score ? String(rec.score).split('/')[0] : null) ?? '0';
    finalScoreEl.innerHTML = `${scoreVal}<span style="font-size: 18px;">점</span>`;
    finalScoreEl.style.color = rec.color;

    renderCarInsuranceBanner(rec);
    renderPhase2Status(rec);
    renderRadarAndBep(rec);
    renderTimeMatrix(rec);
    renderKillerInsights(rec);
    renderBuildingAgingReport(rec);
    fetchBuildingAgingReportForRec(rec).then(() => { renderBuildingAgingReport(rec); });
    renderCharts(rec);
    renderRiskWarnings(rec);
    renderCfoPhase1Extended(rec);

    lastOpenedReportData = { ...rec, region_name: rec.name, dept_name: selectedDeptName || "" };
    const saveBtn = document.getElementById('report-save-btn');
    if (saveBtn) saveBtn.style.display = (typeof getToken === 'function' && getToken()) ? 'inline-block' : 'none';
    modal.style.display = 'flex';
}

async function saveCurrentReport() {
    if (!lastOpenedReportData || typeof saveReportApi !== 'function') return;
    if (!getToken()) { alert("로그인 후 저장할 수 있습니다."); return; }
    try {
        await saveReportApi(lastOpenedReportData, lastOpenedReportData.region_name || "", lastOpenedReportData.dept_name || "");
        alert("리포트가 저장되었습니다. 마이페이지에서 확인할 수 있습니다.");
    } catch (e) {
        alert("저장 실패: " + (e.message || "다시 시도해 주세요."));
    }
}

window.renderReportFromData = function(data) {
    if (!data) return;
    const rec = data;
    lastOpenedReportData = rec;
    document.getElementById('report-dept-badge').innerText = `Top ${rec.rank || 1} | ${rec.dept_name || selectedDeptName || ''} 거시 타당성 분석`;
    document.getElementById('report-region-title').innerText = rec.region_name || rec.name || '';
    document.getElementById('report-insight-text').innerHTML = `"${rec.insight || ''}"`;
    const f = rec.formula || {};
    const parseScore = (str) => { if (!str) return { val: '+0.0', desc: '' }; const p = String(str).split(' '); return { val: p[0], desc: p[1] || '' }; };
    const formulaHtml = `
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>기본 상권 베이스 점수</span><span style="color:#10B981; font-weight:800;">+ 20.0 ~ 30.0점</span></div>
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>타겟 연령 최적화 ${parseScore(f.age_score).desc}</span><span style="color:#10B981; font-weight:800;">${parseScore(f.age_score).val}점</span></div>
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>결제 소비력 및 배후 인구 ${parseScore(f.revenue_score).desc}</span><span style="color:#10B981; font-weight:800;">${parseScore(f.revenue_score).val}점</span></div>
        <div style="display:flex; justify-content:space-between; margin-bottom:8px;"><span>교통/유동인구 앵커 ${parseScore(f.anchor_score).desc}</span><span style="color:#10B981; font-weight:800;">${parseScore(f.anchor_score).val}점</span></div>
        <div style="display:flex; justify-content:space-between; border-top:1px dashed #E5E7EB; padding-top:10px;"><span>리스크 감점</span><span style="color:#EF4444; font-weight:800;">- ${f.risk_penalty || '20.0'}점</span></div>
        <div style="display:flex; justify-content:space-between;"><span>경쟁 밀집도 감점</span><span style="color:#EF4444; font-weight:800;">${f.comp_penalty || '0'}점</span></div>
    `;
    document.getElementById('report-formula-breakdown').innerHTML = formulaHtml;
    const finalScoreEl = document.getElementById('report-final-score');
    finalScoreEl.innerHTML = `${rec.score_val || (rec.score && rec.score.split('/')[0]) || '0'}<span style="font-size: 18px;">점</span>`;
    finalScoreEl.style.color = rec.color || '#0f172a';
    renderCarInsuranceBanner(rec);
    renderPhase2Status(rec);
    renderRadarAndBep(rec);
    renderTimeMatrix(rec);
    renderKillerInsights(rec);
    renderBuildingAgingReport(rec);
    fetchBuildingAgingReportForRec(rec).then(() => { renderBuildingAgingReport(rec); });
    renderCharts(rec);
    renderRiskWarnings(rec);
    renderCfoPhase1Extended(rec);
    const saveBtn = document.getElementById('report-save-btn');
    if (saveBtn) saveBtn.style.display = 'none';
    document.getElementById('report-modal').style.display = 'flex';
};

function closeReportModal() {
    document.getElementById('report-modal').style.display = 'none';
}

/** B2B: 6각 레이더 + BEP 박스 (백엔드 radar_balance, bep_simulation) */
function renderRadarAndBep(nodeData) {
    const bepBox = document.getElementById('bep-simulation-box');
    const radarEl = document.getElementById('radarBalanceChart');
    if (!radarEl || !bepBox) return;

    if (radarChart) {
        radarChart.destroy();
        radarChart = null;
    }

    const radar = nodeData.radar_balance;
    if (radar && radar.labels && radar.values && radar.labels.length === radar.values.length) {
        radarChart = new Chart(radarEl.getContext('2d'), {
            type: 'radar',
            data: {
                labels: radar.labels,
                datasets: [{
                    label: '상권 밸런스',
                    data: radar.values,
                    fill: true,
                    backgroundColor: 'rgba(99, 102, 241, 0.22)',
                    borderColor: 'rgba(79, 70, 229, 0.95)',
                    pointBackgroundColor: 'rgba(79, 70, 229, 1)',
                    pointBorderColor: '#fff',
                    pointHoverBackgroundColor: '#fff',
                    pointHoverBorderColor: 'rgba(79, 70, 229, 1)',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    r: {
                        min: 0,
                        max: 10,
                        ticks: { stepSize: 2, font: { size: 10, family: 'Pretendard' } },
                        grid: { color: 'rgba(148, 163, 184, 0.35)' },
                        angleLines: { color: 'rgba(148, 163, 184, 0.35)' },
                        pointLabels: { font: { size: 11, weight: '700', family: 'Pretendard' }, color: '#475569' }
                    }
                },
                plugins: {
                    legend: { display: false }
                }
            }
        });
    }

    const bep = nodeData.bep_simulation;
    if (bep && bep.cfo_comment) {
        const fmt = (n) => (n != null ? Number(n).toLocaleString('ko-KR') : '-');
        bepBox.innerHTML = `
            <div style="margin-bottom:10px;">
                <span style="display:inline-block; padding:2px 8px; border-radius:6px; font-size:11px; font-weight:800; background:${bep.revenue_model === '비급여중심' ? '#ede9fe' : '#ecfdf5'}; color:${bep.revenue_model === '비급여중심' ? '#5b21b6' : '#047857'};">
                    ${bep.revenue_model || '모델'}
                </span>
            </div>
            <p style="font-weight:700; color:#0f172a; margin-bottom:12px;">${bep.cfo_comment}</p>
            <table style="width:100%; font-size:12px; border-collapse:collapse;">
                <tr><td style="color:#64748b; padding:4px 0;">월 임대료 추정</td><td style="text-align:right; font-weight:800;">${fmt(bep.monthly_rent_krw)}원</td></tr>
                <tr><td style="color:#64748b; padding:4px 0;">월 고정비 합계</td><td style="text-align:right; font-weight:800;">${fmt(bep.monthly_fixed_total_krw)}원</td></tr>
                <tr><td style="color:#64748b; padding:4px 0;">추정 객단가(회당)</td><td style="text-align:right; font-weight:800;">${fmt(bep.estimated_ticket_krw)}원</td></tr>
                <tr style="border-top:1px dashed #cbd5e1;"><td style="color:#0f172a; padding:8px 0 4px; font-weight:800;">BEP 월간 환자</td><td style="text-align:right; font-weight:900; color:#4f46e5; padding:8px 0 4px;">${fmt(bep.breakeven_monthly_patients)}명</td></tr>
                <tr><td style="color:#0f172a; padding:4px 0; font-weight:800;">BEP 일평균(영업일)</td><td style="text-align:right; font-weight:900; color:#4f46e5;">${bep.breakeven_daily_patients != null ? bep.breakeven_daily_patients : '-'}명</td></tr>
            </table>
            <p style="font-size:10px; color:#94a3b8; margin-top:12px;">※ 임대·인건비·객단가는 V7 추정치이며, V8에서 KOSIS·실거래·인건비 테이블로 정밀화됩니다.</p>
        `;
    } else {
        bepBox.innerHTML = '<p style="color:#94a3b8;">BEP 데이터가 없습니다. 서버를 최신 버전으로 실행해 주세요.</p>';
    }
}

/** Phase 3: Time-Matrix 요일별 유동인구 Bar + 진료시간 컨설팅 */
function renderTimeMatrix(nodeData) {
    const tm = nodeData.time_matrix;
    const hoursBox = document.getElementById('hours-consulting-box');
    const chartEl = document.getElementById('timeMatrixChart');
    if (!chartEl || !hoursBox) return;

    if (timeMatrixChart) {
        timeMatrixChart.destroy();
        timeMatrixChart = null;
    }

    if (tm && tm.hours_consulting) {
        hoursBox.innerHTML = `<p style="margin:0;">${tm.hours_consulting}</p>`;
    } else {
        hoursBox.innerHTML = '<p style="color:#94a3b8; margin:0;">진료시간 컨설팅 데이터가 없습니다.</p>';
    }

    const peakEl = document.getElementById('time-matrix-killer-peak');
    if (peakEl) {
        if (tm && tm.killer_narrative) {
            const slot = tm.peak_time_suggestion ? `<div style="margin-top:6px;font-size:11px;color:#7c2d12;">추천 슬롯: ${tm.peak_time_suggestion}</div>` : '';
            peakEl.style.display = 'block';
            peakEl.innerHTML = `<strong>타임 매트릭스 피크</strong>${slot}<p style="margin:8px 0 0; font-weight:600;">${tm.killer_narrative}</p>`;
        } else {
            peakEl.style.display = 'none';
            peakEl.innerHTML = '';
        }
    }

    if (tm && tm.labels && tm.values && tm.labels.length === tm.values.length) {
        timeMatrixChart = new Chart(chartEl.getContext('2d'), {
            type: 'bar',
            data: {
                labels: tm.labels,
                datasets: [{
                    label: '유동인구 지수',
                    data: tm.values,
                    backgroundColor: tm.zone_type === 'office'
                        ? ['rgba(59, 130, 246, 0.7)', 'rgba(59, 130, 246, 0.7)', 'rgba(59, 130, 246, 0.9)', 'rgba(59, 130, 246, 0.9)', 'rgba(59, 130, 246, 0.9)', 'rgba(148, 163, 184, 0.5)', 'rgba(148, 163, 184, 0.5)']
                        : ['rgba(148, 163, 184, 0.5)', 'rgba(148, 163, 184, 0.5)', 'rgba(148, 163, 184, 0.5)', 'rgba(148, 163, 184, 0.5)', 'rgba(148, 163, 184, 0.6)', 'rgba(234, 88, 12, 0.8)', 'rgba(234, 88, 12, 0.7)'],
                    borderRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { min: 0, max: 100, ticks: { stepSize: 20 }, border: { display: false } },
                    x: { grid: { display: false }, ticks: { font: { size: 11, family: 'Pretendard' } } }
                }
            }
        });
    }
}

/** 킬러 인사이트: 경쟁 노후도, 리뷰, 주차·약국 */
function renderKillerInsights(nodeData) {
    const box = document.getElementById('killer-insights-box');
    if (!box) return;
    const ki = nodeData.killer_insights;
    if (!ki) {
        box.innerHTML = '<p style="margin:0;color:#94a3b8;font-size:13px;">킬러 인사이트 데이터가 없습니다. API를 최신화해 주세요.</p>';
        return;
    }
    const card = (title, body, border) => `
        <div style="border:1px solid ${border}; border-radius:12px; padding:14px; background:#fff; font-size:13px; line-height:1.65; color:#334155;">
            <div style="font-weight:800; margin-bottom:8px; color:#0f172a;">${title}</div>
            <div style="font-weight:600;">${body}</div>
        </div>`;
    const parking = ki.parking_infra || {};
    box.innerHTML = [
        card('🏛 경쟁 병원 노후도 (개원 연차 추정)', ki.competitor_age_narrative || '-', '#cbd5e1'),
        card('⭐ 경쟁사 리뷰·서비스 기회 (추정)', ki.review_opportunity_narrative || '-', '#fde68a'),
        card('🅿 주차·핵심 인프라', `${parking.parking_summary || ''}<br/><br/>${parking.pharmacy_infra_summary || ''}`, '#a7f3d0'),
    ].join('');
}

/** Phase 3: Risk Warnings (상권 진입 Check Point) */
function renderRiskWarnings(nodeData) {
    const box = document.getElementById('risk-warnings-box');
    if (!box) return;
    const warnings = nodeData.risk_warnings;
    if (warnings && Array.isArray(warnings) && warnings.length > 0) {
        box.innerHTML = '<ul style="margin:0; padding-left:20px;">' + warnings.map(w => `<li style="margin-bottom:8px;">${w}</li>`).join('') + '</ul>';
    } else {
        box.innerHTML = '<p style="margin:0; color:#94a3b8;">해당 상권에 대한 특별 경고 사항이 없습니다. 현장 실사는 여전히 권장됩니다.</p>';
    }
}

// 📊 Chart.js 렌더링 전용 함수
function renderCharts(nodeData) {
    if(demoChart) demoChart.destroy();
    if(revChart) revChart.destroy();

    const popMatch = nodeData.pop_text.match(/3040비중 ([\d.]+)%/);
    const youngRatio = popMatch ? parseFloat(popMatch[1]) : 30.0;
    const middleRatio = 45.0; 
    let oldRatio = 100 - youngRatio - middleRatio; 
    if(oldRatio < 0) oldRatio = 5;

    const ctxDemo = document.getElementById('demographicChart').getContext('2d');
    demoChart = new Chart(ctxDemo, {
        type: 'doughnut',
        data: {
            labels: ['2030 청년층', '4050 중장년층', '60대 이상 고령층'],
            datasets: [{
                data: [youngRatio, middleRatio, oldRatio],
                backgroundColor: ['#3b82f6', '#10b981', '#f59e0b'],
                borderWidth: 0,
                hoverOffset: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'bottom', labels: { font: { size: 11, family: 'Pretendard' } } }
            },
            cutout: '65%' 
        }
    });

    const hospList = Array.isArray(currentHospitals) ? currentHospitals : [];
    let compHosps = hospList.slice(0, 4);
    let labels = [];
    let revData = [];

    if(compHosps.length === 0) {
        labels = ['경쟁 병원 없음'];
        revData = [0];
    } else {
        compHosps.forEach(h => {
            labels.push((h.name || '').replace(/\s*\(AI추정\)/, '').slice(0, 8));
            let man = h.estimated_revenue_man;
            if (man == null && h.estimated_revenue) {
                const eokMatch = h.estimated_revenue.match(/(\d+)억/);
                const manMatch = h.estimated_revenue.match(/([\d,]+)만/);
                if (eokMatch) man = parseInt(eokMatch[1]) * 10000;
                else if (manMatch) man = parseInt(manMatch[1].replace(/,/g, ''));
            }
            revData.push(man != null ? man : 5000);
        });
    }

    const ctxRev = document.getElementById('revenueChart').getContext('2d');
    revChart = new Chart(ctxRev, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: '월 추정 최고매출 (만원)',
                data: revData,
                backgroundColor: '#8b5cf6',
                borderRadius: 4,
                barPercentage: 0.6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false } 
            },
            scales: {
                x: { ticks: { font: { size: 10, family: 'Pretendard' } }, grid: { display: false } },
                y: { beginAtZero: true, border: { display: false } }
            }
        }
    });
}

function setupBottomSheet() {
    const sheet = document.getElementById('analysis-panel');
    const handle = sheet.querySelector('.sheet-handle');
    let startY, currentY;
    let isDragging = false;

    handle.addEventListener('touchstart', (e) => {
        isDragging = true;
        startY = e.touches[0].clientY;
        sheet.style.transition = 'none';
    }, {passive: true});

    document.addEventListener('touchmove', (e) => {
        if (!isDragging) return;
        currentY = e.touches[0].clientY;
        let diff = currentY - startY;
        if (diff > 0) sheet.style.transform = `translateY(${diff}px)`;
    }, {passive: true});

    document.addEventListener('touchend', (e) => {
        if (!isDragging) return;
        isDragging = false;
        sheet.style.transition = 'transform 0.3s ease';
        let diff = currentY - startY;
        
        if (diff > 50) {
            sheet.classList.remove('expanded');
            sheet.classList.add('peek-mode');
        } else {
            sheet.classList.add('expanded');
            sheet.classList.remove('peek-mode');
        }
        sheet.style.transform = '';
    });

    sheet.addEventListener('mouseenter', () => {
        sheet.classList.add('expanded');
        sheet.classList.remove('peek-mode');
    });

    sheet.addEventListener('mouseleave', () => {
        if(!document.getElementById('payment-modal').style.display || document.getElementById('payment-modal').style.display === 'none') {
            sheet.classList.remove('expanded');
            sheet.classList.add('peek-mode');
        }
    });
}