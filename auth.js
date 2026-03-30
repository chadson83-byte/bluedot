// BLUEDOT - 인증 및 API 헬퍼 (index.html에서 window.BLUEDOT_API_BASE 설정 가능)
const API_BASE = (typeof window !== "undefined" && window.BLUEDOT_API_BASE) ? window.BLUEDOT_API_BASE : "http://127.0.0.1:8000";
const TOKEN_KEY = "bluedot_auth_token";

function getToken() {
    return localStorage.getItem(TOKEN_KEY);
}

function setToken(token) {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
}

function authHeaders() {
    const t = getToken();
    return t ? { "Authorization": `Bearer ${t}` } : {};
}

async function apiGet(path) {
    const res = await fetch(API_BASE + path, { headers: authHeaders() });
    const data = await res.json();
    if (!res.ok && path.indexOf("/auth/me") < 0) throw new Error(data.detail || "요청 실패");
    return data;
}

async function apiPost(path, body) {
    const res = await fetch(API_BASE + path, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify(body)
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "요청 실패");
    return data;
}

async function fetchMe() {
    return apiGet("/api/auth/me");
}

async function fetchCredits() {
    const r = await apiGet("/api/credits");
    return r.credits || 0;
}

async function loginKakao() {
    if (typeof Kakao === "undefined") {
        alert("카카오 SDK를 불러올 수 없습니다. 개발자 도구에서 카카오 JavaScript 키를 확인하세요.");
        return;
    }
    Kakao.Auth.login({
        success: async (authObj) => {
            const res = await apiPost("/api/auth/kakao", { access_token: authObj.access_token });
            if (res.token) {
                setToken(res.token);
                if (typeof onAuthStateChange === "function") onAuthStateChange(res.user);
                closeLoginModal();
                if (typeof closeSidebar === "function") closeSidebar();
            } else alert(res.detail || "로그인 실패");
        },
        fail: (err) => alert("카카오 로그인 실패: " + (err.error_description || err.error))
    });
}

async function loginGoogle() {
    if (typeof google === "undefined" || !google.accounts) {
        alert("구글 SDK를 불러올 수 없습니다.");
        return;
    }
    const client = google.accounts.oauth2.initTokenClient({
        client_id: window.GOOGLE_CLIENT_ID || "",
        scope: "email profile openid",
        callback: async (res) => {
            if (!res.access_token) { alert("구글 로그인 취소"); return; }
            const bodyRes = await fetch(API_BASE + "/api/auth/google", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ access_token: res.access_token })
            });
            const data = await bodyRes.json();
            if (data.token) {
                setToken(data.token);
                if (typeof onAuthStateChange === "function") onAuthStateChange(data.user);
                closeLoginModal();
                if (typeof closeSidebar === "function") closeSidebar();
            } else alert(data.detail || "구글 로그인 실패");
        }
    });
    client.requestAccessToken();
}

async function loginTest() {
    const name = prompt("테스트 사용자 이름", "테스트사용자") || "테스트사용자";
    const res = await apiPost("/api/auth/test", { name });
    if (res.token) {
        setToken(res.token);
        if (typeof onAuthStateChange === "function") onAuthStateChange(res.user);
        closeLoginModal();
        if (typeof closeSidebar === "function") closeSidebar();
    }
}

function logout() {
    setToken(null);
    if (typeof onAuthStateChange === "function") onAuthStateChange(null);
    if (typeof closeSidebar === "function") closeSidebar();
    alert("로그아웃 되었습니다.");
}

async function addCreditsViaApi(plan, amount, creditsAdded, impUid = null) {
    const body = impUid ? { imp_uid: impUid } : { plan, amount, credits_added: creditsAdded };
    return apiPost("/api/credits/add", body);
}

async function useCreditViaApi() {
    return apiPost("/api/credits/use", {});
}

async function saveReportApi(reportData, regionName, deptName) {
    return apiPost("/api/reports/save", {
        report_data: reportData,
        region_name: regionName || "",
        dept_name: deptName || ""
    });
}

async function fetchReports() {
    const r = await apiGet("/api/reports");
    return r.reports || [];
}

async function fetchReport(id) {
    return apiGet("/api/reports/" + id);
}

async function fetchPayments() {
    const r = await apiGet("/api/payments");
    return r.payments || [];
}
