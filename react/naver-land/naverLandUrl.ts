/**
 * 네이버 부동산(new.land.naver.com/articles) 매물 목록 URL 생성 유틸.
 * @see https://new.land.naver.com/articles
 */

const BASE_URL = "https://new.land.naver.com/articles";

/** 개원·상가 입지 분석용 기본 매물 유형 */
export const NAVER_LAND_RETAIL_PROPERTY_TYPES = "SG:SGJT:SM" as const;

/** 월세만 (개원 상가 임차 검색용). 매매·전세 포함 시 A1:B1:B2 */
export const NAVER_LAND_TRADE_TYPES = "B2" as const;

export const NAVER_LAND_RETAIL_FILTER = "RETAIL" as const;

export type BuildNaverLandArticlesUrlOptions = {
  /** 지도 확대 (개원 입지 분석 시 16~17 권장) */
  zoom?: number;
  /** 기본: SG, SGJT, SM */
  propertyTypes?: string;
  /** 기본: A1, B1, B2 */
  tradeTypes?: string;
  /** 기본: RETAIL (실매물 위주) */
  retailFilter?: string;
};

function isValidLatLng(lat: unknown, lng: unknown): lat is number {
  if (typeof lat !== "number" || typeof lng !== "number") return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
}

type QueryParts = { ms: string; a: string; b: string; e: string };

function resolveQueryParts(
  lat: number,
  lng: number,
  options: BuildNaverLandArticlesUrlOptions
): QueryParts | null {
  if (!isValidLatLng(lat, lng)) return null;
  const zoom = options.zoom ?? 16;
  if (!Number.isFinite(zoom) || zoom < 1 || zoom > 22) return null;
  return {
    ms: `${lat},${lng},${Math.round(zoom)}`,
    a: options.propertyTypes ?? NAVER_LAND_RETAIL_PROPERTY_TYPES,
    b: options.tradeTypes ?? NAVER_LAND_TRADE_TYPES,
    e: options.retailFilter ?? NAVER_LAND_RETAIL_FILTER,
  };
}

/**
 * 안전한 쿼리 문자열 생성 (URLSearchParams → 값별 인코딩).
 */
export function buildNaverLandArticlesUrl(
  lat: number,
  lng: number,
  options: BuildNaverLandArticlesUrlOptions = {}
): string | null {
  const parts = resolveQueryParts(lat, lng, options);
  if (!parts) return null;
  const params = new URLSearchParams();
  params.set("ms", parts.ms);
  params.set("a", parts.a);
  params.set("b", parts.b);
  params.set("e", parts.e);
  return `${BASE_URL}?${params.toString()}`;
}

/**
 * encodeURIComponent로 키=값을 명시 조립 (특수문자 대응 동일 목적).
 */
export function buildNaverLandArticlesUrlEncoded(
  lat: number,
  lng: number,
  options: BuildNaverLandArticlesUrlOptions = {}
): string | null {
  const parts = resolveQueryParts(lat, lng, options);
  if (!parts) return null;
  const q = [
    `ms=${encodeURIComponent(parts.ms)}`,
    `a=${encodeURIComponent(parts.a)}`,
    `b=${encodeURIComponent(parts.b)}`,
    `e=${encodeURIComponent(parts.e)}`,
  ].join("&");
  return `${BASE_URL}?${q}`;
}

export type OpenNaverLandArticlesOptions = BuildNaverLandArticlesUrlOptions & {
  /** window.open 두 번째 인자 */
  target?: string;
  /** URL 빌더 (기본: buildNaverLandArticlesUrl) */
  buildUrl?: typeof buildNaverLandArticlesUrl;
};

/**
 * 새 탭에서 네이버 부동산 매물 목록 열기. 좌표가 유효하지 않으면 false.
 * 보안: noopener/noreferrer는 열린 창에 assign 후 rel 대체.
 */
export function openNaverLandArticles(
  lat: number,
  lng: number,
  options: OpenNaverLandArticlesOptions = {}
): boolean {
  const { target = "_blank", buildUrl = buildNaverLandArticlesUrl, ...rest } = options;
  const url = buildUrl(lat, lng, rest);
  if (!url) return false;
  const w = typeof globalThis !== "undefined" && (globalThis as { window?: Window }).window;
  if (!w?.open) return false;
  const tab = w.open(url, target);
  if (tab) {
    try {
      tab.opener = null;
    } catch {
      /* ignore cross-origin */
    }
  }
  return true;
}
