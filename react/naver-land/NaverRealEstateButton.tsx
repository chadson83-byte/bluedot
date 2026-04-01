import * as React from "react";
import {
  buildNaverLandArticlesUrl,
  openNaverLandArticles,
  type BuildNaverLandArticlesUrlOptions,
} from "./naverLandUrl";

export type NaverRealEstateButtonProps = {
  lat: number | null | undefined;
  lng: number | null | undefined;
  /** 건물명·주소 등 (접근성 title/aria) */
  label?: string;
  zoom?: number;
  className?: string;
  disabled?: boolean;
  /** 좌표 없을 때 대체 문구 */
  fallbackText?: string;
  /** URL 옵션 (매물·거래 유형 등) */
  urlOptions?: Omit<BuildNaverLandArticlesUrlOptions, "zoom">;
} & Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, "onClick" | "type">;

const defaultClassName =
  "inline-flex items-center justify-center gap-2 rounded-lg border border-emerald-600/20 " +
  "bg-emerald-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm " +
  "transition hover:bg-emerald-700 focus:outline-none focus-visible:ring-2 " +
  "focus-visible:ring-emerald-500 focus-visible:ring-offset-2 " +
  "disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-emerald-600";

/**
 * 2단계 상가 분석 등에서 선택 좌표 기준 네이버 부동산 매물(상가·상가주택·사무실)을 새 탭으로 연다.
 */
export function NaverRealEstateButton({
  lat,
  lng,
  label,
  zoom = 16,
  className,
  disabled = false,
  fallbackText = "위치 정보 없음",
  urlOptions,
  children,
  title,
  ...rest
}: NaverRealEstateButtonProps) {
  const latNum = typeof lat === "number" ? lat : lat != null ? Number(lat) : NaN;
  const lngNum = typeof lng === "number" ? lng : lng != null ? Number(lng) : NaN;
  const url = buildNaverLandArticlesUrl(latNum, lngNum, { ...urlOptions, zoom });
  const coordsOk = url != null;
  const isDisabled = disabled || !coordsOk;

  const handleClick = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.preventDefault();
    if (!coordsOk) return;
    openNaverLandArticles(latNum, lngNum, { ...urlOptions, zoom });
  };

  const ariaLabel = label
    ? `네이버 부동산에서 매물 보기 (${label})`
    : "네이버 부동산에서 매물 보기";

  return (
    <button
      type="button"
      className={className ?? defaultClassName}
      disabled={isDisabled}
      onClick={handleClick}
      title={title ?? (label ? `${ariaLabel} — ${label}` : ariaLabel)}
      aria-label={ariaLabel}
      {...rest}
    >
      {coordsOk ? (
        children ?? (
          <>
            <NaverMapPinIcon className="h-4 w-4 shrink-0 opacity-95" aria-hidden />
            <span>네이버 부동산에서 매물 보기</span>
          </>
        )
      ) : (
        <span className="font-medium">{fallbackText}</span>
      )}
    </button>
  );
}

function NaverMapPinIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor" aria-hidden>
      <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5S10.62 6.5 12 6.5s2.5 1.12 2.5 2.5S13.38 11.5 12 11.5z" />
    </svg>
  );
}
