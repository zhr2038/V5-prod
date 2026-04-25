import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

const THEME_COLOR_SELECTOR = 'meta[name="theme-color"]:not([media])'
const LIGHT_SCHEME_QUERY = '(prefers-color-scheme: light)'
const MOBILE_FORCE_DARK_QUERY = '(hover: none) and (pointer: coarse)'
const APP_VIEWPORT_HEIGHT_VAR = '--app-viewport-height'

const lightSchemeMedia = window.matchMedia(LIGHT_SCHEME_QUERY)
const mobileForceDarkMedia = window.matchMedia(MOBILE_FORCE_DARK_QUERY)
let viewportHeightRaf: number | null = null

function syncAppViewportHeight() {
  const height = window.visualViewport?.height || window.innerHeight
  if (!Number.isFinite(height) || height <= 0) return
  document.documentElement.style.setProperty(APP_VIEWPORT_HEIGHT_VAR, `${Math.round(height)}px`)
}

function scheduleAppViewportHeightSync() {
  if (viewportHeightRaf !== null) {
    window.cancelAnimationFrame(viewportHeightRaf)
  }
  viewportHeightRaf = window.requestAnimationFrame(() => {
    viewportHeightRaf = null
    syncAppViewportHeight()
  })
}

function shouldUseLightChromeTheme() {
  return lightSchemeMedia.matches && !mobileForceDarkMedia.matches
}

function syncThemeColor() {
  const root = getComputedStyle(document.documentElement)
  const token = shouldUseLightChromeTheme() ? '--theme-chrome-light' : '--theme-chrome-dark'
  const fallback = shouldUseLightChromeTheme() ? '#f3efed' : '#322f34'
  const color = root.getPropertyValue(token).trim() || fallback
  const themeMeta = document.querySelector<HTMLMetaElement>(THEME_COLOR_SELECTOR)
  if (themeMeta) {
    themeMeta.setAttribute('content', color)
  }
}

syncAppViewportHeight()
syncThemeColor()

lightSchemeMedia.addEventListener?.('change', syncThemeColor)
mobileForceDarkMedia.addEventListener?.('change', syncThemeColor)
window.addEventListener('resize', scheduleAppViewportHeightSync, { passive: true })
window.addEventListener('orientationchange', scheduleAppViewportHeightSync)
window.visualViewport?.addEventListener('resize', scheduleAppViewportHeightSync, { passive: true })
window.visualViewport?.addEventListener('scroll', scheduleAppViewportHeightSync, { passive: true })

const observer = new MutationObserver(() => {
  syncThemeColor()
})

observer.observe(document.documentElement, {
  attributes: true,
  attributeFilter: ['class', 'style', 'data-theme'],
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)

requestAnimationFrame(() => {
  const bootShell = document.getElementById('boot-shell')
  if (!bootShell) return
  bootShell.classList.add('boot-shell-hidden')
  globalThis.setTimeout(() => bootShell.remove(), 240)
})
