import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

const THEME_COLOR_SELECTOR = 'meta[name="theme-color"]:not([media])'
const LIGHT_SCHEME_QUERY = '(prefers-color-scheme: light)'
const MOBILE_FORCE_DARK_QUERY = '(hover: none) and (pointer: coarse)'

const lightSchemeMedia = window.matchMedia(LIGHT_SCHEME_QUERY)
const mobileForceDarkMedia = window.matchMedia(MOBILE_FORCE_DARK_QUERY)

declare global {
  interface Window {
    __checkMobileScroll?: () => void
  }
}

function shouldUseLightChromeTheme() {
  return lightSchemeMedia.matches && !mobileForceDarkMedia.matches
}

function syncThemeColor() {
  const root = getComputedStyle(document.documentElement)
  const token = shouldUseLightChromeTheme() ? '--theme-chrome-light' : '--theme-chrome-dark'
  const fallback = shouldUseLightChromeTheme() ? '#f3efed' : '#141018'
  const color = root.getPropertyValue(token).trim() || fallback
  const themeMeta = document.querySelector<HTMLMetaElement>(THEME_COLOR_SELECTOR)
  if (themeMeta) {
    themeMeta.setAttribute('content', color)
  }
}

syncThemeColor()

lightSchemeMedia.addEventListener?.('change', syncThemeColor)
mobileForceDarkMedia.addEventListener?.('change', syncThemeColor)

if (import.meta.env.DEV) {
  window.__checkMobileScroll = () => {
    const html = document.documentElement
    const body = document.body
    const root = document.getElementById('root')

    console.table({
      innerHeight: window.innerHeight,
      visualViewportHeight: window.visualViewport?.height,
      scrollY: window.scrollY,
      bodyScrollHeight: body.scrollHeight,
      htmlScrollHeight: html.scrollHeight,
      bodyOverflowY: getComputedStyle(body).overflowY,
      htmlOverflowY: getComputedStyle(html).overflowY,
      rootHeight: root ? getComputedStyle(root).height : '',
      rootOverflowY: root ? getComputedStyle(root).overflowY : '',
      scrollingElement: document.scrollingElement?.tagName,
    })
  }
}

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
