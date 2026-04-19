import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

const THEME_COLOR_SELECTOR = 'meta[name="theme-color"]:not([media])'

function syncThemeColor() {
  const root = getComputedStyle(document.documentElement)
  const token = '--theme-chrome-dark'
  const fallback = '#322f34'
  const color = root.getPropertyValue(token).trim() || fallback
  const themeMeta = document.querySelector<HTMLMetaElement>(THEME_COLOR_SELECTOR)
  if (themeMeta) {
    themeMeta.setAttribute('content', color)
  }
}

syncThemeColor()

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
