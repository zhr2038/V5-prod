import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

const THEME_COLOR_SELECTOR = 'meta[name="theme-color"]:not([media])'

function syncThemeColor() {
  const root = getComputedStyle(document.documentElement)
  const prefersLight = window.matchMedia('(prefers-color-scheme: light)').matches
  const token = prefersLight ? '--theme-chrome-light' : '--theme-chrome-dark'
  const fallback = prefersLight ? '#f3efed' : '#322f34'
  const color = root.getPropertyValue(token).trim() || fallback
  const themeMeta = document.querySelector<HTMLMetaElement>(THEME_COLOR_SELECTOR)
  if (themeMeta) {
    themeMeta.setAttribute('content', color)
  }
}

syncThemeColor()

const media = window.matchMedia('(prefers-color-scheme: light)')
media.addEventListener?.('change', syncThemeColor)

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
