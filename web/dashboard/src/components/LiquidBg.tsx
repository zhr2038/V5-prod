export function LiquidBg() {
  return (
    <>
      <svg className="liquid-filters" aria-hidden="true" focusable="false">
        <defs>
          <filter id="liquid-distortion" x="-20%" y="-20%" width="140%" height="140%">
            <feTurbulence
              type="fractalNoise"
              baseFrequency="0.012 0.026"
              numOctaves="2"
              seed="7"
              result="noise"
            >
              <animate
                attributeName="baseFrequency"
                dur="18s"
                values="0.012 0.026;0.016 0.02;0.012 0.026"
                repeatCount="indefinite"
              />
            </feTurbulence>
            <feGaussianBlur in="noise" stdDeviation="2.2" result="softNoise" />
            <feDisplacementMap
              in="SourceGraphic"
              in2="softNoise"
              scale="26"
              xChannelSelector="R"
              yChannelSelector="B"
            />
          </filter>
          <filter id="liquid-distortion-strong" x="-30%" y="-30%" width="160%" height="160%">
            <feTurbulence
              type="fractalNoise"
              baseFrequency="0.007 0.016"
              numOctaves="3"
              seed="11"
              result="noise"
            >
              <animate
                attributeName="baseFrequency"
                dur="22s"
                values="0.007 0.016;0.011 0.022;0.007 0.016"
                repeatCount="indefinite"
              />
            </feTurbulence>
            <feGaussianBlur in="noise" stdDeviation="1.8" result="softNoise" />
            <feDisplacementMap
              in="SourceGraphic"
              in2="softNoise"
              scale="44"
              xChannelSelector="R"
              yChannelSelector="G"
            />
          </filter>
        </defs>
      </svg>

      <div className="liquid-bg" aria-hidden="true">
        <div className="liquid-aura aura-1" />
        <div className="liquid-aura aura-2" />
        <div className="liquid-caustic caustic-1" />
        <div className="liquid-caustic caustic-2" />
        <div className="liquid-sheet sheet-1" />
        <div className="liquid-sheet sheet-2" />
        <div className="liquid-orb orb-1" />
        <div className="liquid-orb orb-2" />
        <div className="liquid-orb orb-3" />
        <div className="liquid-mesh" />
        <div className="liquid-noise" />
      </div>
    </>
  );
}
