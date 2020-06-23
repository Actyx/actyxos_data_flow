// src/root.tsx
import * as React from 'react'
import * as ReactDOM from 'react-dom'
import { Pond } from '@actyx-contrib/react-pond'
import { App } from './App'

ReactDOM.render(
  <React.StrictMode>
    <Pond>
      <App />
    </Pond>
  </React.StrictMode>,
  document.getElementById('root'),
)
