// src/App.tsx
import * as React from 'react'
import { useFish } from '@actyx-contrib/react-pond'
import { MachineFish } from './machineFish'
import { Button } from '@actyx/industrial-ui'

export const App = (): JSX.Element => {
  const [order, setOrder] = React.useState('4711')
  const [machine, selectMachine] = useFish(MachineFish, 'Drill1')

  const selector = (
    <select onChange={(e) => selectMachine(e.target.value)}>
      <option>Drill1</option>
      <option>Drill2</option>
      <option>Drill3</option>
    </select>
  )

  if (machine === undefined) {
    return <p>Please select a machine: {selector}</p>
  }

  const { working_on } = machine.state
  const status = working_on === null ? 'idle' : `working on order ${machine.state.working_on}`
  return (
    <div>
      <p>
        {selector} <input type="text" onChange={(e) => setOrder(e.target.value)} value={order} />
      </p>
      <h1>
        Machine {machine.name}: {status}
      </h1>
      <div style={{ display: 'flex', columnGap: '2rem' }}>
        <Button
          disabled={working_on !== null}
          variant="raised"
          color="primary"
          text="start"
          onClick={() => machine.feed({ type: 'start', order })}
        />
        {working_on !== null && (
          <Button
            variant="raised"
            color="red"
            text="stop"
            onClick={() => machine.feed({ type: 'stop', order: working_on })}
          />
        )}
      </div>
    </div>
  )
}
