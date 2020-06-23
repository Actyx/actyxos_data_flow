import {
  OnEvent,
  OnCommand,
  unreachableOrElse,
  FishType,
  Semantics,
  OnStateChange,
  Subscription,
} from '@actyx/pond'

export type State = {
  name: string
  working_on: string | null
}

export type Event = { type: 'started'; order: string } | { type: 'stopped'; order: string }

export type Command = { type: 'start'; order: string } | { type: 'stop'; order: string }

const onEvent: OnEvent<State, Event> = (state, event) => {
  const { payload } = event
  switch (payload.type) {
    case 'started':
      return { ...state, working_on: payload.order }
    case 'stopped':
      return { ...state, working_on: null }
    default:
      return unreachableOrElse(payload, state)
  }
}

const onCommand: OnCommand<State, Command, Event> = (_state, command) => {
  const { order } = command
  switch (command.type) {
    case 'start':
      return [{ type: 'started', order }]
    case 'stop':
      return [{ type: 'stopped', order }]
    default:
      return unreachableOrElse(command, [])
  }
}

const semantics = Semantics.of('machineFish')

export const MachineFish = FishType.of({
  semantics,
  initialState: (name) => ({
    state: { name, working_on: null },
    subscriptions: [Subscription.of(semantics, name)],
  }),
  onEvent,
  onCommand,
  onStateChange: OnStateChange.publishPrivateState(),
})
