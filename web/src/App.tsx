import { useEffect, useState, type FormEvent } from 'react'
import './App.css'

type Endpoint = {
  id: string
  name: string
  url: string
  createdAt: string
}

type Delivery = {
  id: string
  endpointId: string
  payload: string
  status: string
  attempts: number
  lastError: string | null
  createdAt: string
  updatedAt: string
}

type GraphQLResponse<T> = {
  data?: T
  errors?: { message: string }[]
}

async function graphqlRequest<T>(query: string, variables?: Record<string, unknown>) {
  const response = await fetch('/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  })
  if (!response.ok) {
    throw new Error(`GraphQL request failed (${response.status})`)
  }

  const result = (await response.json()) as GraphQLResponse<T>
  if (result.errors?.length) {
    throw new Error(result.errors.map((error) => error.message).join(', '))
  }
  if (!result.data) {
    throw new Error('GraphQL response did not contain data')
  }
  return result.data
}

const dashboardQuery = `
  query Dashboard($endpointId: ID) {
    endpoints { id name url createdAt }
    deliveries(endpointId: $endpointId) {
      id endpointId payload status attempts lastError createdAt updatedAt
    }
  }
`

function App() {
  const [endpoints, setEndpoints] = useState<Endpoint[]>([])
  const [deliveries, setDeliveries] = useState<Delivery[]>([])
  const [selectedEndpoint, setSelectedEndpoint] = useState('')
  const [endpointName, setEndpointName] = useState('')
  const [endpointUrl, setEndpointUrl] = useState('')
  const [payload, setPayload] = useState('{\n  "type": "invoice.paid"\n}')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [busy, setBusy] = useState(false)

  async function refresh() {
    try {
      const data = await graphqlRequest<{
        endpoints: Endpoint[]
        deliveries: Delivery[]
      }>(dashboardQuery, { endpointId: selectedEndpoint || null })
      setEndpoints(data.endpoints)
      setDeliveries(data.deliveries)
      if (!selectedEndpoint && data.endpoints.length > 0) {
        setSelectedEndpoint(data.endpoints[0].id)
      }
      setError('')
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Request failed')
    }
  }

  useEffect(() => {
    let active = true
    graphqlRequest<{ endpoints: Endpoint[]; deliveries: Delivery[] }>(
      dashboardQuery,
      { endpointId: selectedEndpoint || null },
    ).then((data) => {
      if (!active) return
      setEndpoints(data.endpoints)
      setDeliveries(data.deliveries)
      setError('')
    }).catch((requestError: unknown) => {
      if (!active) return
      setError(requestError instanceof Error ? requestError.message : 'Request failed')
    })
    return () => {
      active = false
    }
  }, [selectedEndpoint])

  async function createEndpoint(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setBusy(true)
    setNotice('')
    try {
      const data = await graphqlRequest<{ createEndpoint: Endpoint }>(
        `mutation CreateEndpoint($name: String!, $url: String!) {
          createEndpoint(name: $name, url: $url) { id name url createdAt }
        }`,
        { name: endpointName, url: endpointUrl },
      )
      setEndpointName('')
      setEndpointUrl('')
      setSelectedEndpoint(data.createEndpoint.id)
      setNotice('Endpoint created')
      await refresh()
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Request failed')
    } finally {
      setBusy(false)
    }
  }

  async function dispatchEvent(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!selectedEndpoint) {
      setError('Create an endpoint before sending an event')
      return
    }
    try {
      JSON.parse(payload)
    } catch {
      setError('Payload must be valid JSON')
      return
    }

    setBusy(true)
    setNotice('')
    try {
      await graphqlRequest<{ dispatchEvent: Delivery }>(
        `mutation DispatchEvent($endpointId: ID!, $payload: String!) {
          dispatchEvent(endpointId: $endpointId, payload: $payload) { id status }
        }`,
        { endpointId: selectedEndpoint, payload },
      )
      setNotice('Event queued for delivery')
      await refresh()
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Request failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand-mark" aria-hidden="true">W</div>
        <div>
          <p className="eyebrow">Webhook delivery</p>
          <h1>Reliable events, <span>clearly tracked.</span></h1>
        </div>
        <button className="quiet-button refresh-button" onClick={() => void refresh()} disabled={busy}>
          Refresh
        </button>
      </header>

      {error && <div className="notice error" role="alert">{error}</div>}
      {notice && <div className="notice success" role="status">{notice}</div>}

      <section className="workspace">
        <aside className="sidebar">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Workspace</p>
              <h2>Endpoints</h2>
            </div>
            <span className="count">{endpoints.length}</span>
          </div>

          <div className="endpoint-list">
            {endpoints.map((endpoint) => (
              <button
                className={`endpoint-item ${selectedEndpoint === endpoint.id ? 'selected' : ''}`}
                key={endpoint.id}
                onClick={() => setSelectedEndpoint(endpoint.id)}
              >
                <span className="endpoint-dot" />
                <span className="endpoint-copy">
                  <strong>{endpoint.name}</strong>
                  <small>{endpoint.url}</small>
                </span>
              </button>
            ))}
            {endpoints.length === 0 && <p className="empty-copy">Add an endpoint to get started.</p>}
          </div>

          <form className="form-card create-form" onSubmit={createEndpoint}>
            <p className="eyebrow">New destination</p>
            <label>
              Name
              <input value={endpointName} onChange={(event) => setEndpointName(event.target.value)} required maxLength={100} placeholder="Production API" />
            </label>
            <label>
              Destination URL
              <input value={endpointUrl} onChange={(event) => setEndpointUrl(event.target.value)} required type="url" placeholder="https://example.com/webhooks" />
            </label>
            <button className="secondary-button" disabled={busy}>Add endpoint</button>
          </form>
        </aside>

        <div className="main-column">
          <section className="form-card dispatch-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Send a test event</p>
                <h2>Dispatch webhook</h2>
              </div>
              <span className="step-label">01 / SEND</span>
            </div>
            <form onSubmit={dispatchEvent}>
              <label>
                Endpoint
                <select value={selectedEndpoint} onChange={(event) => setSelectedEndpoint(event.target.value)} disabled={endpoints.length === 0}>
                  <option value="">Select an endpoint</option>
                  {endpoints.map((endpoint) => (
                    <option key={endpoint.id} value={endpoint.id}>{endpoint.name} · {endpoint.url}</option>
                  ))}
                </select>
              </label>
              <label>
                JSON payload
                <textarea value={payload} onChange={(event) => setPayload(event.target.value)} rows={6} spellCheck={false} />
              </label>
              <div className="form-footer">
                <p>Failed deliveries retry automatically.</p>
                <button className="primary-button" disabled={busy || endpoints.length === 0}>
                  {busy ? 'Working…' : 'Send event'}
                  <span aria-hidden="true">↗</span>
                </button>
              </div>
            </form>
          </section>

          <section className="delivery-section">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Activity</p>
                <h2>Recent deliveries</h2>
              </div>
              <span className="count">{deliveries.length}</span>
            </div>
            <div className="delivery-list">
              {deliveries.map((delivery) => (
                <article className="delivery-row" key={delivery.id}>
                  <span className={`status-dot ${delivery.status}`} />
                  <div className="delivery-details">
                    <div className="delivery-title">
                      <strong>{delivery.status}</strong>
                      <time>{new Date(delivery.createdAt).toLocaleString()}</time>
                    </div>
                    <code>{delivery.payload}</code>
                    {delivery.lastError && <p className="delivery-error">{delivery.lastError}</p>}
                  </div>
                  <span className="attempt-count">{delivery.attempts} attempts</span>
                </article>
              ))}
              {deliveries.length === 0 && (
                <div className="empty-state">
                  <span className="empty-icon">↗</span>
                  <strong>No deliveries yet</strong>
                  <p>Send an event to see its delivery status here.</p>
                </div>
              )}
            </div>
          </section>
        </div>
      </section>
      <footer>Webhook relay <span>·</span> Local development preview</footer>
    </main>
  )
}

export default App
