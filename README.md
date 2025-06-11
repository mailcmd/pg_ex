# PgSQL

**WARNING**: This is an elixir Postgrex wrapper just for me. I don't think it will be useful for 
anyone more.
 
## Exposed functions summary

`connect/1`
`connect/4`
`connect!/1`
`connect!/4`
`close/1`
`query/3`
`raw_query/3`

## In action 

### Connecting
```elixir
iex> conn = PgSQL.connect(hostname, database, username, password)
```
or
```elixir
iex> conn = PgSQL.connect(
  %PgSQL.Conn{
    hostname: hostname,
    username: username,
    password: password,
    database: database,
    port: 5432,
    timeout: 15000,
    connect_timeout: 15000,
    public_access: :disabled # when :enabled make possible access conn in any process using 
                             # PgSQL.Conn.get() 
  }
)
```

### Quering
```elixir
iex(1)> PgSQL.query(conn, "SELECT 1+1 as silly_sum")
[
  %{
    silly_sum: 2
  }
]
```

```elixir
iex(2)> PgSQL.raw_query(conn, "SELECT 1+1 as silly_sum")
%Postgrex.Result{
  command: :select,
  columns: ["silly_sum"],
  rows: [[2]],
  num_rows: 1,
  connection_id: 6668,
  messages: []
}
```

## Installation

Really? Are you going to install it despite my warning?

```elixir
def deps do
  [
    {:pg_ex, "~> 0.1.0"}
  ]
end
```

