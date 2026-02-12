defmodule PgSQL.Conn do
  use GenServer

  @enforce_keys [:hostname, :username, :password, :database]
  defstruct  [
    :hostname,
    :username,
    :password,
    :database,
    name: nil,
    port: 5432,
    parameters: [],
    timeout: 15000,
    connect_timeout: 15000,
    public_access: :enabled,
    supervisor: nil,
    pool_size: 5,
    pool_overflow: 2
  ]

  ##############################################################################################
  ## Public API
  ##############################################################################################
  def start_link({conn, pgdata}) do
    GenServer.start_link(__MODULE__, {conn, pgdata}, name: (pgdata.name || __MODULE__))
  end

  def get(name \\ __MODULE__) do
    {conn, _pgdata} = GenServer.call(name, :get)
    conn
  end

  def get_all(name \\ __MODULE__) do
    GenServer.call(name, :get)
  end

  def update(name \\ __MODULE__, status)
  def update(name, {conn, pgdata}) do 
    GenServer.cast(name, {:update, {conn, pgdata}})
  end
  
  def close(name \\ __MODULE__) do
    {conn, _pgdata} = get_all(name)
    GenServer.stop(conn)
  end

  ##############################################################################################
  ## Callbacks
  ##############################################################################################

  @impl true
  def init({conn, pgdata}) do
    {:ok, {conn, pgdata}}
  end

  @impl true
  def handle_call(:get, _from, status) do
    {:reply, status, status}
  end

  @impl true
  def handle_cast({:update, {conn, pgdata}}, _status) do
    {:noreply, {conn, pgdata}}
  end
  
  @impl true
  def handle_info(message, status) do
    alert("Unexpected message #{inspect message} (#{inspect status})#{IO.ANSI.reset()}")
    {:noreply, status}
  end

  defp alert(message) do
    IO.puts(
      "#{IO.ANSI.red_background()}[PgSQL.Conn] #{message}#{IO.ANSI.reset()}"
    )
  end

end
