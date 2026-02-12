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
    socket_dir: nil,
    public_access: :enabled,
    supervisor: nil,
    monitoring_refs: [],
    connections: 1
  ]

  ##############################################################################################
  ## Public API
  ##############################################################################################
  def start_link({pids_conns, pgdata}) do
    cl_conns = CList.new(pids_conns) 
    GenServer.start_link(__MODULE__, {cl_conns, pgdata}, name: (pgdata.name || __MODULE__))
  end

  def get(name \\ __MODULE__) do
    {cl_conns, pgdata} = GenServer.call(name, :get)
    {pid_conn, cl_conns} = CList.next(cl_conns)
    update(name, {cl_conns, pgdata})
    pid_conn
  end

  def update(name \\ __MODULE__, status)
  def update(name, {cl_conns, pgdata}) when is_map(cl_conns) do 
    GenServer.cast(name, {:update, {cl_conns, pgdata}})
  end
  def update(name, {conns, pgdata}) when is_list(conns) do  
    cl_conns = CList.new(conns) 
    GenServer.cast(name, {:update, {cl_conns, pgdata}})
  end
  
  def close(name \\ __MODULE__) do
    {cl_conns, pgdata} = get(name)
    if pgdata.supervisor do
      Supervisor.stop(pgdata.supervisor, :normal)
    else
      cl_conns |> CList.to_list() |> Enum.each(&GenServer.stop/1)
    end      
    GenServer.stop(name)
  end

  ##############################################################################################
  ## Callbacks
  ##############################################################################################

  @impl true
  def init({cl_conns, pgdata}) do
    # if the connections are supervised, get start monitoring or the connections to recevie
    # update in case of unexepected close connection
    pgdata = 
      if pgdata.supervisor do
        Map.put(pgdata, :monitoring_refs, Enum.map(cl_conns, &Process.monitor/1))
      else
        pgdata
      end
    {:ok, {cl_conns, pgdata}}
  end

  @impl true
  def handle_call(:get, _from, status) do
    {:reply, status, status}
  end

  # update always receive conns as CList
  @impl true
  def handle_cast({:update, {cl_conns, pgdata}}, _status) do
    pgdata =
      if pgdata.supervisor do
        Enum.each(pgdata.monitoring_refs, &Process.demonitor/1)
        Map.put(pgdata, :monitoring_refs, Enum.map(cl_conns, &Process.monitor/1))
      else
        pgdata
      end
    {:noreply, {cl_conns, pgdata}}
  end
  
  # The genserver wont receive this message if the connections are not supervised
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _}, {_cl_conns, pgdata} = status) do
    alert("PgSQL conn closed!!")
    Process.demonitor(ref)
    pids = 
      pgdata.supervisor
      |> Supervisor.which_children()
      |> Enum.map(fn {_, pid, _, _} -> pid end)

    cl_conns = CList.new(pids)
    handle_cast({:update, {cl_conns, pgdata}}, status)
    {:noreply, {cl_conns, pgdata}}
  end

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
