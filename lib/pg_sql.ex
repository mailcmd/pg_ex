defmodule PgSQL do
  @moduledoc """
  Exposed functions:
    - connect
    - close
    - query
    - raw_query


  """

  @type pg_conn() :: pid()

  defmodule Conn do
    use Agent

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
      connections: 1
    ]

    def start_link({pgconnects, pgdata}) do
      conns = CList.new(pgconnects)
      Agent.start_link(fn -> {conns, pgdata} end, name: (pgdata.name || __MODULE__))
    end

    def get(name \\ __MODULE__) do
      {conns, pgdata} = Agent.get(name, fn cinfo -> cinfo end)
      {conn, conns} = CList.next(conns)
      update(name, {conns, pgdata})
      conn
    end
    def update(name \\ __MODULE__, {conns, data}) do
      Agent.update(name, fn _ -> {conns, data} end)
    end
    def close(name \\ __MODULE__) do
      {conns, _} = Agent.get(name, fn cinfo -> cinfo end)
      Enum.map(conns, &GenServer.stop/1)
    end
  end

  ###########################################################################3
  ## To start supervised

  def child_spec(name \\ __MODULE__, connect_data) do
    %{
      id: name,
      start: {__MODULE__, :start_link, [connect_data]}
    }
  end

  def start_link(connect_data) do
    case connect(connect_data) do
      :error ->
        raise("Cannot connect to DB (#{inspect connect_data})")
      pid ->        
        {:ok, pid}
    end
  end

  def start_link(hostname, database, username, password) do
    start_link(
      %Conn{hostname: hostname, username: username, password: password, database: database}
    )
  end
  

  ###########################################################################3
  ## Module API

  # connect/4
  @spec connect(
          hostname :: String.t, database :: String.t, username :: String.t, password :: String.t)
                :: pg_conn() | :error
  def connect(hostname, database, username, password), do:
    connect(%Conn{hostname: hostname, username: username, password: password, database: database})

  # connect/1
  @spec connect(conn :: %Conn{}) :: list(pg_conn()) | :error
  def connect(conn) do
    # Open N connections
    pids = 
      Enum.map(1..conn.connections, fn _ ->
        # This name is just for Postgrex module
        name =
          "pg_"
          |> Kernel.<>("#{conn.name}")
          |> Kernel.<>("#{Enum.random(0..9999)}")
          |> String.to_atom()
    
        kw_conn = Map.to_list(%{conn|name: name})    
        { :ok, pid } =  Postgrex.start_link(kw_conn)

        with true <- Process.alive?(pid),
             { :ok, _ } <- Postgrex.query(pid, "SELECT 1", []) do
          pid
        else
          _ ->
            GenServer.stop(pid)
            :error
        end      
      end)
      |> Enum.filter(&(&1 != :error))

    cond do
      length(pids) == 0 ->
        :error
      
      conn.public_access == :enabled -> 
        case PgSQL.Conn.start_link({pids, conn}) do
          {:error, {:already_started, _}} -> PgSQL.Conn.update({pids, conn})
          _ -> :ok
        end
        pids

      true ->
        pids
    end
  end

  # connect!/4
  @spec connect!(
          hostname :: String.t, database :: String.t, username :: String.t, password :: String.t)
                :: list(pg_conn())
  def connect!(hostname, database, username, password),
      do: connect!(
            %Conn{hostname: hostname, username: username, password: password, database: database}
          )
  
  # connect!/1
  @spec connect!(conn :: %Conn{}) :: list(pg_conn()) 
  def connect!(conn) do
    conn = connect(conn)
    if conn == :error do
      raise("[POSTGRES]: Error connecting to Postgres DB")
    end
    conn
  end

  @spec close() :: list(:ok)
  def close() do
    PgSQL.Conn.close()
  end
  @spec close(conn :: atom()) :: list(:ok)
  def close(conn_name) do
    PgSQL.Conn.close(conn_name)
  end

  @spec query(conn :: pg_conn(), sql :: String.t, opts :: Keyword.t()) :: list()
  def query(conn, sql, opts \\ []) do
    with result <- raw_query(conn, sql, opts),
         true <- is_struct(result),
         :select <- result.command do
      query_result_to_list(result, opts)
    else
      _ -> []
    end
  end

  @spec raw_query(conn :: pg_conn(), sql :: String.t, opts :: Keyword.t()) :: atom() | list() | {:error, Strint.t}
  def raw_query(conn, sql, opts) do
    if Process.alive?(conn) do
      case Postgrex.query(conn, sql, [], opts) do
        { :ok, result } ->
          cond do
            result.command == :select -> result
            result.num_rows > 0 -> (to_string(result.command) <> "_ok") |> String.to_atom()
            true -> (to_string(result.command) <> "_nop") |> String.to_atom()
          end
        { :error, %Postgrex.Error{postgres: %{message: message}} } ->
          # Log.log(:error, "[POSTGRES]: " <> String.Chars.to_string(:binary.bin_to_list(message)))
          {:error, message}
        { :error, %{message: message} } ->
          # Log.log(:error, "[POSTGRES]: " <> String.Chars.to_string(:binary.bin_to_list(message)))
          {:error, message}
      end
    else
      # Log.log(:error, "[POSTGRES]: DB Connection down!")
      {:error, "DB Connection down!"}
    end
  end

  ###########################################################################3
  ## Private Tools

  defp query_result_to_list(result, opts) do
    paste_columns_name_to_rows(result.rows, result.columns, opts)
  end

  defp paste_columns_name_to_rows(rows, columns, opts) do
    Enum.map(rows, fn row ->
      lists_combine(columns, row, opts)
    end)
  end

  defp lists_combine([], _, _), do: %{}
  defp lists_combine([key | t1], [value | t2], opts)  do
    [
      {
        String.to_atom(key),
        binary_to_utf8(value, Keyword.get(opts, :to_utf8, false))
        # value
      }
    ] |> Enum.into(%{})
      |> Map.merge(lists_combine(t1, t2, opts))
  end

  defp binary_to_utf8(bin, _) when not is_binary(bin), do: bin
  defp binary_to_utf8(bin, false), do: bin
  defp binary_to_utf8(bin, true) do
    bin |> :binary.bin_to_list() |> List.to_string()
  end

  # defp binary_to_utf8(bin) when not is_binary(bin), do: bin
  # # defp binary_to_utf8(bin), do: bin
  # defp binary_to_utf8(bin) do
  #   bin |> :binary.bin_to_list() |> List.to_string()
  # end

end
