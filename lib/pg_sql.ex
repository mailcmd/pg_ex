defmodule PgSQL do
  @moduledoc """
  Exposed functions:
    - start_link
    - connect
    - close
    - query
    - raw_query

  """

  alias PgSQL.Conn
  
  @type pg_conn() :: pid()

  #############################################################################################
  ## To start supervised
  def child_spec(name \\ __MODULE__, connect_data) do
    %{
      id: name,
      start: {__MODULE__, :start_link, connect_data}
    }
  end

  #############################################################################################
  ## Module API
  #############################################################################################

  #############################################################################################
  # if the pg connections are init with start_link the module create a supervisor to manage the
  # connections and restart them in case of unexpected close. This function also allow to open
  # connections with a parent supervisor because PgSQL define child_spec and start_link return
  # {:ok, <pid_of_supervisor>}
  def start_link(connect_data) do
    case connect(connect_data) do
      :error ->
        raise("Cannot connect to DB (#{inspect connect_data})")
      conn ->        
        {:ok, conn}
    end
  end

  def start_link(hostname, database, username, password) do
    start_link(
      %Conn{hostname: hostname, username: username, password: password, database: database}
    )
  end
  
  #############################################################################################
  # If you do not want to open connection(s) as part of a supervised tree, directly can use
  # this function and get as return {:ok, [<list_of_pg_conn>]}
  # connect/4
  @spec connect(
          hostname :: String.t, database :: String.t, username :: String.t, password :: String.t)
                :: pg_conn() | :error
  def connect(hostname, database, username, password), do:
    connect(%Conn{hostname: hostname, username: username, password: password, database: database})

  # connect/1
  @spec connect(pgdata :: %Conn{}) :: list(pg_conn()) | :error
  def connect(pgdata) do
    pgdata =
      if not is_struct(pgdata) do
        struct(PgSQL.Conn, pgdata)
      else
        pgdata
      end
    
    kw_conn =
      pgdata
      |> Map.to_list()
      |> Keyword.put(:socket_options, [{:raw, 65535, 8, <<1, 0, 0, 0, 0, 0, 0, 0>>}])

    {:ok, pid} =
      if pgdata.supervisor do
        Supervisor.start_child(pgdata.supervisor, {Postgrex, [kw_conn]})
      else
        Postgrex.start_link(kw_conn)
      end
    
    conn = 
      with true <- Process.alive?(pid),
           {:ok, _} <- Postgrex.query(pid, "SELECT 1", []) do
        pid
      else
        _ ->
          try do
            GenServer.stop(pid)
          rescue
            _ -> :ok
          end
          :error
      end      

    cond do
      pgdata.public_access == :enabled and pgdata.supervisor ->
        Supervisor.start_child(pgdata.supervisor, {PgSQL.Conn, [{conn, pgdata}]})
        
      pgdata.public_access == :enabled -> 
        case PgSQL.Conn.start_link({conn, pgdata}) do
          {:error, {:already_started, _}} ->
            PgSQL.Conn.update({conn, pgdata})
          _ ->
            :ok
        end
    end
    
    conn
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
  @spec connect!(pgdata :: %Conn{}) :: list(pg_conn()) 
  def connect!(pgdata) do
    conn = connect(pgdata)
    if conn == :error do
      raise("[POSTGRES]: Error connecting to Postgres DB")
    end
    conn
  end

  @spec close() :: :ok
  def close() do
    PgSQL.Conn.close()
  end
  @spec close(conn :: atom()) :: :ok
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

  @spec raw_query(
          conn :: pg_conn(),
          sql :: String.t,
          opts :: Keyword.t()
        ) :: atom() | list() | {:error, Strint.t}
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
