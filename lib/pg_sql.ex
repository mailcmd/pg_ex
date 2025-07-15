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
      public_access: :disabled,
      supervisor: nil
    ]

    def start_link({pgconnect, pgdata}) do
      Agent.start_link(fn -> {pgconnect, pgdata} end, name: (pgdata.name || __MODULE__))
    end

    def make_persistent(pgconnect, pgdata, sup \\ nil) do
      if sup do
        Supervisor.start_child(sup, child_spec({pgconnect, pgdata}))
      else
        start_link({pgconnect, pgdata})
      end
    end

    def get(name \\ __MODULE__) do
      {conn, data} = Agent.get(name, fn cinfo -> cinfo end)
      if Process.alive?(conn) do
        conn
      else
        conn = PgSQL.connect(data)
        Agent.update(name, fn {_, data} -> {conn, data} end)
        conn
      end
    end
  end

  ###########################################################################3
  ## Module API

  # connect/4
  @spec connect(hostname :: String.t, database :: String.t, username :: String.t, password :: String.t)
                :: pg_conn() | :error
  def connect(hostname, database, username, password), do:
    connect(%Conn{hostname: hostname, username: username, password: password, database: database})

  # connect/1
  @spec connect(conn :: %Conn{}) :: pg_conn() | :error
  def connect(conn) do
    name = String.to_atom("pg_" <> to_string(conn.name) <> (0..9999 |> Enum.random() |> to_string()))
    kw_conn = Map.to_list(%{conn|name: name})
    { :ok, pid } =
      if conn.supervisor do
        Supervisor.start_child(conn.supervisor, Postgrex.child_spec(kw_conn))
      else
        Postgrex.start_link(kw_conn)
      end
    with true <- Process.alive?(pid),
      { :ok, _ } <- Postgrex.query(pid, "SELECT 1", []) do
        if conn.public_access == :enabled, do: PgSQL.Conn.make_persistent(pid, conn, conn.supervisor)
        pid
    else
      _ ->
        close(pid)
        :error
    end
  end

  # connect!/4
  @spec connect(hostname :: String.t, database :: String.t, username :: String.t, password :: String.t)
                :: pg_conn()
  def connect!(hostname, database, username, password), do:
    connect!(%Conn{hostname: hostname, username: username, password: password, database: database})
  # connect!/1
  @spec connect(conn :: %Conn{}) :: pg_conn()
  def connect!(conn) do
    conn = connect(conn)
    if conn == :error do
      raise("[POSTGRES]: Error connecting to Postgres DB")
    end
    conn
  end

  @spec close(conn :: pg_conn(), reason :: atom()) :: :ok
  def close(conn, reason \\ :normal) do
    GenServer.stop(conn, reason)
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
