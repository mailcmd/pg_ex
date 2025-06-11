defmodule PgSQLTest do
  use ExUnit.Case
  doctest PgSQL

  test "greets the world" do
    assert PgSQL.hello() == :world
  end
end
