defmodule ArgoTest do
  use ExUnit.Case
  doctest Argo

  test "greets the world" do
    assert Argo.hello() == :world
  end
end
