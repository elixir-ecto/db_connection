defmodule DBConnection.BackoffTest do
  use ExUnit.Case, async: true

  alias DBConnection.Backoff

  @moduletag backoff_min: 1_000
  @moduletag backoff_max: 30_000

  @tag backoff_type: :exp
  test "exponential backoffs aways in [min, max]", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    assert Enum.all?(delays, fn(delay) ->
      delay >= context[:backoff_min] and delay <= context[:backoff_max]
    end)
  end

  @tag backoff_type: :exp
  test "exponential backoffs double until max", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    Enum.reduce(delays, fn(next, prev) ->
      assert div(next, 2) == prev or next == context[:backoff_max]
      next
    end)
  end

  @tag backoff_type: :exp
  test "exponential backoffs reset to min", context do
    backoff = new(context)
    {[delay | _], backoff} = backoff(backoff, 20)
    assert delay == context[:backoff_min]

    backoff = Backoff.reset(backoff)
    {[delay], _} = backoff(backoff, 1)
    assert delay == context[:backoff_min]
  end

  @tag backoff_type: :rand
  test "random backoffs aways in [min, max]", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    assert Enum.all?(delays, fn(delay) ->
      delay >= context[:backoff_min] and delay <= context[:backoff_max]
    end)
  end

  @tag backoff_type: :rand
  test "random backoffs are not all the same value", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    ## If the stars align this test could fail ;)
    refute Enum.all?(delays, &(hd(delays) == &1))
  end

  @tag backoff_type: :rand
  test "random backoffs repeat", context do
    backoff = new(context)
    assert backoff(backoff, 20) == backoff(backoff, 20)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs aways in [min, max]", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    assert Enum.all?(delays, fn(delay) ->
      delay >= context[:backoff_min] and delay <= context[:backoff_max]
    end)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs increase until a third of max", context do
    backoff = new(context)
    {delays, _} = backoff(backoff, 20)
    Enum.reduce(delays, fn(next, prev) ->
      assert next >= prev or (next >= div(context[:backoff_max], 3))
      next
    end)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs repeat", context do
    backoff = new(context)
    assert backoff(backoff, 20) == backoff(backoff, 20)
  end

  @tag backoff_type: :rand_exp
  test "random exponential backoffs reset in [min, min * 3]", context do
    backoff = new(context)
    {[delay | _], backoff} = backoff(backoff, 20)
    assert delay in context[:backoff_min]..(context[:backoff_min]*3)

    backoff = Backoff.reset(backoff)
    {[delay], _} = backoff(backoff, 1)
    assert delay in context[:backoff_min]..(context[:backoff_min]*3)
  end

  ## Helpers

  def new(context) do
    Backoff.new(Enum.into(context, []))
  end

  defp backoff(backoff, n) do
    Enum.map_reduce(1..n, backoff, fn(_, acc) -> Backoff.backoff(acc) end)
  end
end
