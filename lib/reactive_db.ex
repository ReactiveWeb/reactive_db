defmodule ReactiveDb do
  use Application

  def start(_type, []) do
    import Supervisor.Spec, warn: false

    children = [
      # Define workers and child supervisors to be supervised
      # worker(ReactiveDb.Worker, [arg1, arg2, arg3])
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ReactiveDb.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
